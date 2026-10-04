"""Run artifacts: payloads too large for a run's checkpoint or an interrupt (an ``ask``
table, a diff) go to blob storage, and the run carries an ``ArtifactRef`` to them
(``Interrupt.payload_ref``)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Header, Path, Query, Request, Response
from fastapi.responses import StreamingResponse
from trellis.contracts.artifacts import ArtifactRef
from trellis.contracts.ids import new_id, now

from agent_runs.api.deps import Session, Who
from agent_runs.api.openapi import conflict
from agent_runs.api.routers.runs import RunId, WorkerId
from agent_runs.blob import BlobCorrupt, BlobNotFound, BlobStore, read
from agent_runs.config.constants import MAX_ARTIFACT_BYTES
from agent_runs.domain.errors import NotFound, ServiceError, TooLarge, Unprocessable
from agent_runs.store.artifacts import (
    ARTIFACTS_PATH,
    SHA256,
    ArtifactStore,
    artifact_ref,
    sha256_of,
)
from agent_runs.store.runs import RunStore

router = APIRouter(tags=["artifacts"])

_CREATED = 201
_OK = 200
_NOT_MODIFIED = 304
#: Checksum-addressed bytes never change under their id: cache them, privately (they are a
#: tenant's), for as long as a cache will.
CACHE_CONTROL = "private, max-age=31536000, immutable"
DEFAULT_MIME = "application/octet-stream"
_TOO_LARGE = f"an artifact is at most {MAX_ARTIFACT_BYTES} bytes"

#: The SHA-256 the caller computed, ``sha256:<hex>``: the upload is refused (422) unless the
#: bytes that arrived match it.
Checksum = Annotated[
    str | None,
    Query(
        pattern=r"^sha256:[0-9a-f]{64}$",
        description="The SHA-256 the caller computed, `sha256:<hex>`: the upload is refused "
        "(422) unless the bytes that arrived match it.",
    ),
]
ArtifactId = Annotated[str, Path(description="The artifact's id (`art_…`).")]

_UPLOAD_BODY = {
    "requestBody": {
        "required": True,
        "description": "The artifact's bytes; Content-Type is its mime type "
        "(application/json for an ask table).",
        "content": {
            "*/*": {
                "schema": {"type": "string", "format": "binary"},
                "examples": {
                    "ask_table": {
                        "summary": "An ask table, as JSON",
                        "value": {"columns": ["sku", "qty"], "rows": [["A-1", 12]]},
                    }
                },
            }
        },
    }
}


def _blobs(request: Request) -> BlobStore:
    return request.app.state.blobs


async def _body(request: Request) -> AsyncIterator[bytes]:
    """The request body as it arrives, refused (413) past ``MAX_ARTIFACT_BYTES``: at once
    when ``Content-Length`` says so, else as soon as the bytes counted pass it. Nothing is
    held but the chunk in hand; empty chunks are dropped."""
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > MAX_ARTIFACT_BYTES:
        raise TooLarge(_TOO_LARGE)
    seen = 0
    async for chunk in request.stream():
        if not chunk:
            continue
        seen += len(chunk)
        if seen > MAX_ARTIFACT_BYTES:
            raise TooLarge(_TOO_LARGE)
        yield chunk


async def _nonempty(request: Request) -> AsyncIterator[bytes]:
    """The body (``_body``), refused (422) before anything is stored when it is empty."""
    chunks = _body(request)
    first = await anext(chunks, None)
    if first is None:
        raise Unprocessable("an artifact has at least one byte")

    async def body() -> AsyncIterator[bytes]:
        yield first
        async for chunk in chunks:
            yield chunk

    return body()


@router.post(
    "/v1/runs/{run_id}/artifacts",
    status_code=_CREATED,
    summary="Store an artifact of a run",
    response_description="Created: the artifact's reference (for `Interrupt.payload_ref`); "
    "`Location` names its bytes.",
    responses={
        _OK: {
            "model": ArtifactRef,
            "description": "A repeat: the same bytes were already stored for this run; the "
            "first artifact.",
        },
        403: {
            "description": "AUTHORIZATION: the run is paused and the key's role is not "
            "`service`, or the key may not act in this tenant."
        },
        **conflict(
            "LEASE_LOST: a `worker_id` that does not hold the run's lease. CONFLICT: a leased "
            "run without `worker_id`, or a run that is no longer running or paused."
        ),
    },
    openapi_extra=_UPLOAD_BODY,
)
async def upload(
    run_id: RunId,
    request: Request,
    db: Session,
    who: Who,
    response: Response,
    worker_id: WorkerId = None,
    checksum: Checksum = None,
) -> ArtifactRef:
    """Store the body as an artifact of the run and return its reference. While the run is
    ``RUNNING`` a leased run takes only its lease holder's ``worker_id`` (409 otherwise);
    while ``PAUSED`` only a service key may add one (403); never after (409). The same bytes
    uploaded to the same run again answer 200 with the first artifact. At most 50 MiB,
    streamed to the blob store as it arrives (413 past it); an empty body is 422."""
    runs = RunStore(db)
    # fail fast, before the bytes are read and stored; checked again, locked, below
    await runs.check_artifact_writer(
        who.tenant_id, run_id, worker_id=worker_id, role=who.credential.role, lock=False
    )
    await db.commit()
    chunks = await _nonempty(request)
    mime = request.headers.get("content-type") or DEFAULT_MIME
    blobs = _blobs(request)
    artifact_id = new_id("art_")
    # streamed to the store as it arrives: a 50 MiB upload never sits whole in memory
    blob = await blobs.put_stream(f"artifacts/{artifact_id}", chunks, content_type=mime)
    if checksum is not None and checksum != SHA256 + blob.sha256:
        await blobs.delete(blob.key)
        raise Unprocessable(f"the body's checksum is {SHA256}{blob.sha256}, not {checksum}")
    try:
        await runs.check_artifact_writer(
            who.tenant_id, run_id, worker_id=worker_id, role=who.credential.role, lock=True
        )
        row, created = await ArtifactStore(db).add(
            artifact_id=artifact_id,
            tenant_id=who.tenant_id,
            run_id=run_id,
            blob=blob,
            mime=mime,
            now=now(),
        )
        await db.commit()
    except BaseException:
        await blobs.delete(blob.key)
        raise
    ref = artifact_ref(row)
    if created:
        response.headers["Location"] = f"{ARTIFACTS_PATH}/{row.artifact_id}"
    else:
        await blobs.delete(blob.key)
        response.status_code = _OK
    return ref


@router.get(
    "/v1/artifacts/{artifact_id}",
    response_class=StreamingResponse,
    summary="Read an artifact's bytes",
    responses={
        _OK: {
            "content": {"*/*": {"schema": {"type": "string", "format": "binary"}}},
            "description": "The artifact's bytes, with its `Content-Type`, `Content-Length`, "
            "`ETag` (its checksum) and `Cache-Control: private, max-age=31536000, immutable`.",
        },
        _NOT_MODIFIED: {"description": "The client's copy (`If-None-Match`) is this artifact."},
        500: {
            "description": "INTERNAL: the stored bytes no longer match their checksum; "
            "they are never served."
        },
    },
)
async def download(
    artifact_id: ArtifactId,
    request: Request,
    db: Session,
    who: Who,
    if_none_match: Annotated[
        str | None,
        Header(
            description="ETags the client holds (`*` for any); naming this artifact's is a 304."
        ),
    ] = None,
) -> Response:
    """The artifact's bytes, streamed with its mime type, verified against its checksum as
    they are read (a corrupted object is cut short, never served whole). 404 for another
    tenant's artifact, or one deleted after its run's retention. An artifact never changes
    (its ETag is its checksum): ``If-None-Match`` naming it is a 304 with no body, and a
    private cache may keep it for a year."""
    row = await ArtifactStore(db).get(who.tenant_id, artifact_id)
    await db.close()  # nothing more to read: no connection held while streaming
    etag = f'"{row.checksum}"'
    cached = {"ETag": etag, "Cache-Control": CACHE_CONTROL}
    if _matches(if_none_match, etag):
        return Response(status_code=_NOT_MODIFIED, headers=cached)
    stream = read(_blobs(request), row.blob_key, sha256=sha256_of(row), size=row.size)
    try:
        first = await anext(stream)
    except BlobNotFound as exc:
        raise NotFound(f"artifact {artifact_id} has no bytes") from exc
    except BlobCorrupt as exc:
        raise ServiceError(f"artifact {artifact_id} does not match its checksum") from exc

    async def body() -> AsyncIterator[bytes]:
        yield first
        async for chunk in stream:
            yield chunk

    return StreamingResponse(
        body(), media_type=row.mime, headers={"Content-Length": str(row.size), **cached}
    )


def _matches(if_none_match: str | None, etag: str) -> bool:
    """RFC 9110 ``If-None-Match``: ``*``, or a list of tags compared weakly."""
    if if_none_match is None:
        return False
    tags = [tag.strip().removeprefix("W/") for tag in if_none_match.split(",")]
    return "*" in tags or etag in tags
