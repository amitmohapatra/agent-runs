"""Run artifacts: payloads too large for a run's checkpoint or an interrupt (an ``ask``
table, a diff) go to blob storage, and the run carries an ``ArtifactRef`` to them
(``Interrupt.payload_ref``)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import StreamingResponse
from trellis.contracts.artifacts import ArtifactRef
from trellis.contracts.ids import new_id, now

from agent_runs.api.deps import Session, Who
from agent_runs.api.routers.runs import WorkerId
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
DEFAULT_MIME = "application/octet-stream"
_TOO_LARGE = f"an artifact is at most {MAX_ARTIFACT_BYTES} bytes"

#: The SHA-256 the caller computed, ``sha256:<hex>``: the upload is refused (422) unless the
#: bytes that arrived match it.
Checksum = Annotated[str | None, Query(pattern=r"^sha256:[0-9a-f]{64}$")]

_UPLOAD_BODY = {
    "requestBody": {
        "required": True,
        "description": "The artifact's bytes; Content-Type is its mime type "
        "(application/json for an ask table).",
        "content": {"*/*": {"schema": {"type": "string", "format": "binary"}}},
    }
}


def _blobs(request: Request) -> BlobStore:
    return request.app.state.blobs


async def _body(request: Request) -> bytes:
    """The request body, refused (413) past ``MAX_ARTIFACT_BYTES`` before it is all read."""
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > MAX_ARTIFACT_BYTES:
        raise TooLarge(_TOO_LARGE)
    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > MAX_ARTIFACT_BYTES:
            raise TooLarge(_TOO_LARGE)
    if not body:
        raise Unprocessable("an artifact has at least one byte")
    return bytes(body)


@router.post(
    "/v1/runs/{run_id}/artifacts",
    status_code=_CREATED,
    responses={_OK: {"model": ArtifactRef}},
    openapi_extra=_UPLOAD_BODY,
)
async def upload(
    run_id: str,
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
    uploaded to the same run again answer 200 with the first artifact."""
    runs = RunStore(db)
    # fail fast, before the bytes are read and stored; checked again, locked, below
    await runs.check_artifact_writer(
        who.tenant_id, run_id, worker_id=worker_id, role=who.credential.role, lock=False
    )
    await db.commit()
    data = await _body(request)
    mime = request.headers.get("content-type") or DEFAULT_MIME
    blobs = _blobs(request)
    artifact_id = new_id("art_")
    blob = await blobs.put(f"artifacts/{artifact_id}", data, content_type=mime)
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
    responses={_OK: {"content": {"*/*": {}}, "description": "the artifact's bytes"}},
)
async def download(artifact_id: str, request: Request, db: Session, who: Who) -> Response:
    """The artifact's bytes, streamed with its mime type, verified against its checksum as
    they are read (a corrupted object is cut short, never served whole). 404 for another
    tenant's artifact, or one deleted after its run's retention."""
    row = await ArtifactStore(db).get(who.tenant_id, artifact_id)
    await db.close()  # nothing more to read: no connection held while streaming
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
        body(),
        media_type=row.mime,
        headers={"Content-Length": str(row.size), "ETag": f'"{row.checksum}"'},
    )
