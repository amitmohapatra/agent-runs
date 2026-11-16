"""Run artifacts: a payload too large for a checkpoint or an interrupt (an ``ask`` table, a
diff, a report) is uploaded beside the run, and the run carries the ``ArtifactRef``
(typically as ``Interrupt.payload_ref``)."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Final

from trellis.contracts.artifacts import ArtifactRef
from trellis.runs._transport import Transport, worker_params
from trellis.runs.blob import sha256_hex
from trellis.runs.errors import NotFoundError
from trellis.runs.payloads import (
    MAX_READ_BYTES,
    MAX_READ_OBJECTS,
    MEDIA_TYPE,
    PayloadRef,
    frames,
)

#: What an artifact is sent as when the caller names nothing else.
JSON_MIME: Final = "application/json"


class ArtifactsAPI:
    """``runs.artifacts``: ``upload`` and ``download``; ``put_payload`` and ``get_payload``
    for payload objects (``trellis.runs.payloads``: a checkpoint's segments)."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    async def upload(
        self,
        run_id: str,
        data: bytes,
        *,
        mime_type: str = JSON_MIME,
        worker_id: str | None = None,
        tenant: str | None = None,
    ) -> ArtifactRef:
        """Store ``data`` as an artifact of the run (at most 50 MiB). Its SHA-256 goes with
        it, so bytes damaged on the way are refused; the same bytes again answer the artifact
        already stored. A leased run takes an upload only with its lease holder's
        ``worker_id``; a paused one only from a ``service`` key."""
        params = {
            **worker_params(worker_id),
            "checksum": f"sha256:{sha256_hex(data)}",
        }
        body = await self._transport.json(
            "POST",
            f"/v1/runs/{run_id}/artifacts",
            tenant=tenant,
            content=data,
            params=params,
            headers={"Content-Type": mime_type},
        )
        return ArtifactRef.model_validate(body)

    async def download(self, artifact_id: str, *, tenant: str | None = None) -> bytes | None:
        """The artifact's bytes, verified by the service against their SHA-256; None when
        there is no such artifact (or it expired with its run)."""
        try:
            response = await self._transport.send(
                "GET", f"/v1/artifacts/{artifact_id}", tenant=tenant
            )
        except NotFoundError:
            return None
        return response.content

    async def put_payload(
        self,
        run_id: str,
        data: bytes,
        *,
        worker_id: str | None = None,
        tenant: str | None = None,
    ) -> PayloadRef:
        """Store a payload object (``payloads.encode``'s bytes) for the run's tenant; its
        reference, for a checkpoint's ``SEGMENTS``. The same bytes are stored once. Who may
        write is who may ``upload`` an artifact."""
        body = await self._transport.json(
            "POST",
            f"/v1/runs/{run_id}/payloads",
            tenant=tenant,
            content=data,
            params=worker_params(worker_id),
            headers={"Content-Type": MEDIA_TYPE},
        )
        return PayloadRef.model_validate(body)

    async def get_payload(self, ref: PayloadRef, *, tenant: str | None = None) -> bytes:
        """The bytes of a payload object, checked here too against the reference's size
        and SHA-256 (``ValueError`` when they differ)."""
        response = await self._transport.send("GET", f"/v1/payloads/{ref.sha256}", tenant=tenant)
        return _whole(ref, response.content)

    async def get_payloads(
        self, refs: Sequence[PayloadRef], *, tenant: str | None = None
    ) -> list[bytes]:
        """The bytes of several payload objects (a checkpoint's segments), in their order,
        each checked as ``get_payload`` checks it: in one request when one read answers them
        all (``MAX_READ_OBJECTS``, ``MAX_READ_BYTES``), else in as few as answer them."""
        found: list[bytes] = []
        for asked in _reads(refs):
            if len(asked) == 1:
                found.append(await self.get_payload(asked[0], tenant=tenant))
                continue
            response = await self._transport.send(
                "POST",
                "/v1/payloads/read",
                tenant=tenant,
                json={"sha256": [ref.sha256 for ref in asked]},
            )
            answered = zip(asked, frames(response.content), strict=True)
            found += [_whole(ref, data) for ref, data in answered]
        return found


def _reads(refs: Sequence[PayloadRef]) -> Iterator[list[PayloadRef]]:
    """``refs`` in their order, split into what one read answers each."""
    asked: list[PayloadRef] = []
    size = 0
    for ref in refs:
        if asked and (len(asked) == MAX_READ_OBJECTS or size + ref.size > MAX_READ_BYTES):
            yield asked
            asked, size = [], 0
        asked.append(ref)
        size += ref.size
    if asked:
        yield asked


def _whole(ref: PayloadRef, data: bytes) -> bytes:
    if len(data) != ref.size or sha256_hex(data) != ref.sha256:
        raise ValueError(f"payload {ref.sha256} did not arrive whole")
    return data
