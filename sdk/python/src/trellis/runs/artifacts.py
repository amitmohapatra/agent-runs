"""Run artifacts: a payload too large for a checkpoint or an interrupt (an ``ask`` table, a
diff, a report) is uploaded beside the run, and the run carries the ``ArtifactRef``
(typically as ``Interrupt.payload_ref``)."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Final

from trellis.contracts.artifacts import ArtifactRef
from trellis.runs._transport import Transport, worker_params
from trellis.runs.errors import NotFoundError
from trellis.runs.payloads import MEDIA_TYPE, PayloadRef, frames

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
            "checksum": f"sha256:{hashlib.sha256(data).hexdigest()}",
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
        """The bytes of several payload objects (a checkpoint's segments) in one request,
        in their order, each checked as ``get_payload`` checks it."""
        response = await self._transport.send(
            "POST",
            "/v1/payloads/read",
            tenant=tenant,
            json={"sha256": [ref.sha256 for ref in refs]},
        )
        return [_whole(ref, data) for ref, data in zip(refs, frames(response.content), strict=True)]


def _whole(ref: PayloadRef, data: bytes) -> bytes:
    if len(data) != ref.size or hashlib.sha256(data).hexdigest() != ref.sha256:
        raise ValueError(f"payload {ref.sha256} did not arrive whole")
    return data
