"""Run artifacts: a payload too large for a checkpoint or an interrupt (an ``ask`` table, a
diff, a report) is uploaded beside the run, and the run carries the ``ArtifactRef``
(typically as ``Interrupt.payload_ref``)."""

from __future__ import annotations

import hashlib
from typing import Final

from trellis.contracts.artifacts import ArtifactRef
from trellis.runs._transport import Transport, worker_params
from trellis.runs.errors import NotFoundError

#: What an artifact is sent as when the caller names nothing else.
JSON_MIME: Final = "application/json"


class ArtifactsAPI:
    """``runs.artifacts``: ``upload`` and ``download``."""

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
