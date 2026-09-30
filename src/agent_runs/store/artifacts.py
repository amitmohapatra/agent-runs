"""Run artifacts' records. The bytes are in the blob store; a row says whose they are, what
they are and when they go. Nothing here reads the clock."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from trellis.contracts.artifacts import ArtifactRef

from agent_runs.blob import StoredBlob
from agent_runs.domain.errors import NotFound
from agent_runs.store.tables import ArtifactRow

ARTIFACTS_PATH = "/v1/artifacts"
SHA256 = "sha256:"


def artifact_ref(row: ArtifactRow) -> ArtifactRef:
    """The contracts' reference: what an ``Interrupt.payload_ref`` carries. ``uri`` is this
    service's download route, never the blob store's own address."""
    return ArtifactRef(
        artifact_id=row.artifact_id,
        uri=f"{ARTIFACTS_PATH}/{row.artifact_id}",
        mime_type=row.mime,
        checksum=row.checksum,
        size_bytes=row.size,
        created_at=row.created_at,
        metadata={"run_id": row.run_id},
    )


def sha256_of(row: ArtifactRow) -> str:
    return row.checksum.removeprefix(SHA256)


class ArtifactStore:
    """The caller commits."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self,
        *,
        artifact_id: str,
        tenant_id: str,
        run_id: str,
        blob: StoredBlob,
        mime: str,
        now: datetime,
    ) -> tuple[ArtifactRow, bool]:
        """Record an uploaded blob. Returns ``(row, created)``: the same bytes uploaded to
        the same run again are the artifact the first upload made (``created`` false, and
        the caller deletes the duplicate blob)."""
        checksum = SHA256 + blob.sha256
        row = await self._session.scalar(
            insert(ArtifactRow)
            .values(
                artifact_id=artifact_id,
                run_id=run_id,
                tenant_id=tenant_id,
                blob_key=blob.key,
                mime=mime,
                size=blob.size,
                checksum=checksum,
                created_at=now,
            )
            .on_conflict_do_nothing(constraint="uq_run_artifacts_content")
            .returning(ArtifactRow)
        )
        if row is not None:
            return row, True
        existing = await self._session.scalar(
            select(ArtifactRow).where(
                ArtifactRow.run_id == run_id, ArtifactRow.checksum == checksum
            )
        )
        assert existing is not None  # the conflict was on exactly this row
        return existing, False

    async def get(self, tenant_id: str, artifact_id: str) -> ArtifactRow:
        row = await self._session.scalar(
            select(ArtifactRow).where(
                ArtifactRow.tenant_id == tenant_id, ArtifactRow.artifact_id == artifact_id
            )
        )
        if row is None:
            raise NotFound(f"no artifact {artifact_id}")
        return row

    async def expire_with(self, run_ids: Sequence[str], *, at: datetime) -> None:
        """The runs ended: their artifacts go at ``at``."""
        if run_ids:
            await self._session.execute(
                update(ArtifactRow)
                .where(ArtifactRow.run_id.in_(run_ids), ArtifactRow.expires_at.is_(None))
                .values(expires_at=at)
            )

    async def expired(self, *, now: datetime, limit: int) -> Sequence[ArtifactRow]:
        """Artifacts past ``expires_at``, held for this transaction (another ticker skips
        them)."""
        query = (
            select(ArtifactRow)
            .where(ArtifactRow.expires_at < now)
            .order_by(ArtifactRow.expires_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(query)).all()

    async def remove(self, artifact_ids: Sequence[str]) -> None:
        if artifact_ids:
            await self._session.execute(
                delete(ArtifactRow).where(ArtifactRow.artifact_id.in_(artifact_ids))
            )
