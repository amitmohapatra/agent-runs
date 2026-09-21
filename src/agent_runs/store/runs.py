"""Reading and writing runs. The state machine is enforced here, not in the router.

Transitions are checked and applied inside one transaction with a row lock, because the
interesting failures are concurrent: a worker finishing a run at the same moment a user
cancels it, or two replicas resuming the same paused run when a person clicks twice.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from agent_runs.domain.models import RUNNING, Run, RunCreate, RunTransition, check
from agent_runs.store.tables import RunRow


def _to_run(row: RunRow) -> Run:
    return Run(
        run_id=row.run_id,
        tenant_id=row.tenant_id,
        agent_id=row.agent_id,
        status=row.status,
        parent_run_id=row.parent_run_id,
        thread_id=row.thread_id,
        user_id=row.user_id,
        on_behalf_of=row.on_behalf_of,
        input=row.input,
        output=row.output,
        error=row.error,
        awaiting=row.awaiting,
        attempt=row.attempt,
        deadline=row.deadline,
        idempotency_key=row.idempotency_key,
        metadata=row.run_metadata or {},
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


class RunStore:
    """Every run operation, in one place."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def start(self, spec: RunCreate) -> tuple[Run, bool]:
        """Create a run. Returns ``(run, created)``.

        A repeated start with the same idempotency key returns the original run rather than
        a second one — a scheduler delivering at-least-once would otherwise run the same
        9am job twice whenever its acknowledgement was lost.
        """
        if spec.idempotency_key:
            existing = await self._session.scalar(
                select(RunRow).where(
                    RunRow.tenant_id == spec.tenant_id,
                    RunRow.idempotency_key == spec.idempotency_key,
                )
            )
            if existing is not None:
                return _to_run(existing), False

        row = RunRow(
            run_id=spec.run_id or f"run_{uuid.uuid4().hex}",
            tenant_id=spec.tenant_id,
            agent_id=spec.agent_id,
            status=RUNNING,
            parent_run_id=spec.parent_run_id,
            thread_id=spec.thread_id,
            user_id=spec.user_id,
            on_behalf_of=spec.on_behalf_of,
            input=spec.input,
            deadline=spec.deadline,
            idempotency_key=spec.idempotency_key,
            run_metadata=spec.metadata or None,
        )
        self._session.add(row)
        await self._session.flush()
        return _to_run(row), True

    async def get(self, tenant_id: str, run_id: str) -> Run | None:
        row = await self._session.scalar(
            select(RunRow).where(RunRow.tenant_id == tenant_id, RunRow.run_id == run_id)
        )
        return _to_run(row) if row else None

    async def transition(self, tenant_id: str, run_id: str, change: RunTransition) -> Run | None:
        """Apply a state change under a row lock, or raise ``InvalidTransition``."""
        row = await self._session.scalar(
            select(RunRow)
            .where(RunRow.tenant_id == tenant_id, RunRow.run_id == run_id)
            .with_for_update()
        )
        if row is None:
            return None
        check(run_id, row.status, change.status)

        if change.status == RUNNING:
            # Resuming: a new attempt, and whatever it was waiting for is answered.
            row.attempt += 1
            row.awaiting = None
        else:
            row.awaiting = change.awaiting
        row.status = change.status
        if change.output is not None:
            row.output = change.output
        if change.error is not None:
            row.error = change.error
        if change.metadata:
            row.run_metadata = {**(row.run_metadata or {}), **change.metadata}
        row.updated_at = datetime.now(UTC)
        await self._session.flush()
        return _to_run(row)

    async def list(
        self,
        tenant_id: str,
        *,
        status: str | None = None,
        agent_id: str | None = None,
        thread_id: str | None = None,
        parent_run_id: str | None = None,
        limit: int = 50,
    ) -> list[Run]:
        query = select(RunRow).where(RunRow.tenant_id == tenant_id)
        filters: dict[Any, Any] = {
            RunRow.status: status,
            RunRow.agent_id: agent_id,
            RunRow.thread_id: thread_id,
            RunRow.parent_run_id: parent_run_id,
        }
        for column, value in filters.items():
            if value is not None:
                query = query.where(column == value)
        query = query.order_by(RunRow.created_at.desc()).limit(min(limit, 500))
        return [_to_run(r) for r in (await self._session.scalars(query)).all()]

    async def lineage(self, tenant_id: str, run_id: str) -> list[Run]:
        """A run and its ancestors, nearest first.

        The memory service carries only one level of parent on its context, so a grandchild
        cannot see a grandparent's RUN-visible memories. Owning the whole chain here is what
        makes answering "every ancestor of C" possible at all.
        """
        chain: list[Run] = []
        seen: set[str] = set()
        current = await self.get(tenant_id, run_id)
        while current is not None and current.run_id not in seen:
            chain.append(current)
            seen.add(current.run_id)
            if not current.parent_run_id:
                break
            current = await self.get(tenant_id, current.parent_run_id)
        return chain
