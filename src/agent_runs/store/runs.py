"""Reading and writing runs. Every state change is checked by the contracts' one rule,
``RunStatus.can_become``, under a row lock, because the failures that matter are concurrent:
a worker finishing a run a person just cancelled, two clicks resuming one pause, two workers
claiming one queued run.

Nothing here reads the clock: ``now`` is a parameter, so a lapsed lease or an overdue
interrupt is a test rather than a wait.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Select, literal, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from trellis.contracts.errors import AgentError, ErrorCategory
from trellis.contracts.runs import (
    Interrupt,
    InterruptDecision,
    InterruptResolution,
    RunRecord,
    RunStart,
    RunStatus,
)

from agent_runs.config.constants import DEFAULT_PAGE, MAX_ATTEMPTS, MAX_LINEAGE
from agent_runs.domain.errors import Conflict, NotFound, Unprocessable
from agent_runs.domain.runs import Claimed, ClaimRequest, HeartbeatRequest, Lease, RunFinish
from agent_runs.store.tables import RunRow

_RECORD_FIELDS = (
    "run_id",
    "tenant_id",
    "agent_id",
    "status",
    "parent_run_id",
    "thread_id",
    "user_id",
    "workspace_id",
    "on_behalf_of",
    "input",
    "output",
    "error",
    "awaiting",
    "last_resolution",
    "attempt",
    "deadline",
    "idempotency_key",
    "webhook_url",
    "created_at",
    "updated_at",
)


def _record(row: RunRow) -> RunRecord:
    fields: dict[str, Any] = {name: getattr(row, name) for name in _RECORD_FIELDS}
    return RunRecord.model_validate({**fields, "metadata": row.run_metadata or {}})


def _json(model: Any) -> dict[str, Any] | None:
    return model.model_dump(mode="json", exclude_none=True) if model is not None else None


def _move(row: RunRow, to: RunStatus, now: datetime) -> None:
    """The one transition check. Leaving RUNNING or PAUSED clears what belonged to it."""
    if not RunStatus(row.status).can_become(to):
        raise Conflict(f"run {row.run_id} cannot move from {row.status} to {to.value}")
    row.status = to.value
    row.updated_at = now
    if to is not RunStatus.RUNNING:
        row.lease_owner = row.lease_expires_at = None
    if to is not RunStatus.PAUSED:
        row.awaiting = row.assignee = row.awaiting_deadline = None


def _requeue(row: RunRow, now: datetime) -> None:
    """Back on the queue for a worker, as the next attempt."""
    _move(row, RunStatus.QUEUED, now)
    row.attempt += 1
    row.queued_at = now


class RunStore:
    """Every run operation, in one place. The caller commits."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ------------------------------------------------------------------ starting
    async def start(self, start: RunStart, *, queue: bool, now: datetime) -> tuple[RunRecord, bool]:
        """Record a run as ``RUNNING``, or ``QUEUED`` for a worker. Returns ``(run, created)``.

        Idempotent on the run id and on ``(tenant, idempotency_key)``: a repeat returns the
        run the first start made. ``ON CONFLICT DO NOTHING`` makes that hold under
        concurrency too: the loser of two simultaneous starts waits on the unique index and
        then reads the winner's row.
        """
        status = RunStatus.QUEUED if queue else RunStatus.RUNNING
        record = RunRecord.from_start(start, status=status)
        row = await self._session.scalar(
            insert(RunRow)
            .values(
                run_id=record.run_id,
                tenant_id=record.tenant_id,
                agent_id=record.agent_id,
                status=status.value,
                parent_run_id=record.parent_run_id,
                thread_id=record.thread_id,
                user_id=record.user_id,
                workspace_id=record.workspace_id,
                on_behalf_of=record.on_behalf_of,
                input=record.input,
                deadline=record.deadline,
                idempotency_key=record.idempotency_key,
                webhook_url=record.webhook_url,
                run_metadata=record.metadata or None,
                attempt=1,
                queued_at=now if queue else None,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing()
            .returning(RunRow)
        )
        if row is not None:
            return _record(row), True
        existing = None
        if start.idempotency_key:
            existing = await self._one(
                RunRow.tenant_id == start.tenant_id,
                RunRow.idempotency_key == start.idempotency_key,
            )
        existing = existing or await self._one(
            RunRow.tenant_id == start.tenant_id, RunRow.run_id == start.run_id
        )
        if existing is None:
            raise Conflict(f"run id {start.run_id} is taken")
        return _record(existing), False

    # ------------------------------------------------------------------ transitions
    async def pause(
        self,
        tenant_id: str,
        run_id: str,
        interrupt: Interrupt,
        *,
        worker_id: str | None,
        now: datetime,
    ) -> RunRecord:
        if (interrupt.tenant_id, interrupt.run_id) != (tenant_id, run_id):
            raise Unprocessable("the interrupt belongs to another run")
        row = await self._locked(tenant_id, run_id, worker_id=worker_id)
        _move(row, RunStatus.PAUSED, now)
        row.awaiting = interrupt.awaiting()
        row.assignee = interrupt.assignee
        row.awaiting_deadline = interrupt.deadline
        return await self._flushed(row)

    async def resume(
        self, tenant_id: str, run_id: str, resolution: InterruptResolution, *, now: datetime
    ) -> RunRecord:
        """Answer the interrupt a paused run waits on. ``CANCEL`` ends the run; any other
        decision continues it as the next attempt: back on the queue when the run is durable
        (it was ever queued, so a worker resumes it), else ``RUNNING`` in the caller's
        process."""
        if resolution.run_id != run_id:
            raise Unprocessable("the resolution answers another run")
        row = await self._locked(tenant_id, run_id, worker_id=None)
        if row.status != RunStatus.PAUSED or row.awaiting is None:
            raise Conflict(f"run {run_id} is {row.status}, not waiting on an answer")
        if Interrupt.model_validate(row.awaiting).interrupt_id != resolution.interrupt_id:
            raise Conflict(f"run {run_id} is waiting on another interrupt")
        row.last_resolution = _json(resolution)
        if resolution.decision is InterruptDecision.CANCEL:
            _move(row, RunStatus.CANCELLED, now)
        elif row.queued_at is not None:
            _requeue(row, now)
        else:
            _move(row, RunStatus.RUNNING, now)
            row.attempt += 1
        return await self._flushed(row)

    async def finish(
        self,
        tenant_id: str,
        run_id: str,
        ending: RunFinish,
        *,
        worker_id: str | None,
        now: datetime,
    ) -> RunRecord:
        row = await self._locked(tenant_id, run_id, worker_id=worker_id)
        _move(row, ending.status, now)
        row.output = ending.output
        row.error = _json(ending.error)
        return await self._flushed(row)

    # ------------------------------------------------------------------ the queue
    async def claim(
        self, tenant_id: str, request: ClaimRequest, *, now: datetime
    ) -> Claimed | None:
        """The oldest queued run of the worker's agents, leased to it; ``None`` when the
        queue is empty. ``SKIP LOCKED`` is what lets many workers claim at once without two
        of them ever getting one run: a row another claim holds is passed over, not waited
        on."""
        row = await self._session.scalar(
            select(RunRow)
            .where(
                RunRow.tenant_id == tenant_id,
                RunRow.status == RunStatus.QUEUED.value,
                RunRow.agent_id.in_(request.agent_ids),
            )
            .order_by(RunRow.queued_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if row is None:
            return None
        _move(row, RunStatus.RUNNING, now)
        lease = self._lease(row, request.worker_id, request.lease_seconds, now)
        return Claimed(run=await self._flushed(row), lease=lease)

    async def heartbeat(
        self, tenant_id: str, run_id: str, request: HeartbeatRequest, *, now: datetime
    ) -> Lease:
        """Extend the caller's lease. A 409 means the lease is gone (it lapsed and the run was
        re-queued, or the run was cancelled or finished): the worker must stop."""
        row = await self._locked(tenant_id, run_id, worker_id=request.worker_id)
        if row.status != RunStatus.RUNNING:
            raise Conflict(f"run {run_id} is {row.status}")
        lease = self._lease(row, request.worker_id, request.lease_seconds, now)
        await self._session.flush()
        return lease

    @staticmethod
    def _lease(row: RunRow, worker_id: str, seconds: int, now: datetime) -> Lease:
        expires_at = now + timedelta(seconds=seconds)
        row.lease_owner, row.lease_expires_at = worker_id, expires_at
        return Lease(run_id=row.run_id, worker_id=worker_id, expires_at=expires_at)

    # ------------------------------------------------------------------ the ticker's sweeps
    async def requeue_lapsed(self, *, now: datetime, limit: int) -> list[RunRecord]:
        """Runs whose worker stopped heartbeating go back on the queue as the next attempt;
        one that has used up ``MAX_ATTEMPTS`` ends as ``ERROR`` instead. Returns every run
        moved."""
        rows = await self._sweep(
            RunRow.status == RunStatus.RUNNING.value,
            RunRow.lease_expires_at < now,
            order=RunRow.lease_expires_at,
            limit=limit,
        )
        for row in rows:
            if row.attempt >= MAX_ATTEMPTS:
                _move(row, RunStatus.ERROR, now)
                row.error = _json(
                    AgentError(
                        code="lease_expired",
                        category=ErrorCategory.TIMEOUT,
                        message=f"the lease lapsed on each of {row.attempt} attempts",
                        source="agent-runs",
                    )
                )
            else:
                _requeue(row, now)
        await self._session.flush()
        return [_record(row) for row in rows]

    async def escalate_overdue(self, *, now: datetime, limit: int) -> list[RunRecord]:
        """Paused runs past their interrupt's deadline go to ``escalate_to`` (once: the new
        interrupt has no deadline), or end as ``TIMEOUT`` when nobody is named."""
        rows = await self._sweep(
            RunRow.status == RunStatus.PAUSED.value,
            RunRow.awaiting_deadline < now,
            order=RunRow.awaiting_deadline,
            limit=limit,
        )
        for row in rows:
            interrupt = Interrupt.model_validate(row.awaiting)
            if interrupt.escalate_to:
                escalated = interrupt.model_copy(
                    update={
                        "assignee": interrupt.escalate_to,
                        "escalate_to": None,
                        "deadline": None,
                    }
                )
                row.awaiting = escalated.awaiting()
                row.assignee = escalated.assignee
                row.awaiting_deadline = None
                row.updated_at = now
            else:
                _move(row, RunStatus.TIMEOUT, now)
                row.error = _json(
                    AgentError(
                        code="interrupt_deadline",
                        category=ErrorCategory.TIMEOUT,
                        message=f"nobody answered by {interrupt.deadline}",
                        source="agent-runs",
                    )
                )
        await self._session.flush()
        return [_record(row) for row in rows]

    async def _sweep(self, *conditions: Any, order: Any, limit: int) -> Sequence[RunRow]:
        query = (
            select(RunRow)
            .where(*conditions)
            .order_by(order)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(query)).all()

    # ------------------------------------------------------------------ reads
    async def get(self, tenant_id: str, run_id: str) -> RunRecord:
        row = await self._one(RunRow.tenant_id == tenant_id, RunRow.run_id == run_id)
        if row is None:
            raise NotFound(f"no run {run_id}")
        return _record(row)

    async def list(
        self,
        tenant_id: str,
        *,
        status: RunStatus | None = None,
        assignee: str | None = None,
        agent_id: str | None = None,
        thread_id: str | None = None,
        parent_run_id: str | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> list[RunRecord]:
        """This tenant's runs, newest first. ``status=PAUSED`` with ``assignee`` is the inbox
        of one person or role, served by ``ix_runs_inbox``."""
        query: Select[tuple[RunRow]] = select(RunRow).where(RunRow.tenant_id == tenant_id)
        filters: dict[Any, Any] = {
            RunRow.status: status.value if status else None,
            RunRow.assignee: assignee,
            RunRow.agent_id: agent_id,
            RunRow.thread_id: thread_id,
            RunRow.parent_run_id: parent_run_id,
        }
        for column, value in filters.items():
            if value is not None:
                query = query.where(column == value)
        rows = await self._session.scalars(query.order_by(RunRow.created_at.desc()).limit(limit))
        return [_record(row) for row in rows.all()]

    async def lineage(self, tenant_id: str, run_id: str) -> list[RunRecord]:
        """A run and its ancestors, nearest first, in one recursive query."""
        chain = (
            select(RunRow.run_id, RunRow.parent_run_id, literal(0).label("depth"))
            .where(RunRow.tenant_id == tenant_id, RunRow.run_id == run_id)
            .cte("chain", recursive=True)
        )
        parent = RunRow.__table__.alias("parent")
        chain = chain.union_all(
            select(parent.c.run_id, parent.c.parent_run_id, chain.c.depth + 1).where(
                parent.c.tenant_id == tenant_id,
                parent.c.run_id == chain.c.parent_run_id,
                chain.c.depth < MAX_LINEAGE,
            )
        )
        rows = await self._session.scalars(
            select(RunRow).join(chain, RunRow.run_id == chain.c.run_id).order_by(chain.c.depth)
        )
        found = [_record(row) for row in rows.all()]
        if not found:
            raise NotFound(f"no run {run_id}")
        return found

    # ------------------------------------------------------------------ internals
    async def _one(self, *conditions: Any) -> RunRow | None:
        return await self._session.scalar(select(RunRow).where(*conditions))

    async def _locked(self, tenant_id: str, run_id: str, *, worker_id: str | None) -> RunRow:
        """The row, locked for this transaction. With ``worker_id``, only while that worker
        still holds the run's lease: a worker whose lease lapsed must not write over the run
        another worker has since claimed."""
        row = await self._session.scalar(
            select(RunRow)
            .where(RunRow.tenant_id == tenant_id, RunRow.run_id == run_id)
            .with_for_update()
        )
        if row is None:
            raise NotFound(f"no run {run_id}")
        if worker_id is not None and row.lease_owner != worker_id:
            raise Conflict(f"worker {worker_id} does not hold the lease on run {run_id}")
        return row

    async def _flushed(self, row: RunRow) -> RunRecord:
        await self._session.flush()
        return _record(row)
