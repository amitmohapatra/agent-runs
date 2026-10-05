"""Reading and writing runs. Every state change is checked by the contracts' one rule,
``RunStatus.can_become``, under a row lock, because the failures that matter are concurrent:
a worker finishing a run a person just cancelled, two clicks resuming one pause, two workers
claiming one queued run.

Nothing here reads the clock: ``now`` is a parameter, so a lapsed lease or an overdue
interrupt is a test rather than a wait.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from trellis.contracts.errors import AgentError, ErrorCategory
from trellis.contracts.ids import stable_id
from trellis.contracts.runs import (
    Interrupt,
    InterruptDecision,
    InterruptResolution,
    RunRecord,
    RunStart,
    RunStatus,
)

from agent_runs.answering import require_may_answer
from agent_runs.config.constants import (
    ARTIFACT_RETENTION,
    DEFAULT_PAGE,
    MAX_LEASE_LAPSES,
    PAUSED_ARTIFACT_ROLE,
)
from agent_runs.domain.errors import Conflict, Forbidden, LeaseLost, NotFound, Unprocessable
from agent_runs.domain.runs import (
    Claimed,
    ClaimRequest,
    HeartbeatRequest,
    Lease,
    ResolutionEntry,
    RunFinish,
    RunPause,
    RunSummary,
    bounded_checkpoint,
)
from agent_runs.keys import KeyInfo
from agent_runs.store.artifacts import ArtifactStore
from agent_runs.store.paging import Page, page_of
from agent_runs.store.tables import ResolutionRow, RunRow

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
    "checkpoint",
    "attempt",
    "deadline",
    "idempotency_key",
    "created_at",
    "updated_at",
)


_SUMMARY_COLUMNS = tuple(getattr(RunRow, name) for name in RunSummary.model_fields)

#: What a start asks for, compared when an ``idempotency_key`` finds an earlier run: the
#: same key with a different request is a client bug to surface, not a run to hand back.
#: ``run_id`` is not compared (the contracts mint one when none is sent, so a retry that
#: sent none differs there by construction).
_START_FIELDS = frozenset(
    {
        "agent_id",
        "parent_run_id",
        "thread_id",
        "user_id",
        "workspace_id",
        "on_behalf_of",
        "input",
        "deadline",
        "metadata",
    }
)


def _record(row: RunRow) -> RunRecord:
    fields: dict[str, Any] = {name: getattr(row, name) for name in _RECORD_FIELDS}
    return RunRecord.model_validate({**fields, "metadata": row.run_metadata or {}})


def _json(model: Any) -> dict[str, Any] | None:
    return model.model_dump(mode="json", exclude_none=True) if model is not None else None


def _differing(existing: RunRecord, start: RunStart, *, queue: bool, queued: bool) -> list[str]:
    """The fields in which a repeated start asks for something other than the run it found."""
    asked = start.model_dump(mode="json", include=set(_START_FIELDS))
    kept = existing.model_dump(mode="json", include=set(_START_FIELDS))
    differing = sorted(name for name in _START_FIELDS if asked.get(name) != kept.get(name))
    return [*differing, "queue"] if queue != queued else differing


def _move(row: RunRow, to: RunStatus, now: datetime) -> None:
    """The one transition check. Leaving RUNNING or PAUSED clears what belonged to it; an
    ending clears the checkpoint, which only a run that may still continue needs. Whoever
    settled the old state did not settle the new one."""
    if not RunStatus(row.status).can_become(to):
        raise Conflict(f"run {row.run_id} cannot move from {row.status} to {to.value}")
    row.status = to.value
    row.updated_at = now
    row.settled_by = None
    if to is not RunStatus.RUNNING:
        row.lease_owner = row.lease_expires_at = None
    if to is not RunStatus.PAUSED:
        row.awaiting = row.assignee = row.awaiting_deadline = None
    if to.final:
        row.checkpoint = None


def _fence(row: RunRow, worker_id: str | None) -> None:
    """A worker's write (``worker_id``) is taken only while it still holds the run's lease:
    a worker whose lease lapsed must not write over the run another worker has since
    claimed."""
    if worker_id is not None and row.lease_owner != worker_id:
        raise LeaseLost(f"worker {worker_id} does not hold the lease on run {row.run_id}")


def _settled(row: RunRow, status: RunStatus, worker_id: str | None) -> bool:
    """Is the run already in ``status``, put there by this caller? Then a pause or finish
    to it is a repeat: the caller never saw the answer to the first one."""
    return row.status == status.value and row.settled_by == worker_id


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
    async def start(
        self, start: RunStart, *, queue: bool, now: datetime, strict: bool = True
    ) -> tuple[RunRecord, bool]:
        """Record a run as ``RUNNING``, or ``QUEUED`` for a worker. Returns ``(run, created)``.

        Idempotent on the run id and on ``(tenant, idempotency_key)``: a repeat returns the
        run the first start made. ``ON CONFLICT DO NOTHING`` makes that hold under
        concurrency too: the loser of two simultaneous starts waits on the unique index and
        then reads the winner's row. ``strict``: a key repeated with a different request is
        a ``Conflict`` (a schedule's fire is not strict: its key is the tick, and the
        schedule may have been edited since the tick's first fire).

        A run id held by another tenant is a ``Conflict`` that says nothing about who holds
        it, or that anyone does.
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
        if start.idempotency_key:
            keyed = await self._one(
                RunRow.tenant_id == start.tenant_id,
                RunRow.idempotency_key == start.idempotency_key,
            )
            if keyed is not None:
                found = _record(keyed)
                queued = keyed.queued_at is not None
                differing = _differing(found, start, queue=queue, queued=queued)
                if strict and differing:
                    raise Conflict(
                        f"idempotency_key {start.idempotency_key!r} started a different run: "
                        f"{', '.join(differing)} differ",
                        details={"differing": differing},
                    )
                return found, False
        existing = await self._one(
            RunRow.tenant_id == start.tenant_id, RunRow.run_id == start.run_id
        )
        if existing is None:
            raise Conflict("this run_id cannot be used: send another, or none and one is minted")
        return _record(existing), False

    # ------------------------------------------------------------------ transitions
    async def pause(
        self,
        tenant_id: str,
        run_id: str,
        pause: RunPause,
        *,
        worker_id: str | None,
        now: datetime,
    ) -> tuple[RunRecord, bool]:
        """The run waits on ``pause.interrupt``. Returns ``(run, paused)``: a repeat of the
        pause that made the current state (the same caller, the same interrupt) answers the
        stored run with ``paused`` false, changing nothing."""
        interrupt = pause.interrupt
        if (interrupt.tenant_id, interrupt.run_id) != (tenant_id, run_id):
            raise Unprocessable("the interrupt belongs to another run")
        checkpoint = pause.bounded_checkpoint()
        row = await self._locked(tenant_id, run_id, worker_id=None)
        waiting_on = (row.awaiting or {}).get("interrupt_id")
        if _settled(row, RunStatus.PAUSED, worker_id) and waiting_on == interrupt.interrupt_id:
            return _record(row), False
        _fence(row, worker_id)
        _move(row, RunStatus.PAUSED, now)
        row.checkpoint = checkpoint
        row.awaiting = interrupt.awaiting()
        row.assignee = interrupt.assignee
        row.awaiting_deadline = interrupt.deadline
        row.settled_by = worker_id
        return await self._flushed(row), True

    async def resume(
        self,
        tenant_id: str,
        run_id: str,
        resolution: InterruptResolution,
        *,
        answerer: KeyInfo,
        now: datetime,
    ) -> RunRecord:
        """Answer the interrupt a paused run waits on, as ``answerer`` may
        (``answering.py``: checked against the run's assignee now, before anything is
        written). ``CANCEL`` ends the run; any other decision continues it as the next
        attempt: back on the queue when the run is durable (it was ever queued, so a worker
        resumes it), else ``RUNNING`` in the caller's process."""
        if resolution.run_id != run_id:
            raise Unprocessable("the resolution answers another run")
        row = await self._locked(tenant_id, run_id, worker_id=None)
        if row.status != RunStatus.PAUSED or row.awaiting is None:
            raise Conflict(f"run {run_id} is {row.status}, not waiting on an answer")
        asked = Interrupt.model_validate(row.awaiting)
        if asked.interrupt_id != resolution.interrupt_id:
            raise Conflict(f"run {run_id} is waiting on another interrupt")
        require_may_answer(answerer, asked.assignee, resolution.reviewer)
        row.last_resolution = _json(resolution)
        self._session.add(
            ResolutionRow(
                resolution_id=stable_id(run_id, resolution.interrupt_id, prefix="res_"),
                run_id=run_id,
                tenant_id=tenant_id,
                interrupt_id=resolution.interrupt_id,
                decision=resolution.decision.value,
                reviewer=resolution.reviewer,
                interrupt=_json(asked) or {},
                resolution=_json(resolution) or {},
                attempt=row.attempt,
                resolved_at=resolution.resolved_at,
                recorded_at=now,
            )
        )
        if resolution.decision is InterruptDecision.CANCEL:
            _move(row, RunStatus.CANCELLED, now)
        elif row.queued_at is not None:
            _requeue(row, now)
        else:
            _move(row, RunStatus.RUNNING, now)
            row.attempt += 1
        await self._ended([row], now)
        return await self._flushed(row)

    async def finish(
        self,
        tenant_id: str,
        run_id: str,
        ending: RunFinish,
        *,
        worker_id: str | None,
        now: datetime,
    ) -> tuple[RunRecord, bool]:
        """End the run. Returns ``(run, ended)``: a repeat of the finish that ended it (the
        same caller, the same status) answers the stored run with ``ended`` false, changing
        nothing, so a worker that never saw the answer may retry."""
        row = await self._locked(tenant_id, run_id, worker_id=None)
        if _settled(row, ending.status, worker_id):
            return _record(row), False
        _fence(row, worker_id)
        _move(row, ending.status, now)
        row.output = ending.output
        row.error = _json(ending.error)
        row.settled_by = worker_id
        await self._ended([row], now)
        return await self._flushed(row), True

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
        """Extend the caller's lease, saving its progress checkpoint when it sends one. A 409
        means the lease is gone (it lapsed and the run was re-queued, or the run was
        cancelled or finished): the worker must stop, and nothing is saved."""
        checkpoint = bounded_checkpoint(request.checkpoint)
        row = await self._locked(tenant_id, run_id, worker_id=request.worker_id)
        if row.status != RunStatus.RUNNING:
            raise LeaseLost(f"run {run_id} is {row.status}: no lease to extend")
        if checkpoint is not None:
            row.checkpoint = checkpoint
            row.updated_at = now
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
        one whose lease has now lapsed ``MAX_LEASE_LAPSES`` times ends as ``ERROR`` instead.
        Only lapses count: a person's answer starts an attempt too, and is no crash. Returns
        every run moved."""
        rows = await self._sweep(
            RunRow.status == RunStatus.RUNNING.value,
            RunRow.lease_expires_at < now,
            order=RunRow.lease_expires_at,
            limit=limit,
        )
        for row in rows:
            row.lease_lapses += 1
            if row.lease_lapses >= MAX_LEASE_LAPSES:
                _move(row, RunStatus.ERROR, now)
                row.error = _json(
                    AgentError(
                        code="lease_expired",
                        category=ErrorCategory.TIMEOUT,
                        message=f"the lease lapsed {row.lease_lapses} times: every worker "
                        "that claimed the run stopped heartbeating",
                        source="agent-runs",
                    )
                )
            else:
                _requeue(row, now)
        await self._ended(rows, now)
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
        await self._ended(rows, now)
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

    # ------------------------------------------------------------------ artifacts
    async def check_artifact_writer(
        self,
        tenant_id: str,
        run_id: str,
        *,
        worker_id: str | None,
        role: str,
        lock: bool,
    ) -> None:
        """May this caller add an artifact to the run now? While ``RUNNING``, only the
        worker holding the lease (``worker_id``) when the run is leased; a run in the
        caller's own process has no lease and takes no ``worker_id``. While ``PAUSED``, only
        the tenant's service principal (``PAUSED_ARTIFACT_ROLE``), e.g. a reviewer's
        corrected table uploaded by the UI backend. Never otherwise (409). ``lock`` holds
        the row until the transaction ends, so the run cannot end between this check and
        the artifact's insert."""
        query = select(RunRow).where(RunRow.tenant_id == tenant_id, RunRow.run_id == run_id)
        row = await self._session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise NotFound(f"no run {run_id}")
        if row.status == RunStatus.RUNNING:
            if worker_id is None and row.lease_owner is not None:
                raise Conflict(f"run {run_id} is leased: only its lease holder adds artifacts")
            if worker_id != row.lease_owner:
                raise LeaseLost(f"worker {worker_id} does not hold the lease on run {run_id}")
        elif row.status == RunStatus.PAUSED:
            if role != PAUSED_ARTIFACT_ROLE:
                raise Forbidden(f"only a {PAUSED_ARTIFACT_ROLE} key adds to a paused run")
        elif worker_id is not None:
            raise LeaseLost(f"run {run_id} is {row.status}; worker {worker_id} holds no lease")
        else:
            raise Conflict(f"run {run_id} is {row.status}; artifacts are added while it runs")

    async def _ended(self, rows: Sequence[RunRow], now: datetime) -> None:
        """Runs that just ended start their artifacts' retention."""
        ended = [row.run_id for row in rows if RunStatus(row.status).final]
        await ArtifactStore(self._session).expire_with(ended, at=now + ARTIFACT_RETENTION)

    # ------------------------------------------------------------------ reads
    async def resolutions(
        self,
        tenant_id: str,
        run_id: str,
        *,
        limit: int = DEFAULT_PAGE,
        after: Mapping[str, Any] | None = None,
    ) -> Page[ResolutionEntry]:
        """Every interrupt the run was asked and how it was answered, oldest first, a page
        at a time (keyset ``recorded_at, resolution_id``)."""
        await self.get(tenant_id, run_id)  # 404 for another tenant's run, as everywhere
        order = (ResolutionRow.recorded_at, ResolutionRow.resolution_id)
        query = select(ResolutionRow).where(
            ResolutionRow.tenant_id == tenant_id, ResolutionRow.run_id == run_id
        )
        if after is not None:
            query = query.where(tuple_(*order) > (after["recorded_at"], after["resolution_id"]))
        rows = (await self._session.scalars(query.order_by(*order).limit(limit + 1))).all()
        return page_of(
            rows,
            limit=limit,
            item=lambda r: ResolutionEntry(
                interrupt=Interrupt.model_validate(r.interrupt),
                resolution=InterruptResolution.model_validate(r.resolution),
                attempt=r.attempt,
                recorded_at=r.recorded_at,
            ),
            position=lambda r: {"recorded_at": r.recorded_at, "resolution_id": r.resolution_id},
        )

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
        after: Mapping[str, Any] | None = None,
    ) -> Page[RunSummary]:
        """This tenant's runs, newest first, as summaries (only the summary's columns are
        read), a page at a time (keyset ``created_at, run_id``, both descending).
        ``status=PAUSED`` with ``assignee`` is the inbox of one person or role, served by
        ``ix_runs_inbox``."""
        order = (RunRow.created_at, RunRow.run_id)
        query = select(*_SUMMARY_COLUMNS, RunRow.created_at).where(RunRow.tenant_id == tenant_id)
        if after is not None:
            query = query.where(tuple_(*order) < (after["created_at"], after["run_id"]))
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
        newest = query.order_by(*(column.desc() for column in order)).limit(limit + 1)
        rows = (await self._session.execute(newest)).all()
        return page_of(
            rows,
            limit=limit,
            item=lambda row: RunSummary.model_validate(dict(row._mapping)),
            position=lambda row: {"created_at": row.created_at, "run_id": row.run_id},
        )

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
        _fence(row, worker_id)
        return row

    async def _flushed(self, row: RunRow) -> RunRecord:
        await self._session.flush()
        return _record(row)
