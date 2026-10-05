"""Reading and writing runs. Every state change is checked by the contracts' one rule,
``RunStatus.can_become``, under a row lock, because the failures that matter are concurrent:
a worker finishing a run a person just cancelled, two clicks resuming one pause, two workers
claiming one queued run.

Nothing here reads the clock: ``now`` is a parameter, so a lapsed lease, an overdue
interrupt or a run that worked too long is a test rather than a wait.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Select, func, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from trellis.contracts.errors import AgentError, ErrorCategory
from trellis.contracts.ids import stable_id
from trellis.contracts.runs import (
    Interrupt,
    InterruptDecision,
    InterruptResolution,
    RunEvent,
    RunRecord,
    RunStart,
    RunStatus,
)
from trellis.runs.answers import answer_problem, schema_problem

from agent_runs.answering import require_may_answer, require_may_cancel
from agent_runs.config.constants import (
    ARTIFACT_RETENTION,
    CONCURRENCY_PER_KEY,
    DEFAULT_PAGE,
    ERROR_RETRY_BASE,
    ERROR_RETRY_CAP,
    LAPSE_RETRY_BASE,
    LAPSE_RETRY_CAP,
    MAX_ERROR_RETRIES,
    MAX_LEASE_LAPSES,
    PAUSED_ARTIFACT_ROLE,
)
from agent_runs.domain.errors import Conflict, Forbidden, LeaseLost, NotFound, Unprocessable
from agent_runs.domain.runs import (
    Claimed,
    ClaimRequest,
    EventsAppend,
    EventsAppended,
    HeartbeatRequest,
    Lease,
    ReleaseRequest,
    ResolutionEntry,
    RunCancel,
    RunEventEntry,
    RunFinish,
    RunPause,
    RunSummary,
    bounded_checkpoint,
)
from agent_runs.keys import KeyInfo
from agent_runs.retry import backoff, jittered
from agent_runs.store.artifacts import ArtifactStore
from agent_runs.store.paging import Page, page_of
from agent_runs.store.tables import ResolutionRow, RunEventRow, RunRow

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
    "timeout_seconds",
    "idempotency_key",
    "agent_version",
    "priority",
    "concurrency_key",
    "created_at",
    "updated_at",
)


_SUMMARY_COLUMNS = tuple(getattr(RunRow, name) for name in RunSummary.model_fields)

#: A run a worker holds: what fair share and the per-tenant cap count.
_LEASED = (RunRow.status == RunStatus.RUNNING.value, RunRow.lease_owner.is_not(None))

#: The statuses a run can still move on from, as ``ix_runs_deadline`` names them: the runs a
#: deadline can still catch.
_UNENDED = (RunStatus.QUEUED.value, RunStatus.RUNNING.value, RunStatus.PAUSED.value)

#: What a start asks for, compared when an ``idempotency_key`` finds an earlier run: the
#: same key with a different request is a client bug to surface, not a run to hand back.
#: ``run_id`` is not compared (the contracts mint one when none is sent, so a retry that
#: sent none differs there by construction), nor is ``agent_version`` (a retry from a newer
#: deploy asks for the same run).
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
        "timeout_seconds",
        "priority",
        "concurrency_key",
        "metadata",
    }
)


def _worked(row: RunRow, now: datetime) -> float:
    """The run's working time at ``now``: its RUNNING stretches that ended, and the one going
    on."""
    if row.running_since is None:
        return row.worked_seconds
    return row.worked_seconds + max(0.0, (now - row.running_since).total_seconds())


def _record(row: RunRow, now: datetime) -> RunRecord:
    fields: dict[str, Any] = {name: getattr(row, name) for name in _RECORD_FIELDS}
    return RunRecord.model_validate(
        {**fields, "metadata": row.run_metadata or {}, "worked_seconds": _worked(row, now)}
    )


def _json(model: Any) -> dict[str, Any] | None:
    return model.model_dump(mode="json", exclude_none=True) if model is not None else None


def _differing(existing: RunRecord, start: RunStart, *, queue: bool, queued: bool) -> list[str]:
    """The fields in which a repeated start asks for something other than the run it found."""
    asked = start.model_dump(mode="json", include=set(_START_FIELDS))
    kept = existing.model_dump(mode="json", include=set(_START_FIELDS))
    differing = sorted(name for name in _START_FIELDS if asked.get(name) != kept.get(name))
    return [*differing, "queue"] if queue != queued else differing


def _move(row: RunRow, to: RunStatus, now: datetime) -> None:
    """The one transition check. Leaving RUNNING adds the stretch to the working time and
    clears the lease; leaving PAUSED clears what it waited on; an ending clears the
    checkpoint, which only a run that may still continue needs. Whoever settled the old state
    did not settle the new one."""
    if not RunStatus(row.status).can_become(to):
        raise Conflict(f"run {row.run_id} cannot move from {row.status} to {to.value}")
    row.worked_seconds = _worked(row, now)
    row.running_since = now if to is RunStatus.RUNNING else None
    row.status = to.value
    row.updated_at = now
    row.settled_by = None
    if to is not RunStatus.QUEUED:
        row.available_at = None
    if to is not RunStatus.RUNNING:
        row.lease_owner = row.lease_expires_at = row.cancel_requested_at = None
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


def _held_by(row: RunRow, worker_id: str | None) -> None:
    """A running run is written by the worker holding its lease (``worker_id``), or, when no
    worker holds it (it runs in its caller's process), by a caller that names none."""
    if worker_id is None and row.lease_owner is not None:
        raise Conflict(f"run {row.run_id} is leased: only its lease holder writes to it")
    if worker_id != row.lease_owner:
        raise LeaseLost(f"worker {worker_id} does not hold the lease on run {row.run_id}")


def _settled(row: RunRow, status: RunStatus, worker_id: str | None) -> bool:
    """Is the run already in ``status``, put there by this caller? Then a pause or finish
    to it is a repeat: the caller never saw the answer to the first one."""
    return row.status == status.value and row.settled_by == worker_id


def _resolution_id(resolution: InterruptResolution) -> str:
    """The one id an interrupt's answer is kept under: an interrupt is answered once."""
    return stable_id(resolution.run_id, resolution.interrupt_id, prefix="res_")


def _requeue(row: RunRow, now: datetime, *, wait: timedelta | None = None) -> None:
    """Back on the queue for a worker, as the next attempt; claimed only once ``wait`` (a
    retry's backoff) has passed."""
    _move(row, RunStatus.QUEUED, now)
    row.attempt += 1
    row.queued_at = now
    row.available_at = None if wait is None else now + wait


def _retried(row: RunRow, ending: RunFinish) -> bool:
    """Does this ending put the run back on the queue instead? It does for a durable run (one
    a worker took from the queue) that failed with a retryable error, ``MAX_ERROR_RETRIES``
    times at most. A run kept in its caller's process is never retried here."""
    error = ending.error
    return (
        ending.status is RunStatus.ERROR
        and error is not None
        and error.retryable
        and row.status == RunStatus.RUNNING
        and row.queued_at is not None
        and row.error_retries < MAX_ERROR_RETRIES
        and row.cancel_requested_at is None
    )


def _cancelled_instead(row: RunRow, now: datetime) -> bool:
    """A running run whose cancel was asked for ends ``CANCELLED`` where it would otherwise
    pause or go back on the queue: a worker that lets go of it lets go for good."""
    if row.cancel_requested_at is None:
        return False
    _move(row, RunStatus.CANCELLED, now)
    return True


def _counted(error: AgentError | None, retries: int) -> AgentError | None:
    """The error a run ends with, saying how often agent-runs retried the run before it."""
    if error is None or not retries:
        return error
    retried = f"(after {retries} of {MAX_ERROR_RETRIES} retries)"
    return error.model_copy(update={"message": f"{error.message} {retried}"})


class RunStore:
    """Every run operation, in one place. The caller commits. ``max_run_seconds`` is the
    service's maximum working time (``RUNS__RUNS__MAX_RUN_SECONDS``): a run's own
    ``timeout_seconds`` may only be shorter. ``concurrency_per_key`` and
    ``max_running_per_tenant`` bound what a claim may take (``RUNS__RUNS__*``)."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        max_run_seconds: float | None = None,
        concurrency_per_key: int = CONCURRENCY_PER_KEY,
        max_running_per_tenant: int | None = None,
    ) -> None:
        self._session = session
        self._max_run_seconds = max_run_seconds
        self._per_key = concurrency_per_key
        self._per_tenant = max_running_per_tenant

    def _limit(self, row: RunRow) -> float | None:
        """The most working time the run may take: its own limit or the service's, the
        lesser; ``None`` when neither is set."""
        limits = [s for s in (row.timeout_seconds, self._max_run_seconds) if s is not None]
        return min(limits, default=None)

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
                timeout_seconds=record.timeout_seconds,
                running_since=None if queue else now,
                idempotency_key=record.idempotency_key,
                agent_version=record.agent_version,
                priority=record.priority,
                concurrency_key=record.concurrency_key,
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
            return _record(row, now), True
        if start.idempotency_key:
            keyed = await self._one(
                RunRow.tenant_id == start.tenant_id,
                RunRow.idempotency_key == start.idempotency_key,
            )
            if keyed is not None:
                found = _record(keyed, now)
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
        return _record(existing, now), False

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
        """The run waits on ``pause.interrupt``, whose ``expects`` must be a JSON Schema
        (``Unprocessable`` otherwise: a question nobody could answer fails where it is
        asked); a run whose cancel was asked for ends ``CANCELLED`` instead. Returns ``(run,
        paused)``: a repeat of the pause that made the current state (the same caller, the
        same interrupt) answers the stored run with ``paused`` false, changing nothing."""
        interrupt = pause.interrupt
        if (interrupt.tenant_id, interrupt.run_id) != (tenant_id, run_id):
            raise Unprocessable("the interrupt belongs to another run")
        if problem := schema_problem(interrupt.expects):
            raise Unprocessable(problem)
        checkpoint = pause.bounded_checkpoint()
        row = await self._locked(tenant_id, run_id, worker_id=None)
        waiting_on = (row.awaiting or {}).get("interrupt_id")
        if _settled(row, RunStatus.PAUSED, worker_id) and waiting_on == interrupt.interrupt_id:
            return _record(row, now), False
        _fence(row, worker_id)
        if _cancelled_instead(row, now):
            row.settled_by = worker_id
            await self._ended([row], now)
            return await self._flushed(row, now), True
        _move(row, RunStatus.PAUSED, now)
        row.checkpoint = checkpoint
        row.awaiting = interrupt.awaiting()
        row.assignee = interrupt.assignee
        row.awaiting_deadline = interrupt.deadline
        row.settled_by = worker_id
        return await self._flushed(row, now), True

    async def resume(
        self,
        tenant_id: str,
        run_id: str,
        resolution: InterruptResolution,
        *,
        answerer: KeyInfo,
        now: datetime,
    ) -> tuple[RunRecord, bool]:
        """Answer the interrupt a paused run waits on, as ``answerer`` may
        (``answering.py``: checked against the run's assignee now) and with an answer that
        fits the question (``trellis.runs.answers``: an ``ANSWER`` fits ``expects`` or is one
        of ``options``, an ``EDIT`` of a question fits ``expects``; ``Unprocessable``
        otherwise), both before anything is written. ``CANCEL`` ends the run; any other
        decision continues it as the next attempt: back on the queue when the run is durable
        (it was ever queued, so a worker resumes it), else ``RUNNING`` in the caller's
        process.

        Returns ``(run, resumed)``: a repeat of the resume that answered the interrupt (the
        very same resolution, as a client retries it after losing the answer) answers the
        run as it is now with ``resumed`` false, changing nothing. Any other answer to an
        interrupt the run no longer waits on is a ``Conflict``: a second click must not
        continue the run twice."""
        if resolution.run_id != run_id:
            raise Unprocessable("the resolution answers another run")
        row = await self._locked(tenant_id, run_id, worker_id=None)
        if (row.awaiting or {}).get("interrupt_id") != resolution.interrupt_id:
            if await self._answered_by(tenant_id, resolution):
                return _record(row, now), False
            if row.status != RunStatus.PAUSED:
                raise Conflict(f"run {run_id} is {row.status}, not waiting on an answer")
            raise Conflict(f"run {run_id} is waiting on another interrupt")
        asked = Interrupt.model_validate(row.awaiting)
        require_may_answer(answerer, asked.assignee, resolution.reviewer)
        if problem := answer_problem(asked, resolution):
            raise Unprocessable(problem)
        row.last_resolution = _json(resolution)
        self._session.add(
            ResolutionRow(
                resolution_id=_resolution_id(resolution),
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
        return await self._flushed(row, now), True

    async def _answered_by(self, tenant_id: str, resolution: InterruptResolution) -> bool:
        """Did this very resolution answer its interrupt? Its ``resolved_at`` is set once,
        when the answer is made, so a retried resume sends the same JSON and a second
        answer (another click, another person) never does."""
        kept = await self._session.scalar(
            select(ResolutionRow.resolution).where(
                ResolutionRow.tenant_id == tenant_id,
                ResolutionRow.resolution_id == _resolution_id(resolution),
            )
        )
        return kept == _json(resolution)

    async def cancel(
        self,
        tenant_id: str,
        run_id: str,
        cancel: RunCancel,
        *,
        canceller: KeyInfo,
        now: datetime,
    ) -> tuple[RunRecord, bool]:
        """Cancel the run, whatever its status, as ``canceller`` may: a key that may answer
        it (``answering.py``, against its assignee now), checked before anything is
        written. Why and who asked are kept with the run.

        A queued or waiting run, and a running one no worker holds (kept in its caller's
        process), ends ``CANCELLED`` at once. A run a worker holds is asked to stop
        (``cancel_requested_at``): from then on its heartbeats say ``cancel_requested`` and
        no longer extend the lease, the worker finishes it ``CANCELLED``, and if it has not
        when the lease runs out, the ticker cancels it (``requeue_lapsed``).

        Returns ``(run, changed)``: a cancel already asked for, or the run already cancelled
        by the same principal for the same reason (a retried request), answers the run as it
        is with ``changed`` false. An ended run is otherwise a ``Conflict``."""
        row = await self._locked(tenant_id, run_id, worker_id=None)
        require_may_cancel(canceller, row.assignee)
        asked = (row.cancelled_by, row.cancel_reason) == (canceller.principal, cancel.reason)
        if row.cancel_requested_at is not None or (row.status == RunStatus.CANCELLED and asked):
            return _record(row, now), False
        if row.status == RunStatus.RUNNING and row.lease_owner is not None:
            row.cancel_requested_at = row.updated_at = now
        else:
            _move(row, RunStatus.CANCELLED, now)
            await self._ended([row], now)
        row.cancel_reason, row.cancelled_by = cancel.reason, canceller.principal
        return await self._flushed(row, now), True

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
        nothing, so a worker that never saw the answer may retry.

        A durable run its worker ends ``ERROR`` with a retryable error goes back on the queue
        instead (``_retried``), as the next attempt after a jittered backoff; its
        ``MAX_ERROR_RETRIES``-th such error stands, saying how often the run was retried. A
        worker's repeat of that finish answers the requeued run."""
        row = await self._locked(tenant_id, run_id, worker_id=None)
        requeued = (
            ending.status is RunStatus.ERROR
            and worker_id is not None
            and _settled(row, RunStatus.QUEUED, worker_id)
        )
        if _settled(row, ending.status, worker_id) or requeued:
            return _record(row, now), False
        _fence(row, worker_id)
        if _retried(row, ending):
            row.error_retries += 1
            wait = backoff(ERROR_RETRY_BASE, row.error_retries, cap=ERROR_RETRY_CAP)
            _requeue(row, now, wait=jittered(wait))
            row.settled_by = worker_id
            return await self._flushed(row, now), True
        _move(row, ending.status, now)
        row.output = ending.output
        row.error = _json(_counted(ending.error, row.error_retries))
        row.settled_by = worker_id
        await self._ended([row], now)
        return await self._flushed(row, now), True

    # ------------------------------------------------------------------ the queue
    async def claim(
        self, tenant_id: str | None, request: ClaimRequest, *, now: datetime
    ) -> Claimed | None:
        """The next queued run of the worker's agents, leased to it; ``None`` when there is
        none it may take. ``tenant_id`` is the tenant's queue; ``None`` (a platform key that
        names no tenant) is every tenant's.

        The next run is available (no retry's backoff still holding it back) and has room:
        fewer than ``concurrency_per_key`` RUNNING runs share its ``concurrency_key``, and
        its tenant's workers hold fewer than ``max_running_per_tenant`` runs. Among those,
        the tenant whose workers hold the fewest runs of these agents comes first (the fair
        share: a tenant takes more of a shared fleet only while no tenant with fewer waits),
        then the highest ``priority``, then the oldest.

        ``SKIP LOCKED`` lets many workers claim at once without two of them ever getting one
        run: a row another claim holds is passed over, not waited on. The room is counted
        again under a transaction lock on the key (and on the tenant, when capped), so two
        claims at once cannot both take the last place; a key or tenant another claim is
        counting right now is passed over the same way, for this claim."""
        passed: list[Any] = []
        while (
            row := await self._session.scalar(self._next_queued(tenant_id, request, now, passed))
        ) is not None:
            if not await self._tenant_has_room(row):
                passed.append(RunRow.tenant_id != row.tenant_id)
            elif not await self._key_has_room(row):
                passed.append(
                    or_(
                        RunRow.tenant_id != row.tenant_id,
                        RunRow.concurrency_key.is_distinct_from(row.concurrency_key),
                    )
                )
            else:
                _move(row, RunStatus.RUNNING, now)
                lease = self._lease(row, request.worker_id, request.lease_seconds, now)
                return Claimed(run=await self._flushed(row, now), lease=lease)
        return None

    def _next_queued(
        self, tenant_id: str | None, request: ClaimRequest, now: datetime, passed: list[Any]
    ) -> Select[tuple[RunRow]]:
        """The query for the next run a claim may take, in claim order, locked."""
        held = (
            select(RunRow.tenant_id, func.count().label("runs"))
            .where(*_LEASED, RunRow.agent_id.in_(request.agent_ids))
            .group_by(RunRow.tenant_id)
            .subquery()
        )
        sharing = aliased(RunRow)
        running_with_key = (
            select(func.count())
            .where(
                sharing.status == RunStatus.RUNNING.value,
                sharing.tenant_id == RunRow.tenant_id,
                sharing.concurrency_key == RunRow.concurrency_key,
            )
            .scalar_subquery()
        )
        query = (
            select(RunRow)
            .outerjoin(held, held.c.tenant_id == RunRow.tenant_id)
            .where(
                RunRow.status == RunStatus.QUEUED.value,
                RunRow.agent_id.in_(request.agent_ids),
                or_(RunRow.available_at.is_(None), RunRow.available_at <= now),
                or_(RunRow.concurrency_key.is_(None), running_with_key < self._per_key),
                *passed,
            )
            .order_by(func.coalesce(held.c.runs, 0), RunRow.priority.desc(), RunRow.queued_at)
            .limit(1)
            .with_for_update(of=RunRow, skip_locked=True)
        )
        return query if tenant_id is None else query.where(RunRow.tenant_id == tenant_id)

    async def _tenant_has_room(self, row: RunRow) -> bool:
        """Do the run's tenant's workers hold fewer runs than the cap (always, uncapped)?"""
        if self._per_tenant is None:
            return True
        if not await self._locked_for_claim("tenant", row.tenant_id):
            return False
        held = await self._session.scalar(
            select(func.count()).where(*_LEASED, RunRow.tenant_id == row.tenant_id)
        )
        return (held or 0) < self._per_tenant

    async def _key_has_room(self, row: RunRow) -> bool:
        """Do fewer runs than allowed share the run's concurrency key (always, with none)?"""
        if row.concurrency_key is None:
            return True
        if not await self._locked_for_claim("key", row.tenant_id, row.concurrency_key):
            return False
        running = await self._session.scalar(
            select(func.count()).where(
                RunRow.status == RunStatus.RUNNING.value,
                RunRow.tenant_id == row.tenant_id,
                RunRow.concurrency_key == row.concurrency_key,
            )
        )
        return (running or 0) < self._per_key

    async def _locked_for_claim(self, *names: str) -> bool:
        """Take the transaction lock claims count a key's or a tenant's room under, unless
        another claim holds it (then pass over, as ``SKIP LOCKED`` passes over a row). The
        count after it sees every claim committed before it."""
        lock = func.pg_try_advisory_xact_lock(func.hashtextextended("\x1f".join(names), 0))
        return bool(await self._session.scalar(select(lock)))

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

    async def release(
        self, tenant_id: str, run_id: str, request: ReleaseRequest, *, now: datetime
    ) -> tuple[RunRecord, bool]:
        """The worker holding the run lets go of it (it is stopping): back on the queue at
        once as the next attempt, for another worker, keeping ``request.checkpoint`` as its
        progress when sent. No lapse is counted: nothing crashed. A run whose cancel was
        asked for ends ``CANCELLED`` instead. Returns ``(run, released)``: the worker's repeat
        answers the run as it is with ``released`` false; any other worker is ``LeaseLost``."""
        checkpoint = bounded_checkpoint(request.checkpoint)
        row = await self._locked(tenant_id, run_id, worker_id=None)
        if _settled(row, RunStatus.QUEUED, request.worker_id):
            return _record(row, now), False
        _fence(row, request.worker_id)
        if _cancelled_instead(row, now):
            await self._ended([row], now)
        else:
            _requeue(row, now)
            if checkpoint is not None:
                row.checkpoint = checkpoint
        row.settled_by = request.worker_id
        return await self._flushed(row, now), True

    def _lease(self, row: RunRow, worker_id: str, seconds: int, now: datetime) -> Lease:
        """Lease the run to ``worker_id`` for ``seconds``, telling it the working time left,
        and whether it was asked to cancel the run: then the lease runs from the request, not
        from now, so the run is cancelled within one lease even by a worker that ignores
        it."""
        cancelling = row.cancel_requested_at
        expires_at = (cancelling or now) + timedelta(seconds=seconds)
        row.lease_owner, row.lease_expires_at = worker_id, expires_at
        limit = self._limit(row)
        remaining = None if limit is None else max(0.0, limit - _worked(row, now))
        return Lease(
            run_id=row.run_id,
            worker_id=worker_id,
            expires_at=expires_at,
            remaining_seconds=remaining,
            cancel_requested=cancelling is not None,
        )

    # ------------------------------------------------------------------ the ticker's sweeps
    async def time_out_past_deadline(self, *, now: datetime, limit: int) -> list[RunRecord]:
        """Runs not yet ended past their own ``deadline`` (``RunStart.deadline``) end as
        ``TIMEOUT``, queued, running or paused alike: a deadline is when the run must be
        done by, so time waiting for a worker or a person counts. A worker still running
        one has lost it: its next heartbeat or write is ``LeaseLost``. Returns every run
        ended."""
        rows = await self._sweep(
            RunRow.status.in_(_UNENDED),
            RunRow.deadline < now,
            order=RunRow.deadline,
            limit=limit,
        )
        for row in rows:
            _move(row, RunStatus.TIMEOUT, now)
            row.error = _json(
                AgentError(
                    code="run_deadline",
                    category=ErrorCategory.TIMEOUT,
                    message=f"the run did not end by its deadline, {row.deadline}",
                    # the category alone would say retryable; a retry would only be later
                    retryable=False,
                    source="agent-runs",
                )
            )
        await self._ended(rows, now)
        await self._session.flush()
        return [_record(row, now) for row in rows]

    async def time_out_overworked(self, *, now: datetime, limit: int) -> list[RunRecord]:
        """Running runs whose working time passed their limit (``_limit``: their own
        ``timeout_seconds`` or the service's maximum, the lesser) end as ``TIMEOUT``. Only
        time RUNNING counts, across attempts: not time queued or waiting for a person. A
        worker still running one has lost it: its next heartbeat or write is ``LeaseLost``.
        Returns every run ended."""
        allowed: Any = RunRow.timeout_seconds
        if self._max_run_seconds is not None:
            # LEAST ignores a NULL: a run with no limit of its own gets the service's
            allowed = func.least(RunRow.timeout_seconds, self._max_run_seconds)
        working = RunRow.worked_seconds + func.extract("epoch", now - RunRow.running_since)
        rows = await self._sweep(
            RunRow.status == RunStatus.RUNNING.value,
            working > allowed,
            order=RunRow.running_since,
            limit=limit,
        )
        for row in rows:
            _move(row, RunStatus.TIMEOUT, now)
            row.error = _json(
                AgentError(
                    code="run_timeout",
                    category=ErrorCategory.TIMEOUT,
                    message=f"the run worked {row.worked_seconds:.0f} s, past its limit of "
                    f"{self._limit(row):g} s",
                    # the category alone would say retryable; a retry would only run as long
                    retryable=False,
                    source="agent-runs",
                )
            )
        await self._ended(rows, now)
        await self._session.flush()
        return [_record(row, now) for row in rows]

    async def requeue_lapsed(self, *, now: datetime, limit: int) -> list[RunRecord]:
        """Runs whose worker stopped heartbeating go back on the queue as the next attempt,
        after a short backoff growing with each lapse; one whose lease has now lapsed
        ``MAX_LEASE_LAPSES`` times ends as ``ERROR`` instead, and one whose cancel was asked
        for ends ``CANCELLED``. Only lapses count: a person's answer starts an attempt too,
        and is no crash. Returns every run moved."""
        rows = await self._sweep(
            RunRow.status == RunStatus.RUNNING.value,
            RunRow.lease_expires_at < now,
            order=RunRow.lease_expires_at,
            limit=limit,
        )
        for row in rows:
            if _cancelled_instead(row, now):
                continue
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
                wait = backoff(LAPSE_RETRY_BASE, row.lease_lapses, cap=LAPSE_RETRY_CAP)
                _requeue(row, now, wait=jittered(wait))
        await self._ended(rows, now)
        await self._session.flush()
        return [_record(row, now) for row in rows]

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
        return [_record(row, now) for row in rows]

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
            _held_by(row, worker_id)
        elif row.status == RunStatus.PAUSED:
            if role != PAUSED_ARTIFACT_ROLE:
                raise Forbidden(f"only a {PAUSED_ARTIFACT_ROLE} key adds to a paused run")
        elif worker_id is not None:
            raise LeaseLost(f"run {run_id} is {row.status}; worker {worker_id} holds no lease")
        else:
            raise Conflict(f"run {run_id} is {row.status}; artifacts are added while it runs")

    # ------------------------------------------------------------------ events
    async def append_events(
        self, tenant_id: str, run_id: str, append: EventsAppend, *, worker_id: str | None
    ) -> EventsAppended:
        """Add events to the run's log, each at the next position. Only while the run is
        ``RUNNING``, by the worker holding its lease or, when none does, by its caller (as a
        heartbeat is fenced); never otherwise (409). Every event names this run and tenant
        (``Unprocessable`` otherwise); one already logged (its attempt and sequence) is
        skipped, so a retried append adds nothing. The run's row lock orders the positions:
        two appends to one run never interleave."""
        if any((e.tenant_id, e.run_id) != (tenant_id, run_id) for e in append.events):
            raise Unprocessable("an event names another run")
        row = await self._locked(tenant_id, run_id, worker_id=None)
        if row.status != RunStatus.RUNNING:
            if worker_id is not None:
                raise LeaseLost(f"run {run_id} is {row.status}; worker {worker_id} holds no lease")
            raise Conflict(f"run {run_id} is {row.status}; events are appended while it runs")
        _held_by(row, worker_id)
        asked = [(event.attempt, event.sequence) for event in append.events]
        logged = select(RunEventRow.attempt, RunEventRow.sequence).where(
            RunEventRow.run_id == run_id,
            tuple_(RunEventRow.attempt, RunEventRow.sequence).in_(asked),
        )
        seen = {(attempt, sequence) for attempt, sequence in await self._session.execute(logged)}
        last = await self._session.scalar(
            select(func.coalesce(func.max(RunEventRow.position), 0)).where(
                RunEventRow.run_id == run_id
            )
        )
        position = int(last or 0)
        added = 0
        for event in append.events:
            if (event.attempt, event.sequence) in seen:
                continue
            seen.add((event.attempt, event.sequence))
            position += 1
            added += 1
            self._session.add(
                RunEventRow(
                    run_id=run_id,
                    position=position,
                    tenant_id=tenant_id,
                    attempt=event.attempt,
                    sequence=event.sequence,
                    event=event.model_dump(mode="json"),
                )
            )
        await self._session.flush()
        return EventsAppended(appended=added, position=position)

    async def events(
        self, tenant_id: str, run_id: str, *, after: int, limit: int
    ) -> tuple[list[RunEventEntry], RunStatus]:
        """The run's events past position ``after``, oldest first, at most ``limit``, and the
        run's status read before them: when it has ended, no event comes after these."""
        status = RunStatus((await self._found(tenant_id, run_id)).status)
        rows = await self._session.scalars(
            select(RunEventRow)
            .where(RunEventRow.run_id == run_id, RunEventRow.position > after)
            .order_by(RunEventRow.position)
            .limit(limit)
        )
        entries = [
            RunEventEntry(position=r.position, event=RunEvent.model_validate(r.event)) for r in rows
        ]
        return entries, status

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
        await self._found(tenant_id, run_id)  # 404 for another tenant's run, as everywhere
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

    async def get(self, tenant_id: str, run_id: str, *, now: datetime) -> RunRecord:
        """The run as it is at ``now`` (its working time counts the stretch going on)."""
        return _record(await self._found(tenant_id, run_id), now)

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

    async def _found(self, tenant_id: str, run_id: str) -> RunRow:
        row = await self._one(RunRow.tenant_id == tenant_id, RunRow.run_id == run_id)
        if row is None:
            raise NotFound(f"no run {run_id}")
        return row

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

    async def _flushed(self, row: RunRow, now: datetime) -> RunRecord:
        await self._session.flush()
        return _record(row, now)
