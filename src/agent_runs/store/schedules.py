"""Reading and writing schedules.

A schedule's identity is ``(tenant_id, agent_id, on_behalf_of, cadence, input_sha256)``:
creating one that exists returns it (the upsert every redeploy of a harness relies on), and
the unique index behind it makes that hold under concurrency. ``name`` is only a label.

One rule runs through all of it: ``next_fire_at`` only ever moves *forward past now*. It is
the clock of an unattended loop, so a value in the past means "due", and a value further in
the past means "due for every tick in between", each a different idempotency key and so a
real run. No caller-supplied instant becomes that clock unfiltered.

Firing locks the row for the rest of the transaction (``claim`` / ``claim_due``), and the
success or failure bookkeeping happens under that lock: two ticker replicas on one tick is
the normal case, and the idempotency key alone dedupes only the run, not these counters.

A schedule with an ``end_at`` is armed only for an occurrence strictly before it (the jittered
instant, the one that fires): ``next_fire_at`` is ``None`` once the next one would be at or
past it, and once the end itself has passed, so an ended schedule is never due again.

How schedules fire is the deployment's (``SchedulesSettings``, ``RUNS__SCHEDULES__*``): the
cadence floor, the jitter, the failed fires before an auto-pause and their backoff, and the
two count limits, on by default and off at 0.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

from pydantic import ValidationError
from sqlalchemy import Select, delete, func, or_, select, text, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from trellis.contracts.errors import AgentError
from trellis.contracts.ids import new_id
from trellis.contracts.runs import Schedule, ScheduleSpec

from agent_runs.config.constants import DEFAULT_PAGE
from agent_runs.config.settings import SchedulesSettings
from agent_runs.domain.cadence import jitter, next_fire_at, validate_cadence
from agent_runs.domain.errors import NotFound, Unprocessable, field_errors
from agent_runs.domain.schedules import (
    DuplicateSchedule,
    SchedulePolicy,
    ScheduleUpdate,
    TooManySchedules,
    input_sha256,
)
from agent_runs.retry import backoff
from agent_runs.store.paging import Page, page_of
from agent_runs.store.tables import ScheduleFireCountRow, ScheduleRow

_UNKEPT = frozenset({"metadata"})
_SPEC_FIELDS = tuple(name for name in ScheduleSpec.model_fields if name not in _UNKEPT)
_RECORD_FIELDS = tuple(name for name in Schedule.model_fields if name not in _UNKEPT)


def _schedule(row: ScheduleRow) -> Schedule:
    fields: dict[str, Any] = {name: getattr(row, name) for name in _RECORD_FIELDS}
    return Schedule.read({**fields, "metadata": row.schedule_metadata or {}})


def _checked(fields: dict[str, Any], policy: SchedulesSettings) -> ScheduleSpec:
    """A spec as the contracts validate it, with a cadence this service will run."""
    try:
        spec = ScheduleSpec.model_validate(fields)
    except ValidationError as exc:
        raise Unprocessable(
            "the schedule is invalid", details={"errors": field_errors(exc.errors())}
        ) from exc
    cadence = validate_cadence(spec.cadence, floor=policy.min_interval)
    return spec.model_copy(update={"cadence": cadence})


_IDENTITY = ("tenant_id", "agent_id", "on_behalf_of", "cadence", "input_sha256")


def _apply(row: ScheduleRow, spec: ScheduleSpec) -> None:
    for name in _SPEC_FIELDS:
        setattr(row, name, getattr(spec, name))
    row.schedule_metadata = spec.metadata or None
    row.input_sha256 = input_sha256(spec.input)


def _before_end(row: ScheduleRow, fire: datetime | None) -> datetime | None:
    """``fire``, unless the schedule ends at or before it."""
    if fire is None or (row.end_at is not None and fire >= row.end_at):
        return None
    return fire


def _arm(row: ScheduleRow, *, after: datetime, policy: SchedulesSettings) -> None:
    tick = next_fire_at(row.cadence, after=after, timezone=row.timezone)
    fire = None if tick is None else tick + jitter(row.schedule_id, spread=policy.jitter)
    row.next_fire_at = _before_end(row, fire)


def _rearm(row: ScheduleRow, *, at: datetime, policy: SchedulesSettings) -> None:
    """Arm the next fire, keeping an occurrence that is merely late rather than superseded:
    the fire a short outage swallowed is the one a person clicking resume came for, while
    one a later occurrence has already replaced is gone. Once the schedule's end has passed
    nothing is kept or armed: a late occurrence would fire after the end."""
    if row.end_at is not None and at >= row.end_at:
        row.next_fire_at = None
        return
    pending = row.next_fire_at
    if pending is not None:
        following = next_fire_at(row.cadence, after=pending, timezone=row.timezone)
        if pending > at or (following is not None and following > at):
            row.next_fire_at = _before_end(row, pending)
            return
    _arm(row, after=at, policy=policy)


class ScheduleStore:
    """Every schedule operation, in one place, under the deployment's ``policy`` (unset: the
    defaults). The caller commits."""

    def __init__(self, session: AsyncSession, policy: SchedulesSettings | None = None) -> None:
        self._session = session
        self._policy = policy or SchedulesSettings()

    async def upsert(
        self, spec: ScheduleSpec, *, created_by: str, now: datetime
    ) -> tuple[Schedule, bool]:
        """The schedule with this spec's identity: created (armed for its first occurrence
        after ``now``; ``created_by`` is the credential's principal) or, when it exists,
        returned as it is. Returns ``(schedule, created)``. ``ON CONFLICT DO NOTHING`` on the
        identity index makes two concurrent creates one schedule. With
        ``RUNS__SCHEDULES__MAX_PER_TENANT`` on (50 unless set; 0 is off), a create of a new
        schedule past it is ``TooManySchedules``; one with an existing identity still returns
        that one."""
        spec = _checked(spec.model_dump(), self._policy)
        row = ScheduleRow(
            schedule_id=new_id("sch_"),
            created_by=created_by,
            consecutive_failures=0,
            created_at=now,
            updated_at=now,
        )
        _apply(row, spec)
        _arm(row, after=now, policy=self._policy)
        if (limit := self._policy.per_tenant_limit) is not None:
            existing = await self._counted(row, limit)
            if existing is not None:
                return _schedule(existing), False
        values = {column.key: getattr(row, column.key) for column in ScheduleRow.__table__.columns}
        inserted = await self._session.scalar(
            insert(ScheduleRow)
            .values(**{k: v for k, v in values.items() if v is not None})
            .on_conflict_do_nothing(index_elements=list(_IDENTITY))
            .returning(ScheduleRow)
        )
        if inserted is not None:
            return _schedule(inserted), True
        existing = await self._session.scalar(
            select(ScheduleRow).where(
                *(getattr(ScheduleRow, name) == getattr(row, name) for name in _IDENTITY)
            )
        )
        if existing is None:  # deleted between the insert and the read: vanishingly rare
            raise DuplicateSchedule(row.schedule_id)
        return _schedule(existing), False

    async def _counted(self, row: ScheduleRow, limit: int) -> ScheduleRow | None:
        """Before a create under a per-tenant limit: ``None`` while the tenant has room (the
        create goes on as without a limit); at the limit, the schedule with ``row``'s identity
        when there is one (a repeated create still answers it), else ``TooManySchedules``. A
        lock on the tenant's count, held to the end of the transaction, makes concurrent
        creates of one tenant count one after another."""
        await self._session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"agent_schedules:{row.tenant_id}"},
        )
        held = await self._count(row.tenant_id)
        if held < limit:
            return None
        existing = (
            await self._session.execute(
                select(ScheduleRow).where(
                    *(getattr(ScheduleRow, name) == getattr(row, name) for name in _IDENTITY)
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            raise TooManySchedules(limit, held)
        return existing

    async def _count(self, tenant_id: str) -> int:
        """The schedules the tenant has."""
        return (
            await self._session.execute(
                select(func.count())
                .select_from(ScheduleRow)
                .where(ScheduleRow.tenant_id == tenant_id)
            )
        ).scalar_one()

    async def fires_on(self, tenant_id: str, day: date) -> int:
        """The fires counted for the tenant on the UTC ``day`` (0 when none were)."""
        fires = await self._session.scalar(
            select(ScheduleFireCountRow.fires).where(
                ScheduleFireCountRow.tenant_id == tenant_id, ScheduleFireCountRow.day == day
            )
        )
        return fires or 0

    async def policy_of(self, tenant_id: str, *, now: datetime) -> SchedulePolicy:
        """The deployment's policy and where the tenant stands: its schedules, and its fires
        of ``now``'s UTC day while the daily limit is on (nothing is counted while it is
        off, so then ``None``)."""
        policy = self._policy
        day = now.astimezone(UTC).date()
        counted = policy.fires_per_day_limit is not None
        return SchedulePolicy(
            min_interval_seconds=policy.min_interval_seconds,
            jitter_seconds=policy.jitter_seconds,
            max_failed_fires=policy.max_failed_fires,
            max_per_tenant=policy.per_tenant_limit,
            max_fires_per_day=policy.fires_per_day_limit,
            schedule_count=await self._count(tenant_id),
            fires_today=await self.fires_on(tenant_id, day) if counted else None,
            day=day,
        )

    async def get(self, tenant_id: str, schedule_id: str) -> Schedule:
        return _schedule(await self._row(tenant_id, schedule_id))

    async def list(
        self,
        tenant_id: str,
        *,
        enabled: bool | None = None,
        agent_id: str | None = None,
        on_behalf_of: str | None = None,
        limit: int = DEFAULT_PAGE,
        after: Mapping[str, Any] | None = None,
    ) -> Page[Schedule]:
        """This tenant's schedules, newest first, a page at a time (keyset ``created_at,
        schedule_id``, both descending)."""
        order = (ScheduleRow.created_at, ScheduleRow.schedule_id)
        query: Select[tuple[ScheduleRow]] = select(ScheduleRow).where(
            ScheduleRow.tenant_id == tenant_id
        )
        if after is not None:
            query = query.where(tuple_(*order) < (after["created_at"], after["schedule_id"]))
        if enabled is not None:
            query = query.where(ScheduleRow.enabled.is_(enabled))
        if agent_id is not None:
            query = query.where(ScheduleRow.agent_id == agent_id)
        if on_behalf_of is not None:
            query = query.where(ScheduleRow.on_behalf_of == on_behalf_of)
        newest = query.order_by(*(column.desc() for column in order)).limit(limit + 1)
        rows = (await self._session.scalars(newest)).all()
        return page_of(
            rows,
            limit=limit,
            item=_schedule,
            position=lambda row: {"created_at": row.created_at, "schedule_id": row.schedule_id},
        )

    async def update(
        self, tenant_id: str, schedule_id: str, change: ScheduleUpdate, *, now: datetime
    ) -> Schedule:
        """Apply the fields sent. The next fire is recomputed when the cadence or zone
        actually *changed* (a client sending the whole representation back must not throw
        tonight's run away). ``enabled: false`` pauses; ``enabled: true`` on a paused schedule
        resumes it: it clears the failure count, last error and backoff of an auto-pause and
        fires from the next occurrence it can still honour, never the backlog, and nothing
        once its end has passed. A changed ``end_at`` drops an armed fire at or past it, and
        re-arms one an earlier end had dropped. A change that would give the schedule another
        schedule's identity is ``DuplicateSchedule``."""
        row = await self._row(tenant_id, schedule_id)
        changes = change.model_dump(exclude_unset=True)
        if "metadata" in changes:
            changes["metadata"] = {**(row.schedule_metadata or {}), **(changes["metadata"] or {})}
        current = {name: getattr(row, name) for name in _SPEC_FIELDS}
        spec = _checked(
            {**current, "metadata": row.schedule_metadata or {}, **changes}, self._policy
        )
        retimed = (spec.cadence, spec.timezone) != (row.cadence, row.timezone)
        re_enabled = spec.enabled and not row.enabled
        re_ended = spec.end_at != row.end_at
        _apply(row, spec)
        if retimed:
            _arm(row, after=now, policy=self._policy)
            if row.end_at is not None and now >= row.end_at:
                row.next_fire_at = None
        elif re_enabled or re_ended:
            _rearm(row, at=now, policy=self._policy)
        if re_enabled:
            row.consecutive_failures = 0
            row.last_error = row.retry_after = None
        row.updated_at = now
        try:
            async with self._session.begin_nested():
                await self._session.flush()
        except IntegrityError as exc:
            raise DuplicateSchedule(schedule_id) from exc
        return _schedule(row)

    async def delete(self, tenant_id: str, schedule_id: str) -> None:
        await self._session.delete(await self._row(tenant_id, schedule_id))
        await self._session.flush()

    # ------------------------------------------------------------------ firing
    async def claim(self, tenant_id: str, schedule_id: str) -> Schedule:
        """Lock the schedule for the rest of this transaction."""
        return _schedule(await self._row(tenant_id, schedule_id, lock=True))

    async def claim_due(self, *, now: datetime) -> Schedule | None:
        """The most overdue enabled schedule of any tenant, locked, skipping any another
        ticker holds. A schedule backing off after a retryable failure is not due until
        ``retry_after``, so three strikes are spaced by the policy, not by the tick."""
        row = await self._session.scalar(
            select(ScheduleRow)
            .where(
                ScheduleRow.enabled.is_(True),
                ScheduleRow.next_fire_at <= now,
                or_(ScheduleRow.retry_after.is_(None), ScheduleRow.retry_after <= now),
            )
            .order_by(ScheduleRow.next_fire_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        return _schedule(row) if row is not None else None

    async def record_success(
        self, schedule_id: str, *, fire_time: datetime, run_id: str, now: datetime
    ) -> Schedule:
        """A fire that queued its run: reset the failure count and arm the next occurrence
        after ``now``, never after a (late) ``fire_time``, or a week of downtime would replay
        as 168 runs."""
        row = await self._claimed(schedule_id)
        row.last_fired_at = fire_time
        row.last_run_id = run_id
        row.consecutive_failures = 0
        row.last_error = row.retry_after = None
        _arm(row, after=max(fire_time, now), policy=self._policy)
        row.updated_at = now
        return await self._flushed(row)

    async def record_skip(self, schedule_id: str, *, now: datetime) -> Schedule:
        """A tick skipped while the last run goes on: arm the next occurrence after ``now``."""
        row = await self._claimed(schedule_id)
        _arm(row, after=now, policy=self._policy)
        row.updated_at = now
        return await self._flushed(row)

    async def record_end(self, schedule_id: str, *, now: datetime) -> Schedule:
        """A tick owed from before the schedule's end, found once the end has passed: it is
        not fired, and nothing is armed again."""
        row = await self._claimed(schedule_id)
        row.next_fire_at = None
        row.updated_at = now
        return await self._flushed(row)

    async def record_refusal(
        self, schedule_id: str, *, error: AgentError, now: datetime
    ) -> Schedule:
        """A fire the tenant's daily fire limit refused: recorded as a failed fire is
        (``last_error``), and the tick skipped as an overlapping one is (armed for the next
        occurrence after ``now``). The failure count, which auto-pauses, is left alone: a
        limit the tenant reached is no reason to stop its schedules."""
        row = await self._claimed(schedule_id)
        row.last_error = error.model_dump(mode="json")
        row.retry_after = None
        _arm(row, after=now, policy=self._policy)
        row.updated_at = now
        return await self._flushed(row)

    async def take_fire(self, tenant_id: str, *, now: datetime, limit: int) -> bool:
        """Count one fire of the tenant's UTC day against ``limit``: ``True`` and counted
        while there is room, else ``False`` and nothing counted. The counter row's lock
        orders concurrent fires of one tenant; the tenant's earlier days are dropped."""
        day = now.astimezone(UTC).date()
        table = ScheduleFireCountRow.__table__
        taken = await self._session.scalar(
            insert(ScheduleFireCountRow)
            .values(tenant_id=tenant_id, day=day, fires=1)
            .on_conflict_do_update(
                index_elements=[table.c.tenant_id, table.c.day],
                set_={"fires": table.c.fires + 1},
                where=table.c.fires < limit,
            )
            .returning(table.c.fires)
        )
        await self._session.execute(
            delete(ScheduleFireCountRow).where(
                ScheduleFireCountRow.tenant_id == tenant_id, ScheduleFireCountRow.day < day
            )
        )
        return taken is not None

    async def record_failure(
        self, schedule_id: str, *, error: AgentError, now: datetime
    ) -> Schedule:
        """A fire that could not queue its run. A permanent failure pauses the schedule at
        once; a retryable one backs off (``RUNS__SCHEDULES__RETRY_BASE_SECONDS`` doubling to
        ``RETRY_CAP_SECONDS``) and pauses after ``RUNS__SCHEDULES__MAX_FAILED_FIRES``
        (``MAX_CONSECUTIVE_FAILURES`` unless set)."""
        policy = self._policy
        row = await self._claimed(schedule_id)
        row.consecutive_failures += 1
        row.last_error = error.model_dump(mode="json")
        if not error.retryable or row.consecutive_failures >= policy.max_failed_fires:
            row.enabled = False
            row.retry_after = None
        else:
            wait = backoff(policy.retry_base, row.consecutive_failures, cap=policy.retry_cap)
            row.retry_after = now + wait
        row.updated_at = now
        return await self._flushed(row)

    # ------------------------------------------------------------------ internals
    async def _row(self, tenant_id: str, schedule_id: str, *, lock: bool = False) -> ScheduleRow:
        query = select(ScheduleRow).where(
            ScheduleRow.tenant_id == tenant_id, ScheduleRow.schedule_id == schedule_id
        )
        row = await self._session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise NotFound(f"no schedule {schedule_id}")
        return row

    async def _claimed(self, schedule_id: str) -> ScheduleRow:
        """The row this transaction locked (from the session, no query)."""
        row = await self._session.get(ScheduleRow, schedule_id)
        if row is None:
            raise NotFound(f"no schedule {schedule_id}")
        return row

    async def _flushed(self, row: ScheduleRow) -> Schedule:
        await self._session.flush()
        return _schedule(row)
