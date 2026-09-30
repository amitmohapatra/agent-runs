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
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import ValidationError
from sqlalchemy import Select, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from trellis.contracts.errors import AgentError
from trellis.contracts.ids import new_id
from trellis.contracts.runs import Schedule, ScheduleSpec

from agent_runs.config.constants import (
    DEFAULT_PAGE,
    FIRE_RETRY_BASE,
    FIRE_RETRY_CAP,
    MAX_CONSECUTIVE_FAILURES,
)
from agent_runs.domain.cadence import next_fire_at, validate_cadence
from agent_runs.domain.errors import WEBHOOK_URL_REFUSED, NotFound, Unprocessable
from agent_runs.domain.schedules import DuplicateSchedule, ScheduleUpdate, input_sha256
from agent_runs.retry import backoff
from agent_runs.store.tables import ScheduleRow

#: ``webhook_url`` is refused: notifications are tenant subscriptions (``/v1/webhooks``)
_UNKEPT = frozenset({"metadata", "webhook_url"})
_SPEC_FIELDS = tuple(name for name in ScheduleSpec.model_fields if name not in _UNKEPT)
_RECORD_FIELDS = tuple(name for name in Schedule.model_fields if name not in _UNKEPT)


def _schedule(row: ScheduleRow) -> Schedule:
    fields: dict[str, Any] = {name: getattr(row, name) for name in _RECORD_FIELDS}
    return Schedule.model_validate({**fields, "metadata": row.schedule_metadata or {}})


def _checked(fields: dict[str, Any]) -> ScheduleSpec:
    """A spec as the contracts validate it, with a cadence this service will run."""
    try:
        spec = ScheduleSpec.model_validate(fields)
    except ValidationError as exc:
        raise Unprocessable(str(exc)) from exc
    if spec.webhook_url is not None:
        raise Unprocessable(WEBHOOK_URL_REFUSED)
    return spec.model_copy(update={"cadence": validate_cadence(spec.cadence)})


_IDENTITY = ("tenant_id", "agent_id", "on_behalf_of", "cadence", "input_sha256")


def _apply(row: ScheduleRow, spec: ScheduleSpec) -> None:
    for name in _SPEC_FIELDS:
        setattr(row, name, getattr(spec, name))
    row.schedule_metadata = spec.metadata or None
    row.input_sha256 = input_sha256(spec.input)


def _arm(row: ScheduleRow, *, after: datetime) -> None:
    row.next_fire_at = next_fire_at(row.cadence, after=after, timezone=row.timezone)


def _rearm(row: ScheduleRow, *, at: datetime) -> None:
    """Arm the next fire, keeping an occurrence that is merely late rather than superseded:
    the fire a short outage swallowed is the one a person clicking resume came for, while
    one a later occurrence has already replaced is gone."""
    pending = row.next_fire_at
    if pending is not None:
        following = next_fire_at(row.cadence, after=pending, timezone=row.timezone)
        if pending > at or (following is not None and following > at):
            return
    _arm(row, after=at)


class ScheduleStore:
    """Every schedule operation, in one place. The caller commits."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self, spec: ScheduleSpec, *, created_by: str, now: datetime
    ) -> tuple[Schedule, bool]:
        """The schedule with this spec's identity: created (armed for its first occurrence
        after ``now``; ``created_by`` is the credential's principal) or, when it exists,
        returned as it is. Returns ``(schedule, created)``. ``ON CONFLICT DO NOTHING`` on the
        identity index makes two concurrent creates one schedule."""
        spec = _checked(spec.model_dump())
        row = ScheduleRow(
            schedule_id=new_id("sch_"),
            created_by=created_by,
            consecutive_failures=0,
            created_at=now,
            updated_at=now,
        )
        _apply(row, spec)
        _arm(row, after=now)
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

    async def get(self, tenant_id: str, schedule_id: str) -> Schedule:
        return _schedule(await self._row(tenant_id, schedule_id))

    async def list(
        self,
        tenant_id: str,
        *,
        enabled: bool | None = None,
        agent_id: str | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> list[Schedule]:
        query: Select[tuple[ScheduleRow]] = select(ScheduleRow).where(
            ScheduleRow.tenant_id == tenant_id
        )
        if enabled is not None:
            query = query.where(ScheduleRow.enabled.is_(enabled))
        if agent_id is not None:
            query = query.where(ScheduleRow.agent_id == agent_id)
        rows = await self._session.scalars(
            query.order_by(ScheduleRow.created_at.desc()).limit(limit)
        )
        return [_schedule(row) for row in rows.all()]

    async def update(
        self, tenant_id: str, schedule_id: str, change: ScheduleUpdate, *, now: datetime
    ) -> Schedule:
        """Apply the fields sent. The next fire is recomputed when the cadence or zone
        actually *changed* (a client sending the whole representation back must not throw
        tonight's run away). ``enabled: false`` pauses; ``enabled: true`` on a paused schedule
        resumes it: it clears the failure count, last error and backoff of an auto-pause and
        fires from the next occurrence it can still honour, never the backlog. A change that
        would give the schedule another schedule's identity is ``DuplicateSchedule``."""
        row = await self._row(tenant_id, schedule_id)
        changes = change.model_dump(exclude_unset=True)
        if "metadata" in changes:
            changes["metadata"] = {**(row.schedule_metadata or {}), **(changes["metadata"] or {})}
        current = {name: getattr(row, name) for name in _SPEC_FIELDS}
        spec = _checked({**current, "metadata": row.schedule_metadata or {}, **changes})
        retimed = (spec.cadence, spec.timezone) != (row.cadence, row.timezone)
        re_enabled = spec.enabled and not row.enabled
        _apply(row, spec)
        if retimed:
            _arm(row, after=now)
        elif re_enabled:
            _rearm(row, at=now)
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
        _arm(row, after=max(fire_time, now))
        row.updated_at = now
        return await self._flushed(row)

    async def record_failure(
        self, schedule_id: str, *, error: AgentError, now: datetime
    ) -> Schedule:
        """A fire that could not queue its run. A permanent failure pauses the schedule at
        once; a retryable one backs off and pauses after ``MAX_CONSECUTIVE_FAILURES``."""
        row = await self._claimed(schedule_id)
        row.consecutive_failures += 1
        row.last_error = error.model_dump(mode="json")
        if not error.retryable or row.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            row.enabled = False
            row.retry_after = None
        else:
            wait = backoff(FIRE_RETRY_BASE, row.consecutive_failures, cap=FIRE_RETRY_CAP)
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
