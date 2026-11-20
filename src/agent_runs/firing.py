"""Firing a schedule: queue one run for one tick, in the same transaction that advances the
schedule, idempotent on ``(schedule_id, fire_time)``.

The run is built from the stored schedule and nothing else, by the contracts'
``Schedule.run_start`` (tenant, agent, input, its working-time limit, agent version, priority,
concurrency key and metadata, and above all ``on_behalf_of``), so no request can make a
schedule fire as someone it was not made for, and a scheduled run carries everything a started
one can. The schedule's metadata goes under the fire's own keys (``schedule_id``,
``schedule_name``, ``fire_time``, ``created_by``), which win on conflict: a schedule cannot
pass its run off as another's fire. A schedule whose metadata names a ``thread_id`` fires its
runs into that conversation. A repeated fire for the same tick finds the run the first one
queued.

Nothing fires at or after a schedule's ``end_at``: not a tick at or past it, and nothing at all
once the end has passed, even a tick owed from before it (``ScheduleEnded``). With
``RUNS__SCHEDULES__MAX_FIRES_PER_DAY`` on (500 unless set; 0 is off), each fire that queues a
new run takes one from its tenant's count for the UTC day, in the fire's transaction; past it
the run is not queued (``FireRefused``, recorded on the schedule). A repeat that finds its
run takes nothing.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

import structlog
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from trellis.contracts.errors import AgentError, ErrorCategory
from trellis.contracts.runs import RunStatus, Schedule

from agent_runs.config.constants import MAX_FIRE_SKEW
from agent_runs.config.settings import SchedulesSettings
from agent_runs.domain.errors import Unavailable
from agent_runs.domain.schedules import (
    FIRES_PER_DAY,
    FireFailed,
    FireRefused,
    FireResult,
    FireSkipped,
    FireTimeOutOfRange,
    ScheduleEnded,
    ScheduleNotFiring,
)
from agent_runs.store.runs import RunStore
from agent_runs.store.schedules import ScheduleStore
from agent_runs.store.tables import RunRow

log = structlog.get_logger(__name__)


class Firing:
    """Fires schedules within the caller's transaction, queueing their runs through ``runs``
    (the store on ``session``) under the deployment's schedule ``policy`` (unset: the
    defaults); the caller commits, also after a :class:`FireFailed` or :class:`FireRefused`
    (recorded on the schedule) and, from :meth:`fire_due`, a :class:`ScheduleEnded`."""

    def __init__(
        self, session: AsyncSession, runs: RunStore, policy: SchedulesSettings | None = None
    ) -> None:
        self._session = session
        self._policy = policy or SchedulesSettings()
        self._schedules = ScheduleStore(session, self._policy)
        self._runs = runs

    async def fire(
        self, tenant_id: str, schedule_id: str, *, at: datetime | None, now: datetime
    ) -> FireResult:
        """Fire one schedule for ``at`` (an instant that has arrived), else for the tick it
        is due for, else for ``now``."""
        schedule = await self._schedules.claim(tenant_id, schedule_id)
        if not schedule.enabled:
            raise ScheduleNotFiring(schedule_id)
        if at is not None and at > now + MAX_FIRE_SKEW:
            raise FireTimeOutOfRange(at, now)
        due = schedule.next_fire_at
        fire_time = at or (due if due is not None and due <= now else now)
        if _ended(schedule, fire_time=fire_time, now=now):
            raise ScheduleEnded(schedule)
        return await self._fire(schedule, fire_time=fire_time, now=now)

    async def fire_due(self, *, now: datetime) -> FireResult | None:
        """The ticker's step: fire the most overdue schedule for its own tick, or ``None``.
        While the schedule's last run has not ended, the tick is skipped (:class:`FireSkipped`):
        one run of a schedule at a time, so workers down for a day run it once on recovery."""
        schedule = await self._schedules.claim_due(now=now)
        if schedule is None or schedule.next_fire_at is None:
            return None
        if _ended(schedule, fire_time=schedule.next_fire_at, now=now):
            ended = await self._schedules.record_end(schedule.schedule_id, now=now)
            log.info("schedule.ended", schedule_id=schedule.schedule_id)
            raise ScheduleEnded(ended)
        if (active := await self._active(schedule)) is not None:
            skipped = await self._schedules.record_skip(schedule.schedule_id, now=now)
            log.info("schedule.skipped", schedule_id=schedule.schedule_id, active_run_id=active)
            raise FireSkipped(skipped, active)
        return await self._fire(schedule, fire_time=schedule.next_fire_at, now=now)

    async def _active(self, schedule: Schedule) -> str | None:
        """The schedule's last run when it has not ended (queued, running or paused)."""
        if schedule.last_run_id is None:
            return None
        status = await self._session.scalar(
            select(RunRow.status).where(RunRow.run_id == schedule.last_run_id)
        )
        return schedule.last_run_id if status and not RunStatus(status).final else None

    async def _fire(self, schedule: Schedule, *, fire_time: datetime, now: datetime) -> FireResult:
        start = schedule.run_start(fire_time=fire_time, thread_id=_thread(schedule))
        assert start.idempotency_key is not None
        limit = self._policy.fires_per_day_limit
        try:
            async with self._session.begin_nested():
                run, created = await self._runs.start(start, queue=True, now=now, strict=False)
                if (
                    created
                    and limit is not None
                    and not await self._schedules.take_fire(
                        schedule.tenant_id, now=now, limit=limit
                    )
                ):
                    raise _OverTheDay  # the run is rolled back with the savepoint
        except _OverTheDay:
            day = now.astimezone(UTC).date().isoformat()
            error = AgentError(
                code=FIRES_PER_DAY,
                message=f"tenant {schedule.tenant_id} has fired its {limit} fires for {day} "
                "(UTC): this tick queued no run",
                category=ErrorCategory.RATE_LIMIT,
                retryable=False,
                source="agent-runs",
            )
            refused = await self._schedules.record_refusal(
                schedule.schedule_id, error=error, now=now
            )
            log.warning("schedule.fire_refused", schedule_id=schedule.schedule_id, limit=limit)
            raise FireRefused(refused, error) from None
        except (DBAPIError, Unavailable) as exc:
            # the database refused the run, or the blob store did not take its input
            error = AgentError.of(
                exc,
                category=ErrorCategory.DEPENDENCY,
                source="agent-runs",
                retryable=isinstance(exc, OperationalError | Unavailable),
            )
            left = await self._schedules.record_failure(schedule.schedule_id, error=error, now=now)
            log.warning(
                "schedule.fire_failed",
                schedule_id=schedule.schedule_id,
                consecutive_failures=left.consecutive_failures,
                auto_paused=not left.enabled,
            )
            raise FireFailed(left, error) from exc
        fired = await self._schedules.record_success(
            schedule.schedule_id, fire_time=fire_time, run_id=run.run_id, now=now
        )
        log.info("schedule.fired", schedule_id=schedule.schedule_id, run_id=run.run_id)
        return FireResult(
            schedule_id=schedule.schedule_id,
            run_id=run.run_id,
            fire_time=fire_time,
            idempotency_key=start.idempotency_key,
            schedule=fired,
        )


class _OverTheDay(Exception):
    """The tenant's daily fire limit is reached: roll the fire's run back."""


def _ended(schedule: Schedule, *, fire_time: datetime, now: datetime) -> bool:
    """Nothing fires at or after the end: neither for an instant at or past it, nor at all
    once it has passed."""
    end = schedule.end_at
    return end is not None and (fire_time >= end or now >= end)


#: The schedule metadata key naming the conversation its runs are turns of: a schedule set
#: from a chat fires into that chat (its runs' ``thread_id``), so the reply is in its history.
THREAD: Final = "thread_id"


def _thread(schedule: Schedule) -> str | None:
    thread = schedule.metadata.get(THREAD)
    return thread if isinstance(thread, str) and thread else None
