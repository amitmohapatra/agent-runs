"""Firing a schedule: queue one run for one tick, in the same transaction that advances the
schedule, idempotent on ``(schedule_id, fire_time)``.

The run is built from the stored schedule and nothing else (tenant, agent, input and above
all ``on_behalf_of``), so no request can make a schedule fire as someone it was not made
for. A repeated fire for the same tick finds the run the first one queued.
"""

from __future__ import annotations

from datetime import datetime

import structlog
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from trellis.contracts.errors import AgentError, ErrorCategory
from trellis.contracts.runs import RunStart, Schedule

from agent_runs.config.constants import MAX_FIRE_SKEW
from agent_runs.domain.schedules import (
    FireFailed,
    FireResult,
    FireTimeOutOfRange,
    ScheduleNotFiring,
    idempotency_key,
)
from agent_runs.store.runs import RunStore
from agent_runs.store.schedules import ScheduleStore

log = structlog.get_logger(__name__)


class Firing:
    """Fires schedules within the caller's transaction; the caller commits, also after a
    :class:`FireFailed` (the failure is recorded on the schedule)."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._schedules = ScheduleStore(session)
        self._runs = RunStore(session)

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
        return await self._fire(schedule, fire_time=fire_time, now=now)

    async def fire_due(self, *, now: datetime) -> FireResult | None:
        """The ticker's step: fire the most overdue schedule for its own tick, or ``None``."""
        schedule = await self._schedules.claim_due(now=now)
        if schedule is None or schedule.next_fire_at is None:
            return None
        return await self._fire(schedule, fire_time=schedule.next_fire_at, now=now)

    async def _fire(self, schedule: Schedule, *, fire_time: datetime, now: datetime) -> FireResult:
        key = idempotency_key(schedule.schedule_id, fire_time)
        start = RunStart(
            tenant_id=schedule.tenant_id,
            agent_id=schedule.agent_id,
            workspace_id=schedule.workspace_id,
            on_behalf_of=schedule.on_behalf_of,
            input=schedule.input,
            idempotency_key=key,
            webhook_url=schedule.webhook_url,
            metadata={
                "schedule_id": schedule.schedule_id,
                "schedule_name": schedule.name,
                "fire_time": fire_time.isoformat(),
                "created_by": schedule.created_by,
            },
        )
        try:
            async with self._session.begin_nested():
                run, _ = await self._runs.start(start, queue=True, now=now)
        except DBAPIError as exc:
            error = AgentError.of(
                exc,
                category=ErrorCategory.DEPENDENCY,
                source="agent-runs",
                retryable=isinstance(exc, OperationalError),
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
            idempotency_key=key,
            schedule=fired,
        )
