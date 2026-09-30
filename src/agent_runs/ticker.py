"""``agent-runs-ticker``: the one background loop. Each tick, straight against the database:

1. fire every due schedule (one ``SKIP LOCKED`` claim at a time, each in its own transaction,
   queueing its run idempotently on ``(schedule_id, fire_time)``);
2. put runs whose lease lapsed back on the queue (or fail them after ``MAX_ATTEMPTS``);
3. escalate or time out interrupts past their deadline.

Every step is bounded per tick and safe to run in several replicas at once: a row one
ticker holds is skipped by the others, and a fire repeated for one tick finds the same run.
A tick that fails as a whole (the database is down) counts against a breaker, so an outage
does not become a tight retry loop. A heartbeat file, touched after every tick, is the
liveness probe (``agent-runs-ticker --probe``).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import structlog
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from trellis.contracts.ids import now as clock
from trellis.contracts.runs import RunStatus

from agent_runs.config.constants import (
    BREAKER_COOLDOWN,
    BREAKER_THRESHOLD,
    HEARTBEAT_MAX_AGE_SECONDS,
    HEARTBEAT_PATH,
    SWEEP_BATCH,
    TICK_SECONDS,
)
from agent_runs.config.settings import get_settings
from agent_runs.domain.schedules import FireFailed
from agent_runs.firing import Firing
from agent_runs.observability.logging import configure_logging
from agent_runs.retry import Breaker
from agent_runs.store.database import connect
from agent_runs.store.runs import RunStore
from agent_runs.webhooks import WebhookEvent, WebhookSender

log = structlog.get_logger(__name__)

Sessions = Callable[[], AsyncSession] | async_sessionmaker[AsyncSession]


@dataclass(frozen=True)
class TickReport:
    fired: int = 0
    requeued: int = 0
    escalated: int = 0


class Ticker:
    def __init__(
        self,
        sessions: Sessions,
        webhooks: WebhookSender,
        *,
        heartbeat: Path = Path(HEARTBEAT_PATH),
        interval: float = TICK_SECONDS,
    ) -> None:
        self._sessions = sessions
        self._webhooks = webhooks
        self._heartbeat = heartbeat
        self._interval = interval
        self.breaker = Breaker(BREAKER_THRESHOLD, BREAKER_COOLDOWN)

    async def tick(self, *, now: datetime | None = None) -> TickReport:
        """One pass. Raises nothing the loop could not survive: a database failure is
        recorded on the breaker and reported as an empty tick."""
        now = now or clock()
        if not self.breaker.allows(now):
            return TickReport()
        try:
            report = TickReport(
                fired=await self._fire_due(now),
                requeued=await self._requeue_lapsed(now),
                escalated=await self._escalate_overdue(now),
            )
        except (DBAPIError, OSError) as exc:
            self.breaker.record_failure(now)
            log.warning("ticker.tick_failed", error=str(exc), failures=self.breaker.failures)
            return TickReport()
        self.breaker.record_success()
        if report != TickReport():
            log.info("ticker.tick", **report.__dict__)
        return report

    async def _fire_due(self, now: datetime) -> int:
        fired = 0
        for _ in range(SWEEP_BATCH):
            async with self._sessions() as db:
                try:
                    result = await Firing(db).fire_due(now=now)
                except FireFailed:
                    # recorded on the schedule, which now backs off or is paused
                    await db.commit()
                    continue
                await db.commit()
            if result is None:
                break
            fired += 1
        return fired

    async def _requeue_lapsed(self, now: datetime) -> int:
        async with self._sessions() as db:
            moved = await RunStore(db).requeue_lapsed(now=now, limit=SWEEP_BATCH)
            await db.commit()
        for run in moved:
            self._webhooks.notify(run)
        return len(moved)

    async def _escalate_overdue(self, now: datetime) -> int:
        async with self._sessions() as db:
            moved = await RunStore(db).escalate_overdue(now=now, limit=SWEEP_BATCH)
            await db.commit()
        for run in moved:
            self._webhooks.notify(
                run, WebhookEvent.ESCALATED if run.status is RunStatus.PAUSED else None
            )
        return len(moved)

    def beat(self) -> None:
        """Record that the loop came round. Never raises."""
        try:
            self._heartbeat.write_text(str(time.time()))
        except OSError as exc:
            log.warning("ticker.heartbeat_failed", error=str(exc))

    async def run_forever(self, stop: asyncio.Event) -> None:
        """Tick until ``stop``; the tick in flight finishes, the wait between does not."""
        log.info("ticker.started", interval_seconds=self._interval)
        while not stop.is_set():
            try:
                await self.tick()
            except Exception:  # the loop has to outlive any single tick
                log.exception("ticker.tick_crashed")
            self.beat()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._interval)
        log.info("ticker.stopped")


def alive(heartbeat: Path = Path(HEARTBEAT_PATH)) -> bool:
    """Whether the loop came round recently enough (the container healthcheck)."""
    try:
        beat = float(heartbeat.read_text())
    except (OSError, ValueError):
        return False
    return time.time() - beat <= HEARTBEAT_MAX_AGE_SECONDS


async def run() -> None:
    settings = get_settings()
    configure_logging(
        level=settings.observability.log_level, json_output=settings.observability.log_json
    )
    engine = await connect(settings.database)
    webhooks = WebhookSender(
        secret=settings.webhooks.signing_secret, allow_http=settings.service.is_dev
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        ticker = Ticker(async_sessionmaker(engine, expire_on_commit=False), webhooks)
        await ticker.run_forever(stop)
    finally:
        await webhooks.aclose()
        await engine.dispose()


def main() -> None:
    """``agent-runs-ticker`` runs the loop; ``agent-runs-ticker --probe`` exits 0 while it
    is turning."""
    if "--probe" in sys.argv[1:]:
        raise SystemExit(0 if alive() else 1)
    asyncio.run(run())


if __name__ == "__main__":
    main()
