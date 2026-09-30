"""``agent-runs-ticker``: the one background loop. Each tick, straight against the database:

1. fire every due schedule (one ``SKIP LOCKED`` claim at a time, each in its own transaction,
   queueing its run idempotently on ``(schedule_id, fire_time)``);
2. put runs whose lease lapsed back on the queue (or fail them after ``MAX_ATTEMPTS``);
3. escalate or time out interrupts past their deadline;
4. send the webhook deliveries that are due from the outbox (one attempt each).

Steps 2 and 3 write the webhook events they cause into the outbox in their own transaction.

Every step is bounded per tick and safe to run in several replicas at once: a row one
ticker holds is skipped by the others, and a fire repeated for one tick finds the same run.
A tick that fails as a whole (the database is down) counts against a breaker, so an outage
does not become a tight retry loop. A heartbeat file, touched after every tick, is the
liveness probe (``python -m agent_runs.heartbeat``), one file per ticker (see ``heartbeat``).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import structlog
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from trellis.contracts.ids import now as clock
from trellis.contracts.runs import RunStatus

from agent_runs import heartbeat
from agent_runs.config.constants import (
    BREAKER_COOLDOWN,
    BREAKER_THRESHOLD,
    SWEEP_BATCH,
    TICK_SECONDS,
)
from agent_runs.config.settings import get_settings
from agent_runs.domain.schedules import FireFailed
from agent_runs.domain.webhooks import WebhookEvent
from agent_runs.firing import Firing
from agent_runs.observability.logging import configure_logging
from agent_runs.retry import Breaker
from agent_runs.store.database import connect
from agent_runs.store.runs import RunStore
from agent_runs.store.webhooks import WebhookStore
from agent_runs.webhooks import WebhookSender

log = structlog.get_logger(__name__)

Sessions = Callable[[], AsyncSession]


@dataclass(frozen=True)
class TickReport:
    fired: int = 0
    requeued: int = 0
    escalated: int = 0
    sent: int = 0


class Ticker:
    def __init__(
        self,
        sessions: Sessions,
        webhooks: WebhookSender,
        *,
        heartbeat_path: Path,
        interval: float = TICK_SECONDS,
    ) -> None:
        self._sessions = sessions
        self._webhooks = webhooks
        self._heartbeat = heartbeat_path
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
                sent=await self._send_webhooks(now),
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
            for run in moved:
                await WebhookStore(db).announce(run, now=now)
            await db.commit()
        return len(moved)

    async def _escalate_overdue(self, now: datetime) -> int:
        async with self._sessions() as db:
            moved = await RunStore(db).escalate_overdue(now=now, limit=SWEEP_BATCH)
            for run in moved:
                event = WebhookEvent.ESCALATED if run.status is RunStatus.PAUSED else None
                await WebhookStore(db).announce(run, event, now=now)
            await db.commit()
        return len(moved)

    async def _send_webhooks(self, now: datetime) -> int:
        """Hold the due deliveries (a short transaction), send them concurrently with no
        transaction open, then settle each: gone once accepted or given up, else backing
        off. Returns how many were accepted."""
        async with self._sessions() as db:
            due = await WebhookStore(db).claim_due(now=now, limit=SWEEP_BATCH)
            await db.commit()
        if not due:
            return 0
        retries = await self._webhooks.send_all(due)
        async with self._sessions() as db:
            store = WebhookStore(db)
            for delivery, retry in zip(due, retries, strict=True):
                if not await store.settle(delivery, retry=retry, now=now) and retry:
                    log.warning("webhook.gave_up", event_id=delivery.payload["event_id"])
            await db.commit()
        return retries.count(False)

    def beat(self) -> None:
        """Record that the loop came round. Never raises: a full disk on the liveness file
        must not stop work that is still going through."""
        try:
            heartbeat.beat(self._heartbeat)
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


async def run() -> None:
    settings = get_settings()
    configure_logging(
        level=settings.observability.log_level, json_output=settings.observability.log_json
    )
    engine = await connect(settings.database)
    webhooks = WebhookSender(allow_http=settings.service.is_dev)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        beat = heartbeat.path_for(settings.ticker.heartbeat_file)
        log.info("ticker.heartbeat", path=str(beat))
        ticker = Ticker(
            async_sessionmaker(engine, expire_on_commit=False), webhooks, heartbeat_path=beat
        )
        await ticker.run_forever(stop)
    finally:
        await webhooks.aclose()
        await engine.dispose()


def main() -> None:
    """``agent-runs-ticker``."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
