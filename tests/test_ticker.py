"""The ticker: one loop that fires due schedules, re-queues lapsed leases and escalates
overdue interrupts, straight against the database, safe in several replicas."""

from __future__ import annotations

import asyncio
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy.exc import OperationalError

from agent_runs.config.constants import BREAKER_COOLDOWN, BREAKER_THRESHOLD
from agent_runs.heartbeat import alive
from agent_runs.retry import Breaker, backoff
from agent_runs.ticker import Ticker
from tests.conftest import Receiver, arm, at, pause, queued_runs, scheduled, sender, started

NOW = datetime(2026, 3, 1, 9, 0, tzinfo=UTC)


# ------------------------------------------------------------------ the retry helpers


def test_backoff_doubles_and_is_capped() -> None:
    base, cap = timedelta(seconds=1), timedelta(seconds=5)
    assert [backoff(base, n, cap=cap).total_seconds() for n in range(1, 6)] == [1, 2, 4, 5, 5]


def test_the_breaker_opens_at_the_threshold_and_admits_one_trial_after_cooldown() -> None:
    breaker = Breaker(threshold=2, cooldown=timedelta(seconds=60))
    breaker.record_failure(NOW)
    assert breaker.allows(NOW), "one failure is not an outage"
    breaker.record_failure(NOW)
    assert breaker.is_open and not breaker.allows(NOW + timedelta(seconds=59))
    assert breaker.allows(NOW + timedelta(seconds=60))
    breaker.record_success()
    assert not breaker.is_open


def test_a_failed_trial_restarts_the_cooldown() -> None:
    breaker = Breaker(threshold=1, cooldown=timedelta(seconds=60))
    breaker.record_failure(NOW)
    breaker.record_failure(NOW + timedelta(seconds=61))  # the trial fails
    assert not breaker.allows(NOW + timedelta(seconds=70))


# ------------------------------------------------------------------ the loop itself


class DeadDatabase:
    """A session factory whose every session fails the way an unreachable database does."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> DeadDatabase:
        self.calls += 1
        return self

    async def __aenter__(self) -> DeadDatabase:
        raise OperationalError("SELECT", {}, Exception("connection refused"))

    async def __aexit__(self, *exc: object) -> None:
        return None


def _dead_ticker(tmp_path: Path, **kwargs) -> tuple[Ticker, DeadDatabase]:
    dead = DeadDatabase()
    ticker = Ticker(dead, sender(Receiver()), heartbeat_path=tmp_path / "beat", **kwargs)  # pyright: ignore[reportArgumentType]
    return ticker, dead


async def test_a_dead_database_opens_the_breaker_and_the_ticker_stops_calling(tmp_path) -> None:
    ticker, dead = _dead_ticker(tmp_path)
    for _ in range(BREAKER_THRESHOLD):
        assert (await ticker.tick(now=NOW)).fired == 0
    assert dead.calls == BREAKER_THRESHOLD and ticker.breaker.is_open
    for _ in range(5):
        await ticker.tick(now=NOW + timedelta(seconds=1))
    assert dead.calls == BREAKER_THRESHOLD
    await ticker.tick(now=NOW + BREAKER_COOLDOWN)
    assert dead.calls == BREAKER_THRESHOLD + 1, "one trial after the cooldown"


async def test_the_loop_beats_even_when_ticks_fail_and_stops_promptly(tmp_path) -> None:
    ticker, _ = _dead_ticker(tmp_path, interval=30)
    stop = asyncio.Event()
    task = asyncio.create_task(ticker.run_forever(stop))
    for _ in range(200):
        if (tmp_path / "beat").exists():
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=2)  # not the 30 s interval
    assert alive(tmp_path / "beat")


async def test_a_crashing_tick_does_not_kill_the_loop(tmp_path, monkeypatch) -> None:
    ticker, _ = _dead_ticker(tmp_path, interval=0.01)
    stop = asyncio.Event()
    ticks = 0

    async def crash(**_: object) -> None:
        nonlocal ticks
        ticks += 1
        if ticks == 3:
            stop.set()
        raise RuntimeError("something unexpected inside a tick")

    monkeypatch.setattr(ticker, "tick", crash)
    await asyncio.wait_for(ticker.run_forever(stop), timeout=2)
    assert ticks == 3


def test_a_missing_stale_or_garbled_heartbeat_reads_as_dead(tmp_path) -> None:
    beat = tmp_path / "beat"
    assert not alive(beat)
    beat.write_text(str(time.time() - 3600))
    assert not alive(beat)
    beat.write_text("not a number")
    assert not alive(beat)
    beat.write_text(str(time.time()))
    assert alive(beat)


# ------------------------------------------------------------------ against the database


async def test_two_tickers_on_one_tick_queue_one_run(app, client, tmp_path) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    await arm(app, sid, datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=5))
    one = Ticker(app.state.sessions, sender(Receiver()), heartbeat_path=tmp_path / "a")
    two = Ticker(app.state.sessions, sender(Receiver()), heartbeat_path=tmp_path / "b")
    reports = await asyncio.gather(one.tick(), two.tick())
    assert sum(r.fired for r in reports) == 1
    assert len(await queued_runs(app)) == 1


async def test_a_fire_repeated_for_the_same_tick_finds_the_same_run(app, client, ticker) -> None:
    """Idempotency on (schedule_id, fire_time), even when the schedule row is rewound to a
    tick that already queued its run."""
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    tick = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    await arm(app, sid, tick)
    assert (await ticker.tick()).fired == 1
    await arm(app, sid, tick)
    assert (await ticker.tick()).fired == 1
    assert len(await queued_runs(app)) == 1


async def test_one_tick_requeues_lapsed_leases_and_escalates_with_webhooks(
    app, client, ticker, receiver
) -> None:
    hook = {"url": "https://ui.example/h", "events": ["run.escalated", "run.finished"]}
    assert (await client.post("/v1/webhooks", json=hook)).status_code == 201
    await client.post("/v1/runs", json=started(queue=True))
    claim = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 5}
    leased = (await client.post("/v1/runs/claim", json=claim)).json()["run"]["run_id"]

    waiting = (await client.post("/v1/runs", json=started())).json()["run_id"]
    body = pause(
        waiting, assignee="user:u1", deadline=at(1).isoformat(), escalate_to="role:managers"
    )
    await client.post(f"/v1/runs/{waiting}/pause", json=body)

    report = await ticker.tick(now=at(2))
    assert (report.requeued, report.escalated, report.sent) == (1, 1, 1)
    assert (await client.get(f"/v1/runs/{leased}")).json()["status"] == "QUEUED"
    assert [e["type"] for e in receiver.events()] == ["run.escalated"]


def test_each_ticker_process_has_its_own_heartbeat_unless_one_is_configured(tmp_path) -> None:
    from agent_runs.heartbeat import path_for

    mine = path_for(None)
    assert str(os.getpid()) in mine.name
    assert path_for(tmp_path / "beat") == tmp_path / "beat"


def test_the_probe_reads_the_configured_file(tmp_path, monkeypatch) -> None:
    from agent_runs.config.settings import reset_settings_cache
    from agent_runs.heartbeat import main

    monkeypatch.delenv("RUNS__TICKER__HEARTBEAT_FILE", raising=False)
    monkeypatch.chdir(tmp_path)  # no .env
    reset_settings_cache()
    assert main() == 1  # nothing to probe
    beat = tmp_path / "beat"
    beat.write_text(str(time.time()))
    monkeypatch.setenv("RUNS__TICKER__HEARTBEAT_FILE", str(beat))
    reset_settings_cache()
    assert main() == 0
    reset_settings_cache()
