"""A fire that is late or missed because the ticker was down is caught up exactly once.

The ticker down across one tick or several, then up: the schedule's one owed fire happens
once, for the tick it was owed (``next_fire_at`` is the clock of an unattended loop, so the
first tick missed is the one fired, and the schedule is armed past now, never replaying the
ticks in between). It is never skipped silently: the run is there, its metadata and its
idempotency key name the owed tick, and the schedule records it (``last_fired_at``,
``last_run_id``). It is never doubled: not by a second ticker on the same tick, not by a
ticker that died mid-fire and the one that took over, not by ticking again.

What the other suites already prove, and this one does not repeat: a tick six hours late
fires once on a single ticker (``test_schedules::test_a_missed_tick_is_fired_once_rather_
than_replayed``), two tickers on a tick that is on time queue one run (``test_ticker::
test_two_tickers_on_one_tick_queue_one_run``), and a tick rewound onto one that already
fired finds the same run (``test_ticker::test_a_fire_repeated_for_the_same_tick_finds_the_
same_run``).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from agent_runs.domain.cadence import jitter, next_fire_at
from agent_runs.firing import Firing
from agent_runs.store.runs import RunStore
from agent_runs.ticker import Ticker
from tests.conftest import SETTINGS, Receiver, arm, queued_runs, scheduled, sender

#: A test that would otherwise wait forever on a lock fails instead.
_PATIENCE = 30


def _hour(at: datetime) -> datetime:
    return at.replace(minute=0, second=0, microsecond=0)


async def _owed(app: Any, client: Any, *, hours: int, now: datetime) -> tuple[str, datetime]:
    """An hourly schedule whose fire, ``hours`` ticks before ``now``'s, was owed when the
    ticker went down: the tick it had armed, with its own jitter, as the ticker left it.
    Every tick after it, up to ``now``, was missed too."""
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    return sid, await arm(app, sid, _hour(now) - timedelta(hours=hours) + jitter(sid))


def _ticker(app: Any, tmp_path: Any, name: str) -> Ticker:
    return Ticker(
        app.state.sessions,
        sender(Receiver()),
        app.state.blobs,
        heartbeat_path=tmp_path / name,
        limits=SETTINGS.limits,
        schedules=SETTINGS.schedules,
    )


async def _caught_up_once(app: Any, client: Any, sid: str, owed: datetime, now: datetime) -> None:
    """One run, for the owed tick, recorded on the run and on the schedule; the schedule is
    armed for its first occurrence after ``now``."""
    [run] = await queued_runs(app)
    assert datetime.fromisoformat(run["run_metadata"]["fire_time"]) == owed
    assert run["run_metadata"]["schedule_id"] == sid
    assert run["idempotency_key"] == f"{sid}@{owed.astimezone(UTC).isoformat()}"
    schedule = (await client.get(f"/v1/schedules/{sid}")).json()
    assert datetime.fromisoformat(schedule["last_fired_at"]) == owed
    assert schedule["last_run_id"] == run["run_id"]
    assert (schedule["last_error"], schedule["consecutive_failures"]) == (None, 0)
    following = next_fire_at("hourly", after=now, timezone="UTC")
    assert following is not None
    assert datetime.fromisoformat(schedule["next_fire_at"]) == following + jitter(sid)


@pytest.mark.parametrize(
    "hours", [1, 2, 7, 72], ids=["one-tick", "two-ticks", "seven-ticks", "three-days"]
)
async def test_a_fire_missed_while_the_ticker_was_down_is_caught_up_once_and_recorded(
    app, client, tmp_path, hours: int
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    sid, owed = await _owed(app, client, hours=hours, now=now)
    ticker = _ticker(app, tmp_path, "back")
    assert (await ticker.tick(now=now)).fired == 1
    await _caught_up_once(app, client, sid, owed, now)
    for later in (now, now + timedelta(seconds=30), now + timedelta(minutes=5)):
        assert (await ticker.tick(now=later)).fired == 0
    assert len(await queued_runs(app)) == 1


async def test_two_tickers_back_from_an_outage_catch_up_once(app, client, tmp_path) -> None:
    """Both replicas come back on the same instant after several missed ticks: one fires,
    the other finds nothing due, for every schedule owed."""
    now = datetime.now(UTC).replace(microsecond=0)
    owed = [await _owed(app, client, hours=5, now=now) for _ in range(3)]
    one, two = _ticker(app, tmp_path, "a"), _ticker(app, tmp_path, "b")
    reports = await asyncio.wait_for(
        asyncio.gather(one.tick(now=now), two.tick(now=now)), _PATIENCE
    )
    assert sum(r.fired for r in reports) == len(owed)
    runs = await queued_runs(app)
    assert sorted(r["run_metadata"]["schedule_id"] for r in runs) == sorted(s for s, _ in owed)
    for sid, tick in owed:
        [run] = [r for r in runs if r["run_metadata"]["schedule_id"] == sid]
        assert datetime.fromisoformat(run["run_metadata"]["fire_time"]) == tick
    again = await asyncio.gather(one.tick(now=now), two.tick(now=now))
    assert sum(r.fired for r in again) == 0
    assert len(await queued_runs(app)) == len(owed)


async def test_a_ticker_that_dies_mid_fire_leaves_the_fire_owed_and_it_happens_once(
    app, client, tmp_path
) -> None:
    """A ticker back from an outage dies with the owed fire queued but not committed (the
    process gone, its transaction rolled back). Meanwhile the other ticker skips the row it
    holds; once it is gone, the other fires the owed tick, once, and a restarted ticker
    finds nothing left to fire."""
    now = datetime.now(UTC).replace(microsecond=0)
    sid, owed = await _owed(app, client, hours=3, now=now)
    survivor = _ticker(app, tmp_path, "survivor")
    async with app.state.sessions() as dying:
        firing = Firing(dying, RunStore(dying, SETTINGS.limits), SETTINGS.schedules)
        mid = await firing.fire_due(now=now)
        assert mid is not None and mid.fire_time == owed  # queued, in its transaction
        held = await asyncio.wait_for(survivor.tick(now=now), _PATIENCE)
        assert held.fired == 0  # the row is locked by the one mid-fire: skipped, not doubled
        # the process dies here: no commit, its transaction is rolled back
    assert await queued_runs(app) == []
    still = (await client.get(f"/v1/schedules/{sid}")).json()
    assert datetime.fromisoformat(still["next_fire_at"]) == owed and still["last_run_id"] is None

    assert (await survivor.tick(now=now)).fired == 1
    await _caught_up_once(app, client, sid, owed, now)
    restarted = _ticker(app, tmp_path, "restarted")
    assert (await restarted.tick(now=now)).fired == 0
    assert (await survivor.tick(now=now + timedelta(minutes=1))).fired == 0
    assert len(await queued_runs(app)) == 1


async def test_a_ticker_restarted_after_its_fire_committed_does_not_fire_it_again(
    app, client, tmp_path
) -> None:
    """The other side of a crash mid-fire: the commit went through and the process died
    before it could say so. The restarted ticker, on the same instant, finds the schedule
    armed past now; and should anything hand it the same tick again, the idempotency key
    finds the run already queued."""
    now = datetime.now(UTC).replace(microsecond=0)
    sid, owed = await _owed(app, client, hours=2, now=now)
    async with app.state.sessions() as db:
        fired = await Firing(db, RunStore(db, SETTINGS.limits), SETTINGS.schedules).fire_due(
            now=now
        )
        assert fired is not None
        await db.commit()
    restarted = _ticker(app, tmp_path, "restarted")
    assert (await restarted.tick(now=now)).fired == 0
    await _caught_up_once(app, client, sid, owed, now)
    async with app.state.sessions() as db:
        again = await Firing(db, RunStore(db, SETTINGS.limits), SETTINGS.schedules).fire(
            "acme", sid, at=owed, now=now
        )
        await db.commit()
    assert again.run_id == fired.run_id
    assert len(await queued_runs(app)) == 1
