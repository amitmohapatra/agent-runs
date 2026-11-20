"""The schedule policy a deployment may set (``RUNS__SCHEDULES__*``): the floor, the jitter,
the failed fires before an auto-pause and their backoff, each wired to where it acts; and the
two count limits, on by default (50 schedules per tenant, 500 fires per tenant per UTC day)
and off at 0.

The defaults (``test_settings``) are the policy as it was hard-coded, with those two limits;
every other schedule test runs on them."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text

from agent_runs.config.settings import SchedulesSettings, Settings
from agent_runs.domain.cadence import jitter
from agent_runs.ticker import Ticker
from tests.conftest import SETTINGS, arm, client_of, queued_runs, scheduled, sender, serving


def _with(**policy: Any) -> Settings:
    return SETTINGS.model_copy(update={"schedules": SchedulesSettings(**policy)})


@dataclass(frozen=True)
class _Parts:
    memory: Any
    blobs: Any
    receiver: Any
    tmp_path: Any


@pytest.fixture
def parts(migrated, memory, blobs, receiver, tmp_path) -> _Parts:
    """What an app and its ticker are served with."""
    return _Parts(memory, blobs, receiver, tmp_path)


@asynccontextmanager
async def _served(parts: _Parts, **policy: Any) -> AsyncIterator[tuple[Any, Any, Ticker]]:
    """The app and a ticker on one policy, with acme's client."""
    memory, blobs, receiver, tmp_path = parts.memory, parts.blobs, parts.receiver, parts.tmp_path
    settings = _with(**policy)
    async with serving(settings, memory, blobs) as app, client_of(app) as client:
        hooks = sender(receiver)
        ticker = Ticker(
            app.state.sessions,
            hooks,
            app.state.blobs,
            heartbeat_path=tmp_path / "beat",
            limits=settings.limits,
            schedules=settings.schedules,
        )
        try:
            yield app, client, ticker
        finally:
            await hooks.aclose()


def _last_tick() -> datetime:
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)


async def _due(app, client, **over: Any) -> str:
    """A new hourly schedule, due for the last hour's tick."""
    response = await client.post("/v1/schedules", json=scheduled(cadence="hourly", **over))
    assert response.status_code == 201, response.text
    sid = response.json()["schedule_id"]
    await arm(app, sid, _last_tick() - timedelta(minutes=55))
    return sid


async def _end_runs(app) -> None:
    """Every run ends, so no overlap skip stands between a schedule and its next fire."""
    async with app.state.engine.begin() as conn:
        await conn.execute(text("UPDATE agent_runs SET status = 'SUCCESS'"))


# ------------------------------------------------------------------ the policy


async def test_a_lower_floor_accepts_a_cadence_the_default_refuses(parts) -> None:
    async with _served(parts, min_interval_seconds=900) as served:
        _, client, _ = served
        quarter = await client.post("/v1/schedules", json=scheduled(cadence="*/15 * * * *"))
        assert quarter.status_code == 201, quarter.text
        faster = await client.post("/v1/schedules", json=scheduled(cadence="*/5 * * * *"))
        assert faster.status_code == 422 and "0:15:00" in faster.text
    async with serving(SETTINGS, parts.memory, parts.blobs) as app, client_of(app) as client:
        refused = await client.post("/v1/schedules", json=scheduled(cadence="*/15 * * * *"))
        assert refused.status_code == 422 and "per-minute" in refused.text


async def test_the_jitter_is_the_deployments(parts) -> None:
    async with _served(parts, jitter_seconds=10) as served:
        _, client, _ = served
        created = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
        due = datetime.fromisoformat(created["next_fire_at"])
        shift = due - due.replace(minute=0, second=0, microsecond=0)
        assert shift == jitter(created["schedule_id"], spread=timedelta(seconds=10))
        assert shift < timedelta(seconds=10)


async def test_the_failed_fires_before_an_auto_pause_are_the_deployments(parts, broken) -> None:
    async with _served(parts, max_failed_fires=1) as served:
        _, client, _ = served
        sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
            "schedule_id"
        ]
        broken.fail()
        failed = await client.post(f"/v1/schedules/{sid}/fire")
        assert failed.status_code == 503 and failed.json()["details"]["auto_paused"] is True
        paused = (await client.get(f"/v1/schedules/{sid}")).json()
        assert (paused["enabled"], paused["consecutive_failures"]) == (False, 1)


async def test_the_fire_backoff_is_the_deployments(parts, broken) -> None:
    """60 s doubling to at most 90 s: the first retry waits 60 s, the second 90 s (capped)."""
    policy = {"retry_base_seconds": 60, "retry_cap_seconds": 90, "max_failed_fires": 5}
    async with _served(parts, **policy) as served:
        app, client, ticker = served
        sid = await _due(app, client)
        broken.fail()
        start = datetime.now(UTC)
        await ticker.tick(now=start)
        first = (await client.get(f"/v1/schedules/{sid}")).json()
        assert datetime.fromisoformat(first["retry_after"]) == start + timedelta(seconds=60)
        await ticker.tick(now=start + timedelta(seconds=61))
        second = (await client.get(f"/v1/schedules/{sid}")).json()
        retry = datetime.fromisoformat(second["retry_after"])
        assert retry == start + timedelta(seconds=61 + 90)
        assert second["consecutive_failures"] == 2 and second["enabled"] is True


# ------------------------------------------------------------------ schedules per tenant


async def test_a_tenant_has_at_most_the_schedules_the_deployment_allows(parts) -> None:
    """``max_per_tenant``: a create past it is ``409``; a repeated create of one it has still
    answers that one (an upsert adds nothing); another tenant has its own count; a delete
    makes room."""
    async with _served(parts, max_per_tenant=2) as served:
        app, client, _ = served
        first = scheduled()
        made = [await client.post("/v1/schedules", json=body) for body in (first, scheduled())]
        assert [r.status_code for r in made] == [201, 201]
        refused = await client.post("/v1/schedules", json=scheduled())
        assert refused.status_code == 409, refused.text
        assert refused.json()["code"] == "CONFLICT" and "2 schedules" in refused.json()["detail"]
        again = await client.post("/v1/schedules", json=first)
        assert again.status_code == 200
        assert again.json()["schedule_id"] == made[0].json()["schedule_id"]
        async with client_of(app, "other-key") as globex:
            theirs = await globex.post("/v1/schedules", json=scheduled(tenant_id="globex"))
            assert theirs.status_code == 201
        await client.delete(f"/v1/schedules/{made[1].json()['schedule_id']}")
        assert (await client.post("/v1/schedules", json=scheduled())).status_code == 201


async def test_concurrent_creates_never_pass_the_tenants_limit(parts) -> None:
    import asyncio

    async with _served(parts, max_per_tenant=3) as served:
        _, client, _ = served
        answers = await asyncio.gather(
            *(client.post("/v1/schedules", json=scheduled()) for _ in range(8))
        )
        assert sorted(r.status_code for r in answers) == [201] * 3 + [409] * 5
        assert len((await client.get("/v1/schedules")).json()) == 3


# ------------------------------------------------------------------ fires per tenant per day


async def test_a_fire_over_the_daily_limit_is_skipped_recorded_and_does_not_pause(parts) -> None:
    """``max_fires_per_day``: the tenant's first fires of the UTC day queue their runs; a tick
    past the limit queues none, and the schedule records why (``last_error``, as a failed fire
    does) and is armed for its next occurrence (as a skipped tick is). It is never paused for
    it and its failure count does not grow, however many ticks are refused; the next UTC day
    it fires again."""
    async with _served(parts, max_fires_per_day=1, max_failed_fires=2) as served:
        app, client, ticker = served
        first, second = await _due(app, client), await _due(app, client)
        now = datetime.now(UTC)
        assert (await ticker.tick(now=now)).fired == 1
        assert len(await queued_runs(app)) == 1
        states = {s["schedule_id"]: s for s in (await client.get("/v1/schedules")).json()}
        fired, refused = sorted(
            states.values(), key=lambda s: s["last_run_id"] is None
        )  # the one fired first
        assert fired["last_error"] is None
        error = refused["last_error"]
        assert (error["code"], error["category"], error["retryable"]) == (
            "FIRES_PER_DAY",
            "RATE_LIMIT",
            False,
        )
        assert refused["enabled"] is True and refused["consecutive_failures"] == 0
        assert refused["retry_after"] is None
        assert datetime.fromisoformat(refused["next_fire_at"]) > now
        assert {first, second} == set(states)

        for minutes in (1, 2, 3):  # more refused ticks than max_failed_fires: still enabled
            await _end_runs(app)
            for sid in (first, second):  # due again, each time for its own instant
                await arm(app, sid, _last_tick() - timedelta(minutes=55 - minutes))
            assert (await ticker.tick(now=now)).fired == 0
        still = (await client.get(f"/v1/schedules/{refused['schedule_id']}")).json()
        assert still["enabled"] is True and still["consecutive_failures"] == 0

        await _end_runs(app)
        assert (await ticker.tick(now=now + timedelta(days=2))).fired == 1


async def test_a_fire_by_hand_over_the_daily_limit_is_refused_but_a_repeat_is_not(parts) -> None:
    """A fire by hand counts as a ticker's does: past the limit it is ``409`` and recorded on
    the schedule; a repeat for a tick already fired returns its run (it queues nothing), and
    another tenant has its own count."""
    async with _served(parts, max_fires_per_day=1) as served:
        app, client, _ = served
        sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
            "schedule_id"
        ]
        tick = _last_tick().isoformat()
        fired = await client.post(f"/v1/schedules/{sid}/fire", json={"at": tick})
        assert fired.status_code == 200, fired.text
        repeat = await client.post(f"/v1/schedules/{sid}/fire", json={"at": tick})
        assert repeat.status_code == 200 and repeat.json()["run_id"] == fired.json()["run_id"]
        earlier = (_last_tick() - timedelta(hours=1)).isoformat()
        over = await client.post(f"/v1/schedules/{sid}/fire", json={"at": earlier})
        assert over.status_code == 409, over.text
        assert "1 fires" in over.json()["detail"]
        recorded = (await client.get(f"/v1/schedules/{sid}")).json()
        assert recorded["last_error"]["code"] == "FIRES_PER_DAY" and recorded["enabled"]
        assert len(await queued_runs(app)) == 1
        async with client_of(app, "other-key") as globex:
            theirs = (await globex.post("/v1/schedules", json=scheduled(tenant_id="globex"))).json()
            assert (
                await globex.post(f"/v1/schedules/{theirs['schedule_id']}/fire")
            ).status_code == 200


async def test_the_default_limits_count_every_fire_and_every_schedule(app, client, ticker) -> None:
    """The defaults: each fire that queues a run is counted for its tenant's day (500 a day)."""
    sids = [await _due(app, client) for _ in range(3)]
    assert (await ticker.tick()).fired == 3
    assert len(sids) == len(await queued_runs(app))
    async with app.state.engine.connect() as conn:
        assert await conn.scalar(text("SELECT fires FROM schedule_fire_counts")) == 3


async def test_the_default_limit_of_schedules_per_tenant_is_fifty(app, client) -> None:
    made = [await client.post("/v1/schedules", json=scheduled()) for _ in range(50)]
    assert {r.status_code for r in made} == {201}
    refused = await client.post("/v1/schedules", json=scheduled())
    assert refused.status_code == 409 and "50 schedules" in refused.json()["detail"]


async def test_zero_turns_both_limits_off(parts) -> None:
    """``MAX_PER_TENANT=0`` and ``MAX_FIRES_PER_DAY=0``: past where either limit would stand
    nothing is refused, and no fire is counted."""
    async with _served(parts, max_per_tenant=0, max_fires_per_day=0) as served:
        app, client, ticker = served
        sids = [await _due(app, client) for _ in range(3)]
        assert (await ticker.tick()).fired == 3
        assert len(sids) == len(await queued_runs(app))
        async with app.state.engine.connect() as conn:
            assert await conn.scalar(text("SELECT count(*) FROM schedule_fire_counts")) == 0
    many = SchedulesSettings(max_per_tenant=0, max_fires_per_day=0)
    assert many.per_tenant_limit is None and many.fires_per_day_limit is None
    async with _served(parts, max_per_tenant=0) as served:
        _, client, _ = served
        made = [await client.post("/v1/schedules", json=scheduled()) for _ in range(51)]
        assert {r.status_code for r in made} == {201}
