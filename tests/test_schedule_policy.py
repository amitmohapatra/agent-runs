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
from trellis.contracts.runs import ScheduleSpec

from agent_runs.config.settings import SchedulesSettings, Settings
from agent_runs.domain.cadence import jitter
from agent_runs.domain.schedules import TooManySchedules
from agent_runs.store.schedules import ScheduleStore
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
        assert (
            refused.json()["code"] == "SCHEDULE_LIMIT" and "2 schedules" in refused.json()["detail"]
        )
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


# ------------------------------------------------------------------ each limit, its own code


async def test_a_create_past_the_tenants_limit_has_its_own_code_and_says_the_numbers(
    parts,
) -> None:
    """``SCHEDULE_LIMIT``, not ``CONFLICT``: a caller tells "you have too many" from a lost
    race by the code alone, and reads the limit and the tenant's count from ``details``. The
    status stays ``409``."""
    async with _served(parts, max_per_tenant=2) as served:
        _, client, _ = served
        for _ in range(2):
            assert (await client.post("/v1/schedules", json=scheduled())).status_code == 201
        refused = await client.post("/v1/schedules", json=scheduled())
        assert refused.status_code == 409, refused.text
        problem = refused.json()
        assert problem["code"] == "SCHEDULE_LIMIT"
        assert problem["type"] == "urn:trellis:problem:schedule-limit"
        assert problem["details"] == {"max_per_tenant": 2, "schedule_count": 2}
        assert problem["retryable"] is False and "2 schedules" in problem["detail"]


async def test_the_schedule_limit_counts_what_the_tenant_really_has(app, client) -> None:
    """A limit lowered under what a tenant already has: the count is the real one."""
    for _ in range(3):
        assert (await client.post("/v1/schedules", json=scheduled())).status_code == 201
    async with app.state.sessions() as db:
        lowered = ScheduleStore(db, SchedulesSettings(max_per_tenant=2))
        with pytest.raises(TooManySchedules) as refused:
            await lowered.upsert(
                ScheduleSpec.model_validate(scheduled()),
                created_by="user_ada",
                now=datetime.now(UTC),
            )
    assert refused.value.code == "SCHEDULE_LIMIT"
    assert refused.value.details == {"max_per_tenant": 2, "schedule_count": 3}


async def test_a_fire_by_hand_past_the_day_has_its_own_code_and_says_the_numbers(parts) -> None:
    """``POST /fire`` past ``MAX_FIRES_PER_DAY``: ``409 FIRES_PER_DAY``, the code the schedule
    records in ``last_error``, with the limit, the fires the tenant used and the UTC day in
    ``details``; the same numbers are in ``last_error.details``."""
    async with _served(parts, max_fires_per_day=1) as served:
        _, client, _ = served
        sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
            "schedule_id"
        ]
        assert (
            await client.post(f"/v1/schedules/{sid}/fire", json={"at": _last_tick().isoformat()})
        ).status_code == 200
        earlier = (_last_tick() - timedelta(hours=1)).isoformat()
        over = await client.post(f"/v1/schedules/{sid}/fire", json={"at": earlier})
        assert over.status_code == 409, over.text
        problem = over.json()
        day = datetime.now(UTC).date().isoformat()
        assert problem["code"] == "FIRES_PER_DAY" and problem["retryable"] is False
        assert problem["type"] == "urn:trellis:problem:fires-per-day"
        numbers = {"max_fires_per_day": 1, "fires_today": 1, "day": day}
        assert {k: problem["details"][k] for k in numbers} == numbers
        recorded = (await client.get(f"/v1/schedules/{sid}")).json()["last_error"]
        assert recorded["code"] == "FIRES_PER_DAY"
        assert recorded["details"] == numbers
        assert problem["details"]["error"] == recorded


async def test_a_tick_refused_by_the_day_records_the_numbers(parts) -> None:
    async with _served(parts, max_fires_per_day=1) as served:
        app, client, ticker = served
        await _due(app, client)
        await _due(app, client)
        now = datetime.now(UTC)
        assert (await ticker.tick(now=now)).fired == 1
        errors = [s["last_error"] for s in (await client.get("/v1/schedules")).json()]
        [refused] = [e for e in errors if e is not None]
        day = now.date().isoformat()
        assert refused["code"] == "FIRES_PER_DAY"
        assert refused["details"] == {"max_fires_per_day": 1, "fires_today": 1, "day": day}


@pytest.mark.parametrize(
    ("cadence", "gap"), [("*/5 * * * *", 300), ("0,30 * * * *", 1800), ("0 * * * * *", 1)]
)
async def test_a_cadence_under_the_floor_has_its_own_code_and_says_the_numbers(
    client, cadence: str, gap: int
) -> None:
    """``422 CADENCE_TOO_FREQUENT``, with the floor and the cadence's real shortest gap in
    seconds, on a create and on an update."""
    refused = await client.post("/v1/schedules", json=scheduled(cadence=cadence))
    assert refused.status_code == 422, refused.text
    problem = refused.json()
    assert problem["code"] == "CADENCE_TOO_FREQUENT"
    assert problem["type"] == "urn:trellis:problem:cadence-too-frequent"
    assert problem["details"] == {"min_interval_seconds": 3600, "interval_seconds": gap}
    sid = (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
    patched = await client.patch(f"/v1/schedules/{sid}", json={"cadence": cadence})
    assert (patched.status_code, patched.json()["code"]) == (422, "CADENCE_TOO_FREQUENT")
    assert patched.json()["details"] == problem["details"]


async def test_the_cadence_floor_in_the_details_is_the_deployments(parts) -> None:
    async with _served(parts, min_interval_seconds=900) as served:
        _, client, _ = served
        refused = (await client.post("/v1/schedules", json=scheduled(cadence="*/5 * * * *"))).json()
        assert refused["details"] == {"min_interval_seconds": 900, "interval_seconds": 300}


async def test_a_cadence_that_is_not_one_stays_a_validation_error(client) -> None:
    refused = await client.post("/v1/schedules", json=scheduled(cadence="every so often"))
    assert (refused.status_code, refused.json()["code"]) == (422, "VALIDATION")


# ------------------------------------------------------------------ the policy, read


async def test_the_policy_is_the_deployments_defaults_and_the_tenants_numbers(app, client) -> None:
    """``GET /v1/schedules/policy``: the limits as facts, and where the tenant stands."""
    day = datetime.now(UTC).date().isoformat()
    empty = await client.get("/v1/schedules/policy")
    assert empty.status_code == 200, empty.text
    assert empty.json() == {
        "min_interval_seconds": 3600,
        "jitter_seconds": 60,
        "max_failed_fires": 3,
        "max_per_tenant": 50,
        "max_fires_per_day": 500,
        "schedule_count": 0,
        "fires_today": 0,
        "day": day,
    }
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    await client.post("/v1/schedules", json=scheduled())
    assert (await client.post(f"/v1/schedules/{sid}/fire")).status_code == 200
    now = (await client.get("/v1/schedules/policy")).json()
    assert (now["schedule_count"], now["fires_today"]) == (2, 1)


async def test_the_policy_is_the_deployments_and_a_limit_off_is_null(parts) -> None:
    policy = {
        "min_interval_seconds": 900,
        "jitter_seconds": 5,
        "max_failed_fires": 7,
        "max_per_tenant": 0,
        "max_fires_per_day": 0,
    }
    async with _served(parts, **policy) as served:
        _, client, _ = served
        await client.post("/v1/schedules", json=scheduled())
        read = (await client.get("/v1/schedules/policy")).json()
        assert read == {
            **policy,
            "max_per_tenant": None,
            "max_fires_per_day": None,
            "schedule_count": 1,
            "fires_today": None,  # nothing is counted while the limit is off
            "day": datetime.now(UTC).date().isoformat(),
        }


async def test_the_policy_counts_only_the_callers_tenant_and_only_today(app, client) -> None:
    await client.post("/v1/schedules", json=scheduled())
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO schedule_fire_counts (tenant_id, day, fires) VALUES (:t, :d, 9)"),
            {"t": "acme", "d": yesterday},
        )
    async with client_of(app, "other-key") as globex:
        theirs = (await globex.get("/v1/schedules/policy")).json()
        assert (theirs["schedule_count"], theirs["fires_today"]) == (0, 0)
    ours = (await client.get("/v1/schedules/policy")).json()
    assert (ours["schedule_count"], ours["fires_today"]) == (1, 0)


async def test_the_policy_needs_a_key_and_a_platform_key_names_the_tenant(app) -> None:
    async with client_of(app, "no-such-key") as stranger:
        assert (await stranger.get("/v1/schedules/policy")).status_code == 401
    async with client_of(app, "platform-key") as platform:
        assert (await platform.get("/v1/schedules/policy")).status_code == 400
        named = await platform.get("/v1/schedules/policy", headers={"X-Trellis-Tenant": "acme"})
        assert named.status_code == 200 and named.json()["max_per_tenant"] == 50
