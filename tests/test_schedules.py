"""Schedules end to end over HTTP and through the ticker, against the real database.

Every test is a failure mode an unattended scheduler has to survive: two tickers on one
tick, a request trying to name its own identity, a database refusing the run, a schedule
failing forever with nobody watching, one tenant reaching for another's schedules.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from trellis.contracts.errors import AgentError
from trellis.contracts.ids import now
from trellis.contracts.runs import ScheduleSpec

from agent_runs.domain.errors import NotFound
from agent_runs.domain.schedules import DuplicateSchedule
from agent_runs.store.schedules import ScheduleStore
from tests.conftest import arm, queued_runs, scheduled


def _last_tick() -> datetime:
    """The most recent hourly boundary: an instant that has arrived."""
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)


def _instant(attempt: int) -> str:
    """A distinct past fire time per attempt, so a retry is a new tick."""
    return (_last_tick() - timedelta(hours=attempt)).isoformat()


async def _make_due(app, schedule_id: str) -> datetime:
    return await arm(
        app, schedule_id, (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0)
    )


async def test_a_schedule_is_created_armed_for_its_next_occurrence(client) -> None:
    response = await client.post("/v1/schedules", json=scheduled(input={"topic": "inbox"}))
    assert response.status_code == 201
    created = response.json()
    assert created["enabled"] is True
    assert created["consecutive_failures"] == 0
    assert created["next_fire_at"] is not None
    assert created["created_by"] == "user_ada"
    assert created["schedule_id"].startswith("sch_")
    fetched = (await client.get(f"/v1/schedules/{created['schedule_id']}")).json()
    assert fetched == created


async def test_the_same_schedule_and_fire_time_never_queue_two_runs(app, client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    at = _last_tick().isoformat()
    first = (await client.post(f"/v1/schedules/{sid}/fire", json={"at": at})).json()
    second = (await client.post(f"/v1/schedules/{sid}/fire", json={"at": at})).json()
    assert first["run_id"] == second["run_id"]
    assert first["idempotency_key"] == second["idempotency_key"]
    assert len(await queued_runs(app)) == 1


async def test_a_bare_fire_uses_the_tick_it_is_due_for(app, client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    due_at = await _make_due(app, sid)
    bare = (await client.post(f"/v1/schedules/{sid}/fire")).json()
    assert datetime.fromisoformat(bare["fire_time"]) == due_at
    explicit = await client.post(f"/v1/schedules/{sid}/fire", json={"at": due_at.isoformat()})
    assert explicit.json()["run_id"] == bare["run_id"]
    assert len(await queued_runs(app)) == 1


async def test_a_fired_run_is_queued_as_the_schedules_own_identity(app, client) -> None:
    schedule = (
        await client.post(
            "/v1/schedules",
            json=scheduled(on_behalf_of="user_ada", workspace_id="ws1", input={"topic": "x"}),
        )
    ).json()
    fired = (await client.post(f"/v1/schedules/{schedule['schedule_id']}/fire")).json()

    run = (await client.get(f"/v1/runs/{fired['run_id']}")).json()
    assert run["status"] == "QUEUED"
    assert (run["on_behalf_of"], run["tenant_id"], run["agent_id"]) == (
        "user_ada",
        "acme",
        "briefing",
    )
    assert run["workspace_id"] == "ws1"
    assert run["input"] == {"topic": "x"}
    assert run["metadata"]["schedule_id"] == schedule["schedule_id"]
    assert run["metadata"]["created_by"] == "user_ada"
    assert run["idempotency_key"] == fired["idempotency_key"]


async def test_a_fire_sends_todays_instruction_not_the_one_frozen_at_creation(app, client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(input={"topic": "inbox"}))).json()[
        "schedule_id"
    ]
    await client.patch(f"/v1/schedules/{sid}", json={"input": {"topic": "calendar"}})
    await client.post(f"/v1/schedules/{sid}/fire")
    assert (await queued_runs(app))[-1]["input"] == {"topic": "calendar"}


async def test_a_fired_run_takes_the_schedules_working_time_limit_and_agent_version(
    app, client
) -> None:
    body = scheduled(timeout_seconds=600, agent_version="2026.10.05-3f2a1c")
    schedule = (await client.post("/v1/schedules", json=body)).json()
    assert (schedule["timeout_seconds"], schedule["agent_version"]) == (600, "2026.10.05-3f2a1c")
    fired = (await client.post(f"/v1/schedules/{schedule['schedule_id']}/fire")).json()
    run = (await client.get(f"/v1/runs/{fired['run_id']}")).json()
    assert (run["timeout_seconds"], run["agent_version"]) == (600, "2026.10.05-3f2a1c")
    # an update changes what the next fire copies; null removes it
    sid = schedule["schedule_id"]
    changed = await client.patch(
        f"/v1/schedules/{sid}", json={"timeout_seconds": None, "agent_version": "v8"}
    )
    assert (changed.json()["timeout_seconds"], changed.json()["agent_version"]) == (None, "v8")
    again = (await client.post(f"/v1/schedules/{sid}/fire")).json()
    run = (await client.get(f"/v1/runs/{again['run_id']}")).json()
    assert (run["timeout_seconds"], run["agent_version"]) == (None, "v8")
    refused = await client.patch(f"/v1/schedules/{sid}", json={"timeout_seconds": 0})
    assert refused.status_code == 422
    # a schedule that sets neither fires runs that carry neither
    plain = (await client.post("/v1/schedules", json=scheduled())).json()
    fired = (await client.post(f"/v1/schedules/{plain['schedule_id']}/fire")).json()
    run = (await client.get(f"/v1/runs/{fired['run_id']}")).json()
    assert run["timeout_seconds"] is None and run["agent_version"] is None


async def test_a_fired_run_takes_the_schedules_queue_order_and_metadata(app, client) -> None:
    # what a started run can carry, a scheduled one carries: the harness keeps a run's
    # selection and framework options in its metadata
    meta = {"selection": {"without": ["memory"]}, "schedule_id": "sch_forged", "team": "ops"}
    body = scheduled(priority=10, concurrency_key="digest:acme", metadata=meta)
    schedule = (await client.post("/v1/schedules", json=body)).json()
    assert (schedule["priority"], schedule["concurrency_key"]) == (10, "digest:acme")
    sid = schedule["schedule_id"]
    fired = (await client.post(f"/v1/schedules/{sid}/fire")).json()
    run = (await client.get(f"/v1/runs/{fired['run_id']}")).json()
    assert (run["priority"], run["concurrency_key"]) == (10, "digest:acme")
    assert run["metadata"]["selection"] == {"without": ["memory"]}
    assert run["metadata"]["team"] == "ops"
    # the fire's own keys win: a schedule cannot pass its run off as another's fire
    assert run["metadata"]["schedule_id"] == sid
    assert run["metadata"]["created_by"] == "user_ada"
    # an update changes what the next fire copies; null removes the key
    changed = await client.patch(
        f"/v1/schedules/{sid}", json={"priority": -5, "concurrency_key": None}
    )
    assert (changed.json()["priority"], changed.json()["concurrency_key"]) == (-5, None)
    again = (await client.post(f"/v1/schedules/{sid}/fire")).json()
    run = (await client.get(f"/v1/runs/{again['run_id']}")).json()
    assert (run["priority"], run["concurrency_key"]) == (-5, None)
    for refused in ({"priority": 1001}, {"concurrency_key": ""}, {"priority": None}):
        assert (await client.patch(f"/v1/schedules/{sid}", json=refused)).status_code == 422
    assert (await client.post("/v1/schedules", json=scheduled(priority=-1001))).status_code == 422
    # a schedule that sets neither fires runs at priority 0 with no key
    plain = (await client.post("/v1/schedules", json=scheduled())).json()
    fired = (await client.post(f"/v1/schedules/{plain['schedule_id']}/fire")).json()
    run = (await client.get(f"/v1/runs/{fired['run_id']}")).json()
    assert run["priority"] == 0 and run["concurrency_key"] is None


async def test_on_behalf_of_is_required_and_not_blank(client) -> None:
    body = scheduled()
    body.pop("on_behalf_of")
    assert (await client.post("/v1/schedules", json=body)).status_code == 422
    assert (await client.post("/v1/schedules", json=scheduled(on_behalf_of=" "))).status_code == 422


async def test_an_update_cannot_change_on_behalf_of_or_tenant(client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
    for field, value in (("on_behalf_of", "user_root"), ("tenant_id", "globex")):
        assert (await client.patch(f"/v1/schedules/{sid}", json={field: value})).status_code == 422
    assert (await client.get(f"/v1/schedules/{sid}")).json()["on_behalf_of"] == "user_ada"


async def test_a_fire_request_cannot_name_an_identity(app, client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
    refused = await client.post(
        f"/v1/schedules/{sid}/fire", json={"on_behalf_of": "user_root", "agent_id": "shell"}
    )
    assert refused.status_code == 422
    assert await queued_runs(app) == []


async def test_a_paused_or_disabled_schedule_does_not_fire(app, client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
    assert (await client.patch(f"/v1/schedules/{sid}", json={"enabled": False})).json()[
        "enabled"
    ] is False
    assert (await client.post(f"/v1/schedules/{sid}/fire")).status_code == 409
    disabled = (await client.post("/v1/schedules", json=scheduled(enabled=False))).json()
    assert (await client.post(f"/v1/schedules/{disabled['schedule_id']}/fire")).status_code == 409
    assert await queued_runs(app) == []


async def test_a_paused_schedule_is_not_due(app, client, ticker) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    await client.patch(f"/v1/schedules/{sid}", json={"enabled": False})
    assert (await ticker.tick(now=datetime.now(UTC) + timedelta(days=30))).fired == 0


async def test_resuming_looks_forward_instead_of_replaying_the_backlog(client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    await client.patch(f"/v1/schedules/{sid}", json={"enabled": False})
    resumed = (await client.patch(f"/v1/schedules/{sid}", json={"enabled": True})).json()
    assert resumed["enabled"] is True
    assert datetime.fromisoformat(resumed["next_fire_at"]) > datetime.now(UTC)


# ------------------------------------------------------------------ failures


async def test_repeated_failures_auto_pause_the_schedule(client, broken) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    broken.fail()
    for attempt in range(1, 4):
        failed = await client.post(f"/v1/schedules/{sid}/fire", json={"at": _instant(attempt)})
        assert failed.status_code == 503
    paused = (await client.get(f"/v1/schedules/{sid}")).json()
    assert (paused["consecutive_failures"], paused["enabled"]) == (3, False)
    broken.heal()
    assert (await client.post(f"/v1/schedules/{sid}/fire")).status_code == 409


async def test_a_successful_fire_resets_the_failure_counter(client, broken) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    broken.fail()
    for attempt in range(1, 3):
        await client.post(f"/v1/schedules/{sid}/fire", json={"at": _instant(attempt)})
    assert (await client.get(f"/v1/schedules/{sid}")).json()["consecutive_failures"] == 2
    broken.heal()
    assert (
        await client.post(f"/v1/schedules/{sid}/fire", json={"at": _instant(3)})
    ).status_code == 200
    healthy = (await client.get(f"/v1/schedules/{sid}")).json()
    assert (healthy["consecutive_failures"], healthy["last_error"], healthy["enabled"]) == (
        0,
        None,
        True,
    )


async def test_resuming_clears_the_failure_count_that_caused_the_auto_pause(client, broken) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    broken.fail()
    for attempt in range(1, 4):
        await client.post(f"/v1/schedules/{sid}/fire", json={"at": _instant(attempt)})
    resumed = (await client.patch(f"/v1/schedules/{sid}", json={"enabled": True})).json()
    assert (resumed["consecutive_failures"], resumed["last_error"]) == (0, None)


async def test_a_failed_fire_is_recorded_loudly_and_does_not_advance(client, broken) -> None:
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    broken.fail()
    failed = await client.post(f"/v1/schedules/{sid}/fire")
    assert failed.status_code == 503
    problem = failed.json()
    assert (problem["code"], problem["retryable"]) == ("DEPENDENCY_UNAVAILABLE", True)
    assert failed.headers["retry-after"].isdigit()
    assert "INSERT" not in problem["detail"], "the database's words stay in details"
    detail = problem["details"]
    assert (detail["consecutive_failures"], detail["auto_paused"]) == (1, False)

    recorded = (await client.get(f"/v1/schedules/{sid}")).json()
    assert recorded["last_error"]["category"] == "DEPENDENCY"
    assert recorded["last_error"]["code"] == "OperationalError"
    assert recorded["last_fired_at"] is None and recorded["last_run_id"] is None
    assert recorded["next_fire_at"] == schedule["next_fire_at"]


async def test_a_refusal_that_will_not_change_its_mind_pauses_at_once(client, broken) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    broken.fail(retryable=False)
    failed = await client.post(f"/v1/schedules/{sid}/fire")
    assert failed.json()["details"]["auto_paused"] is True
    assert failed.json()["retryable"] is False, "a paused schedule will not fire on a repeat"
    assert "retry-after" not in failed.headers
    paused = (await client.get(f"/v1/schedules/{sid}")).json()
    assert (paused["enabled"], paused["consecutive_failures"]) == (False, 1)
    assert paused["last_error"]["retryable"] is False


async def test_a_retryable_failure_backs_off_on_the_schedules_clock(
    app, client, broken, ticker
) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    await _make_due(app, sid)
    broken.fail()
    assert (await ticker.tick()).fired == 0
    assert broken.calls == 1, "a failed fire is not retried within the same tick"

    backing_off = (await client.get(f"/v1/schedules/{sid}")).json()
    assert backing_off["enabled"] is True and backing_off["retry_after"] is not None
    await ticker.tick()
    assert broken.calls == 1, "nor on the next tick before retry_after"

    broken.heal()
    later = await ticker.tick(now=datetime.now(UTC) + timedelta(minutes=30))
    assert later.fired == 1


# ------------------------------------------------------------------ timing


async def test_a_successful_fire_advances_the_schedule(client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    at = _last_tick()
    fired = (await client.post(f"/v1/schedules/{sid}/fire", json={"at": at.isoformat()})).json()
    assert datetime.fromisoformat(fired["schedule"]["last_fired_at"]) == at
    assert fired["schedule"]["last_run_id"] == fired["run_id"]
    assert datetime.fromisoformat(fired["schedule"]["next_fire_at"]) == at + timedelta(hours=1)


async def test_a_per_minute_cadence_is_refused_at_creation_and_on_update(client) -> None:
    refused = await client.post("/v1/schedules", json=scheduled(cadence="*/5 * * * *"))
    assert refused.status_code == 422 and "per-minute" in refused.text
    sid = (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
    assert (
        await client.patch(f"/v1/schedules/{sid}", json={"cadence": "* * * * *"})
    ).status_code == 422


async def test_an_unknown_timezone_is_refused(client) -> None:
    assert (
        await client.post("/v1/schedules", json=scheduled(timezone="Mars/Olympus_Mons"))
    ).status_code == 422


async def test_changing_the_cadence_rearms_the_schedule(client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="daily"))).json()[
        "schedule_id"
    ]
    updated = (await client.patch(f"/v1/schedules/{sid}", json={"cadence": "0 9 * * 3"})).json()
    rearmed = datetime.fromisoformat(updated["next_fire_at"])
    assert (rearmed.weekday(), rearmed.hour, rearmed.minute) == (2, 9, 0)


async def test_a_manual_schedule_never_comes_due_but_can_be_fired(app, client, ticker) -> None:
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="manual"))).json()
    assert schedule["next_fire_at"] is None
    assert (await ticker.tick(now=datetime.now(UTC) + timedelta(days=400))).fired == 0
    assert (await client.post(f"/v1/schedules/{schedule['schedule_id']}/fire")).status_code == 200
    assert len(await queued_runs(app)) == 1


async def test_the_ticker_fires_only_what_has_come_due(app, client, ticker) -> None:
    overdue = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    await client.post("/v1/schedules", json=scheduled(cadence="weekly"))
    await _make_due(app, overdue["schedule_id"])
    assert (await ticker.tick()).fired == 1
    [run] = await queued_runs(app)
    assert run["run_metadata"]["schedule_id"] == overdue["schedule_id"]


async def test_a_fire_for_a_stale_instant_does_not_rewind_the_schedule(app, client, ticker) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    stale = _last_tick() - timedelta(days=30)
    fired = await client.post(f"/v1/schedules/{sid}/fire", json={"at": stale.isoformat()})
    assert fired.status_code == 200
    assert datetime.fromisoformat(fired.json()["schedule"]["next_fire_at"]) > datetime.now(UTC)
    for _ in range(3):
        assert (await ticker.tick()).fired == 0
    assert len(await queued_runs(app)) == 1


async def test_a_missed_tick_is_fired_once_rather_than_replayed(app, client, ticker) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    await arm(app, sid, _last_tick() - timedelta(hours=6))
    assert (await ticker.tick()).fired == 1
    assert (await ticker.tick()).fired == 0
    assert len(await queued_runs(app)) == 1


async def test_a_fire_for_an_instant_that_has_not_arrived_is_refused(app, client) -> None:
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="daily"))).json()
    future = (datetime.now(UTC) + timedelta(days=365 * 5)).isoformat()
    refused = await client.post(
        f"/v1/schedules/{schedule['schedule_id']}/fire", json={"at": future}
    )
    assert refused.status_code == 422
    assert await queued_runs(app) == []
    untouched = (await client.get(f"/v1/schedules/{schedule['schedule_id']}")).json()
    assert untouched["next_fire_at"] == schedule["next_fire_at"]


async def test_a_fire_tolerates_a_clock_a_little_ahead(client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    just_ahead = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
    assert (
        await client.post(f"/v1/schedules/{sid}/fire", json={"at": just_ahead})
    ).status_code == 200


async def test_a_naive_fire_instant_is_refused(client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
    naive = await client.post(f"/v1/schedules/{sid}/fire", json={"at": "2026-09-21T09:00:00"})
    assert naive.status_code == 422


async def test_an_update_that_changes_no_timing_field_keeps_the_pending_occurrence(
    app, client, ticker
) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    due_at = await _make_due(app, sid)
    renamed = await client.patch(
        f"/v1/schedules/{sid}",
        json={"name": "renamed nightly", "cadence": "hourly", "timezone": "UTC", "enabled": True},
    )
    assert renamed.status_code == 200
    assert datetime.fromisoformat(renamed.json()["next_fire_at"]) == due_at
    assert (await ticker.tick()).fired == 1


async def test_resuming_keeps_an_occurrence_it_can_still_honour(app, client, ticker) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    missed = await arm(app, sid, _last_tick())
    await client.patch(f"/v1/schedules/{sid}", json={"enabled": False})
    resumed = (await client.patch(f"/v1/schedules/{sid}", json={"enabled": True})).json()
    assert datetime.fromisoformat(resumed["next_fire_at"]) == missed
    assert (await ticker.tick()).fired == 1


async def test_resuming_looks_past_an_occurrence_a_later_one_superseded(app, client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()[
        "schedule_id"
    ]
    await arm(app, sid, _last_tick() - timedelta(days=30))
    await client.patch(f"/v1/schedules/{sid}", json={"enabled": False})
    resumed = (await client.patch(f"/v1/schedules/{sid}", json={"enabled": True})).json()
    assert datetime.fromisoformat(resumed["next_fire_at"]) > datetime.now(UTC)


# ------------------------------------------------------------------ records and tenants


async def test_listing_filters_by_agent_and_by_enabled(client) -> None:
    mine = (await client.post("/v1/schedules", json=scheduled(agent_id="briefing"))).json()
    other = (await client.post("/v1/schedules", json=scheduled(agent_id="billing"))).json()
    await client.patch(f"/v1/schedules/{other['schedule_id']}", json={"enabled": False})
    briefing = (await client.get("/v1/schedules", params={"agent_id": "briefing"})).json()
    assert [s["schedule_id"] for s in briefing] == [mine["schedule_id"]]
    paused = (await client.get("/v1/schedules", params={"enabled": False})).json()
    assert [s["schedule_id"] for s in paused] == [other["schedule_id"]]


async def test_a_repeated_create_returns_the_schedule_it_made(client) -> None:
    """The create is an upsert on (agent_id, on_behalf_of, cadence, input): a redeploy gets
    200 and the existing schedule, unchanged, never a second one firing."""
    body = scheduled(name="nightly digest", input={"a": 1, "b": [1, 2]})
    first = await client.post("/v1/schedules", json=body)
    assert first.status_code == 201
    again = await client.post(
        "/v1/schedules", json={**body, "name": "renamed", "input": {"b": [1, 2], "a": 1}}
    )
    assert again.status_code == 200
    assert again.json()["schedule_id"] == first.json()["schedule_id"]
    assert again.json()["name"] == "nightly digest"
    assert len((await client.get("/v1/schedules")).json()) == 1


async def test_concurrent_creates_of_one_schedule_make_one(client) -> None:
    body = scheduled()
    answers = await asyncio.gather(*(client.post("/v1/schedules", json=body) for _ in range(5)))
    assert sorted(r.status_code for r in answers) == [200, 200, 200, 200, 201]
    assert len({r.json()["schedule_id"] for r in answers}) == 1


async def test_any_part_of_the_identity_makes_another_schedule(client) -> None:
    body = scheduled(input={"topic": "inbox"})
    base = (await client.post("/v1/schedules", json=body)).json()["schedule_id"]
    for change in (
        {"input": {"topic": "calendar"}},
        {"cadence": "hourly"},
        {"agent_id": "digest"},
        {"on_behalf_of": "user_bob"},
    ):
        other = await client.post("/v1/schedules", json={**body, **change})
        assert other.status_code == 201, change
        assert other.json()["schedule_id"] != base
    assert (await client.post("/v1/schedules", json=scheduled(name=body["name"]))).status_code == (
        201
    )  # a name is a label, not an identity


async def test_an_update_onto_another_schedules_identity_is_refused(client) -> None:
    a = (await client.post("/v1/schedules", json=scheduled(input={"x": 1}))).json()
    b = (await client.post("/v1/schedules", json=scheduled(input={"x": 2}))).json()
    clash = await client.patch(f"/v1/schedules/{b['schedule_id']}", json={"input": {"x": 1}})
    assert clash.status_code == 409
    assert (await client.get(f"/v1/schedules/{b['schedule_id']}")).json()["input"] == {"x": 2}
    assert a["schedule_id"] != b["schedule_id"]


async def test_there_are_no_pause_or_resume_routes(client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
    for verb in ("pause", "resume"):
        assert (await client.post(f"/v1/schedules/{sid}/{verb}")).status_code in {404, 405}


async def test_two_tenants_may_use_the_same_schedule_name(client, other_tenant) -> None:
    body = scheduled(name="nightly")
    assert (await client.post("/v1/schedules", json=body)).status_code == 201
    theirs = await other_tenant.post("/v1/schedules", json={**body, "tenant_id": "globex"})
    assert theirs.status_code == 201


async def test_one_tenant_cannot_read_or_fire_another_tenants_schedule(
    app, client, other_tenant
) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
    assert (await other_tenant.get(f"/v1/schedules/{sid}")).status_code == 404
    assert (await other_tenant.post(f"/v1/schedules/{sid}/fire")).status_code == 404
    assert (await other_tenant.delete(f"/v1/schedules/{sid}")).status_code == 404
    assert await queued_runs(app) == []


async def test_creating_a_schedule_for_another_tenant_is_refused(client) -> None:
    assert (
        await client.post("/v1/schedules", json=scheduled(tenant_id="globex"))
    ).status_code == 403


async def test_a_deleted_schedule_stops_existing(client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
    assert (await client.delete(f"/v1/schedules/{sid}")).status_code == 204
    assert (await client.get(f"/v1/schedules/{sid}")).status_code == 404
    assert (await client.delete(f"/v1/schedules/{sid}")).status_code == 404


# ------------------------------------------------------------------ who may act as whom


async def test_a_schedule_cannot_be_created_as_a_principal_the_key_may_not_be(app, narrow) -> None:
    refused = await narrow.post("/v1/schedules", json=scheduled(on_behalf_of="user_root"))
    assert refused.status_code == 403
    mine = await narrow.post("/v1/schedules", json=scheduled(on_behalf_of="user_bob"))
    assert mine.status_code == 201
    assert (
        await narrow.post(f"/v1/schedules/{mine.json()['schedule_id']}/fire")
    ).status_code == 200
    assert [r["on_behalf_of"] for r in await queued_runs(app)] == ["user_bob"]


async def test_created_by_comes_from_the_key_not_the_body(narrow) -> None:
    refused = await narrow.post(
        "/v1/schedules", json=scheduled(on_behalf_of="user_bob", created_by="user_root")
    )
    assert refused.status_code == 422
    created = (await narrow.post("/v1/schedules", json=scheduled(on_behalf_of="user_bob"))).json()
    assert created["created_by"] == "user_bob"


async def test_a_key_cannot_repoint_a_schedule_that_runs_as_someone_else(
    app, client, narrow
) -> None:
    victim = (
        await client.post("/v1/schedules", json=scheduled(on_behalf_of="user_ada", input={"t": 1}))
    ).json()
    sid = victim["schedule_id"]
    repoint = {"agent_id": "shell", "input": {"command": "exfiltrate"}}
    assert (await narrow.patch(f"/v1/schedules/{sid}", json=repoint)).status_code == 403
    assert (await narrow.post(f"/v1/schedules/{sid}/fire")).status_code == 403
    assert (await narrow.patch(f"/v1/schedules/{sid}", json={"enabled": False})).status_code == 403
    assert (await narrow.delete(f"/v1/schedules/{sid}")).status_code == 403
    assert await queued_runs(app) == []
    untouched = (await client.get(f"/v1/schedules/{sid}")).json()
    assert (untouched["agent_id"], untouched["input"], untouched["enabled"]) == (
        "briefing",
        {"t": 1},
        True,
    )


async def test_a_platform_key_administers_a_tenants_schedules(client, platform) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
    assert (
        await platform.patch(f"/v1/schedules/{sid}", json={"enabled": False})
    ).status_code == 200


# ------------------------------------------------------------------ updates, edges, unknowns


async def test_an_update_merges_metadata_and_changes_only_what_it_sends(client) -> None:
    created = (
        await client.post("/v1/schedules", json=scheduled(metadata={"team": "ops", "tier": 1}))
    ).json()
    sid = created["schedule_id"]
    updated = (
        await client.patch(
            f"/v1/schedules/{sid}", json={"name": "renamed", "metadata": {"tier": 2, "x": True}}
        )
    ).json()
    assert updated["name"] == "renamed"
    assert updated["metadata"] == {"team": "ops", "tier": 2, "x": True}
    unchanged = {"agent_id", "cadence", "timezone", "input", "on_behalf_of", "next_fire_at"}
    assert {k: updated[k] for k in unchanged} == {k: created[k] for k in unchanged}


async def test_an_update_the_contracts_refuse_is_422_and_changes_nothing(client) -> None:
    created = (await client.post("/v1/schedules", json=scheduled())).json()
    sid = created["schedule_id"]
    response = await client.patch(f"/v1/schedules/{sid}", json={"timezone": "Mars/Olympus_Mons"})
    assert response.status_code == 422
    assert (await client.get(f"/v1/schedules/{sid}")).json() == created


async def test_resuming_a_manual_schedule_arms_nothing(client) -> None:
    sid = (await client.post("/v1/schedules", json=scheduled(cadence="manual"))).json()[
        "schedule_id"
    ]
    await client.patch(f"/v1/schedules/{sid}", json={"enabled": False})
    resumed = (await client.patch(f"/v1/schedules/{sid}", json={"enabled": True})).json()
    assert (resumed["enabled"], resumed["next_fire_at"]) == (True, None)


async def test_every_verb_on_an_unknown_schedule_is_404(client) -> None:
    assert (await client.get("/v1/schedules/sch_nope")).status_code == 404
    assert (
        await client.patch("/v1/schedules/sch_nope", json={"enabled": False})
    ).status_code == 404
    assert (await client.post("/v1/schedules/sch_nope/fire")).status_code == 404
    assert (await client.delete("/v1/schedules/sch_nope")).status_code == 404


async def test_a_listing_outside_its_bounds_is_refused(client) -> None:
    for limit in (0, 501):
        assert (await client.get("/v1/schedules", params={"limit": limit})).status_code == 422


async def test_a_create_racing_a_delete_of_the_same_identity_is_a_conflict(
    app, client, monkeypatch
) -> None:
    """The insert meets the existing schedule, which is deleted before it can be read back:
    vanishingly rare, and answered as a conflict rather than an empty 200."""
    body = scheduled()
    existing = (await client.post("/v1/schedules", json=body)).json()
    async with app.state.sessions() as db:
        real = db.scalar
        calls = 0

        async def scalar(statement, *args, **kwargs):
            nonlocal calls
            calls += 1
            result = await real(statement, *args, **kwargs)
            if calls == 1:
                async with app.state.engine.begin() as conn:
                    await conn.execute(
                        text("DELETE FROM agent_schedules WHERE schedule_id = :s"),
                        {"s": existing["schedule_id"]},
                    )
            return result

        monkeypatch.setattr(db, "scalar", scalar)
        with pytest.raises(DuplicateSchedule):
            await ScheduleStore(db).upsert(
                ScheduleSpec.model_validate(body), created_by="user_ada", now=now()
            )


async def test_bookkeeping_for_a_schedule_that_is_gone_is_not_found(app) -> None:

    async with app.state.sessions() as db:
        store = ScheduleStore(db)
        with pytest.raises(NotFound):
            await store.record_success("sch_gone", fire_time=now(), run_id="run_x", now=now())
        with pytest.raises(NotFound):
            await store.record_failure("sch_gone", error=AgentError(code="x"), now=now())
