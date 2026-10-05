"""A run's working time (``RunRecord.worked_seconds``) and its limit
(``RunStart.timeout_seconds``, or the service's ``RUNS__RUNS__MAX_RUN_SECONDS``, the lesser):
only time ``RUNNING`` counts, across attempts and crashes; the ticker ends a run past its
limit as ``TIMEOUT``, and every lease tells the worker the time it has left."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import text
from trellis.contracts.errors import ErrorCategory
from trellis.contracts.ids import now

from agent_runs.store.runs import RunStore
from tests.conftest import pause, resolution, started

_CLAIM = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 600}


async def _queued(client, **over: Any) -> str:
    response = await client.post("/v1/runs", json=started(queue=True, **over))
    assert response.status_code == 201, response.text
    return response.json()["run_id"]


async def _claimed(client) -> dict[str, Any]:
    response = await client.post("/v1/runs/claim", json=_CLAIM)
    assert response.status_code == 200, response.text
    return response.json()


async def _ran_for(app, run_id: str, seconds: float) -> None:
    """Pretend the run has been RUNNING for ``seconds`` (its stretch began that long ago)."""
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE agent_runs SET running_since = running_since - :d WHERE run_id = :r"),
            {"d": timedelta(seconds=seconds), "r": run_id},
        )


async def _sweep(app, seconds: float, *, max_run_seconds: float | None = None) -> list:
    async with app.state.sessions() as db:
        store = RunStore(db, max_run_seconds=max_run_seconds)
        ended = await store.time_out_overworked(now=now() + timedelta(seconds=seconds), limit=10)
        await db.commit()
    return ended


async def test_the_start_keeps_the_limit_and_the_agent_version(client) -> None:
    run = await client.post(
        "/v1/runs", json=started(timeout_seconds=90.5, agent_version="2026.10.05-3f2a1c")
    )
    assert run.status_code == 201, run.text
    record = run.json()
    assert (record["timeout_seconds"], record["agent_version"]) == (90.5, "2026.10.05-3f2a1c")
    assert record["worked_seconds"] == 0
    read = (await client.get(f"/v1/runs/{record['run_id']}")).json()
    assert (read["timeout_seconds"], read["agent_version"]) == (90.5, "2026.10.05-3f2a1c")


async def test_a_repeated_start_must_ask_for_the_same_limit_but_any_agent_version(client) -> None:
    first = await client.post(
        "/v1/runs", json=started(idempotency_key="k1", timeout_seconds=60, agent_version="v1")
    )
    newer_deploy = await client.post(
        "/v1/runs", json=started(idempotency_key="k1", timeout_seconds=60, agent_version="v2")
    )
    assert newer_deploy.status_code == 200
    assert newer_deploy.json()["run_id"] == first.json()["run_id"]
    assert newer_deploy.json()["agent_version"] == "v1"
    longer = await client.post("/v1/runs", json=started(idempotency_key="k1", timeout_seconds=99))
    assert longer.status_code == 409
    assert longer.json()["details"]["differing"] == ["timeout_seconds"]


async def test_only_time_running_counts_across_attempts(app, client) -> None:
    """Queued and waiting for a person are not work; each RUNNING stretch adds up, and the one
    going on shows on every read."""
    run_id = await _queued(client)
    await _claimed(client)
    await _ran_for(app, run_id, 30)
    waiting = await client.post(
        f"/v1/runs/{run_id}/pause", params={"worker_id": "w1"}, json=pause(run_id)
    )
    assert 30 <= waiting.json()["worked_seconds"] < 31
    await _ran_for(app, run_id, 3600)  # an hour waiting for a person: not counted
    resumed = await client.post(f"/v1/runs/{run_id}/resume", json=resolution(waiting.json()))
    assert resumed.json()["status"] == "QUEUED"
    assert 30 <= resumed.json()["worked_seconds"] < 31

    await _claimed(client)
    await _ran_for(app, run_id, 20)
    live = (await client.get(f"/v1/runs/{run_id}")).json()
    assert (live["attempt"], live["status"]) == (2, "RUNNING")
    assert 50 <= live["worked_seconds"] < 51
    done = await client.post(
        f"/v1/runs/{run_id}/finish", params={"worker_id": "w1"}, json={"status": "SUCCESS"}
    )
    assert 50 <= done.json()["worked_seconds"] < 51


async def test_a_run_past_its_limit_times_out_and_its_worker_is_fenced_off(app, client) -> None:
    run_id = await _queued(client, timeout_seconds=60)
    unlimited = await _queued(client)
    await _claimed(client)
    await _claimed(client)
    assert await _sweep(app, 30) == [], "not past it yet"

    [ended] = await _sweep(app, 61)
    assert (ended.run_id, ended.status.value) == (run_id, "TIMEOUT")
    assert ended.worked_seconds >= 61
    assert ended.error is not None
    assert (ended.error.code, ended.error.category) == ("run_timeout", ErrorCategory.TIMEOUT)
    assert ended.error.retryable is False and "past its limit of 60 s" in ended.error.message
    beat = await client.post(f"/v1/runs/{run_id}/heartbeat", json={"worker_id": "w1"})
    assert (beat.status_code, beat.json()["code"]) == (409, "LEASE_LOST")
    assert (await client.get(f"/v1/runs/{unlimited}")).json()["status"] == "RUNNING"
    assert await _sweep(app, 61) == [], "an ended run is never ended again"


async def test_time_queued_or_waiting_is_never_swept(client, app) -> None:
    await _queued(client, timeout_seconds=1)
    waiting = (await client.post("/v1/runs", json=started(timeout_seconds=1))).json()
    await client.post(f"/v1/runs/{waiting['run_id']}/pause", json=pause(waiting["run_id"]))
    assert await _sweep(app, 3600) == []


async def test_the_clock_survives_a_crash(app, client) -> None:
    """A worker that died after 50 s of a 60 s run: the next attempt has 10 s left."""
    run_id = await _queued(client, timeout_seconds=60)
    await client.post("/v1/runs/claim", json={**_CLAIM, "lease_seconds": 5})
    await _ran_for(app, run_id, 50)
    async with app.state.sessions() as db:
        [requeued] = await RunStore(db).requeue_lapsed(now=now() + timedelta(seconds=6), limit=9)
        await db.commit()
    assert 50 <= requeued.worked_seconds < 57
    again = await _claimed(client)
    assert again["run"]["attempt"] == 2
    assert 3 < again["lease"]["remaining_seconds"] <= 10
    [ended] = await _sweep(app, 11)
    assert (ended.run_id, ended.status.value) == (run_id, "TIMEOUT")


@pytest.mark.parametrize(
    ("own", "platform", "times_out_after"), [(None, 30, 31), (10, 30, 11), (100, 30, 31)]
)
async def test_the_service_maximum_bounds_every_run(
    app, client, own: float | None, platform: float, times_out_after: float
) -> None:
    run_id = await _queued(client, timeout_seconds=own)
    await _claimed(client)
    assert await _sweep(app, times_out_after - 2, max_run_seconds=platform) == []
    [ended] = await _sweep(app, times_out_after, max_run_seconds=platform)
    assert ended.run_id == run_id and ended.error is not None
    assert f"past its limit of {min(own or platform, platform):g} s" in ended.error.message


async def test_every_lease_says_the_working_time_left(app, client) -> None:
    limited = await _queued(client, timeout_seconds=60)
    claimed = await _claimed(client)
    assert claimed["run"]["run_id"] == limited
    assert 59 < claimed["lease"]["remaining_seconds"] <= 60
    await _ran_for(app, limited, 20)
    beat = await client.post(f"/v1/runs/{limited}/heartbeat", json={"worker_id": "w1"})
    assert 39 < beat.json()["remaining_seconds"] <= 40
    await _ran_for(app, limited, 600)
    late = await client.post(f"/v1/runs/{limited}/heartbeat", json={"worker_id": "w1"})
    assert late.json()["remaining_seconds"] == 0, "never below nothing"

    unlimited = await _queued(client)
    free = await _claimed(client)
    assert free["run"]["run_id"] == unlimited and free["lease"]["remaining_seconds"] is None
    settings = app.state.settings
    app.state.settings = settings.model_copy(
        update={"runs": settings.runs.model_copy(update={"max_run_seconds": 300})}
    )
    capped = await client.post(f"/v1/runs/{unlimited}/heartbeat", json={"worker_id": "w1"})
    assert 299 < capped.json()["remaining_seconds"] <= 300


async def test_a_tick_times_out_an_overworked_run_with_a_webhook(
    app, client, ticker, receiver
) -> None:
    hook = {"url": "https://ui.example/h", "events": ["run.finished"]}
    assert (await client.post("/v1/webhooks", json=hook)).status_code == 201
    run_id = (await client.post("/v1/runs", json=started(timeout_seconds=5))).json()["run_id"]
    report = await ticker.tick(now=now() + timedelta(seconds=6))
    assert (report.overworked, report.sent) == (1, 1)
    [event] = receiver.events()
    assert (event["data"]["run"]["run_id"], event["data"]["run"]["status"]) == (run_id, "TIMEOUT")
