"""A run's own deadline (``RunStart.deadline``): when it must be done by. The ticker ends a run
not yet ended past it as ``TIMEOUT``, whether it is queued, running or waiting for a person,
and a worker still running it is fenced off."""

from __future__ import annotations

from trellis.contracts.errors import ErrorCategory

from agent_runs.store.runs import RunStore
from tests.conftest import at, pause, started

_CLAIM = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 600}


async def _sweep(app, minutes: float, limit: int = 10):
    async with app.state.sessions() as db:
        ended = await RunStore(db).time_out_past_deadline(now=at(minutes), limit=limit)
        await db.commit()
    return ended


async def _start(client, minutes: float | None = 10, **over) -> str:
    deadline = None if minutes is None else at(minutes).isoformat()
    response = await client.post("/v1/runs", json=started(deadline=deadline, **over))
    assert response.status_code == 201, response.text
    return response.json()["run_id"]


async def test_a_run_past_its_deadline_times_out_queued_running_or_paused(app, client) -> None:
    queued = await _start(client, queue=True)
    running = await _start(client)
    paused = await _start(client)
    await client.post(f"/v1/runs/{paused}/pause", json=pause(paused))
    later = await _start(client, 30)
    undated = await _start(client, None)
    assert await _sweep(app, 5) == [], "not due yet"

    ended = await _sweep(app, 11)
    assert {run.run_id for run in ended} == {queued, running, paused}
    for run in ended:
        assert run.status.value == "TIMEOUT" and run.awaiting is None
        assert run.error is not None
        assert (run.error.code, run.error.category) == ("run_deadline", ErrorCategory.TIMEOUT)
        assert run.error.retryable is False and run.error.source == "agent-runs"
    for run_id in (later, undated):
        assert (await client.get(f"/v1/runs/{run_id}")).json()["status"] == "RUNNING"
    assert await _sweep(app, 11) == [], "an ended run is never ended again"


async def test_a_deadline_counts_the_time_spent_waiting_for_a_person(app, client) -> None:
    """The interrupt may give its person longer than the run has: the run's deadline wins."""
    run_id = await _start(client)
    body = pause(run_id, assignee="user:u1", deadline=at(60).isoformat(), escalate_to="role:x")
    await client.post(f"/v1/runs/{run_id}/pause", json=body)
    [ended] = await _sweep(app, 11)
    assert (ended.run_id, ended.status.value) == (run_id, "TIMEOUT")
    async with app.state.sessions() as db:
        assert await RunStore(db).escalate_overdue(now=at(61), limit=10) == []


async def test_a_worker_running_a_timed_out_run_is_fenced_off(app, client) -> None:
    """Its next heartbeat says LEASE_LOST (the SDK's worker cancels its handler on that), and
    so does any write it still tries."""
    run_id = await _start(client, queue=True)
    claimed = await client.post("/v1/runs/claim", json=_CLAIM)
    assert claimed.json()["run"]["run_id"] == run_id
    await _sweep(app, 11)

    fenced = {"worker_id": "w1"}
    beat = await client.post(f"/v1/runs/{run_id}/heartbeat", json=fenced)
    finish = await client.post(
        f"/v1/runs/{run_id}/finish", params=fenced, json={"status": "SUCCESS"}
    )
    paused = await client.post(f"/v1/runs/{run_id}/pause", params=fenced, json=pause(run_id))
    for refused in (beat, finish, paused):
        assert refused.status_code == 409 and refused.json()["code"] == "LEASE_LOST"
    assert (await client.get(f"/v1/runs/{run_id}")).json()["status"] == "TIMEOUT"


async def test_the_sweep_is_bounded(app, client) -> None:
    for _ in range(3):
        await _start(client, 1)
    assert len(await _sweep(app, 2, limit=2)) == 2
    assert len(await _sweep(app, 2, limit=2)) == 1


async def test_a_tick_times_out_runs_past_their_deadline_with_a_webhook(
    app, client, ticker, receiver
) -> None:
    hook = {"url": "https://ui.example/h", "events": ["run.finished"]}
    assert (await client.post("/v1/webhooks", json=hook)).status_code == 201
    run_id = await _start(client, 1, queue=True)
    report = await ticker.tick(now=at(2))
    assert (report.timed_out, report.sent) == (1, 1)
    [event] = receiver.events()
    assert event["type"] == "run.finished"
    assert (event["data"]["run"]["run_id"], event["data"]["run"]["status"]) == (run_id, "TIMEOUT")
