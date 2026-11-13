"""A run kept in its caller's process (started ``RUNNING``, never queued) that its caller
holds: each heartbeat of the caller holds it for ``lease_seconds`` more, and once the hold
lapses (the process that runs it died: nothing else could) the ticker ends it ``ERROR``. It
is never put on the queue, which would have a worker run what only that process could.
Holding fences nothing: the caller writes to the run as before, without a ``worker_id``. A
run nobody holds is never ended this way (a sub-agent's run may wait on its parent's person
for days)."""

from __future__ import annotations

from typing import Any

from trellis.contracts.runs import RunEventType

from tests.conftest import at, pause, resolution, started


async def _running(client, **over: Any) -> str:
    response = await client.post("/v1/runs", json=started(**over))
    assert response.status_code == 201, response.text
    return response.json()["run_id"]


async def _hold(client, run_id: str, holder: str = "server-1", seconds: int = 30):
    return await client.post(
        f"/v1/runs/{run_id}/heartbeat", json={"worker_id": holder, "lease_seconds": seconds}
    )


async def _run(client, run_id: str) -> dict[str, Any]:
    return (await client.get(f"/v1/runs/{run_id}")).json()


async def test_a_run_kept_in_its_callers_process_is_held_by_its_heartbeats(client) -> None:
    run_id = await _running(client)
    held = await _hold(client, run_id)
    assert held.status_code == 200, held.text
    lease = held.json()
    assert (lease["run_id"], lease["worker_id"], lease["cancel_requested"]) == (
        run_id,
        "server-1",
        False,
    )
    again = await _hold(client, run_id, seconds=60)
    assert again.status_code == 200 and again.json()["expires_at"] > lease["expires_at"]
    run = await _run(client, run_id)
    assert (run["status"], run["attempt"]) == ("RUNNING", 1)


async def test_a_run_whose_holder_stopped_ends_in_error_once_the_hold_lapses(
    client, ticker
) -> None:
    held, renewed, nobodys = [await _running(client) for _ in range(3)]
    await _hold(client, held, seconds=30)
    await _hold(client, renewed, holder="server-2", seconds=600)

    assert (await ticker.tick(now=at(0.25))).abandoned == 0  # within the hold
    report = await ticker.tick(now=at(1))
    assert report.abandoned == 1 and report.requeued == 0

    ended = await _run(client, held)
    assert ended["status"] == "ERROR" and ended["attempt"] == 1
    error = ended["error"]
    assert (error["code"], error["category"], error["source"]) == (
        "holder_lost",
        "TIMEOUT",
        "agent-runs",
    )
    assert error["retryable"] is False
    assert "server-1" in error["message"] and "stopped" in error["message"]
    # a run whose holder is alive, and one nobody holds, are untouched
    assert (await _run(client, renewed))["status"] == "RUNNING"
    assert (await _run(client, nobodys))["status"] == "RUNNING"
    # the holder, back, is told it holds nothing any more
    assert (await _hold(client, held)).status_code == 409


async def test_another_holder_cannot_take_a_hold_in_force(client) -> None:
    run_id = await _running(client)
    await _hold(client, run_id, holder="server-1")
    taken = await _hold(client, run_id, holder="server-2")
    assert taken.status_code == 409
    assert taken.json()["code"] == "LEASE_LOST"


async def test_holding_fences_nothing_the_caller_writes(client) -> None:
    run_id = await _running(client)
    await _hold(client, run_id)
    event = {
        "tenant_id": "acme",
        "run_id": run_id,
        "sequence": 0,
        "attempt": 1,
        "type": RunEventType.STEP_STARTED.value,
    }
    appended = await client.post(f"/v1/runs/{run_id}/events", json={"events": [event]})
    assert appended.status_code == 200, appended.text
    finished = await client.post(f"/v1/runs/{run_id}/finish", json={"status": "SUCCESS"})
    assert finished.status_code == 200, finished.text
    assert (await _hold(client, run_id)).status_code == 409


async def test_a_pause_lets_go_of_the_hold_and_the_next_heartbeat_takes_it_again(
    client, ticker
) -> None:
    """A paused run waits for a person, not for its holder: the sweep leaves it. Answered, it
    runs in its caller's process again, held once its holder's next heartbeat comes."""
    run_id = await _running(client)
    await _hold(client, run_id, seconds=5)
    response = await client.post(f"/v1/runs/{run_id}/pause", json=pause(run_id))
    assert response.status_code == 200, response.text
    assert (await _hold(client, run_id)).status_code == 409  # nothing to hold while paused
    assert (await ticker.tick(now=at(5))).abandoned == 0
    resumed = await client.post(f"/v1/runs/{run_id}/resume", json=resolution(response.json()))
    assert resumed.json()["status"] == "RUNNING"
    assert (await ticker.tick(now=at(5))).abandoned == 0  # nobody holds it yet
    assert (await _hold(client, run_id, seconds=5)).status_code == 200
    assert (await ticker.tick(now=at(5))).abandoned == 1


async def test_a_held_run_is_cancelled_at_once_and_its_holder_told(client, ticker) -> None:
    run_id = await _running(client)
    await _hold(client, run_id)
    cancelled = await client.post(f"/v1/runs/{run_id}/cancel", json={"reason": "enough"})
    assert cancelled.json()["status"] == "CANCELLED"
    assert (await _hold(client, run_id)).status_code == 409
    assert (await ticker.tick(now=at(5))).abandoned == 0


async def test_a_held_run_takes_no_workers_place_and_is_never_queued(client, ticker) -> None:
    """Holding is not a worker's lease: no claim sees the run, and a lapse never puts it on
    the queue, whatever the lease sweep does with the runs workers hold."""
    run_id = await _running(client)
    await _hold(client, run_id, seconds=5)
    claim = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 30}
    assert (await client.post("/v1/runs/claim", json=claim)).status_code == 204
    await ticker.tick(now=at(5))
    assert (await client.post("/v1/runs/claim", json=claim)).status_code == 204
    released = await client.post(f"/v1/runs/{run_id}/release", json={"worker_id": "server-1"})
    assert released.status_code == 409


async def test_a_queued_run_is_held_only_by_the_worker_that_claimed_it(client) -> None:
    """A durable run's heartbeat is a worker's lease, as before: a heartbeat of a worker that
    holds no lease on it takes nothing."""
    queued = (await client.post("/v1/runs", json=started(queue=True))).json()["run_id"]
    assert (await _hold(client, queued)).status_code == 409
