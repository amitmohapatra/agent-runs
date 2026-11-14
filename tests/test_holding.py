"""A run kept in its caller's process (started ``RUNNING``, never queued) that its caller
holds: each heartbeat of the caller holds it for ``lease_seconds`` more, and once the hold
lapses (the process that runs it died: nothing else could) the ticker ends it ``ERROR``. It
is never put on the queue, which would have a worker run what only that process could.
Holding fences nothing: the caller writes to the run as before, without a ``worker_id``. A
run nobody holds is never ended this way (a sub-agent's run may wait on its parent's person
for days)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
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


async def test_a_pause_lets_go_of_the_hold_and_whoever_continues_the_run_takes_it_again(
    client, ticker
) -> None:
    """A paused run waits for a person, not for its holder: the sweep leaves it. Answered, it
    runs in its caller's process again — this server's or another's, whichever continues
    it — held once that one's first heartbeat comes."""
    run_id = await _running(client)
    await _hold(client, run_id, seconds=5)
    response = await client.post(f"/v1/runs/{run_id}/pause", json=pause(run_id))
    assert response.status_code == 200, response.text
    assert (await _hold(client, run_id)).status_code == 409  # nothing to hold while paused
    assert (await ticker.tick(now=at(5))).abandoned == 0
    resumed = await client.post(f"/v1/runs/{run_id}/resume", json=resolution(response.json()))
    assert resumed.json()["status"] == "RUNNING"
    assert (await _hold(client, run_id, holder="server-2", seconds=5)).status_code == 200
    assert (await ticker.tick(now=at(0.05))).abandoned == 0
    assert (await ticker.tick(now=at(5))).abandoned == 1  # server-2 stopped too


async def test_an_answered_run_nothing_continues_ends_in_error_once_the_grace_passes(
    client, ticker
) -> None:
    """A run held before it paused, whose holder stopped while it waited (its server was
    killed): answered, it runs in its caller's process — which is gone. Whoever continues it
    holds it at once; nobody did within ``RESUME_GRACE``, so the ticker ends it ``ERROR``
    (``holder_lost``), saying nobody took it up, instead of leaving it ``RUNNING``."""
    run_id = await _running(client)
    await _hold(client, run_id)
    response = await client.post(f"/v1/runs/{run_id}/pause", json=pause(run_id))
    resumed = await client.post(f"/v1/runs/{run_id}/resume", json=resolution(response.json()))
    assert resumed.json()["status"] == "RUNNING"
    assert (await ticker.tick(now=at(0.5))).abandoned == 0  # within the grace
    assert (await ticker.tick(now=at(2))).abandoned == 1
    ended = await _run(client, run_id)
    assert ended["status"] == "ERROR" and ended["error"]["code"] == "holder_lost"
    assert "nobody took the run up after its answer" in ended["error"]["message"]
    assert ended["error"]["retryable"] is False


async def test_a_run_never_held_is_left_running_after_its_answer(client, ticker) -> None:
    """A caller that never held its run (one that does not know holds) is not timed out
    after an answer: nothing says a holder ever ran it."""
    run_id = await _running(client)
    response = await client.post(f"/v1/runs/{run_id}/pause", json=pause(run_id))
    await client.post(f"/v1/runs/{run_id}/resume", json=resolution(response.json()))
    assert (await ticker.tick(now=at(60))).abandoned == 0
    assert (await _run(client, run_id))["status"] == "RUNNING"


async def test_its_holder_lets_go_of_a_held_run_which_goes_on_unheld(client, ticker) -> None:
    """A release of a run kept in its caller's process lets go of the hold (its holder no
    longer runs it, and does not end it: a helper whose parent waits for a person): the run
    stays ``RUNNING`` and the sweep leaves it. Only its holder lets go; nothing held is 409."""
    run_id = await _running(client)
    await _hold(client, run_id, seconds=5)
    other = await client.post(f"/v1/runs/{run_id}/release", json={"worker_id": "server-2"})
    assert other.status_code == 409
    released = await client.post(f"/v1/runs/{run_id}/release", json={"worker_id": "server-1"})
    assert released.status_code == 200, released.text
    assert (released.json()["status"], released.json()["attempt"]) == ("RUNNING", 1)
    assert (await ticker.tick(now=at(5))).abandoned == 0
    assert (await _run(client, run_id))["status"] == "RUNNING"
    again = await client.post(f"/v1/runs/{run_id}/release", json={"worker_id": "server-1"})
    assert again.status_code == 409
    assert (await _hold(client, run_id, holder="server-2")).status_code == 200  # taken anew


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


async def test_a_keys_run_started_in_process_is_ended_when_nobody_ever_holds_it(
    client, ticker
) -> None:
    """ARCH-5: a run started ``RUNNING`` with a ``concurrency_key`` takes its key's place, so
    the process that started it must hold it within ``RESUME_GRACE`` (60 s), as after an
    answer: one killed between the start and its first heartbeat leaves a run the ticker
    ends, not one its key's line waits behind for ever. A run with no key is as before."""
    orphan = await _running(client, concurrency_key="thread:1")
    held = await _running(client, concurrency_key="thread:2")
    keyless = await _running(client)
    await _hold(client, held, seconds=600)
    assert (await ticker.tick(now=at(0.5))).abandoned == 0  # within the grace
    assert (await ticker.tick(now=at(1.1))).abandoned == 1
    run = await _run(client, orphan)
    assert (run["status"], run["error"]["code"]) == ("ERROR", "holder_lost")
    assert [(await _run(client, r))["status"] for r in (held, keyless)] == ["RUNNING", "RUNNING"]


async def test_only_a_key_that_may_act_for_the_runs_person_holds_it(client, narrow, app) -> None:
    """SEC-R-4: a hold decides when a run ends (its lapse ends it), so only a key that may
    act for the person the run is for takes, extends or drops one: an ordinary user's key is
    refused another person's run (403), and nothing is held."""
    run_id = await _running(client, on_behalf_of="user:ada")
    refused = await _hold(narrow, run_id)
    assert (refused.status_code, refused.json()["code"]) == (403, "AUTHORIZATION")
    assert (
        await narrow.post(f"/v1/runs/{run_id}/release", json={"worker_id": "x"})
    ).status_code == 403
    async with app.state.engine.connect() as conn:
        kept = await conn.execute(
            text("SELECT holder, held_until FROM agent_runs WHERE run_id = :r"), {"r": run_id}
        )
        assert tuple(kept.one()) == (None, None)
    own = await _running(narrow, on_behalf_of="user_bob")
    assert (await _hold(narrow, own)).status_code == 200  # its own person's run


async def test_a_refused_hold_does_not_say_who_holds_the_run_and_a_hold_is_short(client) -> None:
    """SEC-R-4: the holder's name is what lets another caller extend or drop its hold, so a
    refusal does not give it; and a hold lasts at most 300 s, whatever was asked (a worker's
    lease may last 3600 s: a lapsed hold is how a dead server's run ends)."""
    run_id = await _running(client)
    held = await _hold(client, run_id, holder="trellis@host:1:secret", seconds=3600)
    lasting = datetime.fromisoformat(held.json()["expires_at"]) - datetime.now(UTC)
    assert timedelta(seconds=200) < lasting <= timedelta(seconds=300)
    refused = await _hold(client, run_id, holder="someone-else")
    assert refused.status_code == 409 and "trellis@host" not in refused.text
