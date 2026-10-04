"""The worker queue: queued runs, ``SKIP LOCKED`` claims, leases a worker must keep alive,
and what happens to a run whose worker went quiet."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from sqlalchemy import text
from trellis.contracts.ids import now

from agent_runs.config.constants import MAX_ATTEMPTS, MAX_CHECKPOINT_BYTES
from agent_runs.domain.runs import ClaimRequest
from agent_runs.store.runs import RunStore
from tests.conftest import pause, resolution, started


def claim(worker: str = "w1", agents: tuple[str, ...] = ("triage",), lease: int = 30) -> dict:
    return {"worker_id": worker, "agent_ids": list(agents), "lease_seconds": lease}


async def queued(client, **over) -> dict:
    response = await client.post("/v1/runs", json=started(queue=True, **over))
    assert response.status_code == 201, response.text
    return response.json()


async def test_a_queued_run_is_claimed_once_with_a_lease(client) -> None:
    run = await queued(client, input={"q": 1})
    assert run["status"] == "QUEUED"

    response = await client.post("/v1/runs/claim", json=claim())
    assert response.status_code == 200, response.text
    claimed = response.json()
    assert claimed["run"]["run_id"] == run["run_id"]
    assert (claimed["run"]["status"], claimed["run"]["attempt"]) == ("RUNNING", 1)
    assert claimed["lease"]["worker_id"] == "w1"

    assert (await client.post("/v1/runs/claim", json=claim("w2"))).status_code == 204


async def test_a_claim_takes_the_oldest_run_of_its_own_agents(client) -> None:
    other = await queued(client, agent_id="billing")
    first = await queued(client)
    second = await queued(client)
    got = [
        (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
        for _ in range(2)
    ]
    assert got == [first["run_id"], second["run_id"]]
    assert (await client.post("/v1/runs/claim", json=claim())).status_code == 204
    billing = (await client.post("/v1/runs/claim", json=claim(agents=("billing",)))).json()
    assert billing["run"]["run_id"] == other["run_id"]


async def test_a_claim_is_scoped_to_the_tenant(client, other_tenant) -> None:
    await queued(client)
    assert (await other_tenant.post("/v1/runs/claim", json=claim())).status_code == 204


async def test_a_claim_skips_a_run_another_claim_holds(app, client) -> None:
    """The mechanism, shown directly: claim A holds its row (uncommitted) and claim B, in
    its own transaction, is handed the next run instead of waiting for A or taking A's."""
    one, two = await queued(client), await queued(client)
    request = ClaimRequest(**claim())
    async with app.state.sessions() as a, app.state.sessions() as b:
        first = await RunStore(a).claim("acme", request, now=now())
        second = await RunStore(b).claim("acme", request, now=now())
        third = await RunStore(b).claim("acme", request, now=now())
        await a.commit()
        await b.commit()
    assert first is not None and second is not None and third is None
    assert {first.run.run_id, second.run.run_id} == {one["run_id"], two["run_id"]}


async def test_concurrent_claimers_never_get_the_same_run(client) -> None:
    ids = {(await queued(client))["run_id"] for _ in range(20)}

    async def worker(name: str) -> list[str]:
        got = []
        while (
            response := await client.post("/v1/runs/claim", json=claim(name))
        ).status_code == 200:
            got.append(response.json()["run"]["run_id"])
        return got

    claimed = await asyncio.gather(*(worker(f"w{i}") for i in range(8)))
    flat = [run_id for batch in claimed for run_id in batch]
    assert len(flat) == len(set(flat)) == 20
    assert set(flat) == ids


async def test_a_heartbeat_extends_only_the_holders_lease(client) -> None:
    await queued(client)
    claimed = (await client.post("/v1/runs/claim", json=claim(lease=10))).json()
    rid = claimed["run"]["run_id"]

    beat = await client.post(
        f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1", "lease_seconds": 60}
    )
    assert beat.status_code == 200
    assert beat.json()["expires_at"] > claimed["lease"]["expires_at"]
    assert (
        await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "intruder"})
    ).status_code == 409


async def test_a_cancelled_run_answers_its_workers_heartbeat_with_409(client) -> None:
    """The cancel signal a worker gets: its next heartbeat is refused."""
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    await client.post(f"/v1/runs/{rid}/finish", json={"status": "CANCELLED"})
    assert (
        await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1"})
    ).status_code == 409


async def test_a_worker_fences_its_writes_with_its_id(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    stale = await client.post(
        f"/v1/runs/{rid}/finish", params={"worker_id": "w_old"}, json={"status": "SUCCESS"}
    )
    assert stale.status_code == 409
    mine = await client.post(
        f"/v1/runs/{rid}/finish", params={"worker_id": "w1"}, json={"status": "SUCCESS"}
    )
    assert mine.status_code == 200


async def test_lease_bounds_are_enforced(client) -> None:
    assert (await client.post("/v1/runs/claim", json=claim(lease=1))).status_code == 422
    assert (
        await client.post("/v1/runs/claim", json={**claim(), "agent_ids": []})
    ).status_code == 422


async def test_a_lapsed_lease_puts_the_run_back_on_the_queue_as_the_next_attempt(
    app, client
) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim(lease=10))).json()["run"]["run_id"]

    async with app.state.sessions() as db:
        assert await RunStore(db).requeue_lapsed(now=now(), limit=10) == [], "not lapsed yet"
        moved = await RunStore(db).requeue_lapsed(now=now() + timedelta(seconds=11), limit=10)
        await db.commit()
    assert [(r.run_id, r.status.value, r.attempt) for r in moved] == [(rid, "QUEUED", 2)]

    assert (
        await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1"})
    ).status_code == 409
    again = (await client.post("/v1/runs/claim", json=claim("w2"))).json()
    assert (again["run"]["run_id"], again["run"]["attempt"]) == (rid, 2)


async def test_a_run_whose_lease_keeps_lapsing_ends_in_error(app, client) -> None:
    await queued(client)
    moved = []
    for attempt in range(1, MAX_ATTEMPTS + 1):
        claimed = (await client.post("/v1/runs/claim", json=claim(lease=5))).json()
        assert claimed["run"]["attempt"] == attempt
        async with app.state.sessions() as db:
            moved = await RunStore(db).requeue_lapsed(now=now() + timedelta(seconds=6), limit=10)
            await db.commit()
    assert moved[0].status.value == "ERROR"
    assert moved[0].error is not None and moved[0].error.code == "lease_expired"


async def test_a_durable_run_resumes_onto_the_queue(client) -> None:
    """A paused queued run is continued by a worker, not by whoever answered."""
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    paused = (
        await client.post(f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=pause(rid))
    ).json()
    assert paused["status"] == "PAUSED"

    resumed = (await client.post(f"/v1/runs/{rid}/resume", json=resolution(paused))).json()
    assert (resumed["status"], resumed["attempt"]) == ("QUEUED", 2)
    again = (await client.post("/v1/runs/claim", json=claim("w2"))).json()
    assert again["run"]["last_resolution"]["decision"] == "APPROVE"


async def test_a_queued_run_can_be_cancelled_before_anyone_claims_it(client) -> None:
    run = await queued(client)
    cancelled = await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "CANCELLED"})
    assert cancelled.status_code == 200
    assert (await client.post("/v1/runs/claim", json=claim())).status_code == 204


# ------------------------------------------------------------------ the executor's checkpoint

_JOURNAL = {
    "asks": {"ask_1": "yes"},
    "tools": {"sha256:9f2c": {"output": {"po": "PO-7"}}},
    "framework": {"langgraph": {"interrupt_id": "lg_1"}},
}


async def _claimed_and_paused(client, checkpoint=_JOURNAL) -> dict:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    response = await client.post(
        f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=pause(rid, checkpoint)
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_a_pause_keeps_the_checkpoint_for_the_worker_that_resumes(client) -> None:
    """Paused by one worker, resumed by another: the second gets the first's journal, so it
    repeats no side effect."""
    paused = await _claimed_and_paused(client)
    rid = paused["run_id"]
    assert paused["checkpoint"] == _JOURNAL
    assert (await client.get(f"/v1/runs/{rid}")).json()["checkpoint"] == _JOURNAL
    [listed] = (await client.get("/v1/runs", params={"status": "PAUSED"})).json()
    assert "checkpoint" not in listed  # a listing is summaries; the record carries it

    resumed = (await client.post(f"/v1/runs/{rid}/resume", json=resolution(paused))).json()
    assert (resumed["status"], resumed["checkpoint"]) == ("QUEUED", _JOURNAL)
    again = (await client.post("/v1/runs/claim", json=claim("w2"))).json()
    assert again["run"]["checkpoint"] == _JOURNAL
    assert again["run"]["last_resolution"]["decision"] == "APPROVE"


async def test_a_later_pause_replaces_the_checkpoint(client) -> None:
    paused = await _claimed_and_paused(client)
    rid = paused["run_id"]
    await client.post(f"/v1/runs/{rid}/resume", json=resolution(paused))
    await client.post("/v1/runs/claim", json=claim("w2"))
    newer = {"asks": {"ask_1": "yes", "ask_2": "no"}}
    repaused = await client.post(
        f"/v1/runs/{rid}/pause", params={"worker_id": "w2"}, json=pause(rid, newer)
    )
    assert repaused.json()["checkpoint"] == newer


async def test_finishing_clears_the_checkpoint(client) -> None:
    paused = await _claimed_and_paused(client)
    rid = paused["run_id"]
    await client.post(f"/v1/runs/{rid}/resume", json=resolution(paused))
    await client.post("/v1/runs/claim", json=claim("w2"))
    done = await client.post(
        f"/v1/runs/{rid}/finish", params={"worker_id": "w2"}, json={"status": "SUCCESS"}
    )
    assert done.status_code == 200, done.text
    assert done.json()["checkpoint"] is None
    assert (await client.get(f"/v1/runs/{rid}")).json()["checkpoint"] is None


async def test_cancelling_a_paused_run_clears_the_checkpoint(client) -> None:
    paused = await _claimed_and_paused(client)
    cancelled = await client.post(
        f"/v1/runs/{paused['run_id']}/resume", json=resolution(paused, "CANCEL")
    )
    assert (cancelled.json()["status"], cancelled.json()["checkpoint"]) == ("CANCELLED", None)


async def test_a_checkpoint_past_the_bound_is_refused_and_nothing_moves(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    huge = {"blob": "x" * MAX_CHECKPOINT_BYTES}
    refused = await client.post(
        f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=pause(rid, huge)
    )
    assert refused.status_code == 413
    run = (await client.get(f"/v1/runs/{rid}")).json()
    assert (run["status"], run["checkpoint"]) == ("RUNNING", None)


async def test_a_worker_without_the_lease_cannot_write_a_checkpoint(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    fenced = await client.post(
        f"/v1/runs/{rid}/pause", params={"worker_id": "w2"}, json=pause(rid, _JOURNAL)
    )
    assert fenced.status_code == 409
    run = (await client.get(f"/v1/runs/{rid}")).json()
    assert (run["status"], run["checkpoint"]) == ("RUNNING", None)


async def test_a_bare_interrupt_is_no_longer_a_pause_body(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    bare = pause(rid)["interrupt"]
    response = await client.post(f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=bare)
    assert response.status_code == 422


async def test_a_heartbeat_is_refused_once_the_run_stops_running_whatever_the_row_says(
    app, client
) -> None:
    """Defence in depth: every way out of RUNNING clears the lease, but the heartbeat checks
    the status itself too, so a row that still names the worker cannot keep a lease alive on
    a run that is not running."""
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE agent_runs SET status = 'PAUSED' WHERE run_id = :r"), {"r": rid}
        )
    beat = await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1"})
    assert beat.status_code == 409 and "PAUSED" in beat.json()["detail"]


async def test_heartbeat_lease_bounds_are_enforced(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    for lease in (4, 3601):
        body = {"worker_id": "w1", "lease_seconds": lease}
        assert (await client.post(f"/v1/runs/{rid}/heartbeat", json=body)).status_code == 422
    blank = {"worker_id": "   "}
    assert (await client.post(f"/v1/runs/{rid}/heartbeat", json=blank)).status_code == 422


async def test_a_claim_without_a_worker_or_with_too_many_agents_is_refused(client) -> None:
    assert (await client.post("/v1/runs/claim", json={"agent_ids": ["triage"]})).status_code == 422
    many = claim(agents=tuple(f"a{i}" for i in range(101)))
    assert (await client.post("/v1/runs/claim", json=many)).status_code == 422
