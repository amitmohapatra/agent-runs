"""The worker queue: queued runs, ``SKIP LOCKED`` claims, leases a worker must keep alive,
and what happens to a run whose worker went quiet."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from trellis.contracts.ids import now

from agent_runs.config.constants import MAX_ATTEMPTS
from agent_runs.domain.runs import ClaimRequest
from agent_runs.store.runs import RunStore
from tests.conftest import interrupt, resolution, started


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
        await client.post(f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=interrupt(rid))
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
