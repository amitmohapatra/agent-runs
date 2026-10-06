"""Which queued run a claim takes: the highest priority, then the oldest; never past a
concurrency key's or a tenant's room; and from a platform key that names no tenant, the
tenant whose workers hold the fewest runs first (the fair share of a shared fleet)."""

from __future__ import annotations

from typing import Any

from trellis.contracts.ids import now

from agent_runs.config.settings import RunsSettings
from agent_runs.domain.runs import ClaimRequest
from agent_runs.store.runs import RunStore
from tests.conftest import SETTINGS, client_of, serving, started

_CLAIM = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 30}


async def _queued(client, **over: Any) -> str:
    response = await client.post("/v1/runs", json=started(queue=True, **over))
    assert response.status_code == 201, response.text
    return response.json()["run_id"]


async def _claimed(client) -> str | None:
    response = await client.post("/v1/runs/claim", json=_CLAIM)
    return None if response.status_code == 204 else response.json()["run"]["run_id"]


async def _finished(client, run_id: str) -> None:
    body = {"status": "SUCCESS"}
    response = await client.post(f"/v1/runs/{run_id}/finish?worker_id=w1", json=body)
    assert response.status_code == 200, response.text


def _limited(**limits: Any) -> Any:
    return SETTINGS.model_copy(update={"runs": RunsSettings(**limits)})


async def test_the_highest_priority_is_claimed_first_then_the_oldest(client) -> None:
    low = await _queued(client, priority=-5)
    plain = await _queued(client)
    urgent = await _queued(client, priority=10)
    later_plain = await _queued(client)
    assert [await _claimed(client) for _ in range(5)] == [urgent, plain, later_plain, low, None]
    stored = (await client.get(f"/v1/runs/{urgent}")).json()
    assert (stored["priority"], stored["concurrency_key"]) == (10, None)


async def test_runs_sharing_a_concurrency_key_run_one_at_a_time(client, other_tenant) -> None:
    first = await _queued(client, concurrency_key="thread:42")
    second = await _queued(client, concurrency_key="thread:42", priority=10)
    other_key = await _queued(client, concurrency_key="thread:7")
    # another tenant's key of the same name is another key
    assert await _queued(other_tenant, tenant_id="globex", concurrency_key="thread:42")
    assert await _claimed(client) == second
    assert await _claimed(client) == other_key
    assert await _claimed(client) is None, "the first waits while its key's run runs"
    await _finished(client, second)
    assert await _claimed(client) == first
    assert (await other_tenant.post("/v1/runs/claim", json=_CLAIM)).status_code == 200


async def test_a_run_in_its_callers_process_takes_its_keys_place_too(client) -> None:
    running = (await client.post("/v1/runs", json=started(concurrency_key="k"))).json()
    queued = await _queued(client, concurrency_key="k")
    assert await _claimed(client) is None
    await client.post(f"/v1/runs/{running['run_id']}/finish", json={"status": "SUCCESS"})
    assert await _claimed(client) == queued


async def test_the_deployment_may_let_a_key_run_several(migrated, memory, blobs) -> None:
    async with (
        serving(_limited(concurrency_per_key=2), memory, blobs) as app,
        client_of(app) as client,
    ):
        ids = [await _queued(client, concurrency_key="k") for _ in range(3)]
        assert [await _claimed(client) for _ in range(3)] == [*ids[:2], None]


async def test_a_tenant_never_holds_more_than_its_cap(migrated, memory, blobs) -> None:
    async with (
        serving(_limited(max_running_per_tenant=2), memory, blobs) as app,
        client_of(app) as client,
        client_of(app, "other-key") as globex,
    ):
        ids = [await _queued(client) for _ in range(3)]
        # a run in its caller's process is no worker's: it takes no place
        await client.post("/v1/runs", json=started())
        assert [await _claimed(client) for _ in range(3)] == [*ids[:2], None]
        assert await _queued(globex, tenant_id="globex")
        assert (await globex.post("/v1/runs/claim", json=_CLAIM)).status_code == 200
        await _finished(client, ids[0])
        assert await _claimed(client) == ids[2]


async def test_a_platform_claim_shares_the_fleet_between_tenants(app, client, other_tenant) -> None:
    acme = [await _queued(client) for _ in range(3)]
    globex = [await _queued(other_tenant, tenant_id="globex") for _ in range(2)]
    async with client_of(app, "platform-key") as fleet:
        got = []
        while (response := await fleet.post("/v1/runs/claim", json=_CLAIM)).status_code == 200:
            run = response.json()["run"]
            got.append((run["tenant_id"], run["run_id"]))
        # acme queued first, so it goes first; then whoever holds fewer, then the oldest
        assert got == [
            ("acme", acme[0]),
            ("globex", globex[0]),
            ("acme", acme[1]),
            ("globex", globex[1]),
            ("acme", acme[2]),
        ]
        # the worker's later calls name the run's tenant
        tenant = {"X-Trellis-Tenant": "globex"}
        beat = await fleet.post(
            f"/v1/runs/{globex[0]}/heartbeat", json={"worker_id": "w1"}, headers=tenant
        )
        assert beat.status_code == 200, beat.text
        # any other call still names its tenant
        assert (await fleet.get(f"/v1/runs/{globex[0]}")).status_code == 400


async def test_a_platform_claim_naming_a_tenant_takes_only_that_tenants_runs(
    client, other_tenant, platform
) -> None:
    await _queued(other_tenant, tenant_id="globex")
    assert (await platform.post("/v1/runs/claim", json=_CLAIM)).status_code == 204
    mine = await _queued(client)
    assert (await platform.post("/v1/runs/claim", json=_CLAIM)).json()["run"]["run_id"] == mine


async def test_two_claims_at_once_never_both_take_a_keys_last_place(app, client) -> None:
    """The mechanism, shown directly: claim A has taken the key's place (uncommitted), and
    claim B, counting before A commits, passes the key over instead of taking a second
    place; after A commits, B's count sees A's run."""
    first = await _queued(client, concurrency_key="k")
    await _queued(client, concurrency_key="k")
    keyless = await _queued(client)
    request = ClaimRequest(**_CLAIM)
    async with app.state.sessions() as a, app.state.sessions() as b:
        taken = await RunStore(a).claim("acme", request, now=now())
        passed = await RunStore(b).claim("acme", request, now=now())
        await a.commit()
        await b.commit()
    assert taken is not None and taken.run.run_id == first
    assert passed is not None and passed.run.run_id == keyless
    async with app.state.sessions() as c:
        assert await RunStore(c).claim("acme", request, now=now()) is None


async def test_two_claims_at_once_never_both_take_a_tenants_last_place(app, client) -> None:
    for _ in range(2):
        await _queued(client)
    request = ClaimRequest(**_CLAIM)
    async with app.state.sessions() as a, app.state.sessions() as b:
        taken = await RunStore(a, max_running_per_tenant=1).claim("acme", request, now=now())
        passed = await RunStore(b, max_running_per_tenant=1).claim("acme", request, now=now())
        await a.commit()
        await b.commit()
    assert taken is not None and passed is None
    async with app.state.sessions() as c:
        capped = RunStore(c, max_running_per_tenant=1)
        assert await capped.claim("acme", request, now=now()) is None


async def test_a_repeated_start_asking_another_priority_or_key_is_a_conflict(client) -> None:
    body = started(queue=True, idempotency_key="k1", priority=1)
    assert (await client.post("/v1/runs", json=body)).status_code == 201
    again = await client.post("/v1/runs", json={**body, "priority": 2, "concurrency_key": "c"})
    assert again.status_code == 409
    assert again.json()["details"]["differing"] == ["concurrency_key", "priority"]
