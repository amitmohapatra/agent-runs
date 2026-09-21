"""The run lifecycle, end to end over HTTP.

Every test here is a failure mode a run service has to survive, not a demonstration that
CRUD works: a scheduler delivering twice, two replicas resuming the same paused run, a
worker finishing a run that was already cancelled, one tenant reaching for another's work.
"""

from __future__ import annotations

from tests.conftest import started


async def test_a_run_starts_in_running_and_comes_back_by_id(client) -> None:
    created = (await client.post("/v1/runs", json=started(input={"q": "hi"}))).json()
    assert created["status"] == "RUNNING"
    assert created["attempt"] == 1

    fetched = (await client.get(f"/v1/runs/{created['run_id']}")).json()
    assert fetched["run_id"] == created["run_id"]
    assert fetched["input"] == {"q": "hi"}


async def test_the_same_idempotency_key_never_starts_a_second_run(client) -> None:
    """At-least-once delivery is the normal case for a scheduler, not the exception: an
    acknowledgement lost after the work was accepted must not run 9am twice."""
    body = started(idempotency_key="sched-2026-09-21T09:00")
    first = (await client.post("/v1/runs", json=body)).json()
    second = (await client.post("/v1/runs", json=body)).json()
    assert first["run_id"] == second["run_id"]

    listed = (await client.get("/v1/runs")).json()
    assert len([r for r in listed if r["run_id"] == first["run_id"]]) == 1


async def test_two_tenants_may_use_the_same_idempotency_key(client, other_tenant) -> None:
    """The key is scoped to a tenant. A global unique index would let one tenant's key
    collide with another's and hand back somebody else's run."""
    body = started(idempotency_key="nightly")
    mine = (await client.post("/v1/runs", json=body)).json()

    theirs = await other_tenant.post("/v1/runs", json={**body, "tenant_id": "globex"})
    assert theirs.status_code == 201
    assert theirs.json()["run_id"] != mine["run_id"]


async def test_a_run_pauses_for_a_human_and_resumes_as_a_new_attempt(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    rid = run["run_id"]

    paused = (
        await client.post(
            f"/v1/runs/{rid}/transition",
            json={"status": "PAUSED", "awaiting": {"question": "approve the refund?"}},
        )
    ).json()
    assert paused["status"] == "PAUSED"
    assert paused["awaiting"] == {"question": "approve the refund?"}

    resumed = (await client.post(f"/v1/runs/{rid}/resume", params={"answer": "yes"})).json()
    assert resumed["status"] == "RUNNING"
    assert resumed["attempt"] == 2
    # what it was waiting for is answered, so it must not still be advertised as waiting
    assert resumed["awaiting"] is None


async def test_paused_runs_are_the_human_inbox(client) -> None:
    a = (await client.post("/v1/runs", json=started())).json()
    b = (await client.post("/v1/runs", json=started(agent_id="billing"))).json()
    await client.post(f"/v1/runs/{a['run_id']}/transition", json={"status": "PAUSED"})

    waiting = (await client.get("/v1/runs", params={"status": "PAUSED"})).json()
    assert [r["run_id"] for r in waiting] == [a["run_id"]]
    assert b["run_id"] not in [r["run_id"] for r in waiting]


async def test_a_finished_run_cannot_finish_again(client) -> None:
    """The double-delivery case. Without this a retried completion would overwrite the
    result and 'why did this run succeed twice' has no answer."""
    run = (await client.post("/v1/runs", json=started())).json()
    rid = run["run_id"]
    first = await client.post(
        f"/v1/runs/{rid}/transition", json={"status": "SUCCESS", "output": {"ok": True}}
    )
    assert first.status_code == 200

    again = await client.post(
        f"/v1/runs/{rid}/transition", json={"status": "SUCCESS", "output": {"ok": False}}
    )
    assert again.status_code == 409
    assert (await client.get(f"/v1/runs/{rid}")).json()["output"] == {"ok": True}


async def test_a_paused_run_cannot_jump_straight_to_success(client) -> None:
    """Something has to actually run to produce a result."""
    run = (await client.post("/v1/runs", json=started())).json()
    rid = run["run_id"]
    await client.post(f"/v1/runs/{rid}/transition", json={"status": "PAUSED"})
    refused = await client.post(f"/v1/runs/{rid}/transition", json={"status": "SUCCESS"})
    assert refused.status_code == 409


async def test_a_paused_run_can_still_be_cancelled(client) -> None:
    """Abandoning a question nobody answered is legitimate; it must not require a resume."""
    run = (await client.post("/v1/runs", json=started())).json()
    rid = run["run_id"]
    await client.post(f"/v1/runs/{rid}/transition", json={"status": "PAUSED"})
    cancelled = await client.post(f"/v1/runs/{rid}/transition", json={"status": "CANCELLED"})
    assert cancelled.status_code == 200


async def test_lineage_returns_the_whole_ancestor_chain(client) -> None:
    """Three generations. The harness carries only the immediate parent on its context, so
    a grandchild cannot reach a grandparent's run-scoped memories — the chain has to be
    answerable from somewhere, and this is that somewhere."""
    a = (await client.post("/v1/runs", json=started(agent_id="planner"))).json()
    b = (
        await client.post("/v1/runs", json=started(agent_id="inventory", parent_run_id=a["run_id"]))
    ).json()
    c = (
        await client.post("/v1/runs", json=started(agent_id="pricing", parent_run_id=b["run_id"]))
    ).json()

    chain = (await client.get(f"/v1/runs/{c['run_id']}/lineage")).json()
    assert [r["run_id"] for r in chain] == [c["run_id"], b["run_id"], a["run_id"]]


async def test_children_are_listed_by_parent(client) -> None:
    parent = (await client.post("/v1/runs", json=started())).json()
    child = (await client.post("/v1/runs", json=started(parent_run_id=parent["run_id"]))).json()
    kids = (await client.get("/v1/runs", params={"parent_run_id": parent["run_id"]})).json()
    assert [r["run_id"] for r in kids] == [child["run_id"]]


async def test_one_tenant_cannot_read_another_tenants_run(client, other_tenant) -> None:
    mine = (await client.post("/v1/runs", json=started())).json()
    assert (await other_tenant.get(f"/v1/runs/{mine['run_id']}")).status_code == 404


async def test_starting_a_run_for_another_tenant_is_refused(client) -> None:
    """The body must not be able to widen what the credential allows."""
    refused = await client.post("/v1/runs", json=started(tenant_id="globex"))
    assert refused.status_code == 403


async def test_a_missing_api_key_is_rejected(client) -> None:
    # httpx merges request headers over the client's defaults rather than replacing them, so
    # a key cannot be removed per-request — it has to be explicitly wrong instead.
    assert (await client.get("/v1/runs", headers={"X-Api-Key": "nope"})).status_code == 401


async def test_health_reports_the_database(client) -> None:
    assert (await client.get("/health/live")).json() == {"status": "ok"}
    assert (await client.get("/health/ready")).json() == {"status": "ok"}


# ----------------------------------------------------------------- authentication


async def test_the_credential_decides_the_tenant_not_the_header(client, other_tenant) -> None:
    """A key issued to one tenant cannot act as another by changing a header.

    This was the whole flaw: the key check ran only when auth_mode was the literal string
    "trusted_dev", so the startup check's own advice — use another mode outside dev — turned
    authentication off, and the tenant came from attacker-controlled bytes regardless. Every
    production configuration was unauthenticated.
    """
    mine = (await client.post("/v1/runs", json=started())).json()
    stolen = await client.get(
        f"/v1/runs/{mine['run_id']}", headers={"X-Api-Key": "dev-key", "X-Tenant-Id": "globex"}
    )
    assert stolen.status_code == 403


async def test_a_header_that_agrees_with_the_credential_is_fine(client) -> None:
    """Sending the tenant is allowed; it just cannot disagree. Clients that set it for
    logging or for symmetry with other services should not be broken."""
    assert (await client.get("/v1/runs", headers={"X-Api-Key": "dev-key"})).status_code == 200


async def test_an_unknown_key_is_refused_before_anything_else(client) -> None:
    assert (await client.get("/v1/runs", headers={"X-Api-Key": "nope"})).status_code == 401


def test_a_deployment_with_no_keys_is_refused_at_startup() -> None:
    """Empty keys used to mean "trust everyone"; now it means nobody can call, which is a
    configuration error worth failing on rather than a silent open door."""
    import pytest

    from agent_runs.config.settings import ServiceSettings, Settings

    with pytest.raises(ValueError, match="api_keys is empty"):
        Settings(service=ServiceSettings(api_keys={})).check()


def test_the_development_key_cannot_survive_into_production() -> None:
    import pytest

    from agent_runs.config.settings import ServiceSettings, Settings

    with pytest.raises(ValueError, match="development credential"):
        Settings(service=ServiceSettings(environment="prod")).check()
