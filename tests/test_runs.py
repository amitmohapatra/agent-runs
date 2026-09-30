"""The run lifecycle over HTTP: the contracts' records, the one state machine, and the
failures a run service has to survive (a repeated start, a double answer, a worker finishing
what a person cancelled, one tenant reaching for another's work)."""

from __future__ import annotations

import asyncio

from sqlalchemy import text
from trellis.contracts.runs import RunStart

from agent_runs.store.runs import RunStore
from tests.conftest import interrupt, pause, paused, resolution, started


async def test_a_run_starts_running_and_comes_back_by_id(client) -> None:
    response = await client.post("/v1/runs", json=started(input={"q": "hi"}, workspace_id="ws1"))
    assert response.status_code == 201
    created = response.json()
    assert (created["status"], created["attempt"]) == ("RUNNING", 1)
    assert created["run_id"].startswith("run_"), "the contracts mint the id when none is given"

    fetched = (await client.get(f"/v1/runs/{created['run_id']}")).json()
    assert fetched["input"] == {"q": "hi"}
    assert fetched["workspace_id"] == "ws1"


async def test_the_same_idempotency_key_never_starts_a_second_run(client) -> None:
    body = started(idempotency_key="sched-2026-09-21T09:00")
    first = await client.post("/v1/runs", json=body)
    second = await client.post("/v1/runs", json=body)
    assert (first.status_code, second.status_code) == (201, 200)
    assert first.json()["run_id"] == second.json()["run_id"]
    assert len((await client.get("/v1/runs")).json()) == 1


async def test_the_same_run_id_is_the_same_run(client) -> None:
    """The harness derives run ids, so a retried turn reopens nothing."""
    first = (await client.post("/v1/runs", json=started(run_id="run_abc"))).json()
    again = await client.post("/v1/runs", json=started(run_id="run_abc", agent_id="other"))
    assert again.status_code == 200
    assert again.json()["agent_id"] == first["agent_id"]


async def test_a_run_id_another_tenant_holds_is_a_conflict_not_a_read(client, other_tenant) -> None:
    await client.post("/v1/runs", json=started(run_id="run_shared"))
    theirs = await other_tenant.post(
        "/v1/runs", json=started(tenant_id="globex", run_id="run_shared")
    )
    assert theirs.status_code == 409
    assert "acme" not in theirs.text


async def test_two_tenants_may_use_the_same_idempotency_key(client, other_tenant) -> None:
    body = started(idempotency_key="nightly")
    mine = (await client.post("/v1/runs", json=body)).json()
    theirs = await other_tenant.post("/v1/runs", json={**body, "tenant_id": "globex"})
    assert theirs.status_code == 201
    assert theirs.json()["run_id"] != mine["run_id"]


async def test_concurrent_starts_on_one_key_make_one_run_and_no_error(app) -> None:
    """The loser of a real race: its insert waits on the winner's uncommitted row and must
    come back with the winner's run once that commits, not with a 500."""
    spec = RunStart(**started(idempotency_key="sched-1"))
    from trellis.contracts.ids import now

    async with app.state.sessions() as winner_db, app.state.sessions() as loser_db:
        winner, created = await RunStore(winner_db).start(spec, queue=False, now=now())
        loser = asyncio.create_task(
            RunStore(loser_db).start(
                spec.model_copy(update={"run_id": "run_x"}), queue=False, now=now()
            )
        )
        await asyncio.sleep(0.2)
        assert not loser.done(), "the loser must be waiting on the winner's unique key"
        await winner_db.commit()
        lost, created_again = await loser
        await loser_db.commit()

    assert created and not created_again
    assert lost.run_id == winner.run_id
    async with app.state.engine.connect() as conn:
        assert (await conn.scalar(text("SELECT count(*) FROM agent_runs"))) == 1


# ------------------------------------------------------------------ pause and resume


async def test_a_run_pauses_on_a_typed_interrupt_and_lands_in_the_assignees_inbox(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    body = pause(run["run_id"], assignee="role:procurement", ui="table")
    response = await client.post(f"/v1/runs/{run['run_id']}/pause", json=body)
    assert response.status_code == 200, response.text
    record = response.json()
    assert record["status"] == "PAUSED"
    assert record["awaiting"]["assignee"] == "role:procurement"
    assert record["awaiting"]["ui"] == "table"

    inbox = await client.get(
        "/v1/runs", params={"status": "PAUSED", "assignee": "role:procurement"}
    )
    assert [r["run_id"] for r in inbox.json()] == [run["run_id"]]
    elsewhere = await client.get("/v1/runs", params={"status": "PAUSED", "assignee": "user:u1"})
    assert elsewhere.json() == []


async def test_an_interrupt_for_another_run_is_refused(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    other = (await client.post("/v1/runs", json=started())).json()
    refused = await client.post(f"/v1/runs/{run['run_id']}/pause", json=pause(other["run_id"]))
    assert refused.status_code == 422


async def test_an_invalid_interrupt_is_refused_by_the_contract(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    body = {"interrupt": {**interrupt(run["run_id"]), "reason": "CHOICE"}}  # needs options
    assert (await client.post(f"/v1/runs/{run['run_id']}/pause", json=body)).status_code == 422


async def test_resuming_continues_the_run_as_the_next_attempt_with_the_answer_kept(client) -> None:
    run = await paused(client)
    answer = {"approved": True, "note": "within policy"}
    response = await client.post(
        f"/v1/runs/{run['run_id']}/resume",
        json=resolution(run, "ANSWER", answer=answer, reviewer="alice"),
    )
    assert response.status_code == 200, response.text
    resumed = response.json()
    assert (resumed["status"], resumed["attempt"]) == ("RUNNING", 2)
    assert resumed["awaiting"] is None
    assert resumed["last_resolution"]["answer"] == answer
    assert resumed["last_resolution"]["reviewer"] == "alice"


async def test_a_falsy_answer_is_kept(client) -> None:
    run = await paused(client)
    await client.post(
        f"/v1/runs/{run['run_id']}/resume", json=resolution(run, "ANSWER", answer=False)
    )
    stored = (await client.get(f"/v1/runs/{run['run_id']}")).json()
    assert stored["last_resolution"]["answer"] is False


async def test_an_answer_to_another_question_is_a_conflict(client) -> None:
    run = await paused(client)
    wrong = {**resolution(run), "interrupt_id": "int_other"}
    assert (await client.post(f"/v1/runs/{run['run_id']}/resume", json=wrong)).status_code == 409


async def test_a_second_answer_is_a_conflict(client) -> None:
    """Two clicks, one pause: the second finds nothing waiting."""
    run = await paused(client)
    body = resolution(run)
    assert (await client.post(f"/v1/runs/{run['run_id']}/resume", json=body)).status_code == 200
    assert (await client.post(f"/v1/runs/{run['run_id']}/resume", json=body)).status_code == 409


async def test_a_resolution_for_another_run_is_refused(client) -> None:
    run = await paused(client)
    body = {**resolution(run), "run_id": "run_elsewhere"}
    assert (await client.post(f"/v1/runs/{run['run_id']}/resume", json=body)).status_code == 422


async def test_a_cancel_decision_ends_the_run(client) -> None:
    run = await paused(client)
    cancelled = (
        await client.post(f"/v1/runs/{run['run_id']}/resume", json=resolution(run, "CANCEL"))
    ).json()
    assert cancelled["status"] == "CANCELLED"
    assert cancelled["last_resolution"]["decision"] == "CANCEL"


async def test_an_unknown_field_in_a_resolution_is_refused(client) -> None:
    run = await paused(client)
    body = {**resolution(run), "anwser": "yes"}
    assert (await client.post(f"/v1/runs/{run['run_id']}/resume", json=body)).status_code == 422


# ------------------------------------------------------------------ endings


async def test_a_finished_run_cannot_finish_again(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    rid = run["run_id"]
    first = await client.post(f"/v1/runs/{rid}/finish", json={"status": "SUCCESS", "output": 1})
    assert first.status_code == 200
    again = await client.post(f"/v1/runs/{rid}/finish", json={"status": "SUCCESS", "output": 2})
    assert again.status_code == 409
    assert (await client.get(f"/v1/runs/{rid}")).json()["output"] == 1


async def test_a_paused_run_cannot_jump_to_success_but_can_be_cancelled(client) -> None:
    run = await paused(client)
    rid = run["run_id"]
    assert (
        await client.post(f"/v1/runs/{rid}/finish", json={"status": "SUCCESS"})
    ).status_code == 409
    cancelled = await client.post(f"/v1/runs/{rid}/finish", json={"status": "CANCELLED"})
    assert cancelled.status_code == 200
    assert cancelled.json()["awaiting"] is None


async def test_a_failure_carries_a_typed_error(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    error = {"code": "ToolFailed", "category": "TOOL", "message": "erp down", "retryable": True}
    ended = (
        await client.post(
            f"/v1/runs/{run['run_id']}/finish", json={"status": "ERROR", "error": error}
        )
    ).json()
    assert ended["error"]["category"] == "TOOL"


async def test_an_ending_that_is_not_one_or_a_success_with_an_error_is_refused(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    rid = run["run_id"]
    assert (
        await client.post(f"/v1/runs/{rid}/finish", json={"status": "PAUSED"})
    ).status_code == 422
    with_error = {"status": "SUCCESS", "error": {"code": "x"}}
    assert (await client.post(f"/v1/runs/{rid}/finish", json=with_error)).status_code == 422


# ------------------------------------------------------------------ reads


async def test_the_listing_is_summaries_and_the_record_is_one_get_away(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    body = pause(run["run_id"], assignee="user:u1")
    assert (await client.post(f"/v1/runs/{run['run_id']}/pause", json=body)).status_code == 200
    [summary] = (await client.get("/v1/runs")).json()
    assert set(summary) == {
        "run_id",
        "agent_id",
        "status",
        "awaiting",
        "assignee",
        "deadline",
        "updated_at",
    }
    assert summary["status"] == "PAUSED" and summary["assignee"] == "user:u1"
    assert summary["awaiting"]["question"] == "Approve?"
    full = (await client.get(f"/v1/runs/{run['run_id']}")).json()
    assert full["input"] is None and "checkpoint" in full


async def test_there_is_no_lineage_route(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    assert (await client.get(f"/v1/runs/{run['run_id']}/lineage")).status_code in {404, 405}


async def test_children_are_listed_by_parent(client) -> None:
    parent = (await client.post("/v1/runs", json=started())).json()
    child = (await client.post("/v1/runs", json=started(parent_run_id=parent["run_id"]))).json()
    kids = (await client.get("/v1/runs", params={"parent_run_id": parent["run_id"]})).json()
    assert [r["run_id"] for r in kids] == [child["run_id"]]


async def test_one_tenant_cannot_read_or_move_another_tenants_run(client, other_tenant) -> None:
    run = await paused(client)
    rid = run["run_id"]
    assert (await other_tenant.get(f"/v1/runs/{rid}")).status_code == 404
    assert (
        await other_tenant.post(f"/v1/runs/{rid}/resume", json=resolution(run))
    ).status_code == 404
    assert (await other_tenant.get("/v1/runs", params={"status": "PAUSED"})).json() == []


async def test_health_reports_the_database(client) -> None:
    assert (await client.get("/health/live")).json() == {"status": "ok"}
    assert (await client.get("/health/ready")).json() == {"status": "ok"}
