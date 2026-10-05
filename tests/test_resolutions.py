"""Every answer a run's interrupts got is kept, append-only, beside ``last_resolution``: the
HITL audit trail. A row exists exactly when the resume took effect."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from sqlalchemy import text
from trellis.contracts.runs import ToolCall

from tests.conftest import pause, resolution, started


async def _pause(client, run_id: str, **over) -> dict:
    response = await client.post(f"/v1/runs/{run_id}/pause", json=pause(run_id, **over))
    assert response.status_code == 200, response.text
    return response.json()


async def _resume(client, run: dict, decision: str, **over) -> dict:
    response = await client.post(
        f"/v1/runs/{run['run_id']}/resume", json=resolution(run, decision, **over)
    )
    assert response.status_code == 200, response.text
    return response.json()


async def _history(client, run_id: str) -> list[dict]:
    response = await client.get(f"/v1/runs/{run_id}/resolutions")
    assert response.status_code == 200, response.text
    return response.json()


async def test_every_decision_of_every_pause_is_kept_in_order(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    run_id = run["run_id"]
    call = ToolCall(tool="billing.refund", args={"amount": 240}).model_dump(mode="json")
    steps = [
        ("ANSWER", {"answer": {"customer": "Acme"}, "reviewer": "alice"}, {}),
        ("APPROVE", {"reviewer": "bob"}, {"reason": "APPROVAL", "tool_call": call}),
        ("EDIT", {"payload": {"amount": 200}, "reviewer": "bob"}, {"tool_call": call}),
        ("REJECT", {"reviewer": "carol"}, {"tool_call": call}),
        ("CANCEL", {"reviewer": "carol"}, {}),
    ]
    asked = []
    last: dict = {}
    for decision, answer, question in steps:
        waiting = await _pause(client, run_id, **question)
        asked.append(waiting["awaiting"]["interrupt_id"])
        last = await _resume(client, waiting, decision, **answer)
    assert last["status"] == "CANCELLED"
    assert last["last_resolution"]["decision"] == "CANCEL", "only the latest is on the run"

    history = await _history(client, run_id)
    assert [h["resolution"]["decision"] for h in history] == [d for d, _, _ in steps]
    assert [h["interrupt"]["interrupt_id"] for h in history] == asked
    assert [h["attempt"] for h in history] == [1, 2, 3, 4, 5]
    assert [h["resolution"]["reviewer"] for h in history] == [
        "alice",
        "bob",
        "bob",
        "carol",
        "carol",
    ]
    assert history[0]["resolution"]["answer"] == {"customer": "Acme"}
    assert history[1]["interrupt"]["tool_call"]["tool"] == "billing.refund"
    assert history[2]["resolution"]["payload"] == {"amount": 200}
    recorded = [h["recorded_at"] for h in history]
    assert recorded == sorted(recorded)


async def test_a_refused_resume_leaves_no_record(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    waiting = await _pause(client, run["run_id"])
    url = f"/v1/runs/{run['run_id']}/resume"
    wrong_question = {**resolution(waiting), "interrupt_id": "int_other"}
    wrong_run = {**resolution(waiting), "run_id": "run_elsewhere"}
    assert (await client.post(url, json=wrong_question)).status_code == 409
    assert (await client.post(url, json=wrong_run)).status_code == 422
    assert await _history(client, run["run_id"]) == []
    # the real answer is kept once; a second click on the same pause is refused and not kept
    await _resume(client, waiting, "APPROVE")
    assert (await client.post(url, json=resolution(waiting))).status_code == 409
    assert len(await _history(client, run["run_id"])) == 1


async def test_a_run_never_paused_has_no_resolutions(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    assert await _history(client, run["run_id"]) == []


async def test_another_tenant_cannot_read_the_trail(client, other_tenant) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    await _resume(client, await _pause(client, run["run_id"]), "APPROVE", reviewer="alice")
    theirs = await other_tenant.get(f"/v1/runs/{run['run_id']}/resolutions")
    assert theirs.status_code == 404
    assert "alice" not in theirs.text
    assert (await client.get("/v1/runs/run_missing/resolutions")).status_code == 404


async def test_a_queued_run_answered_back_onto_the_queue_is_kept_too(client) -> None:
    run = (await client.post("/v1/runs", json=started(queue=True))).json()
    claimed = await client.post(
        "/v1/runs/claim", json={"worker_id": "w1", "agent_ids": [run["agent_id"]]}
    )
    assert claimed.status_code == 200, claimed.text
    paused = await client.post(
        f"/v1/runs/{run['run_id']}/pause?worker_id=w1", json=pause(run["run_id"])
    )
    assert paused.status_code == 200, paused.text
    resumed = await _resume(client, paused.json(), "APPROVE")
    assert resumed["status"] == "QUEUED"
    [entry] = await _history(client, run["run_id"])
    assert entry["resolution"]["decision"] == "APPROVE" and entry["attempt"] == 1


# ------------------------------------------------------------------ a retried resume


def _later(body: dict) -> dict:
    """The same answer, made a second later: what a second click sends."""
    when = datetime.fromisoformat(body["resolved_at"]) + timedelta(seconds=1)
    return {**body, "resolved_at": when.isoformat()}


async def test_a_retried_resume_answers_the_run_and_changes_nothing(client) -> None:
    """A client that lost the answer to its resume sends the very same resolution again: it
    gets the run, not a conflict, and the run moves on once."""
    run = (await client.post("/v1/runs", json=started())).json()
    body = resolution(await _pause(client, run["run_id"]), "ANSWER", answer="yes")
    url = f"/v1/runs/{run['run_id']}/resume"
    first = await client.post(url, json=body)
    again = await client.post(url, json=body)
    assert first.status_code == again.status_code == 200, again.text
    # the run as it is now: the same, but for the working time of the stretch it is running
    running = {"worked_seconds"}
    assert {k: v for k, v in again.json().items() if k not in running} == {
        k: v for k, v in first.json().items() if k not in running
    }
    assert again.json()["worked_seconds"] >= first.json()["worked_seconds"]
    assert (again.json()["status"], again.json()["attempt"]) == ("RUNNING", 2)
    assert len(await _history(client, run["run_id"])) == 1


async def test_a_retry_after_the_run_moved_on_answers_it_as_it_is_now(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    body = resolution(await _pause(client, run["run_id"]))
    url = f"/v1/runs/{run['run_id']}/resume"
    await client.post(url, json=body)
    asked_again = await _pause(client, run["run_id"])
    retried = await client.post(url, json=body)
    assert retried.status_code == 200, retried.text
    assert retried.json()["awaiting"] == asked_again["awaiting"]
    assert len(await _history(client, run["run_id"])) == 1


async def test_any_other_answer_to_an_answered_interrupt_is_a_conflict(client) -> None:
    """Only the very same resolution is a repeat: answered a second later, by someone else or
    otherwise, it is a second answer and is refused, so the run never continues twice."""
    run = (await client.post("/v1/runs", json=started())).json()
    waiting = await _pause(client, run["run_id"])
    body = resolution(waiting, reviewer="alice")
    url = f"/v1/runs/{run['run_id']}/resume"
    await client.post(url, json=body)
    for second in (_later(body), {**body, "reviewer": "bob"}, {**body, "decision": "REJECT"}):
        refused = await client.post(url, json=second)
        assert refused.status_code == 409 and refused.json()["code"] == "CONFLICT"
    assert len(await _history(client, run["run_id"])) == 1


async def test_concurrent_resumes_continue_the_run_once(app, client) -> None:
    """Under the row lock the second of two simultaneous resumes sees the first's answer: the
    same resolution twice answers both with the run, two different ones refuse one."""
    hook = {"url": "https://ui.example/h", "events": ["run.finished"]}
    assert (await client.post("/v1/webhooks", json=hook)).status_code == 201
    run = (await client.post("/v1/runs", json=started())).json()
    body = resolution(await _pause(client, run["run_id"]), "CANCEL")
    url = f"/v1/runs/{run['run_id']}/resume"
    both = await asyncio.gather(client.post(url, json=body), client.post(url, json=body))
    assert [r.status_code for r in both] == [200, 200]
    assert both[0].json() == both[1].json()
    async with app.state.engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM webhook_deliveries")) == 1

    other = (await client.post("/v1/runs", json=started())).json()
    body = resolution(await _pause(client, other["run_id"]))
    url = f"/v1/runs/{other['run_id']}/resume"
    raced = await asyncio.gather(client.post(url, json=body), client.post(url, json=_later(body)))
    assert sorted(r.status_code for r in raced) == [200, 409]
    for history in (await _history(client, run["run_id"]), await _history(client, other["run_id"])):
        assert len(history) == 1
