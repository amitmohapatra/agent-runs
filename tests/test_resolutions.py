"""Every answer a run's interrupts got is kept, append-only, beside ``last_resolution``: the
HITL audit trail. A row exists exactly when the resume took effect."""

from __future__ import annotations

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
