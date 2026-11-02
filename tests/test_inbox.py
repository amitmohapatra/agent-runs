"""A run's inbox (``POST``/``GET /v1/runs/{run_id}/messages``): an instruction or a pause sent
to a run on any worker, kept in order and read past a position; the lease says the newest
position, so a worker knows there is something to read. Who may send or read: the keys that
may act for the run's user. And what a run has used (``usage``), saved with its heartbeat,
pause and ending, on its record and its summary."""

from __future__ import annotations

import asyncio
from typing import Any

from tests.conftest import client_of, pause, started

_CLAIM = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 60}
_USAGE = {"model_calls": 3, "tool_calls": 2, "commands": 1, "tokens": 1200, "cost": 0.004}


def _instruction(text: str = "also check March", **over: Any) -> dict[str, Any]:
    return {"message_id": "m1", "kind": "INSTRUCTION", "text": text, **over}


async def _send(client, run_id: str, body: dict[str, Any], status: int = 201) -> dict[str, Any]:
    response = await client.post(f"/v1/runs/{run_id}/messages", json=body)
    assert response.status_code == status, response.text
    return response.json()


async def _held(client, **over: Any) -> str:
    await client.post("/v1/runs", json=started(queue=True, user_id="ada", **over))
    claimed = await client.post("/v1/runs/claim", json=_CLAIM)
    return claimed.json()["run"]["run_id"]


async def test_an_instruction_is_kept_in_order_and_read_past_a_position(client) -> None:
    run_id = await _held(client)
    first = await _send(client, run_id, _instruction(sender="user:ada"))
    assert (first["position"], first["sender"], first["kind"]) == (1, "user:ada", "INSTRUCTION")
    await _send(client, run_id, _instruction("and April", message_id="m2"))
    everything = (await client.get(f"/v1/runs/{run_id}/messages")).json()
    assert [m["text"] for m in everything] == ["also check March", "and April"]
    # the key's own principal when the sender is not named
    assert everything[1]["sender"] == "user_ada"
    past = (await client.get(f"/v1/runs/{run_id}/messages", params={"after": 1})).json()
    assert [m["position"] for m in past] == [2]
    page = (await client.get(f"/v1/runs/{run_id}/messages", params={"limit": 1})).json()
    assert [m["position"] for m in page] == [1]


async def test_sending_the_same_message_again_adds_nothing(client) -> None:
    run_id = await _held(client)
    sent = await _send(client, run_id, _instruction())
    again = await _send(client, run_id, _instruction(), status=200)
    assert again == sent
    other = await client.post(f"/v1/runs/{run_id}/messages", json=_instruction("else"))
    assert (other.status_code, other.json()["code"]) == (409, "CONFLICT")
    assert len((await client.get(f"/v1/runs/{run_id}/messages")).json()) == 1


async def test_the_lease_says_the_newest_position(client) -> None:
    run_id = await _held(client)
    beat = {"worker_id": "w1", "lease_seconds": 60}
    lease = (await client.post(f"/v1/runs/{run_id}/heartbeat", json=beat)).json()
    assert lease["inbox"] == 0
    await _send(client, run_id, _instruction())
    await _send(client, run_id, {"message_id": "p1", "kind": "PAUSE"})
    lease = (await client.post(f"/v1/runs/{run_id}/heartbeat", json=beat)).json()
    assert lease["inbox"] == 2


async def test_a_queued_or_paused_run_takes_instructions_for_when_it_works(client) -> None:
    queued = (await client.post("/v1/runs", json=started(queue=True))).json()["run_id"]
    await _send(client, queued, _instruction())
    run_id = (await client.post("/v1/runs", json=started())).json()["run_id"]
    await client.post(f"/v1/runs/{run_id}/pause", json=pause(run_id))
    await _send(client, run_id, _instruction())
    # a pause of a run that already waits on a person asks nothing
    again = await client.post(
        f"/v1/runs/{run_id}/messages", json={"message_id": "p1", "kind": "PAUSE"}
    )
    assert (again.status_code, again.json()["code"]) == (409, "CONFLICT")


async def test_an_ended_run_takes_no_message(client) -> None:
    run_id = (await client.post("/v1/runs", json=started())).json()["run_id"]
    await client.post(f"/v1/runs/{run_id}/finish", json={"status": "SUCCESS"})
    late = await client.post(f"/v1/runs/{run_id}/messages", json=_instruction())
    assert (late.status_code, late.json()["code"]) == (409, "CONFLICT")


async def test_an_instruction_without_text_is_refused(client) -> None:
    run_id = await _held(client)
    bare = await client.post(
        f"/v1/runs/{run_id}/messages", json={"message_id": "m1", "kind": "INSTRUCTION"}
    )
    assert bare.status_code == 422


async def test_only_the_runs_user_is_let_in_to_its_inbox(app, client, other_tenant) -> None:
    ada = await _held(client)
    async with client_of(app, "priya-key") as priya:
        for refused in (
            await priya.post(f"/v1/runs/{ada}/messages", json=_instruction()),
            await priya.get(f"/v1/runs/{ada}/messages"),
            await priya.post(f"/v1/runs/{ada}/messages", json=_instruction(sender="user:ada")),
        ):
            assert (refused.status_code, refused.json()["code"]) == (403, "AUTHORIZATION")
        theirs = (await client.post("/v1/runs", json=started(user_id="priya"))).json()["run_id"]
        sent = await _send(priya, theirs, _instruction())
        assert sent["sender"] == "user:priya"
        assert len((await priya.get(f"/v1/runs/{theirs}/messages")).json()) == 1
    for refused in (
        await other_tenant.post(f"/v1/runs/{ada}/messages", json=_instruction()),
        await other_tenant.get(f"/v1/runs/{ada}/messages"),
    ):
        assert refused.status_code == 404
    assert (await client.get(f"/v1/runs/{ada}/messages")).json() == []


async def test_messages_sent_at_once_each_take_their_own_position(client) -> None:
    run_id = await _held(client)
    sent = await asyncio.gather(
        *(
            client.post(f"/v1/runs/{run_id}/messages", json=_instruction(message_id=f"m{n}"))
            for n in range(20)
        )
    )
    assert {r.status_code for r in sent} == {201}
    positions = sorted(r.json()["position"] for r in sent)
    assert positions == list(range(1, 21))


async def test_usage_is_saved_with_the_heartbeat_the_pause_and_the_ending(client) -> None:
    run_id = await _held(client)
    beat = {"worker_id": "w1", "lease_seconds": 60, "usage": _USAGE}
    await client.post(f"/v1/runs/{run_id}/heartbeat", json=beat)
    record = (await client.get(f"/v1/runs/{run_id}")).json()
    assert record["usage"]["commands"] == 1
    listed = (await client.get("/v1/runs", params={"user_id": "ada"})).json()
    assert [(s["run_id"], s["usage"]["tokens"]) for s in listed] == [(run_id, 1200)]
    assert (await client.get("/v1/runs", params={"user_id": "bob"})).json() == []

    paused = await client.post(
        f"/v1/runs/{run_id}/pause",
        params={"worker_id": "w1"},
        json={**pause(run_id), "usage": {**_USAGE, "model_calls": 4}},
    )
    assert paused.json()["usage"]["model_calls"] == 4

    other = (await client.post("/v1/runs", json=started())).json()["run_id"]
    ended = await client.post(
        f"/v1/runs/{other}/finish", json={"status": "SUCCESS", "usage": _USAGE}
    )
    assert ended.json()["usage"]["cost"] == 0.004
    # a heartbeat without usage keeps what was saved
    assert (await client.get(f"/v1/runs/{run_id}")).json()["usage"]["model_calls"] == 4
