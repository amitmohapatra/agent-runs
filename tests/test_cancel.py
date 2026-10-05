"""Cancelling a run, whatever its status (``POST /v1/runs/{run_id}/cancel``): a queued or
waiting run, or one in its caller's process, ends ``CANCELLED`` at once; a run a worker holds
is asked to stop through its heartbeat, and the ticker cancels it when the lease in force
runs out. The keys that may answer a run may cancel it."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from sqlalchemy import text
from trellis.contracts.ids import now

from agent_runs.store.runs import RunStore
from tests.conftest import client_of, pause, paused, started

_CLAIM = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 60}
_WHY = {"reason": "the customer withdrew the request"}


async def _cancel(client, run_id: str, body: dict[str, Any] = _WHY) -> dict[str, Any]:
    response = await client.post(f"/v1/runs/{run_id}/cancel", json=body)
    assert response.status_code == 200, response.text
    return response.json()


async def _kept(app, run_id: str) -> tuple[Any, ...]:
    async with app.state.engine.connect() as conn:
        row = await conn.execute(
            text(
                "SELECT cancel_reason, cancelled_by, cancel_requested_at FROM agent_runs "
                "WHERE run_id = :r"
            ),
            {"r": run_id},
        )
        return tuple(row.one())


async def _held(client) -> str:
    await client.post("/v1/runs", json=started(queue=True))
    claimed = await client.post("/v1/runs/claim", json=_CLAIM)
    return claimed.json()["run"]["run_id"]


async def test_a_queued_run_is_cancelled_at_once_saying_why_and_who(app, client) -> None:
    await client.post(
        "/v1/webhooks", json={"url": "https://ui.example/h", "events": ["run.finished"]}
    )
    run_id = (await client.post("/v1/runs", json=started(queue=True))).json()["run_id"]
    cancelled = await _cancel(client, run_id)
    assert (cancelled["status"], cancelled["error"]) == ("CANCELLED", None)
    reason, by, requested = await _kept(app, run_id)
    assert (reason, by, requested) == (_WHY["reason"], "user_ada", None)
    assert (await client.post("/v1/runs/claim", json=_CLAIM)).status_code == 204
    async with app.state.engine.connect() as conn:
        [event] = (await conn.execute(text("SELECT payload FROM webhook_deliveries"))).scalars()
    assert (event["type"], event["data"]["run"]["status"]) == ("run.finished", "CANCELLED")


async def test_a_waiting_run_and_a_run_in_its_callers_process_are_cancelled_at_once(
    client,
) -> None:
    waiting = await paused(client)
    cancelled = await _cancel(client, waiting["run_id"], {})
    assert (cancelled["status"], cancelled["awaiting"]) == ("CANCELLED", None)
    in_process = (await client.post("/v1/runs", json=started())).json()["run_id"]
    assert (await _cancel(client, in_process))["status"] == "CANCELLED"
    late = await client.post(f"/v1/runs/{in_process}/finish", json={"status": "SUCCESS"})
    assert (late.status_code, late.json()["code"]) == (409, "CONFLICT")


async def test_a_held_run_is_asked_to_stop_through_its_heartbeat(app, client) -> None:
    run_id = await _held(client)
    asked = await _cancel(client, run_id)
    assert asked["status"] == "RUNNING"
    requested = (await _kept(app, run_id))[2]
    assert requested is not None

    beat = await client.post(
        f"/v1/runs/{run_id}/heartbeat", json={"worker_id": "w1", "lease_seconds": 600}
    )
    lease = beat.json()
    assert lease["cancel_requested"] is True
    # one lease from the request, however long the heartbeat asks for: no longer extended
    async with app.state.engine.connect() as conn:
        expires = await conn.scalar(
            text("SELECT lease_expires_at FROM agent_runs WHERE run_id = :r"), {"r": run_id}
        )
    assert expires == requested + timedelta(seconds=600)
    done = await client.post(
        f"/v1/runs/{run_id}/finish", params={"worker_id": "w1"}, json={"status": "CANCELLED"}
    )
    assert done.json()["status"] == "CANCELLED"
    assert (await _kept(app, run_id))[:2] == (_WHY["reason"], "user_ada")


async def test_a_worker_that_ignores_the_cancel_loses_the_run_when_its_lease_runs_out(
    app, client
) -> None:
    run_id = await _held(client)
    await _cancel(client, run_id)
    async with app.state.sessions() as db:
        store = RunStore(db)
        assert await store.requeue_lapsed(now=now() + timedelta(seconds=30), limit=10) == []
        [ended] = await store.requeue_lapsed(now=now() + timedelta(seconds=61), limit=10)
        await db.commit()
    assert (ended.run_id, ended.status.value, ended.attempt) == (run_id, "CANCELLED", 1)
    beat = await client.post(f"/v1/runs/{run_id}/heartbeat", json={"worker_id": "w1"})
    assert (beat.status_code, beat.json()["code"]) == (409, "LEASE_LOST")


async def test_a_held_run_asked_to_stop_is_not_paused_or_retried(client) -> None:
    pausing = await _held(client)
    await _cancel(client, pausing)
    stopped = await client.post(
        f"/v1/runs/{pausing}/pause", params={"worker_id": "w1"}, json=pause(pausing)
    )
    assert (stopped.json()["status"], stopped.json()["awaiting"]) == ("CANCELLED", None)

    failing = await _held(client)
    await _cancel(client, failing)
    blip = {
        "status": "ERROR",
        "error": {"code": "Busy", "category": "RATE_LIMIT", "message": "busy", "retryable": True},
    }
    ended = await client.post(f"/v1/runs/{failing}/finish", params={"worker_id": "w1"}, json=blip)
    assert (ended.json()["status"], ended.json()["attempt"]) == ("ERROR", 1)


async def test_a_repeated_cancel_answers_the_run_but_an_ended_run_is_a_conflict(client) -> None:
    held = await _held(client)
    first = await _cancel(client, held)
    again = await _cancel(client, held, {"reason": "another"})
    assert (again["status"], again["updated_at"]) == ("RUNNING", first["updated_at"])

    queued = (await client.post("/v1/runs", json=started(queue=True))).json()["run_id"]
    cancelled = await _cancel(client, queued)
    assert await _cancel(client, queued) == cancelled, "a retried request"
    other = await client.post(f"/v1/runs/{queued}/cancel", json={"reason": "another"})
    assert (other.status_code, other.json()["code"]) == (409, "CONFLICT")

    done = (await client.post("/v1/runs", json=started())).json()["run_id"]
    await client.post(f"/v1/runs/{done}/finish", json={"status": "SUCCESS"})
    ended = await client.post(f"/v1/runs/{done}/cancel", json={})
    assert (ended.status_code, ended.json()["code"]) == (409, "CONFLICT")
    missing = await client.post("/v1/runs/run_nope/cancel", json={})
    assert missing.status_code == 404


async def _assigned(client, assignee: str) -> dict[str, Any]:
    run_id = (await client.post("/v1/runs", json=started())).json()["run_id"]
    waiting = await client.post(f"/v1/runs/{run_id}/pause", json=pause(run_id, assignee=assignee))
    return waiting.json()


async def test_the_keys_that_may_answer_a_run_may_cancel_it(app, client, other_tenant) -> None:
    for_raj = await _assigned(client, "user:raj")
    for_priya = await _assigned(client, "user:priya")
    for_role = await _assigned(client, "role:finance")
    unassigned = (await client.post("/v1/runs", json=started(queue=True))).json()
    async with client_of(app, "priya-key") as priya, client_of(app, "admin-key") as admin:
        for refused in (for_raj, for_role):
            answer = await priya.post(f"/v1/runs/{refused['run_id']}/cancel", json={})
            assert (answer.status_code, answer.json()["code"]) == (403, "AUTHORIZATION")
        for allowed in (for_priya, unassigned):
            assert (await _cancel(priya, allowed["run_id"]))["status"] == "CANCELLED"
        assert (await _cancel(admin, for_role["run_id"]))["status"] == "CANCELLED"
    assert (
        await other_tenant.post(f"/v1/runs/{for_raj['run_id']}/cancel", json={})
    ).status_code == 404
    long = await client.post(f"/v1/runs/{for_raj['run_id']}/cancel", json={"reason": "x" * 1001})
    assert long.status_code == 422
