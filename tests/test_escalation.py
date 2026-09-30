"""An interrupt nobody answers in time goes to whoever it escalates to, or times out."""

from __future__ import annotations

from agent_runs.store.runs import RunStore
from tests.conftest import at, interrupt, started


async def _paused(client, **fields) -> dict:
    run = (await client.post("/v1/runs", json=started())).json()
    body = interrupt(run["run_id"], **fields)
    return (await client.post(f"/v1/runs/{run['run_id']}/pause", json=body)).json()


async def _sweep(app, minutes: float):
    async with app.state.sessions() as db:
        moved = await RunStore(db).escalate_overdue(now=at(minutes), limit=10)
        await db.commit()
    return moved


async def test_an_overdue_interrupt_moves_to_its_escalation_assignee_once(app, client) -> None:
    run = await _paused(
        client, assignee="user:u1", deadline=at(10).isoformat(), escalate_to="role:managers"
    )
    assert await _sweep(app, 5) == [], "not due yet"

    [escalated] = await _sweep(app, 11)
    assert escalated.status.value == "PAUSED"
    assert escalated.awaiting is not None
    assert escalated.awaiting.assignee == "role:managers"
    assert escalated.awaiting.deadline is None and escalated.awaiting.escalate_to is None

    inbox = await client.get("/v1/runs", params={"status": "PAUSED", "assignee": "role:managers"})
    assert [r["run_id"] for r in inbox.json()] == [run["run_id"]]
    assert (await client.get("/v1/runs", params={"assignee": "user:u1"})).json() == []
    assert await _sweep(app, 60) == [], "an escalation happens once"


async def test_an_overdue_interrupt_with_nobody_to_escalate_to_times_out(app, client) -> None:
    run = await _paused(client, assignee="user:u1", deadline=at(10).isoformat())
    [timed_out] = await _sweep(app, 11)
    assert timed_out.run_id == run["run_id"]
    assert timed_out.status.value == "TIMEOUT"
    assert timed_out.error is not None and timed_out.error.code == "interrupt_deadline"
    assert timed_out.awaiting is None


async def test_an_answered_interrupt_is_never_escalated(app, client) -> None:
    run = await _paused(client, deadline=at(10).isoformat(), escalate_to="role:managers")
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "CANCELLED"})
    assert await _sweep(app, 11) == []


async def test_the_sweep_is_bounded(app, client) -> None:
    for _ in range(3):
        await _paused(client, deadline=at(1).isoformat())
    async with app.state.sessions() as db:
        assert len(await RunStore(db).escalate_overdue(now=at(2), limit=2)) == 2
        await db.commit()
