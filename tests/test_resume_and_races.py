"""Two failures that a UI hits first, and that the suite did not cover.

Both were measured against a running service before they were fixed: a human's answer
silently discarded, and an idempotency key that answered 500 under concurrency.
"""

from __future__ import annotations

from tests.conftest import started


async def _paused(client, **over) -> str:
    run_id = (await client.post("/v1/runs", json=started(**over))).json()["run_id"]
    response = await client.post(
        f"/v1/runs/{run_id}/transition",
        json={"status": "PAUSED", "metadata": {"question": "Approve the refund?"}},
    )
    assert response.status_code == 200, response.text
    return run_id


async def test_a_human_answer_posted_as_json_is_recorded(client) -> None:
    """The answer is the whole point of a pause.

    ``resume`` declared ``answer: Any = None``, which FastAPI reads as a *query* parameter
    for a non-model type. Every UI that posted ``{"answer": ...}`` as JSON — which is every
    UI — resumed the run with no answer and got a 200 saying it had worked.
    """
    run_id = await _paused(client)
    answer = {"approved": True, "note": "within policy", "by": "alice"}

    response = await client.post(f"/v1/runs/{run_id}/resume", json={"answer": answer})

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "RUNNING"
    stored = (await client.get(f"/v1/runs/{run_id}")).json()
    assert stored["metadata"]["answer"] == answer


async def test_resuming_without_an_answer_is_still_allowed(client) -> None:
    """ "Carry on" is a legitimate reply, and an empty body must not be an error."""
    run_id = await _paused(client)
    response = await client.post(f"/v1/runs/{run_id}/resume")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "RUNNING"


async def test_a_falsy_answer_is_recorded_rather_than_dropped(client) -> None:
    """``False`` is a decision. A truthiness test would throw away every rejection."""
    run_id = await _paused(client)
    await client.post(f"/v1/runs/{run_id}/resume", json={"answer": False})
    assert (await client.get(f"/v1/runs/{run_id}")).json()["metadata"]["answer"] is False


async def test_an_unknown_field_in_a_resume_body_is_refused(client) -> None:
    """A typo'd field name must not look like a successful answer."""
    run_id = await _paused(client)
    response = await client.post(f"/v1/runs/{run_id}/resume", json={"anwser": "yes"})
    assert response.status_code == 422


async def test_two_starts_racing_on_one_key_produce_one_run_and_no_error(client) -> None:
    """At-least-once delivery arrives *at once*, not politely spaced out.

    Reproducing this needs care, and two easier versions of the test are worthless:

    * ``asyncio.gather`` of eight posts *looks* like a race and is not one — the first
      request finishes its insert before the rest reach their look-up.
    * Two sessions stepped by hand, with the winner committed first, is not one either: the
      loser's own look-up then finds the winner's row and returns it through the ordinary
      idempotent path, never reaching the insert.

    Both pass against the very bug they are meant to catch (checked, both ways). What makes
    it a real race is the loser holding a snapshot taken *before* the winner committed, so
    its look-up sees nothing and its insert still violates the constraint — which is exactly
    what two concurrent requests do. Measured against a running service before the fix:
    eight concurrent starts gave seven 201s and one 500, the opposite of the promise an
    idempotency key makes.
    """
    from sqlalchemy import text as sql

    from agent_runs.domain.models import RunCreate
    from agent_runs.store.runs import RunStore

    sessions = client._transport.app.state.sessions
    spec = RunCreate(**started(idempotency_key="sched-2026-09-21T09:00"))

    async with sessions() as loser:
        # REPEATABLE READ fixes this transaction's snapshot at its first statement, which is
        # how we hold the loser inside the window rather than hoping to catch it there.
        await loser.execute(sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await loser.execute(sql("SELECT 1"))

        async with sessions() as winner_session:
            winner, created = await RunStore(winner_session).start(spec)
            await winner_session.commit()
        assert created

        # The loser cannot see the winner's row, so it inserts — and the unique constraint
        # rejects it. It must come back with the winner's run, not a 500.
        loser_run, created_again = await RunStore(loser).start(spec)
        await loser.commit()

    assert not created_again, "the second start must not report a new run"
    assert loser_run.run_id == winner.run_id

    listed = (await client.get("/v1/runs")).json()
    assert len(listed) == 1, f"one idempotency key produced {len(listed)} runs"
