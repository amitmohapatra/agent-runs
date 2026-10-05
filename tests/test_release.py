"""Letting go of a run (``POST /v1/runs/{run_id}/release``): a worker that is stopping hands
the runs it still holds back to the queue at once, for another worker, as their next attempt
and without counting a lapsed lease."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from agent_runs.config.constants import MAX_CHECKPOINT_BYTES
from tests.conftest import started

_CLAIM = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 60}


async def _held(client) -> str:
    await client.post("/v1/runs", json=started(queue=True))
    return (await client.post("/v1/runs/claim", json=_CLAIM)).json()["run"]["run_id"]


async def _release(client, run_id: str, **body: Any):
    return await client.post(f"/v1/runs/{run_id}/release", json={"worker_id": "w1", **body})


async def test_a_released_run_is_queued_at_once_for_another_worker(app, client) -> None:
    run_id = await _held(client)
    progress = {"tools": {"call_1": {"output": "PO-17 created"}}}
    released = await _release(client, run_id, checkpoint=progress)
    assert released.status_code == 200, released.text
    run = released.json()
    assert (run["status"], run["attempt"], run["checkpoint"]) == ("QUEUED", 2, progress)
    async with app.state.engine.connect() as conn:
        lapses = await conn.scalar(
            text("SELECT lease_lapses FROM agent_runs WHERE run_id = :r"), {"r": run_id}
        )
    assert lapses == 0, "nothing crashed"
    again = await client.post("/v1/runs/claim", json={**_CLAIM, "worker_id": "w2"})
    assert (again.json()["run"]["run_id"], again.json()["run"]["checkpoint"]) == (run_id, progress)


async def test_a_release_without_progress_keeps_the_checkpoint(client) -> None:
    run_id = await _held(client)
    saved = {"step": 3}
    await client.post(f"/v1/runs/{run_id}/heartbeat", json={"worker_id": "w1", "checkpoint": saved})
    assert (await _release(client, run_id)).json()["checkpoint"] == saved


async def test_only_the_lease_holder_releases_and_a_repeat_answers_the_run(client) -> None:
    run_id = await _held(client)
    stranger = await client.post(f"/v1/runs/{run_id}/release", json={"worker_id": "w2"})
    assert (stranger.status_code, stranger.json()["code"]) == (409, "LEASE_LOST")
    huge = {"blob": "x" * MAX_CHECKPOINT_BYTES}
    too_big = await _release(client, run_id, checkpoint=huge)
    assert (too_big.status_code, too_big.json()["code"]) == (413, "PAYLOAD_TOO_LARGE")
    first = await _release(client, run_id)
    again = await _release(client, run_id)
    assert (again.status_code, again.json()) == (200, first.json())
    in_process = (await client.post("/v1/runs", json=started())).json()["run_id"]
    unheld = await _release(client, in_process)
    assert (unheld.status_code, unheld.json()["code"]) == (409, "LEASE_LOST")


async def test_a_run_whose_cancel_was_asked_for_ends_cancelled_instead(app, client) -> None:
    await client.post(
        "/v1/webhooks", json={"url": "https://ui.example/h", "events": ["run.finished"]}
    )
    run_id = await _held(client)
    await client.post(f"/v1/runs/{run_id}/cancel", json={"reason": "withdrawn"})
    released = await _release(client, run_id, checkpoint={"step": 1})
    assert (released.json()["status"], released.json()["checkpoint"]) == ("CANCELLED", None)
    async with app.state.engine.connect() as conn:
        owed = await conn.scalar(text("SELECT count(*) FROM webhook_deliveries"))
    assert owed == 1
