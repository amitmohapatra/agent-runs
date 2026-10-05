"""Retryable errors are retried, later: a queued run its worker ends ``ERROR`` with a
retryable error goes back on the queue after a jittered backoff (``available_at``), at most
``MAX_ERROR_RETRIES`` times, and the error then stands saying so. A run kept in its caller's
process, and a run whose error is not retryable, end at once."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import text
from trellis.contracts.ids import now

from agent_runs.config.constants import ERROR_RETRY_BASE, MAX_ERROR_RETRIES
from tests.conftest import backoff_passed, started

_CLAIM = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 60}
_BLIP = {
    "status": "ERROR",
    "error": {
        "code": "RateLimited",
        "category": "RATE_LIMIT",
        "message": "the model is busy",
        "retryable": True,
    },
}


async def _claimed(client) -> str:
    response = await client.post("/v1/runs/claim", json=_CLAIM)
    assert response.status_code == 200, response.text
    return response.json()["run"]["run_id"]


async def _fail(client, run_id: str, body: dict[str, Any] = _BLIP) -> dict[str, Any]:
    response = await client.post(f"/v1/runs/{run_id}/finish", params={"worker_id": "w1"}, json=body)
    assert response.status_code == 200, response.text
    return response.json()


async def _available_at(app, run_id: str) -> datetime:
    async with app.state.engine.connect() as conn:
        return await conn.scalar(
            text("SELECT available_at FROM agent_runs WHERE run_id = :r"), {"r": run_id}
        )


async def test_a_retryable_error_puts_the_run_back_on_the_queue_after_a_backoff(
    app, client
) -> None:
    await client.post(
        "/v1/webhooks", json={"url": "https://ui.example/h", "events": ["run.finished"]}
    )
    await client.post("/v1/runs", json=started(queue=True))
    run_id = await _claimed(client)
    failed_at = now()
    requeued = await _fail(client, run_id)
    assert (requeued["status"], requeued["attempt"], requeued["error"]) == ("QUEUED", 2, None)
    wait = (await _available_at(app, run_id)) - failed_at
    assert ERROR_RETRY_BASE / 2 <= wait <= ERROR_RETRY_BASE + (now() - failed_at)
    assert (await client.post("/v1/runs/claim", json=_CLAIM)).status_code == 204, "not yet"
    async with app.state.engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM webhook_deliveries")) == 0

    repeat = await client.post(f"/v1/runs/{run_id}/finish", params={"worker_id": "w1"}, json=_BLIP)
    assert (repeat.status_code, repeat.json()) == (200, requeued), "a lost answer, retried"
    stranger = await client.post(
        f"/v1/runs/{run_id}/finish", params={"worker_id": "w2"}, json=_BLIP
    )
    assert (stranger.status_code, stranger.json()["code"]) == (409, "LEASE_LOST")

    await backoff_passed(app)
    assert await _claimed(client) == run_id


async def test_the_backoff_doubles_and_the_last_retryable_error_stands(app, client) -> None:
    await client.post("/v1/runs", json=started(queue=True))
    waits = []
    for retry in range(1, MAX_ERROR_RETRIES + 1):
        run_id = await _claimed(client)
        failed_at = now()
        assert (await _fail(client, run_id))["attempt"] == retry + 1
        waits.append((await _available_at(app, run_id)) - failed_at)
        await backoff_passed(app)
    for retry, wait in enumerate(waits):
        assert ERROR_RETRY_BASE * 2**retry / 2 <= wait <= ERROR_RETRY_BASE * 2**retry * 1.01

    run_id = await _claimed(client)
    ended = await _fail(client, run_id)
    assert (ended["status"], ended["attempt"]) == ("ERROR", MAX_ERROR_RETRIES + 1)
    assert ended["error"]["retryable"] is True
    assert ended["error"]["message"] == (
        f"the model is busy (after {MAX_ERROR_RETRIES} of {MAX_ERROR_RETRIES} retries)"
    )


async def test_an_error_that_is_not_retryable_ends_the_run_saying_how_often_it_was_retried(
    app, client
) -> None:
    await client.post("/v1/runs", json=started(queue=True))
    await _fail(client, await _claimed(client))
    await backoff_passed(app)
    broken = {"status": "ERROR", "error": {**_BLIP["error"], "retryable": False}}
    ended = await _fail(client, await _claimed(client), broken)
    assert (ended["status"], ended["error"]["message"]) == (
        "ERROR",
        f"the model is busy (after 1 of {MAX_ERROR_RETRIES} retries)",
    )
    fresh = (await client.post("/v1/runs", json=started(queue=True))).json()["run_id"]
    assert await _claimed(client) == fresh
    at_once = await _fail(client, fresh, broken)
    assert (at_once["status"], at_once["error"]["message"]) == ("ERROR", "the model is busy")


async def test_a_run_kept_in_its_callers_process_is_never_retried(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    ended = await client.post(f"/v1/runs/{run['run_id']}/finish", json=_BLIP)
    assert (ended.json()["status"], ended.json()["attempt"]) == ("ERROR", 1)
