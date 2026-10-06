"""A run's event log: appended by whoever runs it (fenced as a heartbeat is), read and
followed (server-sent events) from any replica by position."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from trellis.contracts.runs import RunEvent, RunEventType

from agent_runs.api.routers import runs as routes
from tests.conftest import pause, started

_CLAIM = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 30}


def _event(run_id: str, sequence: int, *, attempt: int = 1, **fields: Any) -> dict[str, Any]:
    fields.setdefault("type", RunEventType.STEP_STARTED)
    event = RunEvent(tenant_id="acme", run_id=run_id, sequence=sequence, attempt=attempt, **fields)
    return event.model_dump(mode="json")


async def _append(client, run_id: str, *events: dict[str, Any], worker: str | None = None):
    params = {"worker_id": worker} if worker else {}
    return await client.post(
        f"/v1/runs/{run_id}/events", json={"events": list(events)}, params=params
    )


async def _running(client) -> str:
    return (await client.post("/v1/runs", json=started())).json()["run_id"]


def _stream(body: str) -> list[tuple[str | None, str, dict[str, Any]]]:
    """(id, event, data) of each server-sent event; comments are left out."""
    found = []
    for block in body.split("\n\n"):
        fields = dict(
            line.split(": ", 1) for line in block.splitlines() if line and not line.startswith(":")
        )
        if fields:
            found.append((fields.get("id"), fields["event"], json.loads(fields["data"])))
    return found


async def test_events_are_logged_in_order_and_read_by_position(client) -> None:
    run_id = await _running(client)
    appended = await _append(client, run_id, _event(run_id, 0), _event(run_id, 1))
    assert appended.json() == {"appended": 2, "position": 2}
    text = _event(run_id, 2, type="TEXT_MESSAGE_CONTENT", message_id="m1", data={"delta": "hi"})
    assert (await _append(client, run_id, text)).json() == {"appended": 1, "position": 3}
    log = (await client.get(f"/v1/runs/{run_id}/events")).json()
    assert [entry["position"] for entry in log] == [1, 2, 3]
    assert log[2]["event"]["data"] == {"delta": "hi"}
    rest = await client.get(f"/v1/runs/{run_id}/events", params={"after": 1, "limit": 1})
    assert [entry["position"] for entry in rest.json()] == [2]


async def test_a_repeated_append_adds_nothing(client) -> None:
    run_id = await _running(client)
    first = [_event(run_id, 0), _event(run_id, 1)]
    await _append(client, run_id, *first)
    again = await _append(client, run_id, *first, _event(run_id, 1), _event(run_id, 0, attempt=2))
    assert again.json() == {"appended": 1, "position": 3}


async def test_an_event_of_another_run_is_refused(client) -> None:
    run_id = await _running(client)
    response = await _append(client, run_id, _event("run_other", 0))
    assert (response.status_code, response.json()["code"]) == (422, "VALIDATION")
    assert (await client.get(f"/v1/runs/{run_id}/events")).json() == []


async def test_a_leased_run_takes_events_from_its_lease_holder_only(client) -> None:
    await client.post("/v1/runs", json=started(queue=True))
    run_id = (await client.post("/v1/runs/claim", json=_CLAIM)).json()["run"]["run_id"]
    event = _event(run_id, 0)
    unnamed = await _append(client, run_id, event)
    assert (unnamed.status_code, unnamed.json()["code"]) == (409, "CONFLICT")
    stranger = await _append(client, run_id, event, worker="w2")
    assert (stranger.status_code, stranger.json()["code"]) == (409, "LEASE_LOST")
    assert (await _append(client, run_id, event, worker="w1")).status_code == 200


async def test_a_run_that_does_not_run_takes_no_events(client) -> None:
    run_id = await _running(client)
    await client.post(f"/v1/runs/{run_id}/pause", json=pause(run_id))
    refused = await _append(client, run_id, _event(run_id, 0))
    assert (refused.status_code, refused.json()["code"]) == (409, "CONFLICT")
    worker = await _append(client, run_id, _event(run_id, 0), worker="w1")
    assert (worker.status_code, worker.json()["code"]) == (409, "LEASE_LOST")


async def test_another_tenants_run_events_are_not_found(client, other_tenant) -> None:
    run_id = await _running(client)
    for path in ("/events", "/events/stream"):
        assert (await other_tenant.get(f"/v1/runs/{run_id}{path}")).status_code == 404


async def test_the_stream_sends_the_log_then_says_the_run_ended(client) -> None:
    run_id = await _running(client)
    await _append(client, run_id, *(_event(run_id, n) for n in range(3)))
    await client.post(f"/v1/runs/{run_id}/finish", json={"status": "SUCCESS"})
    response = await client.get(f"/v1/runs/{run_id}/events/stream")
    assert response.headers["content-type"].startswith("text/event-stream")
    sent = _stream(response.text)
    assert [(ident, name) for ident, name, _ in sent] == [
        ("1", "STEP_STARTED"),
        ("2", "STEP_STARTED"),
        ("3", "STEP_STARTED"),
        (None, "end"),
    ]
    assert sent[0][2]["event"]["sequence"] == 0 and sent[-1][2] == {"status": "SUCCESS"}
    # a reconnect carries the last position it saw; the larger of it and `after` wins
    resumed = await client.get(
        f"/v1/runs/{run_id}/events/stream", params={"after": 1}, headers={"Last-Event-ID": "2"}
    )
    assert [ident for ident, _, _ in _stream(resumed.text)] == ["3", None]


async def test_the_stream_follows_a_run_as_it_goes(client, monkeypatch) -> None:
    monkeypatch.setattr(routes, "EVENT_POLL_SECONDS", 0.01)
    monkeypatch.setattr(routes, "MAX_PAGE", 2)  # a full page is followed at once by the next
    run_id = await _running(client)
    await _append(client, run_id, *(_event(run_id, n) for n in range(3)))

    async def meanwhile() -> None:
        await asyncio.sleep(0.05)
        await _append(client, run_id, _event(run_id, 3))
        await asyncio.sleep(0.05)
        await client.post(f"/v1/runs/{run_id}/finish", json={"status": "ERROR"})

    response, _ = await asyncio.gather(client.get(f"/v1/runs/{run_id}/events/stream"), meanwhile())
    sent = _stream(response.text)
    assert [ident for ident, _, _ in sent] == ["1", "2", "3", "4", None]
    assert sent[-1][2] == {"status": "ERROR"}


async def test_a_quiet_stream_keeps_its_connection_and_ends_in_time(client, monkeypatch) -> None:
    monkeypatch.setattr(routes, "EVENT_POLL_SECONDS", 0.01)
    monkeypatch.setattr(routes, "EVENT_KEEPALIVE_SECONDS", 0.02)
    monkeypatch.setattr(routes, "EVENT_STREAM_SECONDS", 0.1)
    run_id = await _running(client)
    response = await client.get(f"/v1/runs/{run_id}/events/stream")
    assert ": keepalive" in response.text
    assert _stream(response.text) == [], "ended without `end`: the run still runs"


@pytest.mark.parametrize("path", ["/events", "/events/stream"])
async def test_an_unknown_runs_events_are_not_found(client, path: str) -> None:
    assert (await client.get(f"/v1/runs/run_nope{path}")).status_code == 404
