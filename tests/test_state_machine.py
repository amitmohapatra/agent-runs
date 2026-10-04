"""The run state machine as the API enforces it, verb by verb and status by status: every
transition the contracts' ``RunStatus.can_become`` allows answers 200 and lands where
docs/ARCHITECTURE.md draws it, every other one is a 409 that changes nothing, and every verb
on a run this tenant does not hold is a 404.

The expectations are spelled out here rather than read from ``can_become``, so a change to
the contracts' rule shows up as a failing row instead of a silently redrawn diagram."""

from __future__ import annotations

from typing import Any

import pytest
from httpx import AsyncClient

from tests.conftest import pause, paused, resolution, started

LIVE = ("QUEUED", "RUNNING", "PAUSED")
ENDINGS = ("SUCCESS", "PARTIAL", "ERROR", "TIMEOUT", "CANCELLED", "REJECTED")
FAILED = {"ERROR", "TIMEOUT", "REJECTED"}

#: How each verb may end a run, by the status it is in: the only endings a run that is not
#: RUNNING may take are a cancel and a timeout.
FINISHABLE = {
    "QUEUED": {"CANCELLED", "TIMEOUT"},
    "RUNNING": set(ENDINGS),
    "PAUSED": {"CANCELLED", "TIMEOUT"},
}


def ending(status: str) -> dict[str, Any]:
    body: dict[str, Any] = {"status": status}
    if status in FAILED:
        body["error"] = {"code": "x", "category": "TOOL", "message": "m", "retryable": False}
    return body


async def in_status(client: AsyncClient, status: str) -> dict[str, Any]:
    """A run of this tenant in ``status``, reached only through the API."""
    if status == "PAUSED":
        return await paused(client)
    run = (await client.post("/v1/runs", json=started(queue=status == "QUEUED"))).json()
    if status in ENDINGS:
        response = await client.post(f"/v1/runs/{run['run_id']}/finish", json=ending(status))
        assert response.status_code == 200, response.text
        run = response.json()
    assert run["status"] == status
    return run


async def status_of(client: AsyncClient, run_id: str) -> str:
    return (await client.get(f"/v1/runs/{run_id}")).json()["status"]


# ------------------------------------------------------------------ finish


@pytest.mark.parametrize("start", LIVE + ENDINGS)
async def test_finish_follows_the_state_machine(client, start: str) -> None:
    """Each ending tried on a fresh run in ``start``."""
    for end in ENDINGS:
        run = await in_status(client, start)
        response = await client.post(f"/v1/runs/{run['run_id']}/finish", json=ending(end))
        if end in FINISHABLE.get(start, set()):
            assert response.status_code == 200, (end, response.text)
            ended = response.json()
            assert ended["status"] == end
            assert ended["awaiting"] is None and ended["checkpoint"] is None
        else:
            assert response.status_code == 409, (end, response.text)
            assert await status_of(client, run["run_id"]) == start


# ------------------------------------------------------------------ pause and resume


@pytest.mark.parametrize("start", LIVE + ENDINGS)
async def test_only_a_running_run_pauses(client, start: str) -> None:
    run = await in_status(client, start)
    response = await client.post(f"/v1/runs/{run['run_id']}/pause", json=pause(run["run_id"]))
    if start == "RUNNING":
        assert response.status_code == 200
        assert response.json()["status"] == "PAUSED"
    else:
        assert response.status_code == 409
        assert await status_of(client, run["run_id"]) == start


@pytest.mark.parametrize("start", ("QUEUED", "RUNNING", *ENDINGS))
async def test_only_a_paused_run_is_resumed(client, start: str) -> None:
    run = await in_status(client, start)
    answer = {"interrupt_id": "int_none", "run_id": run["run_id"], "decision": "APPROVE"}
    response = await client.post(f"/v1/runs/{run['run_id']}/resume", json=answer)
    assert response.status_code == 409
    assert await status_of(client, run["run_id"]) == start


@pytest.mark.parametrize(
    ("decision", "queued", "status", "attempt"),
    [
        ("APPROVE", False, "RUNNING", 2),
        ("ANSWER", False, "RUNNING", 2),
        ("REJECT", False, "RUNNING", 2),
        ("EDIT", False, "RUNNING", 2),
        ("CANCEL", False, "CANCELLED", 1),
        ("APPROVE", True, "QUEUED", 2),
        ("CANCEL", True, "CANCELLED", 1),
    ],
)
async def test_a_resume_continues_or_ends_the_run_by_its_decision_and_origin(
    client, decision: str, queued: bool, status: str, attempt: int
) -> None:
    """Any decision but CANCEL continues the run as its next attempt: back on the queue when
    it came from there, else RUNNING for the process that resumes it."""
    if queued:
        await client.post("/v1/runs", json=started(queue=True))
        claim = {"worker_id": "w1", "agent_ids": ["triage"]}
        rid = (await client.post("/v1/runs/claim", json=claim)).json()["run"]["run_id"]
        params = {"worker_id": "w1"}
        run = (await client.post(f"/v1/runs/{rid}/pause", params=params, json=pause(rid))).json()
    else:
        run = await paused(client)
    extra: dict[str, Any] = {"payload": {"qty": 2}} if decision == "EDIT" else {}
    if decision == "ANSWER":
        extra["answer"] = "yes"
    response = await client.post(
        f"/v1/runs/{run['run_id']}/resume", json=resolution(run, decision, **extra)
    )
    assert response.status_code == 200, response.text
    resumed = response.json()
    assert (resumed["status"], resumed["attempt"]) == (status, attempt)
    assert resumed["awaiting"] is None
    assert resumed["last_resolution"]["decision"] == decision


# ------------------------------------------------------------------ the queue verbs


@pytest.mark.parametrize("start", ("RUNNING", "PAUSED", *ENDINGS))
async def test_only_a_queued_run_is_claimed(client, start: str) -> None:
    run = await in_status(client, start)
    claim = {"worker_id": "w1", "agent_ids": ["triage"]}
    assert (await client.post("/v1/runs/claim", json=claim)).status_code == 204
    assert await status_of(client, run["run_id"]) == start


@pytest.mark.parametrize("start", LIVE + ENDINGS)
async def test_a_heartbeat_without_a_lease_is_refused(client, start: str) -> None:
    """Only the holder of a live lease heartbeats; a run that was never leased has none."""
    run = await in_status(client, start)
    beat = await client.post(f"/v1/runs/{run['run_id']}/heartbeat", json={"worker_id": "w1"})
    assert beat.status_code == 409
    assert await status_of(client, run["run_id"]) == start


# ------------------------------------------------------------------ unknown runs


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("GET", "/v1/runs/run_nope", None),
        ("GET", "/v1/runs/run_nope/resolutions", None),
        ("POST", "/v1/runs/run_nope/pause", pause("run_nope")),
        (
            "POST",
            "/v1/runs/run_nope/resume",
            {"interrupt_id": "int_x", "run_id": "run_nope", "decision": "APPROVE"},
        ),
        ("POST", "/v1/runs/run_nope/finish", {"status": "CANCELLED"}),
        ("POST", "/v1/runs/run_nope/heartbeat", {"worker_id": "w1"}),
    ],
)
async def test_every_verb_on_an_unknown_run_is_404(client, method, path, body) -> None:
    response = await client.request(method, path, json=body)
    assert response.status_code == 404
    assert "run_nope" in response.json()["detail"]
