"""Who may answer a paused run (``answering.py``): an admin or platform key and a key that
may act for anyone answer any run; a key restricted to listed people answers only as one of
them, only a run assigned to that person or to nobody, never one assigned to a group. A
refusal is a 403 ``AUTHORIZATION`` problem that says why, and changes nothing."""

from __future__ import annotations

from typing import Any

import pytest
from httpx import AsyncClient

from agent_runs.answering import as_principal, require_may_answer
from agent_runs.domain.errors import Forbidden
from agent_runs.keys import KeyInfo
from agent_runs.store.runs import RunStore
from tests.conftest import KEYS, at, client_of, pause, resolution, started
from tests.test_errors import assert_problem

PRIYA = KeyInfo.model_validate(KEYS["priya-key"])
_GROUP = (
    "the run is assigned to role:finance, a group: a key restricted to listed people cannot "
    "answer it; answer with the application's key or an admin key"
)


# ------------------------------------------------------------------------------ the rule


@pytest.mark.parametrize("key", ["dev-key", "admin-key", "platform-key"])
@pytest.mark.parametrize("assignee", [None, "user:raj", "role:finance"])
def test_an_admin_platform_or_any_principal_key_answers_any_run(key, assignee) -> None:
    require_may_answer(KeyInfo.model_validate(KEYS[key]), assignee, "raj")


@pytest.mark.parametrize(
    ("assignee", "reviewer"),
    [
        ("user:priya", "priya"),
        ("user:priya", "user:priya"),
        ("priya", "user:priya"),
        (None, "priya"),
        (None, None),
        ("key:key_priya", None),
    ],
)
def test_a_restricted_key_answers_as_its_person_a_run_assigned_to_them_or_nobody(
    assignee, reviewer
) -> None:
    require_may_answer(PRIYA, assignee, reviewer)


@pytest.mark.parametrize(
    ("assignee", "reviewer", "detail"),
    [
        (
            "user:priya",
            "raj",
            "this key may not act for user:raj; it may act only for user:priya",
        ),
        (
            "user:raj",
            "priya",
            "the run is assigned to user:raj, not user:priya; this key may act only for user:priya",
        ),
        (
            "user:priya",
            None,
            "the run is assigned to user:priya, not key:key_priya; this key may act only for "
            "user:priya",
        ),
        ("role:finance", "priya", _GROUP),
        ("role:finance", None, _GROUP),
    ],
)
def test_a_restricted_key_is_refused_with_why(assignee, reviewer, detail) -> None:
    with pytest.raises(Forbidden) as refused:
        require_may_answer(PRIYA, assignee, reviewer)
    assert refused.value.detail == detail


def test_a_key_restricted_to_nobody_answers_only_as_itself() -> None:
    itself = KeyInfo.model_validate(KEYS["narrow-key"] | {"principal": "key:key_narrow"})
    require_may_answer(itself, None, None)
    with pytest.raises(Forbidden, match="may act only for key:key_narrow"):
        require_may_answer(itself, None, "priya")


def test_a_bare_id_is_a_user() -> None:
    assert (as_principal("priya"), as_principal("agent:a1")) == ("user:priya", "agent:a1")


# ------------------------------------------------------------------------------ the route


async def _paused(client: AsyncClient, **interrupt: Any) -> dict[str, Any]:
    run = (await client.post("/v1/runs", json=started())).json()
    body = pause(run["run_id"], **interrupt)
    response = await client.post(f"/v1/runs/{run['run_id']}/pause", json=body)
    assert response.status_code == 200, response.text
    return response.json()


async def _answer(app: Any, key: str, run: dict[str, Any], decision: str = "APPROVE", **over):
    async with client_of(app, key) as answering:
        return await answering.post(
            f"/v1/runs/{run['run_id']}/resume", json=resolution(run, decision, **over)
        )


async def test_a_restricted_key_answers_its_persons_run_and_the_reviewer_is_kept_as_given(
    app, client
) -> None:
    for reviewer in ("priya", "user:priya"):
        run = await _paused(client, assignee="user:priya")
        response = await _answer(app, "priya-key", run, reviewer=reviewer)
        assert response.status_code == 200, response.text
        assert response.json()["last_resolution"]["reviewer"] == reviewer


async def test_a_restricted_key_answers_an_unassigned_run(app, client) -> None:
    run = await _paused(client)
    assert (await _answer(app, "priya-key", run, reviewer="priya")).status_code == 200


async def test_a_refusal_is_an_authorization_problem_and_changes_nothing(app, client) -> None:
    run = await _paused(client, assignee="user:raj")
    problem = assert_problem(
        await _answer(app, "priya-key", run, reviewer="priya"), 403, "AUTHORIZATION"
    )
    assert problem["detail"] == (
        "the run is assigned to user:raj, not user:priya; this key may act only for user:priya"
    )
    stored = (await client.get(f"/v1/runs/{run['run_id']}")).json()
    assert (stored["status"], stored["last_resolution"]) == ("PAUSED", None)
    assert (await client.get(f"/v1/runs/{run['run_id']}/resolutions")).json() == []


@pytest.mark.parametrize(
    ("assignee", "over", "detail"),
    [
        ("user:priya", {"reviewer": "raj"}, "this key may not act for user:raj"),
        ("user:priya", {}, "not key:key_priya"),
        ("role:finance", {"reviewer": "priya"}, "answer with the application's key"),
    ],
)
async def test_a_restricted_key_is_refused_for_another_reviewer_itself_or_a_group(
    app, client, assignee, over, detail
) -> None:
    run = await _paused(client, assignee=assignee)
    problem = assert_problem(await _answer(app, "priya-key", run, **over), 403, "AUTHORIZATION")
    assert detail in problem["detail"]


async def test_a_cancel_is_an_answer_like_any_other(app, client) -> None:
    raj = await _paused(client, assignee="user:raj")
    refused = await _answer(app, "priya-key", raj, "CANCEL", reviewer="priya")
    assert refused.status_code == 403
    priya = await _paused(client, assignee="user:priya")
    cancelled = await _answer(app, "priya-key", priya, "CANCEL", reviewer="priya")
    assert cancelled.json()["status"] == "CANCELLED"


@pytest.mark.parametrize("key", ["dev-key", "admin-key", "platform-key"])
async def test_the_applications_admin_and_platform_keys_answer_a_group_or_anyones_run(
    app, client, key
) -> None:
    for assignee in ("role:finance", "user:raj"):
        run = await _paused(client, assignee=assignee)
        headers = {"X-Trellis-Tenant": "acme"} if key == "platform-key" else {}
        async with client_of(app, key, **headers) as answering:
            response = await answering.post(
                f"/v1/runs/{run['run_id']}/resume", json=resolution(run, reviewer="raj")
            )
        assert response.status_code == 200, response.text


async def test_the_assignee_that_counts_is_the_one_after_escalation(app, client) -> None:
    run = await _paused(
        client, assignee="user:raj", deadline=at(10).isoformat(), escalate_to="user:priya"
    )
    assert (await _answer(app, "priya-key", run, reviewer="priya")).status_code == 403
    async with app.state.sessions() as db:
        await RunStore(db).escalate_overdue(now=at(11), limit=10)
        await db.commit()
    assert (await _answer(app, "priya-key", run, reviewer="priya")).status_code == 200


async def test_reading_stays_tenant_wide_for_a_restricted_key(app, client) -> None:
    run = await _paused(client, assignee="user:raj")
    async with client_of(app, "priya-key") as priya:
        assert (await priya.get(f"/v1/runs/{run['run_id']}")).status_code == 200
        inbox = await priya.get("/v1/runs", params={"status": "PAUSED", "assignee": "user:raj"})
        assert [r["run_id"] for r in inbox.json()] == [run["run_id"]]
