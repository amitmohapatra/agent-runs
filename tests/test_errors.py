"""One error shape on the wire: every failure is an RFC 9457 problem
(``application/problem+json``) with a stable ``code``, whether a store raised it, FastAPI
refused the request, the database went away, or something nobody anticipated broke."""

from __future__ import annotations

from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import DataError, DBAPIError, InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.ext.asyncio import create_async_engine

from agent_runs.store import database
from agent_runs.store.runs import RunStore
from tests.conftest import started

PROBLEM = "application/problem+json"


def assert_problem(response: Any, status: int, code: str, *, retryable: bool = False) -> dict:
    assert response.status_code == status, response.text
    assert response.headers["content-type"] == PROBLEM
    problem = response.json()
    assert problem["status"] == status
    assert (problem["code"], problem["retryable"]) == (code, retryable)
    assert problem["type"] == "urn:trellis:problem:" + code.lower().replace("_", "-")
    assert problem["title"] and problem["detail"]
    assert problem["instance"] == response.request.url.path
    assert problem["request_id"] == response.headers["x-request-id"]
    return problem


async def test_a_missing_record_is_a_not_found_problem(client) -> None:
    problem = assert_problem(await client.get("/v1/runs/run_nope"), 404, "NOT_FOUND")
    assert problem["detail"] == "no run run_nope"


async def test_a_route_that_does_not_exist_is_a_problem_too(client) -> None:
    assert_problem(await client.get("/v1/nowhere"), 404, "NOT_FOUND")


async def test_a_method_a_route_does_not_take_keeps_its_allow_header(client) -> None:
    response = await client.put("/v1/runs")
    assert_problem(response, 405, "VALIDATION")
    assert "POST" in response.headers["allow"]


async def test_an_invalid_body_lists_where_and_why_but_never_the_value(client) -> None:
    body = {**started(), "queue": "sekrit-value-1234"}
    response = await client.post("/v1/runs", json=body)
    problem = assert_problem(response, 422, "VALIDATION")
    errors = problem["details"]["errors"]
    assert [e["loc"] for e in errors] == [["body", "queue"]]
    assert set(errors[0]) == {"loc", "msg", "type"}
    assert "sekrit-value-1234" not in response.text


async def test_an_invalid_schedule_lists_where_and_why_but_never_the_value(client) -> None:
    """The contracts' validators run again on a merged update; their errors go out the same
    way as FastAPI's, without the submitted value."""
    created = (
        await client.post(
            "/v1/schedules",
            json={
                "tenant_id": "acme",
                "agent_id": "briefing",
                "name": "n",
                "cadence": "daily",
                "timezone": "UTC",
                "on_behalf_of": "user_ada",
            },
        )
    ).json()
    response = await client.patch(
        f"/v1/schedules/{created['schedule_id']}", json={"timezone": "Mars/Olympus_Mons"}
    )
    problem = assert_problem(response, 422, "VALIDATION")
    assert problem["details"]["errors"], problem
    assert "input_value" not in response.text


async def test_a_lost_lease_and_another_conflict_are_told_apart(client) -> None:
    await client.post("/v1/runs", json=started(queue=True))
    claim = {"worker_id": "w1", "agent_ids": ["triage"]}
    rid = (await client.post("/v1/runs/claim", json=claim)).json()["run"]["run_id"]
    stale = await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w_old"})
    assert_problem(stale, 409, "LEASE_LOST")
    resumed = await client.post(
        f"/v1/runs/{rid}/resume",
        json={"interrupt_id": "int_x", "run_id": rid, "decision": "APPROVE"},
    )
    assert_problem(resumed, 409, "CONFLICT")


@pytest.mark.parametrize(
    "failure",
    [
        OperationalError("SELECT", {}, Exception("connection refused")),
        InterfaceError("SELECT", {}, Exception("connection closed")),
        PoolTimeout("QueuePool limit of size 10 overflow 0 reached"),
        DBAPIError("SELECT", {}, Exception("server closed"), connection_invalidated=True),
    ],
    ids=["operational", "interface", "pool-timeout", "invalidated"],
)
async def test_a_database_that_went_away_is_a_retryable_503(client, monkeypatch, failure) -> None:
    async def down(*args: Any, **kwargs: Any) -> Any:
        raise failure

    monkeypatch.setattr(RunStore, "get", down)
    response = await client.get("/v1/runs/run_x")
    problem = assert_problem(response, 503, "DEPENDENCY_UNAVAILABLE", retryable=True)
    assert response.headers["retry-after"] == "5"
    assert "SELECT" not in problem["detail"] and "QueuePool" not in response.text


async def test_a_statement_the_database_refuses_is_a_500_that_says_nothing(
    client, monkeypatch
) -> None:
    async def refused(*args: Any, **kwargs: Any) -> Any:
        raise DataError("SELECT secret_column FROM agent_runs", {}, Exception("bad value"))

    monkeypatch.setattr(RunStore, "get", refused)
    response = await client.get("/v1/runs/run_x")
    problem = assert_problem(response, 500, "INTERNAL")
    assert problem["detail"] == "internal error"
    assert "secret_column" not in response.text and "retry-after" not in response.headers


async def test_an_unanticipated_failure_is_a_500_problem_without_internals(app) -> None:
    async def boom() -> None:
        raise RuntimeError("the password is hunter2")

    app.add_api_route("/boom", boom)
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://runs") as anon:
        response = await anon.get("/boom", headers={"X-Request-ID": "req-boom-1"})
    problem = assert_problem(response, 500, "INTERNAL")
    assert problem["request_id"] == "req-boom-1"
    assert "hunter2" not in response.text and "RuntimeError" not in response.text


async def test_the_callers_request_id_is_echoed_and_a_bad_one_replaced(client) -> None:
    mine = await client.get("/v1/runs", headers={"X-Request-ID": "req.abc:123-x"})
    assert mine.headers["x-request-id"] == "req.abc:123-x"
    bad = await client.get("/v1/runs/run_nope", headers={"X-Request-ID": "not an id!"})
    assert bad.headers["x-request-id"].startswith("req_")
    assert bad.json()["request_id"] == bad.headers["x-request-id"]


# ------------------------------------------------------------------ readiness


async def test_ready_answers_while_the_database_does(client) -> None:
    response = await client.get("/health/ready")
    assert (response.status_code, response.json()) == (200, {"status": "ok"})


async def test_ready_is_a_503_problem_while_the_database_does_not_answer(
    client, monkeypatch
) -> None:
    async def silent(*args: Any, **kwargs: Any) -> bool:
        return False

    monkeypatch.setattr("agent_runs.api.routers.ops.ping", silent)
    response = await client.get("/health/ready")
    assert_problem(response, 503, "DEPENDENCY_UNAVAILABLE", retryable=True)
    assert response.headers["retry-after"] == "5"
    assert (await client.get("/health/live")).status_code == 200, "liveness asks nobody"


async def test_the_probe_says_no_rather_than_raising(app) -> None:
    assert await database.ping(app.state.engine) is True
    nowhere = create_async_engine("postgresql+psycopg://memory:memory@127.0.0.1:1/none")
    try:
        assert await database.ping(nowhere, within=2.0) is False
    finally:
        await nowhere.dispose()


async def test_the_request_middleware_leaves_other_scopes_alone() -> None:
    from agent_runs.api.middleware import RequestContextMiddleware

    seen: list[str] = []

    async def inner(scope: Any, receive: Any, send: Any) -> None:
        seen.append(scope["type"])

    await RequestContextMiddleware(inner)({"type": "lifespan"}, None, None)  # type: ignore[arg-type]
    assert seen == ["lifespan"]
