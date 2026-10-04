"""Retries (what, how often, how long between) and the error each answer raises."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest
import respx
from conftest import URL, problem, record
from trellis.contracts.runs import RunStatus
from trellis.runs import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    DependencyUnavailableError,
    LeaseLostError,
    NotFoundError,
    PayloadTooLargeError,
    RateLimitedError,
    RunsClient,
    RunsError,
    ValidationError,
)
from trellis.runs import _transport as transport
from trellis.runs.errors import error_from_problem


@pytest.fixture
def runs() -> RunsClient:
    return RunsClient(URL, api_key="k")


# --------------------------------------------------------------------------- retries
@pytest.mark.parametrize("status", [429, 502, 503, 504])
@respx.mock
async def test_a_call_that_failed_on_the_way_is_retried(
    runs: RunsClient, slept: list[float], status: int
) -> None:
    route = respx.get(f"{URL}/v1/runs/run_1")
    route.side_effect = [httpx.Response(status), httpx.Response(200, json=record())]
    found = await runs.get("run_1")
    assert found is not None and route.call_count == 2
    assert len(slept) == 1 and 0 <= slept[0] <= transport.BACKOFF_SECONDS


@respx.mock
async def test_retries_stop_after_max_retries_with_growing_jittered_waits(
    runs: RunsClient, slept: list[float]
) -> None:
    route = respx.post(f"{URL}/v1/runs/run_1/finish").respond(
        503, json=problem(503, "DEPENDENCY_UNAVAILABLE", retryable=True)
    )
    with pytest.raises(DependencyUnavailableError) as failed:
        await runs.finish("run_1", RunStatus.SUCCESS)
    assert route.call_count == 1 + transport.RETRIES
    assert failed.value.retryable and failed.value.status == 503
    assert [w <= transport.BACKOFF_SECONDS * 2**i for i, w in enumerate(slept)] == [True] * 3


def test_the_backoff_ceiling_grows_to_a_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("trellis.runs._transport.random.uniform", lambda low, high: high)
    assert [transport.backoff(n, None) for n in range(6)] == [0.25, 0.5, 1.0, 2.0, 4.0, 5.0]


@respx.mock
async def test_retry_after_is_honoured_up_to_a_cap(runs: RunsClient, slept: list[float]) -> None:
    route = respx.get(f"{URL}/v1/runs/run_1")
    later = format_datetime(datetime.now(UTC) + timedelta(seconds=120), usegmt=True)
    route.side_effect = [
        httpx.Response(
            429, headers={"Retry-After": "2"}, json=problem(429, "RATE_LIMIT", retryable=True)
        ),
        httpx.Response(503, headers={"Retry-After": later}),
        httpx.Response(503, headers={"Retry-After": "soon"}),
        httpx.Response(200, json=record()),
    ]
    assert await runs.get("run_1") is not None
    assert slept[0] == 2.0
    assert slept[1] == transport.RETRY_AFTER_MAX_SECONDS
    assert 0 <= slept[2] <= transport.BACKOFF_SECONDS * 4


def test_retry_after_reads_seconds_and_dates() -> None:
    assert transport.retry_after(None) is None
    assert transport.retry_after("") is None
    assert transport.retry_after("-3") == 0.0
    assert transport.retry_after("not a date") is None
    past = format_datetime(datetime(2020, 1, 1, tzinfo=UTC), usegmt=True)
    assert transport.retry_after(past) == 0.0
    asked = transport.retry_after("Wed, 01 Jan 2120 00:00:00 -0000")  # no zone: UTC
    due = (datetime(2120, 1, 1, tzinfo=UTC) - datetime.now(UTC)).total_seconds()
    assert asked is not None and abs(asked - due) < 60


@respx.mock
async def test_a_refusal_the_service_calls_final_is_not_retried(runs: RunsClient) -> None:
    """A fire that paused its schedule answers 503 with ``retryable: false``: sending it again
    would only meet the paused schedule."""
    route = respx.post(f"{URL}/v1/schedules/sch_1/fire").respond(
        503, json=problem(503, "DEPENDENCY_UNAVAILABLE", details={"auto_paused": True})
    )
    with pytest.raises(DependencyUnavailableError) as failed:
        await runs.schedules.fire("sch_1")
    assert route.call_count == 1 and not failed.value.retryable
    assert failed.value.details == {"auto_paused": True}


@respx.mock
async def test_a_refusal_is_not_retried(runs: RunsClient) -> None:
    route = respx.get(f"{URL}/v1/runs").respond(422, json=problem(422, "VALIDATION"))
    with pytest.raises(ValidationError):
        await runs.list()
    assert route.call_count == 1


@respx.mock
async def test_an_unreachable_service_is_retried_then_unavailable(
    runs: RunsClient, slept: list[float]
) -> None:
    route = respx.post(f"{URL}/v1/runs/claim").mock(side_effect=httpx.ConnectError("refused"))
    with pytest.raises(DependencyUnavailableError) as failed:
        await runs.claim("w-1", ["triage"])
    assert route.call_count == 4 and len(slept) == 3
    assert failed.value.status == 0 and failed.value.retryable
    assert "unreachable: ConnectError: refused" in str(failed.value)
    assert isinstance(failed.value.__cause__, httpx.ConnectError)


@respx.mock
async def test_a_dropped_connection_then_an_answer_succeeds(runs: RunsClient) -> None:
    route = respx.post(f"{URL}/v1/runs/claim")
    route.side_effect = [httpx.ReadTimeout("slow"), httpx.Response(204)]
    assert await runs.claim("w-1", ["triage"]) is None


@respx.mock
async def test_a_failure_that_is_not_the_transports_is_not_retried(runs: RunsClient) -> None:
    route = respx.get(f"{URL}/v1/runs/run_1").mock(side_effect=httpx.TooManyRedirects("loop"))
    with pytest.raises(RunsError) as failed:
        await runs.get("run_1")
    assert route.call_count == 1 and failed.value.status == 0 and not failed.value.retryable
    assert "failed: TooManyRedirects: loop" in str(failed.value)


async def test_no_retries_when_max_retries_is_zero(slept: list[float]) -> None:
    with respx.mock:
        route = respx.get(f"{URL}/v1/runs/run_1").respond(503)
        with pytest.raises(DependencyUnavailableError):
            await RunsClient(URL, max_retries=0).get("run_1")
    assert route.call_count == 1 and slept == []


# --------------------------------------------------------------------------- errors
@pytest.mark.parametrize(
    ("status", "code", "cls"),
    [
        (401, "AUTHENTICATION", AuthenticationError),
        (403, "AUTHORIZATION", AuthorizationError),
        (404, "NOT_FOUND", NotFoundError),
        (409, "CONFLICT", ConflictError),
        (409, "LEASE_LOST", LeaseLostError),
        (400, "VALIDATION", ValidationError),
        (405, "VALIDATION", ValidationError),
        (413, "PAYLOAD_TOO_LARGE", PayloadTooLargeError),
        (422, "VALIDATION", ValidationError),
        (429, "RATE_LIMIT", RateLimitedError),
        (503, "DEPENDENCY_UNAVAILABLE", DependencyUnavailableError),
        (500, "INTERNAL", RunsError),
    ],
)
@respx.mock
async def test_each_problem_code_raises_its_class(
    status: int, code: str, cls: type[RunsError]
) -> None:
    respx.post(f"{URL}/v1/runs/run_1/finish").respond(
        status,
        json=problem(status, code, "worker w-1 does not hold the lease", details={"k": "v"}),
        headers={"Retry-After": "7"},
    )
    with pytest.raises(cls) as raised:
        await RunsClient(URL, max_retries=0).finish("run_1", RunStatus.SUCCESS)
    error = raised.value
    assert type(error) is cls
    assert (error.code, error.status, error.request_id) == (code, status, "req_1")
    assert error.details == {"k": "v"} and error.retry_after == 7.0 and not error.retryable
    assert str(error) == (
        f"agent-runs POST /v1/runs/run_1/finish: HTTP {status} {code}: "
        "worker w-1 does not hold the lease"
    )
    assert repr(error).startswith(f"{cls.__name__}(code={code}, status={status}")


def test_a_lost_lease_is_not_a_conflict() -> None:
    """``except ConflictError`` (re-read the run) must not swallow a lost lease."""
    assert not issubclass(LeaseLostError, ConflictError)
    assert not issubclass(ConflictError, LeaseLostError)
    assert issubclass(LeaseLostError, RunsError) and issubclass(
        PayloadTooLargeError, ValidationError
    )


@pytest.mark.parametrize(
    ("status", "cls", "code", "retryable"),
    [
        (400, ValidationError, "VALIDATION", False),
        (401, AuthenticationError, "AUTHENTICATION", False),
        (403, AuthorizationError, "AUTHORIZATION", False),
        (404, NotFoundError, "NOT_FOUND", False),
        (409, ConflictError, "CONFLICT", False),
        (413, PayloadTooLargeError, "PAYLOAD_TOO_LARGE", False),
        (429, RateLimitedError, "RATE_LIMIT", True),
        (502, DependencyUnavailableError, "DEPENDENCY_UNAVAILABLE", True),
        (504, DependencyUnavailableError, "DEPENDENCY_UNAVAILABLE", True),
        (418, RunsError, "INTERNAL", False),
    ],
)
def test_an_answer_without_a_code_is_classed_by_its_status(
    status: int, cls: type[RunsError], code: str, retryable: bool
) -> None:
    error = error_from_problem(
        status, "<html>proxy</html>", where="GET /x", text="<html>proxy</html>"
    )
    assert type(error) is cls and error.code == code and error.retryable is retryable
    assert str(error) == f"agent-runs GET /x: HTTP {status}: <html>proxy</html>"
    assert error.details == {} and error.request_id is None


def test_an_unknown_code_is_classed_by_its_status() -> None:
    error = error_from_problem(404, {"code": "GONE_FISHING", "title": "Gone"}, where="GET /x")
    assert type(error) is NotFoundError and error.code == "GONE_FISHING"
    assert str(error).endswith("HTTP 404 GONE_FISHING: Gone")


def test_a_gateways_problem_keeps_its_words_and_the_request_id_header() -> None:
    error = error_from_problem(
        503,
        {"title": "Service Unavailable", "details": "not a dict", "retryable": "yes"},
        where="GET /x",
        request_id="req_header",
        retry_after=3.0,
    )
    assert isinstance(error, DependencyUnavailableError) and error.retryable
    assert error.request_id == "req_header" and error.details == {} and error.retry_after == 3.0


@respx.mock
async def test_a_body_that_is_not_json_still_raises_by_status(runs: RunsClient) -> None:
    respx.get(f"{URL}/v1/runs/run_1").respond(403, text="forbidden by the proxy")
    with pytest.raises(AuthorizationError) as refused:
        await runs.get("run_1")
    assert str(refused.value).endswith("HTTP 403: forbidden by the proxy")


def test_next_cursor_reads_the_next_link_only() -> None:
    assert transport.next_cursor(None) is None
    assert transport.next_cursor('<http://r/v1/runs?cursor=a>; rel="prev"') is None
    both = '<http://r/v1/runs?cursor=a>; rel="prev", <http://r/v1/runs?limit=2&cursor=b>; rel=next'
    assert transport.next_cursor(both) == "b"
    assert transport.next_cursor('<http://r/v1/runs?limit=2>; rel="next"') is None
