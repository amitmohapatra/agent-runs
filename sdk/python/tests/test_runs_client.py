"""The run verbs: what each sends (route, body, query, tenant) and what it answers."""

from __future__ import annotations

import json

import httpx
import pytest
import respx
from conftest import URL, interrupt, lease, record, summary
from trellis.contracts.errors import AgentError, ErrorCategory
from trellis.contracts.runs import (
    InterruptDecision,
    InterruptResolution,
    RunStart,
    RunStatus,
)
from trellis.runs import Claimed, Lease, LeaseLostError, RunsClient, RunSummary
from trellis.runs.client import DEFAULT_URL


@pytest.fixture
def runs() -> RunsClient:
    return RunsClient(URL, api_key="k", max_retries=2)


def sent(route: respx.Route) -> httpx.Request:
    return route.calls.last.request


# --------------------------------------------------------------------------- configuration
async def test_the_url_and_key_come_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNS_URL", "http://env.test/")
    monkeypatch.setenv("TRELLIS_API_KEY", "env-key")
    with respx.mock:
        live = respx.get("http://env.test/health/live").respond(200, json={"status": "ok"})
        async with RunsClient() as runs:
            assert await runs.live() == {"status": "ok"}
    assert sent(live).headers["X-API-Key"] == "env-key"
    assert "X-Trellis-Tenant" not in sent(live).headers
    assert sent(live).headers["User-Agent"] == "trellis-runs-python"


async def test_without_an_environment_the_local_stack_is_asked_with_no_key() -> None:
    with respx.mock:
        ready = respx.get(f"{DEFAULT_URL}/health/ready").respond(200, json={"status": "ok"})
        runs = RunsClient()
        assert await runs.ready() == {"status": "ok"}
        await runs.aclose()
    assert "X-API-Key" not in sent(ready).headers


async def test_an_injected_client_gets_the_base_url_and_key_and_stays_open() -> None:
    with respx.mock:
        respx.get(f"{URL}/metrics").respond(200, text="runs_claims_total 3\n")
        mine = httpx.AsyncClient()
        async with RunsClient(URL, api_key="k", http_client=mine) as runs:
            assert await runs.metrics() == "runs_claims_total 3\n"
        assert not mine.is_closed and mine.headers["X-API-Key"] == "k"
        await mine.aclose()


async def test_an_injected_client_keeps_its_own_base_url() -> None:
    with respx.mock:
        respx.get("http://other.test/health/live").respond(200, json={"status": "ok"})
        mine = httpx.AsyncClient(base_url="http://other.test")
        assert await RunsClient(URL, http_client=mine).live() == {"status": "ok"}
        await mine.aclose()


# --------------------------------------------------------------------------- the tenant
@respx.mock
async def test_the_tenant_is_the_bodys_the_calls_or_the_clients() -> None:
    runs = RunsClient(URL, api_key="platform", tenant="default-tenant")
    start = respx.post(f"{URL}/v1/runs").respond(201, json=record())
    get = respx.get(f"{URL}/v1/runs/run_1").respond(200, json=record())
    await runs.start(RunStart(tenant_id="acme", agent_id="triage"))
    assert sent(start).headers["X-Trellis-Tenant"] == "acme"  # the body's
    await runs.get("run_1", tenant="globex")
    assert sent(get).headers["X-Trellis-Tenant"] == "globex"  # the call's
    await runs.get("run_1")
    assert sent(get).headers["X-Trellis-Tenant"] == "default-tenant"  # the client's


# --------------------------------------------------------------------------- the verbs
@respx.mock
async def test_start_records_or_queues_a_run(runs: RunsClient) -> None:
    route = respx.post(f"{URL}/v1/runs").respond(201, json=record(status="QUEUED"))
    made = await runs.start(
        RunStart(tenant_id="acme", agent_id="triage", run_id="run_1"), queue=True
    )
    assert made.status is RunStatus.QUEUED
    body = json.loads(sent(route).content)
    assert body["queue"] is True and body["tenant_id"] == "acme" and body["run_id"] == "run_1"
    await runs.start(RunStart(tenant_id="acme", agent_id="triage"))
    assert json.loads(sent(route).content)["queue"] is False


@respx.mock
async def test_claim_answers_the_run_and_its_lease_or_none(runs: RunsClient) -> None:
    route = respx.post(f"{URL}/v1/runs/claim")
    route.side_effect = [
        httpx.Response(200, json={"run": record(), "lease": lease()}),
        httpx.Response(204),
    ]
    claimed = await runs.claim("w-1", ("triage", "billing"), lease_seconds=30, tenant="acme")
    assert isinstance(claimed, Claimed) and claimed.run.run_id == "run_1"
    assert claimed.lease.worker_id == "w-1"
    assert json.loads(sent(route).content) == {
        "worker_id": "w-1",
        "agent_ids": ["triage", "billing"],
        "lease_seconds": 30,
    }
    assert sent(route).headers["X-Trellis-Tenant"] == "acme"
    assert await runs.claim("w-1", ["triage"]) is None


@respx.mock
async def test_heartbeat_extends_the_lease_and_may_save_progress(runs: RunsClient) -> None:
    route = respx.post(f"{URL}/v1/runs/run_1/heartbeat").respond(200, json=lease())
    assert isinstance(await runs.heartbeat("run_1", "w-1"), Lease)
    assert json.loads(sent(route).content) == {"worker_id": "w-1", "lease_seconds": 60}
    await runs.heartbeat("run_1", "w-1", lease_seconds=90, checkpoint={"tools": {}}, tenant="acme")
    assert json.loads(sent(route).content)["checkpoint"] == {"tools": {}}


@pytest.mark.parametrize("code", ["LEASE_LOST", "CONFLICT", None])
@respx.mock
async def test_any_refused_heartbeat_is_a_lost_lease(runs: RunsClient, code: str | None) -> None:
    body = {"code": code, "detail": "not yours", "request_id": "req_9"} if code else None
    respx.post(f"{URL}/v1/runs/run_1/heartbeat").respond(409, json=body)
    with pytest.raises(LeaseLostError) as lost:
        await runs.heartbeat("run_1", "w-1")
    assert lost.value.status == 409 and lost.value.code == "LEASE_LOST"
    assert not lost.value.retryable


@respx.mock
async def test_pause_sends_the_interrupt_the_checkpoint_and_the_worker(runs: RunsClient) -> None:
    route = respx.post(f"{URL}/v1/runs/run_1/pause").respond(200, json=record(status="PAUSED"))
    asked = interrupt()
    paused = await runs.pause(asked, checkpoint={"step": 3}, worker_id="w-1")
    assert paused.status is RunStatus.PAUSED
    request = sent(route)
    assert request.url.params["worker_id"] == "w-1"
    assert request.headers["X-Trellis-Tenant"] == "acme"
    body = json.loads(request.content)
    assert body["checkpoint"] == {"step": 3} and body["interrupt"]["interrupt_id"] == "int_1"
    await runs.pause(asked)
    assert "worker_id" not in sent(route).url.params
    assert json.loads(sent(route).content)["checkpoint"] is None


@respx.mock
async def test_resume_sends_the_resolution(runs: RunsClient) -> None:
    route = respx.post(f"{URL}/v1/runs/run_1/resume").respond(200, json=record())
    answer = InterruptResolution(
        interrupt_id="int_1", run_id="run_1", decision=InterruptDecision.APPROVE
    )
    assert (await runs.resume(answer, tenant="acme")).run_id == "run_1"
    assert json.loads(sent(route).content)["decision"] == "APPROVE"
    assert sent(route).headers["X-Trellis-Tenant"] == "acme"


@respx.mock
async def test_finish_sends_the_ending_and_its_error(runs: RunsClient) -> None:
    route = respx.post(f"{URL}/v1/runs/run_1/finish").respond(200, json=record(status="SUCCESS"))
    await runs.finish("run_1", RunStatus.SUCCESS, output={"po": "PO-17"}, worker_id="w-1")
    assert json.loads(sent(route).content) == {"status": "SUCCESS", "output": {"po": "PO-17"}}
    assert sent(route).url.params["worker_id"] == "w-1"
    failed = AgentError(code="ToolFailed", category=ErrorCategory.TOOL, message="no")
    await runs.finish("run_1", RunStatus.ERROR, error=failed, tenant="acme")
    body = json.loads(sent(route).content)
    assert body["error"]["code"] == "ToolFailed" and body["output"] is None


@respx.mock
async def test_get_answers_the_record_or_none(runs: RunsClient) -> None:
    route = respx.get(f"{URL}/v1/runs/run_1")
    route.side_effect = [
        httpx.Response(200, json=record()),
        httpx.Response(404, json={"code": "NOT_FOUND", "detail": "no run run_1"}),
    ]
    found = await runs.get("run_1")
    assert found is not None and found.run_id == "run_1"
    assert await runs.get("run_1") is None


# --------------------------------------------------------------------------- listings
NEXT = '<http://runs.test/v1/runs?status=PAUSED&limit=2&cursor=c2>; rel="next"'


@respx.mock
async def test_list_sends_the_filters_and_reads_the_next_cursor(runs: RunsClient) -> None:
    route = respx.get(f"{URL}/v1/runs").respond(
        200, json=[summary("run_1"), summary("run_2")], headers={"Link": NEXT}
    )
    page = await runs.list(
        status=RunStatus.PAUSED,
        assignee="role:procurement",
        agent_id="triage",
        thread_id="thr_1",
        parent_run_id="run_0",
        cursor="c1",
        limit=2,
    )
    assert [s.run_id for s in page.items] == ["run_1", "run_2"]
    assert all(isinstance(s, RunSummary) for s in page.items)
    assert page.next_cursor == "c2" and page.has_more
    assert dict(sent(route).url.params) == {
        "status": "PAUSED",
        "assignee": "role:procurement",
        "agent_id": "triage",
        "thread_id": "thr_1",
        "parent_run_id": "run_0",
        "cursor": "c1",
        "limit": "2",
    }


@respx.mock
async def test_the_last_page_has_no_cursor(runs: RunsClient) -> None:
    route = respx.get(f"{URL}/v1/runs").respond(200, json=[])
    page = await runs.list()
    assert page.items == [] and page.next_cursor is None and not page.has_more
    assert dict(sent(route).url.params) == {"limit": "50"}


@respx.mock
async def test_iterate_follows_the_pages_to_the_end(runs: RunsClient) -> None:
    route = respx.get(f"{URL}/v1/runs")
    route.side_effect = [
        httpx.Response(200, json=[summary("run_1")], headers={"Link": NEXT}),
        httpx.Response(200, json=[summary("run_2")]),
    ]
    seen = [s.run_id async for s in runs.iterate(status=RunStatus.PAUSED, limit=1)]
    assert seen == ["run_1", "run_2"]
    assert route.calls[1].request.url.params["cursor"] == "c2"
    assert route.calls[1].request.url.params["status"] == "PAUSED"


@respx.mock
async def test_iterate_stops_after_max_pages(runs: RunsClient) -> None:
    route = respx.get(f"{URL}/v1/runs").respond(200, json=[summary()], headers={"Link": NEXT})
    seen = [s async for s in runs.iterate(assignee="user:alice", tenant="acme", max_pages=3)]
    assert len(seen) == 3 and route.call_count == 3


@respx.mock
async def test_resolutions_page_the_audit_trail(runs: RunsClient) -> None:
    entry = {
        "interrupt": interrupt().model_dump(mode="json"),
        "resolution": {"interrupt_id": "int_1", "run_id": "run_1", "decision": "APPROVE"},
        "attempt": 1,
        "recorded_at": "2026-10-01T08:00:00Z",
    }
    route = respx.get(f"{URL}/v1/runs/run_1/resolutions").respond(200, json=[entry])
    page = await runs.resolutions("run_1", cursor="c1", limit=10, tenant="acme")
    assert page.items[0].resolution.decision is InterruptDecision.APPROVE
    assert dict(sent(route).url.params) == {"cursor": "c1", "limit": "10"}
    await runs.resolutions("run_1")
    assert dict(sent(route).url.params) == {"limit": "50"}
