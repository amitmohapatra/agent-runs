"""The SDK against agent-runs' committed OpenAPI document: every model the SDK parses an answer
with is the document's schema (the same properties, the same required ones), the closed sets
are its enums, and every call sends a request the document accepts and parses an answer the
document describes, the error answers included."""

from __future__ import annotations

import enum
import json
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from conftest import (
    URL,
    OpenAPI,
    artifact,
    delivery,
    interrupt,
    lease,
    problem,
    record,
    schedule,
    webhook,
)
from pydantic import BaseModel
from trellis.contracts.artifacts import ArtifactRef
from trellis.contracts.errors import AgentError, ErrorCategory
from trellis.contracts.runs import (
    InterruptDecision,
    InterruptReason,
    InterruptResolution,
    RunEvent,
    RunRecord,
    RunStart,
    RunStatus,
    Schedule,
    ScheduleSpec,
)
from trellis.runs import (
    Claimed,
    DeliveryRecord,
    DeliveryState,
    EventsAppended,
    FireResult,
    Lease,
    LeaseLostError,
    ResolutionEntry,
    RunEventEntry,
    RunsClient,
    RunSummary,
    ScheduleUpdate,
    Webhook,
    WebhookCreated,
    WebhookEvent,
)

# --------------------------------------------------------------------------- the schemas
#: the document's component schema, and the model the SDK reads (or sends) it as
MODELS: dict[str, type[BaseModel]] = {
    "RunSummary": RunSummary,
    "Lease": Lease,
    "Claimed": Claimed,
    "ResolutionEntry": ResolutionEntry,
    "RunEventEntry": RunEventEntry,
    "EventsAppended": EventsAppended,
    "ScheduleUpdate": ScheduleUpdate,
    "FireResult": FireResult,
    "Webhook": Webhook,
    "WebhookCreated": WebhookCreated,
    "DeliveryRecord": DeliveryRecord,
    # the contracts' models, used as they are
    "RunRecord": RunRecord,
    "RunStart": RunStart,
    "Schedule": Schedule,
    "ScheduleSpec": ScheduleSpec,
    "InterruptResolution": InterruptResolution,
    "ArtifactRef": ArtifactRef,
    "AgentError": AgentError,
}
ENUMS: dict[str, type[enum.Enum]] = {
    "WebhookEvent": WebhookEvent,
    "DeliveryState": DeliveryState,
    "RunStatus": RunStatus,
    "InterruptReason": InterruptReason,
    "InterruptDecision": InterruptDecision,
    "ErrorCategory": ErrorCategory,
}


@pytest.mark.parametrize("name", list(MODELS))
def test_each_model_is_the_documents_schema(contract: OpenAPI, name: str) -> None:
    # RunStart is documented as RunCreate (a RunStart plus ``queue``)
    schema = contract.schema("RunCreate" if name == "RunStart" else name)
    model = MODELS[name]
    documented = set(schema.get("properties", {})) - ({"queue"} if name == "RunStart" else set())
    assert set(model.model_fields) == documented, name
    required = {n for n, f in model.model_fields.items() if f.is_required()}
    assert required == set(schema.get("required", [])), name


@pytest.mark.parametrize("name", list(ENUMS))
def test_each_closed_set_is_the_documents_enum(contract: OpenAPI, name: str) -> None:
    assert set(contract.schema(name)["enum"]) == {member.value for member in ENUMS[name]}


# --------------------------------------------------------------------------- the wire
NEXT = {"Link": '<http://runs.test/v1/runs?cursor=c2&limit=1>; rel="next"'}
FIRED = {
    "schedule_id": "sch_1",
    "run_id": "run_9",
    "fire_time": "2026-10-01T06:00:00Z",
    "idempotency_key": "sch_1@2026-10-01T06:00:00+00:00",
    "schedule": schedule(),
}
ENTRY = {
    "interrupt": interrupt().model_dump(mode="json"),
    "resolution": {"interrupt_id": "int_1", "run_id": "run_1", "decision": "APPROVE"},
    "attempt": 1,
    "recorded_at": "2026-10-01T08:00:00Z",
}
EVENT = {"type": "STEP_STARTED", "tenant_id": "acme", "run_id": "run_1", "sequence": 0}
#: what agent-runs answers each operation with: status, JSON body (or bytes, or text)
ANSWERS: dict[str, tuple[int, Any]] = {
    "runs.start": (201, record()),
    "runs.claim": (200, {"run": record(), "lease": lease()}),
    "runs.heartbeat": (200, lease()),
    "runs.release": (200, record(status="QUEUED")),
    "runs.pause": (200, record(status="PAUSED")),
    "runs.resume": (200, record()),
    "runs.cancel": (200, record(status="CANCELLED")),
    "runs.finish": (200, record(status="SUCCESS", output={"po": "PO-17"})),
    "runs.get": (200, record()),
    "runs.list": (200, []),
    "runs.resolutions": (200, [ENTRY]),
    "runs.append_events": (200, {"appended": 1, "position": 1}),
    "runs.events": (200, [{"position": 1, "event": EVENT}]),
    "runs.stream_events": (
        200,
        f"id: 1\nevent: STEP_STARTED\ndata: {json.dumps({'position': 1, 'event': EVENT})}\n\n"
        'event: end\ndata: {"status": "SUCCESS"}\n\n',
    ),
    "artifacts.upload": (201, artifact()),
    "artifacts.download": (200, b'{"rows": []}'),
    "schedules.create": (201, schedule()),
    "schedules.list": (200, [schedule()]),
    "schedules.get": (200, schedule()),
    "schedules.update": (200, schedule(enabled=False)),
    "schedules.delete": (204, None),
    "schedules.fire": (200, FIRED),
    "webhooks.create": (201, {**webhook(), "secret": "whsec_x"}),
    "webhooks.list": (200, [webhook()]),
    "webhooks.get": (200, webhook()),
    "webhooks.delete": (204, None),
    "webhooks.rotate_secret": (
        200,
        {**webhook(previous_secret_expires_at="2026-10-02T08:00:00Z"), "secret": "whsec_y"},
    ),
    "webhooks.deliveries": (200, [delivery(), delivery("dlv_2", dead=True)]),
    "webhooks.redeliver": (200, delivery()),
    "ops.live": (200, {"status": "ok"}),
    "ops.ready": (200, {"status": "ok"}),
    "ops.metrics": (200, "runs_claims_total 1\n"),
}


class Service:
    """agent-runs as the document describes it: each request checked, answered from
    :data:`ANSWERS` (or with a problem, when one is queued), the answer checked too."""

    def __init__(self, contract: OpenAPI) -> None:
        self.contract = contract
        self.violations: list[str] = []
        self.seen: set[str] = set()
        #: problem answers to give instead, by operation id
        self.refusals: dict[str, tuple[int, dict[str, Any]]] = {}

    def client(self) -> RunsClient:
        transport = httpx.MockTransport(self.handle)
        http = httpx.AsyncClient(transport=transport, base_url=URL)
        return RunsClient(URL, api_key="k", tenant="acme", max_retries=0, http_client=http)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.violations.extend(self.contract.request(request))
        op = self.contract.operation(request.method, request.url.path)["operationId"]
        self.seen.add(op)
        if op in self.refusals:
            status, body = self.refusals[op]
            response = httpx.Response(
                status,
                content=json.dumps(body).encode(),
                headers={"Content-Type": "application/problem+json"},
            )
        else:
            response = self._answer(op)
        response.request = request
        self.violations.extend(self.contract.response(request, response))
        return response

    @staticmethod
    def _answer(op: str) -> httpx.Response:
        status, body = ANSWERS[op]
        if isinstance(body, bytes):
            return httpx.Response(
                status, content=body, headers={"Content-Type": "application/json"}
            )
        if isinstance(body, str):
            media = "text/event-stream" if op == "runs.stream_events" else "text/plain"
            return httpx.Response(status, text=body, headers={"Content-Type": media})
        if body is None:
            return httpx.Response(status)
        headers = NEXT if op == "runs.list" else {}
        return httpx.Response(status, json=body, headers=headers)


async def test_every_call_is_what_the_document_describes(contract: OpenAPI) -> None:
    service = Service(contract)
    async with service.client() as runs:
        start = RunStart(tenant_id="acme", agent_id="triage", run_id="run_1", input={"q": 1})
        await runs.start(start, queue=True)
        claimed = await runs.claim("w-1", ["triage"], lease_seconds=60)
        assert claimed is not None
        await runs.heartbeat("run_1", "w-1", checkpoint={"tools": {"call_1": {"output": "ok"}}})
        await runs.heartbeat("run_1", "w-1")
        await runs.release("run_1", "w-1", checkpoint={"tools": {}})
        await runs.release("run_1", "w-1")
        ref = await runs.artifacts.upload("run_1", b'{"rows": []}', worker_id="w-1")
        asked = interrupt(payload_ref=ref)
        await runs.pause(asked, checkpoint={"asks": {}}, worker_id="w-1")
        await runs.pause(asked)
        answer = InterruptResolution(
            interrupt_id="int_1", run_id="run_1", decision=InterruptDecision.EDIT, payload={"a": 1}
        )
        await runs.resume(answer)
        await runs.cancel("run_1", reason="the customer withdrew the request")
        await runs.cancel("run_1")
        await runs.finish("run_1", RunStatus.SUCCESS, output={"po": "PO-17"}, worker_id="w-1")
        failed = AgentError(code="ToolFailed", category=ErrorCategory.TOOL, message="no")
        await runs.finish("run_1", RunStatus.ERROR, error=failed)
        await runs.get("run_1")
        await runs.list(
            status=RunStatus.PAUSED,
            assignee="role:procurement",
            agent_id="triage",
            thread_id="thr_1",
            parent_run_id="run_0",
            top_level=True,
            cursor="c1",
            limit=500,
        )
        await runs.resolutions("run_1", cursor="c1", limit=1)
        await runs.append_events("run_1", [RunEvent.model_validate(EVENT)], worker_id="w-1")
        await runs.events("run_1", after=0, limit=5)
        assert [e.position async for e in runs.stream_events("run_1")] == [1]
        await runs.artifacts.download(ref.artifact_id)
        spec = ScheduleSpec(
            tenant_id="acme",
            agent_id="briefing",
            name="morning",
            cadence="0 8 * * 1-5",
            timezone="Europe/Berlin",
            on_behalf_of="user_ada",
            input={"topic": "inbox"},
        )
        await runs.schedules.create(spec)
        await runs.schedules.list(enabled=True, agent_id="briefing", cursor="c1", limit=5)
        await runs.schedules.get("sch_1")
        await runs.schedules.update("sch_1", ScheduleUpdate(enabled=False, metadata={"a": 1}))
        await runs.schedules.update("sch_1", ScheduleUpdate(cadence="daily", input=None))
        await runs.schedules.fire("sch_1", at=datetime(2026, 10, 1, 6, tzinfo=UTC))
        await runs.schedules.fire("sch_1")
        await runs.schedules.delete("sch_1")
        await runs.webhooks.create("https://ui.example/h", [WebhookEvent.PAUSED])
        await runs.webhooks.list(cursor="c1", limit=5)
        await runs.webhooks.get("wh_1")
        await runs.webhooks.delete("wh_1")
        await runs.webhooks.rotate_secret("wh_1")
        await runs.webhooks.deliveries(
            state=DeliveryState.DEAD, webhook_id="wh_1", cursor="c1", limit=5
        )
        await runs.webhooks.deliveries()
        await runs.webhooks.redeliver("dlv_1")
        await runs.live()
        await runs.ready()
        await runs.metrics()
    assert service.violations == []
    assert service.seen == set(ANSWERS)  # every operation, each checked


async def test_the_error_answers_are_problems_the_document_describes(contract: OpenAPI) -> None:
    service = Service(contract)
    service.refusals = {
        "runs.get": (404, problem(404, "NOT_FOUND", "no run run_1")),
        "runs.heartbeat": (409, problem(409, "LEASE_LOST", "worker w-1 does not hold run_1")),
        "artifacts.download": (404, problem(404, "NOT_FOUND", "no artifact art_1")),
    }
    async with service.client() as runs:
        assert await runs.get("run_1") is None
        assert await runs.artifacts.download("art_1") is None
        with pytest.raises(LeaseLostError):
            await runs.heartbeat("run_1", "w-1")
        await runs.release("run_1", "w-1", checkpoint={"tools": {}})
        await runs.release("run_1", "w-1")
    assert service.violations == []
