"""The SDK (``trellis.runs``) against this service, end to end: a platform key that names the
tenant on every call, a run queued, worked by ``trellis.runs.Worker``, paused for a person with
an artifact, found in the inbox, answered, worked again and finished; schedules and webhook
subscriptions; and a lost lease, as the SDK reports it."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from trellis.contracts.runs import (
    Interrupt,
    InterruptDecision,
    InterruptResolution,
    RunStart,
    RunStatus,
    ScheduleSpec,
)
from trellis.runs import (
    ConflictError,
    Job,
    LeaseLostError,
    NotFoundError,
    RunsClient,
    ScheduleUpdate,
    ValidationError,
    WebhookEvent,
    Worker,
)

from agent_runs.store.runs import RunStore
from tests.conftest import at


@pytest.fixture
async def runs(app: Any) -> AsyncIterator[RunsClient]:
    """The SDK with a platform key and no default tenant: every call names one."""
    http = AsyncClient(transport=ASGITransport(app=app), base_url="http://runs")
    async with RunsClient("http://runs", api_key="platform-key", http_client=http) as client:
        yield client
    await http.aclose()


async def test_a_durable_run_through_the_sdk(runs: RunsClient) -> None:
    start = RunStart(tenant_id="acme", agent_id="triage", input={"ticket": 7})
    queued = await runs.start(start, queue=True)
    assert queued.status is RunStatus.QUEUED

    async def first_attempt(job: Job) -> None:
        await job.checkpoint({"tools": {"call_1": {"output": "looked up"}}})
        table = await runs.artifacts.upload(
            job.record.run_id, b'{"rows": [["A-1", 12]]}', worker_id=job.worker_id, tenant="acme"
        )
        asked = Interrupt(
            tenant_id="acme",
            run_id=job.record.run_id,
            question="Order these?",
            assignee="role:procurement",
            payload_ref=table,
        )
        await job.pause(asked, checkpoint={"step": "asked"})

    worker = Worker(runs, first_attempt, ["triage"], tenant="acme", worker_id="w-1")
    assert await worker.run_once() is True

    inbox = [
        s
        async for s in runs.iterate(
            status=RunStatus.PAUSED, assignee="role:procurement", tenant="acme"
        )
    ]
    [waiting] = inbox
    assert waiting.run_id == queued.run_id and waiting.awaiting is not None
    ref = waiting.awaiting.payload_ref
    assert ref is not None
    assert (
        await runs.artifacts.download(ref.artifact_id, tenant="acme") == b'{"rows": [["A-1", 12]]}'
    )

    answer = InterruptResolution(
        interrupt_id=waiting.awaiting.interrupt_id,
        run_id=queued.run_id,
        decision=InterruptDecision.APPROVE,
        reviewer="user:alice",
    )
    resumed = await runs.resume(answer, tenant="acme")
    assert resumed.status is RunStatus.QUEUED and resumed.attempt == 2

    seen: list[Any] = []

    async def second_attempt(job: Job) -> None:
        seen.append((job.record.checkpoint, job.record.last_resolution))
        await job.finish(RunStatus.SUCCESS, output={"ordered": True})

    assert await Worker(runs, second_attempt, ["triage"], tenant="acme").run_once() is True
    [(checkpoint, resolution)] = seen
    assert checkpoint == {"step": "asked"} and resolution.decision is InterruptDecision.APPROVE

    done = await runs.get(queued.run_id, tenant="acme")
    assert (
        done is not None and done.status is RunStatus.SUCCESS and done.output == {"ordered": True}
    )
    trail = await runs.resolutions(queued.run_id, tenant="acme")
    assert [e.resolution.reviewer for e in trail.items] == ["user:alice"]
    assert await runs.get("run_missing", tenant="acme") is None
    retried = await runs.resume(answer, tenant="acme")  # the same answer, sent again
    assert (retried.status, retried.updated_at) == (RunStatus.SUCCESS, done.updated_at)
    second = answer.model_copy(update={"decision": InterruptDecision.REJECT})
    with pytest.raises(ConflictError):  # a second answer: the run is no longer paused
        await runs.resume(second, tenant="acme")


async def test_an_answer_that_does_not_fit_is_a_validation_error_saying_why(
    runs: RunsClient,
) -> None:
    run = await runs.start(RunStart(tenant_id="acme", agent_id="triage"))
    asked = Interrupt(
        tenant_id="acme", run_id=run.run_id, question="How many?", expects={"type": "integer"}
    )
    await runs.pause(asked)
    answer = InterruptResolution(
        interrupt_id=asked.interrupt_id,
        run_id=run.run_id,
        decision=InterruptDecision.ANSWER,
        answer="a few",
    )
    with pytest.raises(ValidationError) as refused:
        await runs.resume(answer, tenant="acme")
    assert refused.value.status == 422 and not refused.value.retryable
    assert "does not fit what was asked" in refused.value.message


async def test_a_lost_lease_is_a_lease_lost_error_not_a_conflict(runs: RunsClient) -> None:
    await runs.start(RunStart(tenant_id="acme", agent_id="triage"), queue=True)
    claimed = await runs.claim("w-1", ["triage"], tenant="acme")
    assert claimed is not None
    run_id = claimed.run.run_id
    await runs.finish(run_id, RunStatus.CANCELLED, tenant="acme")  # cancelled under the worker
    with pytest.raises(LeaseLostError):
        await runs.heartbeat(run_id, "w-1", tenant="acme")
    with pytest.raises(LeaseLostError) as fenced:
        await runs.finish(run_id, RunStatus.SUCCESS, worker_id="w-1", tenant="acme")
    assert not isinstance(fenced.value, ConflictError) and fenced.value.code == "LEASE_LOST"


async def test_a_lost_lease_stops_the_workers_handler(runs: RunsClient, monkeypatch) -> None:
    from trellis.runs import worker as worker_module

    async def soon(seconds: float) -> None:
        await asyncio.sleep(0.01)

    monkeypatch.setattr(worker_module, "_sleep", soon)
    queued = await runs.start(RunStart(tenant_id="acme", agent_id="triage"), queue=True)
    stopped = asyncio.Event()

    async def handler(job: Job) -> None:
        await runs.finish(job.record.run_id, RunStatus.CANCELLED, tenant="acme")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            stopped.set()
            raise

    assert await Worker(runs, handler, ["triage"], tenant="acme").run_once()
    assert stopped.is_set()
    done = await runs.get(queued.run_id, tenant="acme")
    assert done is not None and done.status is RunStatus.CANCELLED


async def test_a_run_past_its_deadline_stops_the_workers_handler(
    app: Any, runs: RunsClient, monkeypatch
) -> None:
    """The ticker times the run out under the worker; its next heartbeat is refused and the
    handler is cancelled, so it writes nothing more."""
    from trellis.runs import worker as worker_module

    async def soon(seconds: float) -> None:
        await asyncio.sleep(0.01)

    monkeypatch.setattr(worker_module, "_sleep", soon)
    start = RunStart(tenant_id="acme", agent_id="triage", deadline=at(1))
    queued = await runs.start(start, queue=True)
    stopped = asyncio.Event()

    async def handler(job: Job) -> None:
        async with app.state.sessions() as db:
            await RunStore(db).time_out_past_deadline(now=at(2), limit=10)
            await db.commit()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            stopped.set()
            raise

    assert await Worker(runs, handler, ["triage"], tenant="acme").run_once()
    assert stopped.is_set()
    done = await runs.get(queued.run_id, tenant="acme")
    assert done is not None and done.status is RunStatus.TIMEOUT
    assert done.error is not None and done.error.code == "run_deadline"


async def test_schedules_through_the_sdk(runs: RunsClient) -> None:
    spec = ScheduleSpec(
        tenant_id="acme",
        agent_id="briefing",
        name="morning",
        cadence="daily",
        on_behalf_of="user_ada",
        input={"topic": "inbox"},
    )
    made = await runs.schedules.create(spec)
    assert (await runs.schedules.create(spec)).schedule_id == made.schedule_id  # an upsert
    page = await runs.schedules.list(enabled=True, agent_id="briefing", tenant="acme")
    assert [s.schedule_id for s in page.items] == [made.schedule_id]
    fired = await runs.schedules.fire(made.schedule_id, tenant="acme")
    queued = await runs.get(fired.run_id, tenant="acme")
    assert queued is not None and queued.status is RunStatus.QUEUED
    paused = await runs.schedules.update(
        made.schedule_id, ScheduleUpdate(enabled=False), tenant="acme"
    )
    assert paused.enabled is False and paused.name == "morning"
    await runs.schedules.delete(made.schedule_id, tenant="acme")
    assert await runs.schedules.get(made.schedule_id, tenant="acme") is None
    with pytest.raises(NotFoundError):
        await runs.schedules.delete(made.schedule_id, tenant="acme")


async def test_webhook_subscriptions_through_the_sdk(runs: RunsClient) -> None:
    made = await runs.webhooks.create(
        "https://ui.example/h", [WebhookEvent.FINISHED, WebhookEvent.PAUSED], tenant="acme"
    )
    assert made.secret.startswith("whsec_")
    [listed] = (await runs.webhooks.list(tenant="acme")).items
    assert listed.webhook_id == made.webhook_id and listed.events == made.events
    found = await runs.webhooks.get(made.webhook_id, tenant="acme")
    assert found is not None and found.url == "https://ui.example/h"
    await runs.webhooks.delete(made.webhook_id, tenant="acme")
    assert await runs.webhooks.get(made.webhook_id, tenant="acme") is None


async def test_the_ops_routes_through_the_sdk(runs: RunsClient) -> None:
    assert await runs.live() == {"status": "ok"}
    assert await runs.ready() == {"status": "ok"}
    assert "runs_http_requests_total" in await runs.metrics()
