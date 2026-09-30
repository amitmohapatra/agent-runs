"""Webhooks: the Memory Service's envelope and signature, retried, off the request path."""

from __future__ import annotations

import asyncio
import hmac
import time
from datetime import timedelta
from hashlib import sha256

import httpx
import pytest
from trellis.contracts.runs import RunRecord, RunStatus

from agent_runs.webhooks import WebhookEvent, WebhookSender, envelope, event_of, sign
from tests.conftest import Receiver, interrupt, sender, started


def verify(secret: str, header: str, body: bytes, tolerance: int = 300) -> bool:
    """The receiver's side, as ``trellis.memory.webhooks.verify_signature`` implements it."""
    parts = dict(part.split("=", 1) for part in header.split(","))
    stamp, digest = int(parts["t"]), parts["v1"]
    expected = hmac.new(secret.encode(), f"{stamp}.".encode() + body, sha256).hexdigest()
    return abs(time.time() - stamp) <= tolerance and hmac.compare_digest(expected, digest)


def _run(**over) -> RunRecord:
    fields = {
        "run_id": "run_1",
        "tenant_id": "acme",
        "agent_id": "refund-bot",
        "webhook_url": "https://ui.example/hooks",
        **over,
    }
    return RunRecord(**fields)


def test_the_signature_is_the_memory_services() -> None:
    body = b'{"type":"run.finished"}'
    header = sign("s3cret", 1_700_000_000, body)
    digest = hmac.new(b"s3cret", b"1700000000." + body, sha256).hexdigest()
    assert header == f"t=1700000000,v1={digest}"


def test_only_pauses_and_endings_are_announced() -> None:
    assert event_of(_run(status=RunStatus.SUCCESS)) is WebhookEvent.FINISHED
    assert event_of(_run(status=RunStatus.RUNNING)) is None
    assert event_of(_run(status=RunStatus.QUEUED)) is None


def test_the_envelope_is_stable_per_event_and_distinct_between_events() -> None:
    done = _run(status=RunStatus.SUCCESS, output={"refunded": 240})
    sent = envelope(done, WebhookEvent.FINISHED)
    assert sent["type"] == "run.finished"
    assert sent["tenant_id"] == "acme"
    assert sent["data"]["run"]["output"] == {"refunded": 240}
    assert sent["event_id"] == envelope(done, WebhookEvent.FINISHED)["event_id"]
    retried = _run(status=RunStatus.SUCCESS, attempt=2)
    assert envelope(retried, WebhookEvent.FINISHED)["event_id"] != sent["event_id"]


async def test_a_delivery_is_signed_over_the_bytes_sent() -> None:
    receiver = Receiver()
    hooks = sender(receiver, secret="s3cret")
    assert await hooks.deliver(
        "https://ui.example/h", envelope(_run(status=RunStatus.SUCCESS), WebhookEvent.FINISHED)
    )
    request = receiver.received[0]
    assert verify("s3cret", request.headers["X-Trellis-Signature"], request.content)
    assert request.headers["X-Trellis-Event"] == "run.finished"
    assert request.headers["X-Trellis-Delivery"].startswith("whd_")
    await hooks.aclose()


def _scripted(statuses: list[int]) -> tuple[WebhookSender, list[int]]:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(statuses[min(len(calls), len(statuses)) - 1])

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return WebhookSender(secret="", allow_http=False, client=client, retry_base=timedelta(0)), calls


async def test_a_failing_receiver_is_retried_then_given_up_on() -> None:
    hooks, calls = _scripted([503])
    assert (
        await hooks.deliver(
            "https://h", envelope(_run(status=RunStatus.ERROR), WebhookEvent.FINISHED)
        )
        is False
    )
    assert len(calls) == 4
    await hooks.aclose()


async def test_a_receiver_that_recovers_is_told_once() -> None:
    hooks, calls = _scripted([500, 200])
    assert await hooks.deliver(
        "https://h", envelope(_run(status=RunStatus.ERROR), WebhookEvent.FINISHED)
    )
    assert len(calls) == 2
    await hooks.aclose()


@pytest.mark.parametrize(
    ("status", "retried"), [(400, False), (404, False), (429, True), (500, True)]
)
async def test_a_refusal_is_not_retried_but_backpressure_is(status: int, retried: bool) -> None:
    hooks, calls = _scripted([status])
    await hooks.deliver("https://h", envelope(_run(status=RunStatus.ERROR), WebhookEvent.FINISHED))
    assert (len(calls) > 1) is retried
    await hooks.aclose()


async def test_plain_http_is_refused_outside_dev() -> None:
    hooks, calls = _scripted([200])
    assert not await hooks.deliver(
        "http://ui/h", envelope(_run(status=RunStatus.ERROR), WebhookEvent.FINISHED)
    )
    assert calls == []
    await hooks.aclose()


async def test_a_pause_through_the_api_notifies_the_start_url(client, receiver) -> None:
    run = (await client.post("/v1/runs", json=started(webhook_url="https://ui.example/h"))).json()
    await client.post(
        f"/v1/runs/{run['run_id']}/pause", json=interrupt(run["run_id"], assignee="user:u1")
    )
    for _ in range(50):
        if receiver.received:
            break
        await asyncio.sleep(0.02)
    [event] = receiver.events()
    assert event["type"] == "run.paused"
    assert event["data"]["run"]["awaiting"]["assignee"] == "user:u1"
    assert verify(
        "s3cret", receiver.received[0].headers["X-Trellis-Signature"], receiver.received[0].content
    )


async def test_a_run_without_a_url_or_in_a_live_state_is_not_announced(
    client, receiver, app
) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    await client.post("/v1/runs", json=started(webhook_url="https://ui.example/h"))
    await app.state.webhooks.aclose()
    assert receiver.received == []


async def test_a_dead_receiver_does_not_slow_the_transition(client, app) -> None:
    async def never(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(30)
        return httpx.Response(200)

    app.state.webhooks = WebhookSender(
        secret="", allow_http=False, client=httpx.AsyncClient(transport=httpx.MockTransport(never))
    )
    run = (await client.post("/v1/runs", json=started(webhook_url="https://ui.example/h"))).json()
    started_at = asyncio.get_running_loop().time()
    response = await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    assert response.status_code == 200
    assert asyncio.get_running_loop().time() - started_at < 2
    for task in list(app.state.webhooks._tasks):
        task.cancel()
