"""Notifying whoever is waiting for a run.

The point of a webhook here is the 3am case: a schedule fires, the agent pauses for an
approval, and nobody is watching a screen. These tests are about the properties that make
such a notification trustworthy — it is signed, it is retried, it never blocks the
transition that caused it, and it never claims something the database did not record.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json

import httpx
import pytest

from agent_runs.config.settings import WebhookSettings
from agent_runs.domain.models import NOTIFIABLE, TERMINAL, Run
from agent_runs.webhooks import WebhookSender, payload, signature
from tests.conftest import started


def _run(**over) -> Run:
    fields = {
        "run_id": "run_1",
        "tenant_id": "acme",
        "agent_id": "refund-bot",
        "webhook_url": "https://ui.example/hooks/runs",
        **over,
    }
    return Run(**fields)


def _sender(handler, **config):
    settings = WebhookSettings(**{"backoff_seconds": 0.0, **config})
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=5)
    return WebhookSender(settings, client=client)


def test_terminal_states_are_derived_not_listed() -> None:
    """A second list is a second thing to forget: a new status that nobody notified on
    would look exactly like a working system."""
    assert "SUCCESS" in TERMINAL and "PAUSED" not in TERMINAL
    assert "PAUSED" in NOTIFIABLE, "a pause is the event a human most needs to hear about"
    assert "RUNNING" not in NOTIFIABLE


def test_the_signature_covers_the_exact_bytes_sent() -> None:
    """Signing a re-serialised dict instead of the bytes is how a receiver that recomputes
    the digest gets a different one the moment key order or spacing differs."""
    body = b'{"run_id":"run_1"}'
    expected = hmac.new(b"s3cret", body, hashlib.sha256).hexdigest()
    assert signature("s3cret", body) == f"sha256={expected}"


def test_a_pause_tells_the_receiver_what_is_being_asked() -> None:
    run = _run(status="PAUSED", awaiting={"question": "Approve EUR 240?"})
    sent = payload(run)
    assert sent["event"] == "run.paused"
    assert sent["awaiting"] == {"question": "Approve EUR 240?"}
    assert sent["delivery_id"] == "run_1:PAUSED:1"


def test_two_attempts_of_one_notification_share_a_delivery_id() -> None:
    """A retry must be discardable by the receiver; a genuinely new event must not be."""
    run = _run(status="SUCCESS", output={"refunded": 240})
    assert payload(run)["delivery_id"] == payload(run)["delivery_id"]
    assert payload(run)["delivery_id"] != payload(_run(status="ERROR"))["delivery_id"]


async def test_a_delivery_is_signed_and_carries_the_result() -> None:
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = request.content
        seen["signature"] = request.headers.get("X-Run-Signature")
        seen["event"] = request.headers.get("X-Run-Event")
        return httpx.Response(200)

    sender = _sender(handler, signing_secret="s3cret")
    assert await sender.deliver(_run(status="SUCCESS", output={"refunded": 240}))

    assert seen["event"] == "SUCCESS"
    assert seen["signature"] == signature("s3cret", seen["body"])
    assert json.loads(seen["body"])["output"] == {"refunded": 240}
    await sender.aclose()


async def test_a_failing_receiver_is_retried_then_given_up_on() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    sender = _sender(handler, max_attempts=3)
    assert await sender.deliver(_run(status="SUCCESS")) is False
    assert calls == 3
    await sender.aclose()


async def test_a_receiver_that_recovers_is_only_told_once() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200 if calls > 1 else 500)

    sender = _sender(handler, max_attempts=3)
    assert await sender.deliver(_run(status="SUCCESS")) is True
    assert calls == 2, "it must stop as soon as the receiver accepts"
    await sender.aclose()


@pytest.mark.parametrize(
    ("status", "retried"),
    [(400, False), (404, False), (429, True), (500, True)],
)
async def test_a_refusal_is_not_retried_but_backpressure_is(status: int, retried: bool) -> None:
    """A 4xx means the receiver understood and declined; repeating it only spends the
    budget. 429 is the exception — that is the receiver asking for time, not declining."""
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status)

    sender = _sender(handler, max_attempts=3)
    await sender.deliver(_run(status="SUCCESS"))
    assert (calls > 1) is retried
    await sender.aclose()


async def test_a_url_with_a_scheme_we_do_not_allow_is_never_called() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200)

    sender = _sender(handler, allowed_schemes=("https",))
    assert await sender.deliver(_run(status="SUCCESS", webhook_url="http://ui/hook")) is False
    assert not called
    await sender.aclose()


async def test_runs_without_a_webhook_or_in_a_live_state_are_not_notified() -> None:
    sender = _sender(lambda r: httpx.Response(200))
    assert not sender.should_notify(_run(status="RUNNING"))
    no_url = Run(run_id="r", tenant_id="acme", agent_id="a", status="SUCCESS")
    assert not sender.should_notify(no_url)
    assert sender.should_notify(_run(status="PAUSED"))
    await sender.aclose()


async def test_scheduling_keeps_a_reference_so_the_delivery_actually_happens() -> None:
    """asyncio holds only a weak reference to a running task: without a strong one the
    delivery can be collected mid-flight and simply not happen, which looks from the
    outside like a flaky receiver."""
    delivered = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        delivered.set()
        return httpx.Response(200)

    sender = _sender(handler)
    sender.schedule(_run(status="SUCCESS"))
    await asyncio.wait_for(delivered.wait(), timeout=5)
    await sender.aclose()


async def test_a_transition_notifies_over_http(client, monkeypatch) -> None:
    """End to end through the API: the run pauses, and the URL it was started with is called."""
    received: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(json.loads(request.content))
        return httpx.Response(200)

    app = client._transport.app
    app.state.webhooks = _sender(handler, signing_secret="s3cret")

    created = (
        await client.post("/v1/runs", json=started(webhook_url="https://ui.example/hooks/runs"))
    ).json()
    assert created["webhook_url"] == "https://ui.example/hooks/runs"

    await client.post(
        f"/v1/runs/{created['run_id']}/transition",
        json={"status": "PAUSED", "metadata": {"question": "Approve?"}},
    )
    for _ in range(50):
        if received:
            break
        await asyncio.sleep(0.02)

    assert received, "a paused run must notify whoever started it"
    assert received[0]["run_id"] == created["run_id"]
    assert received[0]["event"] == "run.paused"
    await app.state.webhooks.aclose()


async def test_a_dead_receiver_does_not_slow_the_transition(client) -> None:
    """Delivery is off the request path: a receiver that never answers must not turn
    "your run finished" into a timeout on the call that finished it."""

    async def never(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(30)
        return httpx.Response(200)

    app = client._transport.app
    app.state.webhooks = _sender(never, max_attempts=1)

    created = (
        await client.post("/v1/runs", json=started(webhook_url="https://ui.example/hook"))
    ).json()

    started_at = asyncio.get_running_loop().time()
    response = await client.post(
        f"/v1/runs/{created['run_id']}/transition", json={"status": "SUCCESS"}
    )
    elapsed = asyncio.get_running_loop().time() - started_at

    assert response.status_code == 200
    assert elapsed < 2, f"the transition waited {elapsed:.1f}s on the receiver"
    await app.state.webhooks.aclose()
