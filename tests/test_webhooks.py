"""Webhooks: tenant subscriptions, written to an outbox with the run change, sent by the
ticker with the event envelope and a per-subscription signature that the SDK's
``trellis.runs.webhooks.verify_signature`` accepts, retried."""

from __future__ import annotations

import hmac
from datetime import timedelta
from hashlib import sha256
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from trellis.contracts.runs import RunRecord, RunStatus
from trellis.runs.webhooks import parse_delivery, sign, verify_signature

from agent_runs.config.constants import WEBHOOK_ATTEMPTS
from agent_runs.domain.webhooks import Attempt, WebhookEvent, event_of
from agent_runs.store.webhooks import Delivery, WebhookStore, envelope
from agent_runs.ticker import Ticker
from agent_runs.webhooks import WebhookSender
from tests.conftest import at, pause, started

HOOK = "https://ui.example/h"


def _run(**over: Any) -> RunRecord:
    return RunRecord(**{"run_id": "run_1", "tenant_id": "acme", "agent_id": "refund-bot", **over})


async def subscribe(client, events: list[str] | None = None, url: str = HOOK) -> dict[str, Any]:
    events = events or ["run.paused", "run.escalated", "run.finished"]
    response = await client.post("/v1/webhooks", json={"url": url, "events": events})
    assert response.status_code == 201, response.text
    return response.json()


# ------------------------------------------------------------------ pure


def test_the_signature_is_hmac_sha256_over_the_timestamp_and_the_body() -> None:
    body = b'{"type":"run.finished"}'
    header = sign("s3cret", 1_700_000_000, body)
    digest = hmac.new(b"s3cret", b"1700000000." + body, sha256).hexdigest()
    assert header == f"t=1700000000,v1={digest}"


def test_only_pauses_and_endings_are_announced() -> None:
    assert event_of(_run(status=RunStatus.SUCCESS)) is WebhookEvent.FINISHED
    assert event_of(_run(status=RunStatus.RUNNING)) is None
    assert event_of(_run(status=RunStatus.QUEUED)) is None


def test_the_envelope_carries_the_summary_and_a_stable_event_id() -> None:
    done = _run(status=RunStatus.SUCCESS, output={"refunded": 240})
    sent = envelope(done, WebhookEvent.FINISHED)
    assert (sent["type"], sent["tenant_id"]) == ("run.finished", "acme")
    assert set(sent["data"]["run"]) == {
        "run_id",
        "agent_id",
        "status",
        "awaiting",
        "assignee",
        "deadline",
        "updated_at",
    }
    assert sent["event_id"] == envelope(done, WebhookEvent.FINISHED)["event_id"]
    retried = _run(status=RunStatus.SUCCESS, attempt=2)
    assert envelope(retried, WebhookEvent.FINISHED)["event_id"] != sent["event_id"]


def _delivery(url: str = "https://h") -> Delivery:
    payload = envelope(_run(status=RunStatus.ERROR), WebhookEvent.FINISHED)
    return Delivery("dlv_1", url, "whsec_x", payload, attempts=1)


def _answering(status: int) -> WebhookSender:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(status)))
    return WebhookSender(allow_http=False, allow_private=True, client=client)


@pytest.mark.parametrize(
    ("status", "retry"), [(200, False), (400, False), (404, False), (429, True), (500, True)]
)
async def test_a_refusal_is_final_but_backpressure_is_retried(status: int, retry: bool) -> None:
    hooks = _answering(status)
    attempt = await hooks.send(_delivery())
    assert attempt.retry is retry
    assert attempt.error == (None if status == 200 else f"answered {status}")
    await hooks.aclose()


async def test_plain_http_is_refused_outside_dev_and_not_retried() -> None:
    hooks = _answering(200)
    attempt = await hooks.send(_delivery("http://ui/h"))
    assert (attempt.retry, attempt.error) == (
        False,
        "refused: this deployment delivers only to https URLs",
    )
    await hooks.aclose()


# ------------------------------------------------------------------ subscriptions


async def test_the_secret_is_shown_once(client) -> None:
    created = await subscribe(client, ["run.finished", "run.paused", "run.finished"])
    assert created["secret"].startswith("whsec_")
    assert created["events"] == ["run.finished", "run.paused"]
    [listed] = (await client.get("/v1/webhooks")).json()
    assert "secret" not in listed and listed["webhook_id"] == created["webhook_id"]
    assert listed["created_by"] == "user_ada"


@pytest.mark.parametrize(
    "body",
    [
        {"url": HOOK, "events": []},
        {"url": HOOK, "events": ["run.started"]},
        {"url": "ftp://ui.example/h", "events": ["run.paused"]},
        {"url": "/relative", "events": ["run.paused"]},
    ],
)
async def test_a_bad_subscription_is_refused(client, body) -> None:
    assert (await client.post("/v1/webhooks", json=body)).status_code == 422


async def test_subscriptions_are_the_tenants_own(client, other_tenant) -> None:
    mine = await subscribe(client)
    assert (await other_tenant.get("/v1/webhooks")).json() == []
    assert (await other_tenant.delete(f"/v1/webhooks/{mine['webhook_id']}")).status_code == 404
    assert (await client.delete(f"/v1/webhooks/{mine['webhook_id']}")).status_code == 204
    assert (await client.get("/v1/webhooks")).json() == []


async def test_a_tenant_has_a_bounded_number_of_subscriptions(client, monkeypatch) -> None:
    monkeypatch.setattr("agent_runs.store.webhooks.MAX_WEBHOOKS_PER_TENANT", 2)
    await subscribe(client)
    await subscribe(client)
    response = await client.post("/v1/webhooks", json={"url": HOOK, "events": ["run.paused"]})
    assert response.status_code == 409


async def test_webhook_url_is_refused_on_runs_and_schedules(client) -> None:
    run = await client.post("/v1/runs", json=started(webhook_url=HOOK))
    assert run.status_code == 422 and "webhook_url" in run.text
    schedule = {
        "tenant_id": "acme",
        "agent_id": "a",
        "name": "n",
        "cadence": "daily",
        "on_behalf_of": "user_ada",
        "webhook_url": HOOK,
    }
    assert (await client.post("/v1/schedules", json=schedule)).status_code == 422


# ------------------------------------------------------------------ delivery


async def test_a_pause_is_delivered_by_the_ticker_signed_with_the_subscriptions_secret(
    client, ticker, receiver
) -> None:
    hook = await subscribe(client, ["run.paused"])
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(
        f"/v1/runs/{run['run_id']}/pause", json=pause(run["run_id"], assignee="user:u1")
    )
    assert receiver.received == []  # nothing on the request path
    assert (await ticker.tick()).sent == 1
    [event] = receiver.events()
    assert event["type"] == "run.paused"
    assert event["data"]["run"]["assignee"] == "user:u1"
    request = receiver.received[0]
    # the receiver's side, with the SDK: the exact bytes sent verify, and parse
    assert verify_signature(hook["secret"], request.headers["X-Trellis-Signature"], request.content)
    assert not verify_signature(
        "whsec_other", request.headers["X-Trellis-Signature"], request.content
    )
    delivery = parse_delivery(request.content)
    assert delivery.event_id == event["event_id"] and delivery.data.run.run_id == run["run_id"]
    assert delivery.type == "run.paused" and delivery.data.run.assignee == "user:u1"
    assert request.headers["X-Trellis-Event"] == "run.paused"
    assert request.headers["X-Trellis-Delivery"] == event["event_id"]
    assert (await ticker.tick()).sent == 0  # delivered once


async def test_each_subscription_hears_only_its_events(client, ticker, receiver) -> None:
    await subscribe(client, ["run.finished"], url="https://a.example/h")
    await subscribe(client, ["run.paused"], url="https://b.example/h")
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    await ticker.tick()
    assert [str(r.url) for r in receiver.received] == ["https://a.example/h"]


async def test_a_live_run_or_a_tenant_without_subscriptions_sends_nothing(
    client, other_tenant, ticker, receiver
) -> None:
    await subscribe(other_tenant)
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    await subscribe(client)
    await client.post("/v1/runs", json=started())
    assert (await ticker.tick()).sent == 0
    assert receiver.received == []


async def _outbox(app) -> list[dict[str, Any]]:
    async with app.state.engine.connect() as conn:
        rows = await conn.execute(text("SELECT * FROM webhook_deliveries"))
        return [dict(row._mapping) for row in rows]


async def test_a_failing_receiver_is_retried_with_backoff_then_given_up_on(
    app, client, tmp_path
) -> None:
    calls: list[int] = []

    def failing(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(503)

    hooks = WebhookSender(
        allow_http=False,
        allow_private=True,
        client=httpx.AsyncClient(transport=httpx.MockTransport(failing)),
    )
    ticker = Ticker(app.state.sessions, hooks, app.state.blobs, heartbeat_path=tmp_path / "beat")
    await subscribe(client)
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "ERROR"})

    await ticker.tick()
    [row] = await _outbox(app)
    assert row["attempts"] == 1
    assert (await ticker.tick()).sent == 0 and len(calls) == 1  # backing off, not retried yet
    for minutes in range(1, 60):
        await ticker.tick(now=at(minutes))
    assert len(calls) == WEBHOOK_ATTEMPTS
    [dead] = await _outbox(app)  # given up on, kept to be redelivered
    assert (dead["attempts"], dead["last_error"]) == (WEBHOOK_ATTEMPTS, "answered 503")
    assert dead["dead_at"] is not None
    await hooks.aclose()


async def test_a_receiver_that_recovers_is_told_once(app, client, tmp_path) -> None:
    answers = iter([500, 200])
    hooks = WebhookSender(
        allow_http=False,
        allow_private=True,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(next(answers)))
        ),
    )
    ticker = Ticker(app.state.sessions, hooks, app.state.blobs, heartbeat_path=tmp_path / "beat")
    await subscribe(client)
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    assert (await ticker.tick()).sent == 0
    assert (await ticker.tick(now=at(1))).sent == 1
    assert await _outbox(app) == []
    await hooks.aclose()


async def test_unsubscribing_drops_what_is_still_owed(app, client, ticker, receiver) -> None:
    hook = await subscribe(client)
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    assert len(await _outbox(app)) == 1
    await client.delete(f"/v1/webhooks/{hook['webhook_id']}")
    assert await _outbox(app) == []
    assert (await ticker.tick()).sent == 0


async def test_a_delivery_held_by_a_dead_ticker_comes_due_again(app, client) -> None:
    await subscribe(client)
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    async with app.state.sessions() as db:
        assert len(await WebhookStore(db).claim_due(now=at(0), limit=10)) == 1
        await db.commit()
    async with app.state.sessions() as db:
        assert await WebhookStore(db).claim_due(now=at(0), limit=10) == []
        again = await WebhookStore(db).claim_due(now=at(0) + timedelta(minutes=1), limit=10)
        assert [d.attempts for d in again] == [2]


async def test_an_unreachable_receiver_is_worth_another_attempt() -> None:
    def unreachable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    hooks = WebhookSender(
        allow_http=False,
        allow_private=True,
        client=httpx.AsyncClient(transport=httpx.MockTransport(unreachable)),
    )
    attempt = await hooks.send(_delivery())
    assert (attempt.retry, attempt.error) == (True, "unreachable: connection refused")
    await hooks.aclose()


async def test_a_delivery_whose_subscription_went_mid_send_is_settled_quietly(app, client) -> None:
    """The ticker holds a delivery, the tenant unsubscribes while it is being sent (the row
    goes with the subscription), and the retry finds nothing to reschedule."""
    hook = await subscribe(client)
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    async with app.state.sessions() as db:
        [held] = await WebhookStore(db).claim_due(now=at(0), limit=10)
        await db.commit()
    assert (await client.delete(f"/v1/webhooks/{hook['webhook_id']}")).status_code == 204
    async with app.state.sessions() as db:
        failed = Attempt("answered 503", retry=True)
        assert await WebhookStore(db).settle(held, failed, now=at(0)) is False
        await db.commit()
    assert await _outbox(app) == []


async def test_unsubscribing_an_unknown_webhook_is_404(client) -> None:
    response = await client.delete("/v1/webhooks/wh_nope")
    assert response.status_code == 404 and "wh_nope" in response.json()["detail"]


async def test_subscriptions_are_listed_oldest_first(client) -> None:
    first = await subscribe(client, ["run.paused"])
    second = await subscribe(client, ["run.finished"])
    listed = (await client.get("/v1/webhooks")).json()
    assert [w["webhook_id"] for w in listed] == [first["webhook_id"], second["webhook_id"]]


async def test_plain_http_subscriptions_are_accepted_only_in_dev(app, client) -> None:
    body = {"url": "http://laptop.local/h", "events": ["run.paused"]}
    assert (await client.post("/v1/webhooks", json=body)).status_code == 201
    app.state.settings = app.state.settings.model_copy(
        update={"service": app.state.settings.service.model_copy(update={"environment": "prod"})}
    )
    assert (await client.post("/v1/webhooks", json=body)).status_code == 422
