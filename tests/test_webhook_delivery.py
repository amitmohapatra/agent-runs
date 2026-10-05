"""What becomes of a delivery: one given up on is kept, dead, to be listed and redelivered
until its retention ends; a subscription's URL must reach a public host outside dev, checked
when it is made and before every attempt; a rotated secret signs alongside the new one for
an overlap, and the SDK's ``verify_signature`` accepts either."""

from __future__ import annotations

import asyncio
import socket
from datetime import timedelta
from typing import Any

import httpx
import pytest
from trellis.contracts.ids import now
from trellis.contracts.runs import RunStatus
from trellis.runs.webhooks import verify_signature

from agent_runs.config.constants import WEBHOOK_DEAD_RETENTION
from agent_runs.domain.webhooks import WebhookEvent
from agent_runs.store.webhooks import Delivery, envelope
from agent_runs.ticker import Ticker
from agent_runs.webhooks import WebhookSender
from tests.conftest import Receiver, at, sender, started
from tests.test_webhooks import _run, subscribe


def resolving(monkeypatch: pytest.MonkeyPatch, *addresses: str) -> None:
    """Every host name resolves to ``addresses`` (none: it does not resolve)."""

    async def getaddrinfo(host: Any, port: Any, **kwargs: Any) -> list[Any]:
        if not addresses:
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (a, 0)) for a in addresses]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", getaddrinfo)


def answering(*statuses: int) -> tuple[WebhookSender, list[httpx.Request]]:
    seen: list[httpx.Request] = []
    answers = iter(statuses)

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(next(answers))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    return WebhookSender(allow_http=False, allow_private=True, client=client), seen


async def _finished(client) -> str:
    run_id = (await client.post("/v1/runs", json=started())).json()["run_id"]
    await client.post(f"/v1/runs/{run_id}/finish", json={"status": "SUCCESS"})
    return run_id


def _ticker(app, hooks: WebhookSender, tmp_path, **over: Any) -> Ticker:
    return Ticker(app.state.sessions, hooks, app.state.blobs, heartbeat_path=tmp_path / "b", **over)


# ------------------------------------------------------------------ dead letters


async def test_a_delivery_refused_for_good_is_kept_dead_and_listed(app, client, tmp_path) -> None:
    hook = await subscribe(client, ["run.finished"])
    other = await subscribe(client, ["run.finished"], url="https://b.example/h")
    hooks, _ = answering(404, 200)
    run_id = await _finished(client)
    assert (await _ticker(app, hooks, tmp_path).tick()).sent == 1

    [dead] = (await client.get("/v1/webhooks/deliveries", params={"state": "dead"})).json()
    assert (dead["webhook_id"], dead["run_id"], dead["type"]) in {
        (hook["webhook_id"], run_id, "run.finished"),
        (other["webhook_id"], run_id, "run.finished"),
    }
    assert (dead["state"], dead["attempts"], dead["last_error"]) == ("dead", 1, "answered 404")
    assert dead["dead_at"] and dead["next_attempt_at"] is None
    assert dead["event_id"].startswith("whd_") and dead["delivery_id"].startswith("dlv_")
    assert (await client.get("/v1/webhooks/deliveries", params={"state": "pending"})).json() == []
    mine = await client.get("/v1/webhooks/deliveries", params={"webhook_id": dead["webhook_id"]})
    assert [d["delivery_id"] for d in mine.json()] == [dead["delivery_id"]]
    await hooks.aclose()


async def test_deliveries_are_listed_newest_first_a_page_at_a_time(client, other_tenant) -> None:
    await subscribe(client, ["run.finished"])
    for _ in range(3):
        await _finished(client)
    pending = await client.get("/v1/webhooks/deliveries", params={"state": "pending", "limit": 2})
    assert len(pending.json()) == 2 and 'rel="next"' in pending.headers["Link"]
    assert all(d["state"] == "pending" and d["next_attempt_at"] for d in pending.json())
    rest = (await client.get(pending.links["next"]["url"])).json()
    assert len(rest) == 1 and rest[0]["delivery_id"] not in {
        d["delivery_id"] for d in pending.json()
    }
    every = (await client.get("/v1/webhooks/deliveries")).json()
    assert [d["created_at"] for d in every] == sorted(
        (d["created_at"] for d in every), reverse=True
    )
    assert (await other_tenant.get("/v1/webhooks/deliveries")).json() == []


async def test_a_dead_delivery_is_redelivered_once_asked(app, client, tmp_path) -> None:
    await subscribe(client, ["run.finished"])
    hooks, seen = answering(410, 200)
    await _finished(client)
    ticker = _ticker(app, hooks, tmp_path)
    await ticker.tick()
    [dead] = (await client.get("/v1/webhooks/deliveries", params={"state": "dead"})).json()

    owed = await client.post(f"/v1/webhooks/deliveries/{dead['delivery_id']}/redeliver")
    assert owed.status_code == 200, owed.text
    assert (owed.json()["state"], owed.json()["attempts"], owed.json()["dead_at"]) == (
        "pending",
        0,
        None,
    )
    again = await client.post(f"/v1/webhooks/deliveries/{dead['delivery_id']}/redeliver")
    assert (again.status_code, again.json()["code"]) == (409, "CONFLICT")
    assert (await ticker.tick()).sent == 1
    assert [r.headers["X-Trellis-Delivery"] for r in seen] == [dead["event_id"]] * 2
    assert (await client.get("/v1/webhooks/deliveries")).json() == []
    await hooks.aclose()


async def test_redelivering_an_unknown_or_anothers_delivery_is_404(
    app, client, other_tenant, tmp_path
) -> None:
    await subscribe(client, ["run.finished"])
    hooks, _ = answering(400)
    await _finished(client)
    await _ticker(app, hooks, tmp_path).tick()
    [dead] = (await client.get("/v1/webhooks/deliveries")).json()
    theirs = await other_tenant.post(f"/v1/webhooks/deliveries/{dead['delivery_id']}/redeliver")
    assert theirs.status_code == 404
    assert (await client.post("/v1/webhooks/deliveries/dlv_nope/redeliver")).status_code == 404
    await hooks.aclose()


async def test_dead_deliveries_are_dropped_after_their_retention(app, client, tmp_path) -> None:
    await subscribe(client, ["run.finished"])
    hooks, _ = answering(400)
    await _finished(client)
    ticker = _ticker(app, hooks, tmp_path)
    await ticker.tick()
    assert (
        await ticker.tick(now=now() + WEBHOOK_DEAD_RETENTION - timedelta(minutes=1))
    ).dropped == 0
    assert (
        await ticker.tick(now=now() + WEBHOOK_DEAD_RETENTION + timedelta(minutes=1))
    ).dropped == 1
    assert (await client.get("/v1/webhooks/deliveries")).json() == []

    await hooks.aclose()

    await _finished(client)
    hooks, _ = answering(400)
    briefly = _ticker(app, hooks, tmp_path, dead_retention=timedelta(hours=1))
    await briefly.tick()
    assert (await briefly.tick(now=at(59))).dropped == 0
    assert (await briefly.tick(now=at(61))).dropped == 1
    await hooks.aclose()


# ------------------------------------------------------------------ the address guard


def _deployed(app, **webhooks: Any) -> None:
    """The app as a deployment (not dev), with these webhook settings."""
    settings = app.state.settings
    app.state.settings = settings.model_copy(
        update={
            "service": settings.service.model_copy(update={"environment": "prod"}),
            "webhooks": settings.webhooks.model_copy(update=webhooks),
        }
    )


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.1.2.3",
        "192.168.0.7",
        "169.254.169.254",
        "100.64.0.1",
        "224.0.0.1",
        "::1",
        "fd00:ec2::254",
        "::ffff:10.0.0.1",
        "0.0.0.0",
    ],
)
async def test_a_url_reaching_a_private_address_is_refused_outside_dev(
    app, client, monkeypatch, address: str
) -> None:
    _deployed(app)
    resolving(monkeypatch, "93.184.215.14", address)
    refused = await client.post(
        "/v1/webhooks", json={"url": "https://hooks.example/h", "events": ["run.paused"]}
    )
    assert refused.status_code == 422
    assert f"resolves to {address.removeprefix('::ffff:')}" in refused.json()["detail"]


async def test_a_public_url_is_accepted_and_an_unresolvable_one_refused(
    app, client, monkeypatch
) -> None:
    _deployed(app)
    resolving(monkeypatch, "93.184.215.14", "2606:2800:21f:cb07:6820:80da:af6b:8b2c")
    assert (await subscribe(client, ["run.paused"]))["url"] == "https://ui.example/h"
    resolving(monkeypatch)
    nowhere = await client.post(
        "/v1/webhooks", json={"url": "https://nowhere.example/h", "events": ["run.paused"]}
    )
    assert nowhere.status_code == 422 and "does not resolve" in nowhere.json()["detail"]


async def test_a_deployment_may_allow_private_targets_and_dev_does(app, client) -> None:
    body = {"url": "https://127.0.0.1/h", "events": ["run.paused"]}
    assert (await client.post("/v1/webhooks", json=body)).status_code == 201, "dev"
    _deployed(app, allow_private_targets=True)
    assert (await client.post("/v1/webhooks", json=body)).status_code == 201
    _deployed(app, allow_private_targets=False)
    assert (await client.post("/v1/webhooks", json=body)).status_code == 422


def _delivery(url: str) -> Delivery:
    payload = envelope(_run(status=RunStatus.SUCCESS), WebhookEvent.FINISHED)
    return Delivery("dlv_1", url, "whsec_new", payload, attempts=1)


async def test_each_attempt_checks_the_address_again(monkeypatch) -> None:
    """A name that resolved to a public address when it was subscribed may not any more."""
    receiver = Receiver()
    client = httpx.AsyncClient(transport=httpx.MockTransport(receiver.handle))
    guarded = WebhookSender(allow_http=False, allow_private=False, client=client)
    resolving(monkeypatch, "10.0.0.8")
    rebound = await guarded.send(_delivery("https://hooks.example/h"))
    assert rebound.retry is False and rebound.error is not None
    assert "resolves to 10.0.0.8" in rebound.error
    resolving(monkeypatch)
    lost = await guarded.send(_delivery("https://hooks.example/h"))
    assert lost.retry is True and lost.error is not None and "does not resolve" in lost.error
    resolving(monkeypatch, "93.184.215.14")
    assert (await guarded.send(_delivery("https://hooks.example/h"))).accepted
    assert len(receiver.received) == 1
    await guarded.aclose()


# ------------------------------------------------------------------ secret rotation


async def test_a_rotated_secret_signs_beside_the_old_one_for_the_overlap(
    app, client, receiver, tmp_path
) -> None:
    hook = await subscribe(client, ["run.finished"])
    rotated = await client.post(f"/v1/webhooks/{hook['webhook_id']}/rotate-secret")
    assert rotated.status_code == 200, rotated.text
    new = rotated.json()
    assert new["secret"].startswith("whsec_") and new["secret"] != hook["secret"]
    read = (await client.get(f"/v1/webhooks/{hook['webhook_id']}")).json()
    assert (
        "secret" not in read
        and read["previous_secret_expires_at"] == new["previous_secret_expires_at"]
    )
    hooks = sender(receiver)
    ticker = _ticker(app, hooks, tmp_path)

    await _finished(client)
    await ticker.tick()
    during = receiver.received[-1]
    signature = during.headers["X-Trellis-Signature"]
    assert signature.count("v1=") == 2
    for secret in (new["secret"], hook["secret"]):  # receivers on either verify
        assert verify_signature(secret, signature, during.content)

    await _finished(client)
    await ticker.tick(now=now() + timedelta(hours=25))
    after = receiver.received[-1]
    assert after.headers["X-Trellis-Signature"].count("v1=") == 1
    assert verify_signature(new["secret"], after.headers["X-Trellis-Signature"], after.content)
    assert not verify_signature(hook["secret"], after.headers["X-Trellis-Signature"], after.content)
    await hooks.aclose()


async def test_a_rotation_without_an_overlap_signs_with_the_new_secret_only(
    app, client, receiver, tmp_path
) -> None:
    settings = app.state.settings
    app.state.settings = settings.model_copy(
        update={"webhooks": settings.webhooks.model_copy(update={"secret_overlap_hours": 0})}
    )
    hook = await subscribe(client, ["run.finished"])
    new = (await client.post(f"/v1/webhooks/{hook['webhook_id']}/rotate-secret")).json()
    hooks = sender(receiver)
    await _finished(client)
    await _ticker(app, hooks, tmp_path).tick()
    [sent] = receiver.received
    assert sent.headers["X-Trellis-Signature"].count("v1=") == 1
    assert verify_signature(new["secret"], sent.headers["X-Trellis-Signature"], sent.content)
    assert (await client.post("/v1/webhooks/wh_nope/rotate-secret")).status_code == 404
    await hooks.aclose()
