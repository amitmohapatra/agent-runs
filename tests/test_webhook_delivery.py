"""What becomes of a delivery: one given up on is kept, dead, to be listed and redelivered
until its retention ends; a subscription's URL must reach a public host outside dev, checked
when it is made and before every attempt; a rotated secret signs alongside the new one for
an overlap, and the SDK's ``verify_signature`` accepts either."""

from __future__ import annotations

import asyncio
import socket
import ssl
from datetime import timedelta
from typing import Any

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from trellis.contracts.ids import now
from trellis.contracts.runs import RunStatus
from trellis.runs.webhooks import verify_signature

from agent_runs.config.constants import WEBHOOK_DEAD_RETENTION
from agent_runs.domain.webhooks import WebhookEvent
from agent_runs.egress import pinned
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


def resolving_in_turn(monkeypatch: pytest.MonkeyPatch, *answers: list[str]) -> list[str]:
    """Each resolution answers the next of ``answers`` (a name that changes: DNS
    rebinding); returns the hosts asked about."""
    asked: list[str] = []
    turns = iter(answers)

    async def getaddrinfo(host: Any, port: Any, **kwargs: Any) -> list[Any]:
        asked.append(host)
        family = socket.AF_INET
        return [(family, socket.SOCK_STREAM, 6, "", (a, 0)) for a in next(turns)]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", getaddrinfo)
    return asked


def _guarded(handle: Any, **client: Any) -> WebhookSender:
    transport = httpx.MockTransport(handle)
    return WebhookSender(
        allow_http=False,
        allow_private=False,
        client=httpx.AsyncClient(transport=transport, **client),
    )


async def test_an_attempt_connects_only_to_the_address_it_checked(monkeypatch) -> None:
    """The name resolves to a public address when checked and to a private one a moment
    later: the attempt connects to the address it checked, naming the host in Host and in
    TLS SNI (which the certificate is checked against), and never resolves it again."""
    asked = resolving_in_turn(monkeypatch, ["93.184.215.14"], ["127.0.0.1"])
    seen = Receiver()
    guarded = _guarded(seen.handle)
    assert (await guarded.send(_delivery("https://hooks.example:8443/h"))).accepted
    [request] = seen.received
    assert (request.url.host, request.url.port, request.url.path) == ("93.184.215.14", 8443, "/h")
    assert request.headers["Host"] == "hooks.example:8443"
    assert request.extensions["sni_hostname"] == "hooks.example"
    assert asked == ["hooks.example"], "resolved once"

    rebound = await guarded.send(_delivery("https://hooks.example:8443/h"))
    assert rebound.error == (
        "refused: hooks.example resolves to 127.0.0.1, which is not a public address"
    )
    assert len(seen.received) == 1, "nothing sent to the address that is not public"
    await guarded.aclose()


async def test_the_checked_addresses_are_tried_in_order(monkeypatch) -> None:
    # a resolver may name an address twice (once per protocol): it is tried once
    twice = ["93.184.215.14", "93.184.215.14", "93.184.215.15"]
    resolving_in_turn(monkeypatch, twice, ["93.184.215.14", "93.184.215.15"])
    tried: list[str] = []

    def first_refuses(request: httpx.Request) -> httpx.Response:
        tried.append(request.url.host)
        if request.url.host == "93.184.215.14":
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(204)

    guarded = _guarded(first_refuses)
    assert (await guarded.send(_delivery("https://hooks.example/h"))).accepted
    assert tried == ["93.184.215.14", "93.184.215.15"]
    await guarded.aclose()

    def all_refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    guarded = _guarded(all_refuse)
    down = await guarded.send(_delivery("https://hooks.example/h"))
    assert (down.retry, down.error) == (True, "unreachable: connection refused")
    await guarded.aclose()


async def test_an_ipv6_address_is_connected_to_as_such(monkeypatch) -> None:
    async def getaddrinfo(host: Any, port: Any, **kwargs: Any) -> list[Any]:
        return [(socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2606:2800:21f:cb07::1", 0, 0, 0))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", getaddrinfo)
    seen = Receiver()
    guarded = _guarded(seen.handle)
    assert (await guarded.send(_delivery("https://hooks.example/h"))).accepted
    [request] = seen.received
    assert (
        request.url.host == "2606:2800:21f:cb07::1" and request.headers["Host"] == "hooks.example"
    )
    await guarded.aclose()


def _certificate(tmp_path: Any, name: str) -> tuple[Any, Any]:
    """A self-signed certificate for ``name`` (its own authority), and its key, as files."""
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now() - timedelta(days=1))
        .not_valid_after(now() + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(name)]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_file, key_file = tmp_path / "cert.pem", tmp_path / "key.pem"
    cert_file.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_file, key_file


async def test_a_pinned_connection_still_checks_the_certificate_against_the_host(
    tmp_path,
) -> None:
    """Over real TLS to 127.0.0.1: the server is asked for hooks.example (SNI), the request
    names it (Host), and the certificate is checked against it, so a certificate for
    another name is refused even at the checked address."""
    cert_file, key_file = _certificate(tmp_path, "hooks.example")
    served = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    served.load_cert_chain(cert_file, key_file)
    names: list[str | None] = []
    served.sni_callback = lambda sock, name, context: names.append(name)
    hosts: list[bytes] = []

    async def answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await reader.readuntil(b"\r\n\r\n")
        hosts.extend(line for line in head.split(b"\r\n") if line.lower().startswith(b"host:"))
        writer.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(answer, "127.0.0.1", 0, ssl=served)
    port = server.sockets[0].getsockname()[1]
    trusting = ssl.create_default_context(cafile=str(cert_file))
    async with server, httpx.AsyncClient(verify=trusting) as client:
        target = pinned(f"https://hooks.example:{port}/h", "127.0.0.1")
        request = client.build_request(
            "POST", target.url, headers=target.headers, extensions=target.extensions
        )
        assert (await client.send(request)).status_code == 204
        assert names == ["hooks.example"] and hosts == [f"Host: hooks.example:{port}".encode()]
        other = pinned(f"https://other.example:{port}/h", "127.0.0.1")
        wrong = client.build_request(
            "POST", other.url, headers=other.headers, extensions=other.extensions
        )
        with pytest.raises(httpx.ConnectError, match="CERTIFICATE_VERIFY_FAILED"):
            await client.send(wrong)


@pytest.mark.parametrize("allow_private", [True, False])
async def test_a_redirect_is_never_followed(monkeypatch, allow_private: bool) -> None:
    """Even by a client that would follow one: a 3xx is an answer that is not a 2xx, final,
    and the delivery dies with it."""
    resolving_in_turn(monkeypatch, ["93.184.215.14"])
    seen: list[httpx.Request] = []

    def redirecting(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302, headers={"Location": "http://169.254.169.254/latest"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(redirecting), follow_redirects=True)
    hooks = WebhookSender(allow_http=False, allow_private=allow_private, client=client)
    attempt = await hooks.send(_delivery("https://hooks.example/h"))
    assert (attempt.retry, attempt.error) == (False, "answered 302")
    assert len(seen) == 1 and seen[0].url.path == "/h"
    await hooks.aclose()


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
