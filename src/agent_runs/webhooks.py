"""Sending webhook deliveries from the outbox.

Every delivery is signed with ``trellis.runs.webhooks.sign``, the SDK's one implementation of
the scheme, so a receiver checks it with ``trellis.runs.webhooks.verify_signature``:
``X-Trellis-Signature: t=<unix seconds>,v1=<hex hmac-sha256 of "<t>.<body>">`` over the exact
bytes sent, keyed by the subscription's secret (and a second ``v1=`` keyed by the secret a
rotation replaced, while it still signs), with ``X-Trellis-Event`` and
``X-Trellis-Delivery`` (the event id, the same on every retry) beside it.

Unless the deployment allows private targets, each attempt resolves the URL's host once,
refuses it when any address is not public, and connects only to the addresses it checked, in
order (``egress.py``): a name that resolved to a public address when the subscription was
made, or a moment ago, cannot send the delivery anywhere else. Redirects are never followed:
a ``3xx`` is an answer like any other that is not a ``2xx``.

One attempt per delivery per tick; the outbox row carries the attempt count, the backoff and
the last error (``WebhookStore.settle``). At least once: the run row stays the source of
truth, so a receiver that missed one reconciles by reading the run.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from urllib.parse import urlparse

import httpx
import structlog
from trellis.runs.webhooks import DELIVERY_HEADER, EVENT_HEADER, SIGNATURE_HEADER, sign

from agent_runs.config.constants import WEBHOOK_RETRYABLE, WEBHOOK_TIMEOUT_SECONDS
from agent_runs.domain.webhooks import Attempt
from agent_runs.egress import NotPublic, Target, pinned, public_addresses
from agent_runs.store.webhooks import Delivery

log = structlog.get_logger(__name__)


class WebhookSender:
    """Signs and sends deliveries; decides whether a failed one is worth another attempt.
    ``allow_http``: plain-http URLs too (dev); ``allow_private``: hosts that resolve to
    private addresses too (``Settings.private_webhook_targets``)."""

    def __init__(
        self, *, allow_http: bool, allow_private: bool, client: httpx.AsyncClient | None = None
    ) -> None:
        self._schemes = {"https", "http"} if allow_http else {"https"}
        self._allow_private = allow_private
        self._client = client or httpx.AsyncClient(
            timeout=WEBHOOK_TIMEOUT_SECONDS, follow_redirects=False
        )

    async def send_all(self, deliveries: list[Delivery]) -> list[Attempt]:
        """Attempt each delivery once, concurrently. For each: how it went."""
        return list(await asyncio.gather(*(self.send(d) for d in deliveries)))

    async def send(self, delivery: Delivery) -> Attempt:
        """One attempt: accepted, or why not and whether another is worth making. Refused
        for good: any answer but a 2xx, 408, 429 or 5xx (a redirect too: it is never
        followed), a URL scheme this deployment refuses, a host that resolves to a private
        address here. Worth another: a 408, 429 or 5xx, a receiver or a host name that
        cannot be reached."""
        payload = delivery.payload
        if urlparse(delivery.url).scheme not in self._schemes:
            log.warning("webhook.refused_scheme", url=delivery.url)
            return Attempt("refused: this deployment delivers only to https URLs")
        try:
            targets = await self._targets(delivery.url)
        except OSError as exc:
            log.warning("webhook.unresolved", url=delivery.url, error=str(exc))
            return Attempt(f"unreachable: the host does not resolve ({exc})", retry=True)
        except NotPublic as exc:
            log.warning("webhook.refused_address", url=delivery.url, address=exc.address)
            return Attempt(f"refused: {exc}")
        body = json.dumps(payload, separators=(",", ":")).encode()
        signature = sign(delivery.secret, int(time.time()), body, previous=delivery.previous_secret)
        headers = {
            "Content-Type": "application/json",
            EVENT_HEADER: payload["type"],
            DELIVERY_HEADER: payload["event_id"],
            SIGNATURE_HEADER: signature,
        }
        try:
            response = await self._post(targets, body, headers)
        except httpx.HTTPError as exc:
            log.warning(
                "webhook.unreachable", url=delivery.url, attempt=delivery.attempts, error=str(exc)
            )
            return Attempt(f"unreachable: {exc}", retry=True)
        if response.is_success:
            return Attempt()
        retry = response.status_code in WEBHOOK_RETRYABLE
        log.warning(
            "webhook.failed" if retry else "webhook.refused",
            url=delivery.url,
            attempt=delivery.attempts,
            status=response.status_code,
        )
        return Attempt(f"answered {response.status_code}", retry=retry)

    async def _post(
        self, targets: list[Target], body: bytes, headers: dict[str, str]
    ) -> httpx.Response:
        """POST to each target in turn until one takes the connection (an address that
        refuses it passes the attempt to the next); raises what the last one met. A redirect
        is an answer, never followed."""
        *others, last = targets
        for target in others:
            with contextlib.suppress(httpx.ConnectError, httpx.ConnectTimeout):
                return await self._send_to(target, body, headers)
        return await self._send_to(last, body, headers)

    async def _send_to(
        self, target: Target, body: bytes, headers: dict[str, str]
    ) -> httpx.Response:
        request = self._client.build_request(
            "POST",
            target.url,
            content=body,
            headers={**headers, **target.headers},
            extensions=target.extensions,
        )
        return await self._client.send(request, follow_redirects=False)

    async def _targets(self, url: str) -> list[Target]:
        """Where the attempt may connect: anywhere the URL leads where private targets are
        allowed, else only to the addresses its host resolves to now, each checked."""
        if self._allow_private:
            return [Target(httpx.URL(url))]
        return [pinned(url, address) for address in await public_addresses(url)]

    async def aclose(self) -> None:
        await self._client.aclose()
