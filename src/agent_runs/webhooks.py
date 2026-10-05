"""Sending webhook deliveries from the outbox.

Every delivery is signed with ``trellis.runs.webhooks.sign``, the SDK's one implementation of
the scheme, so a receiver checks it with ``trellis.runs.webhooks.verify_signature``:
``X-Trellis-Signature: t=<unix seconds>,v1=<hex hmac-sha256 of "<t>.<body>">`` over the exact
bytes sent, keyed by the subscription's secret (and a second ``v1=`` keyed by the secret a
rotation replaced, while it still signs), with ``X-Trellis-Event`` and
``X-Trellis-Delivery`` (the event id, the same on every retry) beside it.

Before each attempt the URL's host is resolved and refused when it is not a public address
(``egress.py``), unless the deployment allows private targets: a name that resolved to a
public address when the subscription was made may not any more.

One attempt per delivery per tick; the outbox row carries the attempt count, the backoff and
the last error (``WebhookStore.settle``). At least once: the run row stays the source of
truth, so a receiver that missed one reconciles by reading the run.
"""

from __future__ import annotations

import asyncio
import json
import time
from urllib.parse import urlparse

import httpx
import structlog
from trellis.runs.webhooks import DELIVERY_HEADER, EVENT_HEADER, SIGNATURE_HEADER, sign

from agent_runs.config.constants import WEBHOOK_RETRYABLE, WEBHOOK_TIMEOUT_SECONDS
from agent_runs.domain.webhooks import Attempt
from agent_runs.egress import private_address
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
        for good: a 4xx other than 408/429, a URL scheme this deployment refuses, a host that
        resolves to a private address here. Worth another: a 408, 429 or 5xx, a receiver or
        a host name that cannot be reached."""
        payload = delivery.payload
        if urlparse(delivery.url).scheme not in self._schemes:
            log.warning("webhook.refused_scheme", url=delivery.url)
            return Attempt("refused: this deployment delivers only to https URLs")
        if not self._allow_private:
            try:
                address = await private_address(delivery.url)
            except OSError as exc:
                log.warning("webhook.unresolved", url=delivery.url, error=str(exc))
                return Attempt(f"unreachable: the host does not resolve ({exc})", retry=True)
            if address is not None:
                log.warning("webhook.refused_address", url=delivery.url, address=address)
                return Attempt(f"refused: the host resolves to {address}, not a public address")
        body = json.dumps(payload, separators=(",", ":")).encode()
        signature = sign(delivery.secret, int(time.time()), body, previous=delivery.previous_secret)
        headers = {
            "Content-Type": "application/json",
            EVENT_HEADER: payload["type"],
            DELIVERY_HEADER: payload["event_id"],
            SIGNATURE_HEADER: signature,
        }
        try:
            response = await self._client.post(delivery.url, content=body, headers=headers)
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

    async def aclose(self) -> None:
        await self._client.aclose()
