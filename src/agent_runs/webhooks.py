"""Sending webhook deliveries from the outbox.

Every delivery is signed with ``trellis.runs.webhooks.sign``, the SDK's one implementation of
the scheme, so a receiver checks it with ``trellis.runs.webhooks.verify_signature``:
``X-Trellis-Signature: t=<unix seconds>,v1=<hex hmac-sha256 of "<t>.<body>">`` over the exact
bytes sent, keyed by the subscription's secret, with ``X-Trellis-Event`` and
``X-Trellis-Delivery`` (the event id, the same on every retry) beside it.

One attempt per delivery per tick; the outbox row carries the attempt count and the backoff
(``WebhookStore.settle``). At least once: the run row stays the source of truth, so a
receiver that missed one reconciles by reading the run.
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
from agent_runs.store.webhooks import Delivery

log = structlog.get_logger(__name__)


class WebhookSender:
    """Signs and sends deliveries; decides whether a failed one is worth another attempt."""

    def __init__(self, *, allow_http: bool, client: httpx.AsyncClient | None = None) -> None:
        self._schemes = {"https", "http"} if allow_http else {"https"}
        self._client = client or httpx.AsyncClient(
            timeout=WEBHOOK_TIMEOUT_SECONDS, follow_redirects=False
        )

    async def send_all(self, deliveries: list[Delivery]) -> list[bool]:
        """Attempt each delivery once, concurrently. For each: whether to retry it."""
        return list(await asyncio.gather(*(self.send(d) for d in deliveries)))

    async def send(self, delivery: Delivery) -> bool:
        """One attempt. Returns whether it is worth another: ``False`` once accepted or
        refused for good (a 4xx other than 408/429, a URL scheme this deployment refuses)."""
        payload = delivery.payload
        if urlparse(delivery.url).scheme not in self._schemes:
            log.warning("webhook.refused_scheme", url=delivery.url)
            return False
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers = {
            "Content-Type": "application/json",
            EVENT_HEADER: payload["type"],
            DELIVERY_HEADER: payload["event_id"],
            SIGNATURE_HEADER: sign(delivery.secret, int(time.time()), body),
        }
        try:
            response = await self._client.post(delivery.url, content=body, headers=headers)
        except httpx.HTTPError as exc:
            log.warning(
                "webhook.unreachable", url=delivery.url, attempt=delivery.attempts, error=str(exc)
            )
            return True
        if response.is_success:
            return False
        retry = response.status_code in WEBHOOK_RETRYABLE
        log.warning(
            "webhook.failed" if retry else "webhook.refused",
            url=delivery.url,
            attempt=delivery.attempts,
            status=response.status_code,
        )
        return retry

    async def aclose(self) -> None:
        await self._client.aclose()
