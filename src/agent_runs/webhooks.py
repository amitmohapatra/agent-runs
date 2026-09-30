"""Telling whoever started a run that it paused, was escalated or finished.

The envelope and the signature are the Memory Service's (ADR 0023), so one receiver verifies
both with ``trellis.memory.webhooks.verify_signature``: ``X-Trellis-Signature: t=<unix
seconds>,v1=<hex hmac-sha256 of "<t>.<body>">`` over the exact bytes sent, with
``X-Trellis-Event`` and ``X-Trellis-Delivery`` beside it. ``event_id`` is derived from the
run, the attempt and the event, so a retried delivery carries the same id and a receiver
drops the repeat.

Delivery is off the request path and at-least-once; the run row stays the source of truth,
so a receiver that missed one reconciles by reading the run.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import time
from datetime import timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Any
from urllib.parse import urlparse

import httpx
import structlog
from trellis.contracts.ids import stable_id
from trellis.contracts.runs import RunRecord, RunStatus

from agent_runs.config.constants import (
    HEADER_DELIVERY,
    HEADER_EVENT,
    HEADER_SIGNATURE,
    WEBHOOK_ATTEMPTS,
    WEBHOOK_RETRY_BASE,
    WEBHOOK_RETRY_CAP,
    WEBHOOK_RETRYABLE,
    WEBHOOK_TIMEOUT_SECONDS,
)
from agent_runs.retry import backoff

log = structlog.get_logger(__name__)

SIGNATURE_VERSION = "v1"


class WebhookEvent(StrEnum):
    PAUSED = "run.paused"
    ESCALATED = "run.escalated"
    FINISHED = "run.finished"


def event_of(run: RunRecord) -> WebhookEvent | None:
    """What a run's current status announces, if anything: a pause or an ending."""
    if run.status is RunStatus.PAUSED:
        return WebhookEvent.PAUSED
    return WebhookEvent.FINISHED if run.final else None


def sign(secret: str, timestamp: int, body: bytes) -> str:
    digest = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, sha256).hexdigest()
    return f"t={timestamp},{SIGNATURE_VERSION}={digest}"


def envelope(run: RunRecord, event: WebhookEvent) -> dict[str, Any]:
    """The Memory Service's event envelope, with the run as the data."""
    assignee = run.awaiting.assignee if run.awaiting else None
    return {
        "event_id": stable_id(run.run_id, run.attempt, run.status, event, assignee, prefix="whd_"),
        "type": event.value,
        "tenant_id": run.tenant_id,
        "workspace_id": run.workspace_id,
        "occurred_at": run.updated_at.isoformat(),
        "data": {"run": run.model_dump(mode="json", exclude_none=True)},
    }


class WebhookSender:
    """Signs and delivers notifications, retried, in tasks the caller never waits on."""

    def __init__(
        self,
        *,
        secret: str,
        allow_http: bool,
        client: httpx.AsyncClient | None = None,
        retry_base: timedelta = WEBHOOK_RETRY_BASE,
    ) -> None:
        self._secret = secret
        self._schemes = {"https", "http"} if allow_http else {"https"}
        self._client = client or httpx.AsyncClient(
            timeout=WEBHOOK_TIMEOUT_SECONDS, follow_redirects=False
        )
        self._owns_client = client is None
        self._retry_base = retry_base
        #: strong references: asyncio keeps only weak ones, and a collected task is a
        #: delivery that silently never happens
        self._tasks: set[asyncio.Task[bool]] = set()

    def notify(self, run: RunRecord, event: WebhookEvent | None = None) -> None:
        """Queue the notification ``run`` warrants (``event`` overrides the one its status
        announces). Returns at once; failures are logged, never raised."""
        event = event or event_of(run)
        if event is None or not run.webhook_url:
            return
        task = asyncio.create_task(self.deliver(run.webhook_url, envelope(run, event)))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def deliver(self, url: str, payload: dict[str, Any]) -> bool:
        """One notification, retried with the service's backoff. True once accepted."""
        if urlparse(url).scheme not in self._schemes:
            log.warning("webhook.refused_scheme", url=url)
            return False
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers = {
            "Content-Type": "application/json",
            HEADER_EVENT: payload["type"],
            HEADER_DELIVERY: payload["event_id"],
        }
        for attempt in range(1, WEBHOOK_ATTEMPTS + 1):
            if self._secret:
                headers[HEADER_SIGNATURE] = sign(self._secret, int(time.time()), body)
            try:
                response = await self._client.post(url, content=body, headers=headers)
            except httpx.HTTPError as exc:
                log.warning("webhook.unreachable", url=url, attempt=attempt, error=str(exc))
            else:
                if response.is_success:
                    return True
                if response.status_code not in WEBHOOK_RETRYABLE:
                    log.warning("webhook.refused", url=url, status=response.status_code)
                    return False
                log.warning("webhook.failed", url=url, attempt=attempt, status=response.status_code)
            if attempt < WEBHOOK_ATTEMPTS:
                wait = backoff(self._retry_base, attempt, cap=WEBHOOK_RETRY_CAP)
                await asyncio.sleep(wait.total_seconds())
        log.warning("webhook.gave_up", url=url, event_id=payload["event_id"])
        return False

    async def aclose(self) -> None:
        """Let deliveries in flight finish (bounded), then close."""
        if self._tasks:
            await asyncio.wait(set(self._tasks), timeout=WEBHOOK_TIMEOUT_SECONDS)
        for task in list(self._tasks):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._owns_client:
            await self._client.aclose()
