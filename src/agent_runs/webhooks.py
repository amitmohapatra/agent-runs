"""Telling someone a run paused or finished.

A person who starts a run at 9am and a schedule that fires at 3am have the same problem:
something happens later and nobody is watching. A UI can poll — and should still be able
to — but polling every run of every tenant to notice one approval request is the wrong
shape for a product where most runs do nothing interesting for minutes at a time.

Three properties are deliberate:

* **Off the request path.** Delivery never blocks the transition that caused it. A slow or
  dead receiver must not turn "your run finished" into a 504 on the call that finished it.
* **At-least-once, not exactly-once.** Retries can duplicate; the ``delivery_id`` is stable
  per (run, status) so a receiver can discard a repeat. The run row stays the source of
  truth, so a client that missed every attempt reconciles by reading the run.
* **Signed.** ``X-Run-Signature: sha256=<hmac>`` over the exact bytes sent, so a receiver
  can tell a real notification from anything else that can reach its URL.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
from typing import Any
from urllib.parse import urlparse

import httpx
import structlog

from agent_runs.config.settings import WebhookSettings
from agent_runs.domain.models import NOTIFIABLE, TERMINAL, Run

log = structlog.get_logger(__name__)

#: Statuses where the same delivery may succeed later. A 4xx means the receiver understood
#: and refused, and repeating it only spends the budget — with one exception: 408 and 429
#: are the receiver asking for time, not declining.
RETRYABLE = frozenset({408, 429, 500, 502, 503, 504})


def signature(secret: str, body: bytes) -> str:
    """``sha256=<hex>`` over the exact bytes sent.

    Over the bytes, not over a re-serialised dict: a receiver that recomputes the digest
    from re-encoded JSON gets a different one the moment key order or spacing differs.
    """
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def payload(run: Run) -> dict[str, Any]:
    """What a receiver is told. Enough to act on; not a copy of the run.

    ``delivery_id`` is derived from the run and the status rather than random, so the two
    attempts of one notification carry the same id and a receiver can tell a retry from a
    second event.
    """
    return {
        "delivery_id": f"{run.run_id}:{run.status}:{run.attempt}",
        "event": "run.paused" if run.status not in TERMINAL else "run.finished",
        "run_id": run.run_id,
        "tenant_id": run.tenant_id,
        "agent_id": run.agent_id,
        "status": run.status,
        "attempt": run.attempt,
        "thread_id": run.thread_id,
        "parent_run_id": run.parent_run_id,
        # What the UI renders: the question on a pause, the result on a finish.
        "awaiting": run.awaiting,
        "output": run.output,
        "error": run.error,
        "occurred_at": run.updated_at.isoformat(),
    }


class WebhookSender:
    """Delivers notifications, with retries, outside the request that triggered them."""

    def __init__(self, config: WebhookSettings, *, client: httpx.AsyncClient | None = None) -> None:
        self._config = config
        self._client = client or httpx.AsyncClient(timeout=config.timeout_seconds)
        self._owns_client = client is None
        self._tasks: set[asyncio.Task[None]] = set()

    def should_notify(self, run: Run) -> bool:
        return bool(self._config.enabled and run.webhook_url and run.status in NOTIFIABLE)

    def schedule(self, run: Run) -> None:
        """Queue a notification. Returns immediately; failures are logged, never raised.

        The task is kept in a set because asyncio only holds a weak reference to a running
        task: without this, a delivery can be garbage collected mid-flight and simply not
        happen, which is the kind of bug that looks like a flaky receiver.
        """
        if not self.should_notify(run):
            return
        task = asyncio.create_task(self.deliver(run))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def deliver(self, run: Run) -> bool:
        """One notification, retried. True if the receiver accepted it."""
        url = run.webhook_url or ""
        if urlparse(url).scheme not in self._config.allowed_schemes:
            log.warning("webhook.refused_scheme", run_id=run.run_id, url=url)
            return False

        body = json.dumps(payload(run), separators=(",", ":")).encode()
        headers = {"Content-Type": "application/json", "X-Run-Event": run.status}
        if self._config.signing_secret:
            headers["X-Run-Signature"] = signature(self._config.signing_secret, body)

        for attempt in range(self._config.max_attempts):
            try:
                response = await self._client.post(url, content=body, headers=headers)
            except httpx.HTTPError as exc:
                log.warning(
                    "webhook.unreachable", run_id=run.run_id, attempt=attempt + 1, error=str(exc)
                )
            else:
                if response.is_success:
                    log.info("webhook.delivered", run_id=run.run_id, status=run.status)
                    return True
                if response.status_code not in RETRYABLE:
                    # The receiver understood and refused. Repeating it changes nothing.
                    log.warning("webhook.refused", run_id=run.run_id, status=response.status_code)
                    return False
                log.warning(
                    "webhook.failed",
                    run_id=run.run_id,
                    attempt=attempt + 1,
                    status=response.status_code,
                )
            if attempt < self._config.max_attempts - 1:
                await asyncio.sleep(self._config.backoff_seconds * (2**attempt))
        log.warning("webhook.gave_up", run_id=run.run_id, attempts=self._config.max_attempts)
        return False

    async def aclose(self) -> None:
        """Let deliveries in flight finish before the process goes away."""
        if self._tasks:
            await asyncio.wait(set(self._tasks), timeout=self._config.timeout_seconds)
        for task in list(self._tasks):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._owns_client:
            await self._client.aclose()
