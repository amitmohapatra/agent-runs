"""Webhooks: the tenant's subscriptions, and the signature every delivery carries.

agent-runs POSTs a :class:`~trellis.runs.models.WebhookDelivery` to each subscription that
wants the event, with three headers:

* ``X-Trellis-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256>``, keyed by the
  subscription's secret, over ``"<t>."`` followed by the exact body bytes; for a while after
  the secret is rotated (``WebhooksAPI.rotate_secret``), a second ``v1=`` keyed by the secret
  it replaced, so a receiver verifies with either while it moves to the new one;
* ``X-Trellis-Event``: the event's type (``run.paused``, ``run.escalated``, ``run.finished``);
* ``X-Trellis-Delivery``: the event's id, the same on every retry.

:func:`sign` is the one implementation of the scheme: the service signs with it and a
receiver checks with :func:`verify_signature`, then reads the body with
:func:`parse_delivery`::

    if not verify_signature(secret, request.headers.get(SIGNATURE_HEADER), body):
        return Response(status_code=401)
    delivery = parse_delivery(body)
"""

from __future__ import annotations

import hmac
import time
from collections.abc import Sequence
from hashlib import sha256
from typing import Any, Final

from trellis.runs._transport import PAGE_LIMIT, Transport
from trellis.runs.models import (
    DeliveryRecord,
    DeliveryState,
    Page,
    Webhook,
    WebhookCreated,
    WebhookDelivery,
    WebhookEvent,
)

SIGNATURE_HEADER: Final = "X-Trellis-Signature"
EVENT_HEADER: Final = "X-Trellis-Event"
DELIVERY_HEADER: Final = "X-Trellis-Delivery"
#: How old (or how far ahead) a signature :func:`verify_signature` accepts: a replayed
#: delivery older than this is refused.
TOLERANCE_SECONDS: Final = 300
SIGNATURE_VERSION: Final = "v1"


def sign(secret: str, timestamp: int, body: bytes, *, previous: str | None = None) -> str:
    """The ``X-Trellis-Signature`` value for ``body`` sent at ``timestamp`` (unix seconds);
    with ``previous`` (a secret being rotated out), a second signature with it."""
    signed = f"{timestamp}.".encode() + body
    keys = [secret] if previous is None else [secret, previous]
    digests = [hmac.new(key.encode(), signed, sha256).hexdigest() for key in keys]
    return ",".join([f"t={timestamp}", *(f"{SIGNATURE_VERSION}={d}" for d in digests)])


def verify_signature(
    secret: str,
    header: str | None,
    body: bytes,
    *,
    now: int | None = None,
    tolerance: int = TOLERANCE_SECONDS,
) -> bool:
    """Whether ``header`` signs ``body`` (the raw bytes received, before any parsing) with
    ``secret``, at most ``tolerance`` seconds from ``now`` (the clock when not given). Any of
    its ``v1`` signatures may match: during a rotation the header carries one per secret. A
    missing or malformed header is ``False``, never an exception; the digests are compared in
    constant time."""
    stamp, digests = "", []
    for part in (header or "").split(","):
        name, separator, value = (piece.strip() for piece in part.partition("="))
        if separator and name == "t":
            stamp = value
        elif separator and name == SIGNATURE_VERSION and value:
            digests.append(value)
    if not (stamp.isascii() and stamp.isdigit()) or not digests:
        return False
    current = int(time.time()) if now is None else now
    if abs(current - int(stamp)) > tolerance:
        return False
    expected = sign(secret, int(stamp), body).partition(f",{SIGNATURE_VERSION}=")[2].encode()
    # bytes, not str: compare_digest refuses a str with non-ASCII characters
    return any(hmac.compare_digest(expected, digest.encode()) for digest in digests)


def parse_delivery(body: bytes | str) -> WebhookDelivery:
    """The delivery a verified body carries (a pydantic ``ValidationError`` when it is not
    one)."""
    return WebhookDelivery.model_validate_json(body)


class WebhooksAPI:
    """``runs.webhooks``: ``create``, ``list``, ``get``, ``delete``, ``rotate_secret`` (the
    tenant's subscriptions), ``deliveries`` and ``redeliver`` (what is owed to them, and what
    was given up on)."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    async def create(
        self, url: str, events: Sequence[WebhookEvent], *, tenant: str | None = None
    ) -> WebhookCreated:
        """Subscribe ``url`` (absolute ``https``) to ``events``. The answer carries the
        subscription's ``secret``, the only time it is shown."""
        body: dict[str, Any] = {"url": url, "events": [WebhookEvent(e).value for e in events]}
        data = await self._transport.json("POST", "/v1/webhooks", tenant=tenant, json=body)
        return WebhookCreated.model_validate(data)

    async def list(
        self, *, cursor: str | None = None, limit: int = PAGE_LIMIT, tenant: str | None = None
    ) -> Page[Webhook]:
        """One page of the tenant's subscriptions, oldest first (never their secrets)."""
        params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        rows, after = await self._transport.page("/v1/webhooks", tenant=tenant, params=params)
        return Page[Webhook](items=[Webhook.model_validate(row) for row in rows], next_cursor=after)

    async def get(self, webhook_id: str, *, tenant: str | None = None) -> Webhook | None:
        """The subscription, or None when there is none."""
        data = await self._transport.found("GET", f"/v1/webhooks/{webhook_id}", tenant=tenant)
        return None if data is None else Webhook.model_validate(data)

    async def delete(self, webhook_id: str, *, tenant: str | None = None) -> None:
        """Unsubscribe; the deliveries still owed to it are dropped."""
        await self._transport.send("DELETE", f"/v1/webhooks/{webhook_id}", tenant=tenant)

    async def rotate_secret(self, webhook_id: str, *, tenant: str | None = None) -> WebhookCreated:
        """A new secret for the subscription, in this answer only. Until
        ``previous_secret_expires_at`` (agent-runs' overlap, 24 h by default) deliveries are
        signed with the old one too, so receivers move to the new one without missing any."""
        data = await self._transport.json(
            "POST", f"/v1/webhooks/{webhook_id}/rotate-secret", tenant=tenant
        )
        return WebhookCreated.model_validate(data)

    async def deliveries(
        self,
        *,
        state: DeliveryState | None = None,
        webhook_id: str | None = None,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT,
        tenant: str | None = None,
    ) -> Page[DeliveryRecord]:
        """One page of the tenant's deliveries, newest first: ``state=DeliveryState.DEAD``
        for the ones given up on (kept for a while, to redeliver), ``PENDING`` for the ones
        still owed; ``webhook_id`` for one subscription's."""
        filters = {
            "state": state.value if state is not None else None,
            "webhook_id": webhook_id,
            "cursor": cursor,
        }
        params = {name: value for name, value in filters.items() if value is not None}
        rows, after = await self._transport.page(
            "/v1/webhooks/deliveries", tenant=tenant, params={**params, "limit": limit}
        )
        return Page[DeliveryRecord](
            items=[DeliveryRecord.model_validate(row) for row in rows], next_cursor=after
        )

    async def redeliver(self, delivery_id: str, *, tenant: str | None = None) -> DeliveryRecord:
        """Owe a dead delivery again: it is sent within a tick, with all its attempts ahead
        of it. One still owed raises :class:`~trellis.runs.errors.ConflictError`."""
        data = await self._transport.json(
            "POST", f"/v1/webhooks/deliveries/{delivery_id}/redeliver", tenant=tenant
        )
        return DeliveryRecord.model_validate(data)
