"""Webhook routes: a tenant says which URL hears which run events, rotates a subscription's
secret, and sees (and redelivers) what was given up on."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Body, Path, Query, Request, Response
from trellis.contracts.ids import now

from agent_runs.api import examples
from agent_runs.api.deps import Session, Who
from agent_runs.api.openapi import conflict
from agent_runs.api.pagination import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    decode_cursor,
    link_next,
)
from agent_runs.domain.webhooks import (
    DeliveryRecord,
    DeliveryState,
    Webhook,
    WebhookCreate,
    WebhookCreated,
)
from agent_runs.egress import require_public
from agent_runs.store.webhooks import WebhookStore

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])

_CREATED = 201
_NO_CONTENT = 204


WebhookId = Annotated[str, Path(description="The subscription's id (`wh_…`).")]
DeliveryId = Annotated[str, Path(description="The delivery's id (`dlv_…`).")]


@router.post(
    "",
    status_code=_CREATED,
    summary="Subscribe to run events",
    response_description="Created: the subscription with its `secret`, shown here only; "
    "`Location` names it.",
    responses=conflict("CONFLICT: the tenant already has 20 subscriptions."),
)
async def create(
    body: Annotated[WebhookCreate, Body(openapi_examples=examples.WEBHOOK)],
    request: Request,
    response: Response,
    db: Session,
    who: Who,
) -> WebhookCreated:
    """Subscribe ``url`` to ``events``. The answer carries the subscription's ``secret``,
    which signs every delivery to it; it is shown here and never again (rotate it with
    ``rotate-secret``). Outside dev the URL must be ``https`` and its host must resolve to
    public addresses only (422 otherwise), unless the deployment allows private targets;
    each delivery checks the host again."""
    settings = request.app.state.settings
    body.check_url(allow_http=settings.service.is_dev)
    if not settings.private_webhook_targets:
        await require_public(body.url)
    created = await WebhookStore(db).create(
        who.tenant_id, body, created_by=who.principal, now=now()
    )
    await db.commit()
    response.headers["Location"] = f"{router.prefix}/{created.webhook_id}"
    return created


_CURSOR = {"created_at": datetime, "webhook_id": str}
_DELIVERIES_CURSOR = {"created_at": datetime, "delivery_id": str}


@router.get(
    "",
    name="list",
    summary="List subscriptions",
    response_description="A page of this tenant's subscriptions, oldest first, without secrets.",
)
async def listing(
    request: Request,
    response: Response,
    db: Session,
    who: Who,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> list[Webhook]:
    """This tenant's subscriptions, oldest first, without their secrets."""
    page = await WebhookStore(db).list(
        who.tenant_id, limit=limit, after=decode_cursor(cursor, fields=_CURSOR)
    )
    link_next(request, response, page.after)
    return page.items


@router.get(
    "/deliveries",
    summary="List deliveries, or the dead ones",
    response_description="A page of this tenant's deliveries, newest first.",
)
async def deliveries(
    request: Request,
    response: Response,
    db: Session,
    who: Who,
    state: Annotated[
        DeliveryState | None,
        Query(description="`dead`: given up on, kept to be redelivered; `pending`: still owed."),
    ] = None,
    webhook_id: Annotated[
        str | None, Query(description="Only the deliveries to this subscription.")
    ] = None,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> list[DeliveryRecord]:
    """This tenant's deliveries, newest first. A delivery that used its attempts, or that
    its receiver (or this deployment's address check) refused for good, is dead: kept with
    its ``last_error`` for ``RUNS__WEBHOOKS__DEAD_RETENTION_DAYS`` (7) to be redelivered,
    then dropped."""
    page = await WebhookStore(db).deliveries(
        who.tenant_id,
        state=state,
        webhook_id=webhook_id,
        limit=limit,
        after=decode_cursor(cursor, fields=_DELIVERIES_CURSOR),
    )
    link_next(request, response, page.after)
    return page.items


@router.post(
    "/deliveries/{delivery_id}/redeliver",
    summary="Redeliver a dead delivery",
    response_description="The delivery, owed again and due now.",
    responses=conflict("CONFLICT: the delivery is still owed: it is being tried already."),
)
async def redeliver(delivery_id: DeliveryId, db: Session, who: Who) -> DeliveryRecord:
    """Owe a dead delivery again: due now, with all its attempts ahead of it, signed with
    the subscription's secret as it is now. The event and its ``event_id`` are unchanged,
    so a receiver that did get it drops the repeat."""
    owed = await WebhookStore(db).redeliver(who.tenant_id, delivery_id, now=now())
    await db.commit()
    return owed


@router.post(
    "/{webhook_id}/rotate-secret",
    summary="Rotate a subscription's secret",
    response_description="The subscription with its new `secret`, shown here only.",
)
async def rotate_secret(
    webhook_id: WebhookId, request: Request, db: Session, who: Who
) -> WebhookCreated:
    """A new secret for the subscription, in this answer only. For
    ``RUNS__WEBHOOKS__SECRET_OVERLAP_HOURS`` (24) after it, every delivery carries a
    signature with each secret (``t=…,v1=<new>,v1=<old>``), which
    ``trellis.runs.webhooks.verify_signature`` accepts with either: move the receiver to the
    new secret within that window (``previous_secret_expires_at``)."""
    overlap = request.app.state.settings.webhooks.secret_overlap
    rotated = await WebhookStore(db).rotate_secret(
        who.tenant_id, webhook_id, overlap=overlap, now=now()
    )
    await db.commit()
    return rotated


@router.get(
    "/{webhook_id}",
    summary="Read a subscription",
    response_description="The subscription, without its secret.",
)
async def get(webhook_id: WebhookId, db: Session, who: Who) -> Webhook:
    """One subscription of this tenant, without its secret. Another tenant's is 404."""
    return await WebhookStore(db).get(who.tenant_id, webhook_id)


@router.delete(
    "/{webhook_id}",
    status_code=_NO_CONTENT,
    summary="Unsubscribe",
    response_description="Deleted, with the deliveries still owed to it.",
)
async def delete(webhook_id: WebhookId, db: Session, who: Who) -> Response:
    """Unsubscribe; deliveries still owed to it are dropped."""
    await WebhookStore(db).delete(who.tenant_id, webhook_id)
    await db.commit()
    return Response(status_code=_NO_CONTENT)
