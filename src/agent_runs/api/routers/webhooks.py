"""Webhook subscription routes: a tenant says which URL hears which run events."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Body, Path, Request, Response
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
from agent_runs.domain.webhooks import Webhook, WebhookCreate, WebhookCreated
from agent_runs.store.webhooks import WebhookStore

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])

_CREATED = 201
_NO_CONTENT = 204


WebhookId = Annotated[str, Path(description="The subscription's id (`wh_…`).")]


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
    which signs every delivery to it; it is shown here and never again."""
    body.check_url(allow_http=request.app.state.settings.service.is_dev)
    created = await WebhookStore(db).create(
        who.tenant_id, body, created_by=who.principal, now=now()
    )
    await db.commit()
    response.headers["Location"] = f"{router.prefix}/{created.webhook_id}"
    return created


_CURSOR = {"created_at": datetime, "webhook_id": str}


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
