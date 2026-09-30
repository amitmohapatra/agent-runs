"""Webhook subscription routes: a tenant says which URL hears which run events."""

from __future__ import annotations

from fastapi import APIRouter, Request, Response
from trellis.contracts.ids import now

from agent_runs.api.deps import Session, Who
from agent_runs.domain.webhooks import Webhook, WebhookCreate, WebhookCreated
from agent_runs.store.webhooks import WebhookStore

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])

_CREATED = 201
_NO_CONTENT = 204


@router.post("", status_code=_CREATED)
async def create(body: WebhookCreate, request: Request, db: Session, who: Who) -> WebhookCreated:
    """Subscribe ``url`` to ``events``. The answer carries the subscription's ``secret``,
    which signs every delivery to it; it is shown here and never again."""
    body.check_url(allow_http=request.app.state.settings.service.is_dev)
    created = await WebhookStore(db).create(
        who.tenant_id, body, created_by=who.principal, now=now()
    )
    await db.commit()
    return created


@router.get("")
async def listing(db: Session, who: Who) -> list[Webhook]:
    """This tenant's subscriptions, oldest first, without their secrets."""
    return await WebhookStore(db).list(who.tenant_id)


@router.delete("/{webhook_id}", status_code=_NO_CONTENT)
async def delete(webhook_id: str, db: Session, who: Who) -> Response:
    """Unsubscribe; deliveries still owed to it are dropped."""
    await WebhookStore(db).delete(who.tenant_id, webhook_id)
    await db.commit()
    return Response(status_code=_NO_CONTENT)
