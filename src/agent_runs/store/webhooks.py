"""Webhook subscriptions and the delivery outbox.

A run event is written to the outbox in the same transaction as the run change that caused
it, one row per subscription of the tenant that wants that event, so an event is never lost
to a crash between the commit and the send and never sent for a change that rolled back. The
ticker delivers from the outbox (``claim_due`` / ``settle``). A delivery given up on stays in
it, dead, for the tenant to list and redeliver, until the ticker drops it (``drop_dead``).
"""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from trellis.contracts.ids import new_id, stable_id
from trellis.contracts.runs import RunRecord

from agent_runs.config.constants import (
    DEFAULT_PAGE,
    MAX_WEBHOOKS_PER_TENANT,
    WEBHOOK_ATTEMPTS,
    WEBHOOK_LEASE,
    WEBHOOK_RETRY_BASE,
    WEBHOOK_RETRY_CAP,
)
from agent_runs.domain.errors import Conflict, NotFound
from agent_runs.domain.runs import RunSummary
from agent_runs.domain.webhooks import (
    Attempt,
    DeliveryRecord,
    DeliveryState,
    TooManyWebhooks,
    Webhook,
    WebhookCreate,
    WebhookCreated,
    WebhookEvent,
    event_of,
)
from agent_runs.retry import backoff
from agent_runs.store.paging import Page, page_of
from agent_runs.store.tables import WebhookDeliveryRow, WebhookRow


def _webhook(row: WebhookRow) -> Webhook:
    return Webhook(
        webhook_id=row.webhook_id,
        url=row.url,
        events=[WebhookEvent(e) for e in row.events],
        created_by=row.created_by,
        created_at=row.created_at,
        previous_secret_expires_at=row.previous_secret_expires_at,
    )


def _secret() -> str:
    return f"whsec_{secrets.token_urlsafe(32)}"


def _delivery(row: WebhookDeliveryRow) -> DeliveryRecord:
    dead = row.dead_at is not None
    return DeliveryRecord(
        delivery_id=row.delivery_id,
        webhook_id=row.webhook_id,
        event_id=row.payload["event_id"],
        type=WebhookEvent(row.payload["type"]),
        run_id=row.payload["data"]["run"]["run_id"],
        state=DeliveryState.DEAD if dead else DeliveryState.PENDING,
        attempts=row.attempts,
        last_error=row.last_error,
        next_attempt_at=None if dead else row.next_attempt_at,
        dead_at=row.dead_at,
        created_at=row.created_at,
    )


def envelope(run: RunRecord, event: WebhookEvent) -> dict[str, Any]:
    """The event envelope (what ``trellis.runs.parse_delivery`` reads), with the run's summary
    as the data. ``event_id`` is derived from the run, its attempt, status and assignee and
    the event, so the same event is the same id however often it is written or retried."""
    assignee = run.awaiting.assignee if run.awaiting else None
    summary = RunSummary(
        run_id=run.run_id,
        agent_id=run.agent_id,
        status=run.status,
        awaiting=run.awaiting,
        assignee=assignee,
        deadline=run.deadline,
        updated_at=run.updated_at,
    )
    return {
        "event_id": stable_id(run.run_id, run.attempt, run.status, event, assignee, prefix="whd_"),
        "type": event.value,
        "tenant_id": run.tenant_id,
        "workspace_id": run.workspace_id,
        "occurred_at": run.updated_at.isoformat(),
        "data": {"run": summary.model_dump(mode="json")},
    }


@dataclass(frozen=True)
class Delivery:
    """One outbox row the ticker holds, with where it goes and what signs it: the
    subscription's secret, and the one a rotation replaced while it still signs too."""

    delivery_id: str
    url: str
    secret: str
    payload: dict[str, Any]
    attempts: int
    previous_secret: str | None = None


class WebhookStore:
    """The caller commits."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ------------------------------------------------------------------ subscriptions
    async def create(
        self, tenant_id: str, body: WebhookCreate, *, created_by: str, now: datetime
    ) -> WebhookCreated:
        held = await self._session.scalar(
            select(func.count()).select_from(WebhookRow).where(WebhookRow.tenant_id == tenant_id)
        )
        if (held or 0) >= MAX_WEBHOOKS_PER_TENANT:
            raise TooManyWebhooks(MAX_WEBHOOKS_PER_TENANT)
        row = WebhookRow(
            webhook_id=new_id("wh_"),
            tenant_id=tenant_id,
            url=body.url,
            events=[e.value for e in body.events],
            secret=_secret(),
            created_by=created_by,
            created_at=now,
        )
        self._session.add(row)
        await self._session.flush()
        return WebhookCreated(**_webhook(row).model_dump(), secret=row.secret)

    async def list(
        self,
        tenant_id: str,
        *,
        limit: int = DEFAULT_PAGE,
        after: Mapping[str, Any] | None = None,
    ) -> Page[Webhook]:
        """This tenant's subscriptions, oldest first, a page at a time (keyset
        ``created_at, webhook_id``)."""
        order = (WebhookRow.created_at, WebhookRow.webhook_id)
        query = select(WebhookRow).where(WebhookRow.tenant_id == tenant_id)
        if after is not None:
            query = query.where(tuple_(*order) > (after["created_at"], after["webhook_id"]))
        rows = (await self._session.scalars(query.order_by(*order).limit(limit + 1))).all()
        return page_of(
            rows,
            limit=limit,
            item=_webhook,
            position=lambda row: {"created_at": row.created_at, "webhook_id": row.webhook_id},
        )

    async def get(self, tenant_id: str, webhook_id: str) -> Webhook:
        return _webhook(await self._subscription(tenant_id, webhook_id))

    async def rotate_secret(
        self, tenant_id: str, webhook_id: str, *, overlap: timedelta, now: datetime
    ) -> WebhookCreated:
        """A new secret for the subscription, shown in this answer only. For ``overlap``
        the secret it replaces signs every delivery too, so a receiver keeps verifying while
        it moves to the new one; a rotation within that window ends the older secret's."""
        row = await self._subscription(tenant_id, webhook_id, lock=True)
        row.previous_secret, row.secret = row.secret, _secret()
        row.previous_secret_expires_at = now + overlap
        await self._session.flush()
        return WebhookCreated(**_webhook(row).model_dump(), secret=row.secret)

    async def _subscription(
        self, tenant_id: str, webhook_id: str, *, lock: bool = False
    ) -> WebhookRow:
        query = select(WebhookRow).where(
            WebhookRow.tenant_id == tenant_id, WebhookRow.webhook_id == webhook_id
        )
        row = await self._session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise NotFound(f"no webhook {webhook_id}")
        return row

    async def delete(self, tenant_id: str, webhook_id: str) -> None:
        """The subscription and every delivery still owed to it."""
        deleted = await self._session.scalar(
            delete(WebhookRow)
            .where(WebhookRow.tenant_id == tenant_id, WebhookRow.webhook_id == webhook_id)
            .returning(WebhookRow.webhook_id)
        )
        if deleted is None:
            raise NotFound(f"no webhook {webhook_id}")

    # ------------------------------------------------------------------ the outbox
    async def announce(
        self, run: RunRecord, event: WebhookEvent | None = None, *, now: datetime
    ) -> None:
        """Write the event ``run`` warrants (``event`` overrides the one its status
        announces) for every subscription of its tenant that wants it. One statement; a
        repeat of the same event to the same subscription is dropped by the primary key."""
        event = event or event_of(run)
        if event is None:
            return
        payload = envelope(run, event)
        wanting = select(WebhookRow.webhook_id).where(
            WebhookRow.tenant_id == run.tenant_id, WebhookRow.events.contains([event.value])
        )
        hooks = (await self._session.scalars(wanting)).all()
        if not hooks:
            return
        await self._session.execute(
            insert(WebhookDeliveryRow)
            .values(
                [
                    {
                        "delivery_id": stable_id(payload["event_id"], hook, prefix="dlv_"),
                        "webhook_id": hook,
                        "payload": payload,
                        "attempts": 0,
                        "next_attempt_at": now,
                        "created_at": now,
                    }
                    for hook in hooks
                ]
            )
            .on_conflict_do_nothing()
        )

    async def claim_due(self, *, now: datetime, limit: int) -> list[Delivery]:
        """Deliveries still owed and due now, held for ``WEBHOOK_LEASE`` (another ticker
        skips them; a ticker that dies holding them lets them come due again) and counted as
        attempted."""
        rows = (
            await self._session.execute(
                select(WebhookDeliveryRow, WebhookRow)
                .join(WebhookRow, WebhookRow.webhook_id == WebhookDeliveryRow.webhook_id)
                .where(
                    WebhookDeliveryRow.dead_at.is_(None),
                    WebhookDeliveryRow.next_attempt_at <= now,
                )
                .order_by(WebhookDeliveryRow.next_attempt_at)
                .limit(limit)
                .with_for_update(of=WebhookDeliveryRow, skip_locked=True)
            )
        ).all()
        held: list[Delivery] = []
        for row, hook in rows:
            row.attempts += 1
            row.next_attempt_at = now + WEBHOOK_LEASE
            overlapping = (hook.previous_secret_expires_at or now) > now
            previous = hook.previous_secret if overlapping else None
            held.append(
                Delivery(
                    row.delivery_id, hook.url, hook.secret, row.payload, row.attempts, previous
                )
            )
        await self._session.flush()
        return held

    async def settle(self, delivery: Delivery, attempt: Attempt, *, now: datetime) -> bool:
        """After an attempt: an accepted delivery leaves the outbox; one worth another
        attempt within its ``WEBHOOK_ATTEMPTS`` waits out the backoff; any other (refused for
        good, or out of attempts) is kept, dead, to be listed and redelivered. Returns
        whether it died."""
        if attempt.accepted:
            await self._session.execute(
                delete(WebhookDeliveryRow).where(
                    WebhookDeliveryRow.delivery_id == delivery.delivery_id
                )
            )
            return False
        row = await self._session.get(WebhookDeliveryRow, delivery.delivery_id)
        if row is None:  # its subscription went while it was being sent
            return False
        row.last_error = attempt.error
        if attempt.retry and delivery.attempts < WEBHOOK_ATTEMPTS:
            wait = backoff(WEBHOOK_RETRY_BASE, delivery.attempts, cap=WEBHOOK_RETRY_CAP)
            row.next_attempt_at = now + wait
        else:
            row.dead_at = now
        await self._session.flush()
        return row.dead_at is not None

    async def deliveries(
        self,
        tenant_id: str,
        *,
        state: DeliveryState | None = None,
        webhook_id: str | None = None,
        limit: int = DEFAULT_PAGE,
        after: Mapping[str, Any] | None = None,
    ) -> Page[DeliveryRecord]:
        """This tenant's deliveries, newest first, a page at a time (keyset ``created_at,
        delivery_id``, both descending): all, the ones still owed or the dead ones, of every
        subscription or of one."""
        order = (WebhookDeliveryRow.created_at, WebhookDeliveryRow.delivery_id)
        query = (
            select(WebhookDeliveryRow)
            .join(WebhookRow, WebhookRow.webhook_id == WebhookDeliveryRow.webhook_id)
            .where(WebhookRow.tenant_id == tenant_id)
        )
        if state is not None:
            dead = WebhookDeliveryRow.dead_at.is_not(None)
            query = query.where(dead if state is DeliveryState.DEAD else ~dead)
        if webhook_id is not None:
            query = query.where(WebhookDeliveryRow.webhook_id == webhook_id)
        if after is not None:
            query = query.where(tuple_(*order) < (after["created_at"], after["delivery_id"]))
        newest = query.order_by(*(column.desc() for column in order)).limit(limit + 1)
        rows = (await self._session.scalars(newest)).all()
        return page_of(
            rows,
            limit=limit,
            item=_delivery,
            position=lambda row: {"created_at": row.created_at, "delivery_id": row.delivery_id},
        )

    async def redeliver(self, tenant_id: str, delivery_id: str, *, now: datetime) -> DeliveryRecord:
        """A dead delivery owed again: due now, with all its attempts ahead of it, signed
        with the subscription's secret as it is now. A delivery still owed is a
        ``Conflict``: it is being tried already."""
        row = await self._session.scalar(
            select(WebhookDeliveryRow)
            .join(WebhookRow, WebhookRow.webhook_id == WebhookDeliveryRow.webhook_id)
            .where(
                WebhookRow.tenant_id == tenant_id,
                WebhookDeliveryRow.delivery_id == delivery_id,
            )
            .with_for_update(of=WebhookDeliveryRow)
        )
        if row is None:
            raise NotFound(f"no delivery {delivery_id}")
        if row.dead_at is None:
            raise Conflict(f"delivery {delivery_id} is still owed: it is being tried already")
        row.dead_at, row.attempts, row.next_attempt_at = None, 0, now
        await self._session.flush()
        return _delivery(row)

    async def drop_dead(self, *, before: datetime, limit: int) -> int:
        """Delete deliveries that died before ``before`` (their retention is over), at most
        ``limit`` of them. Returns how many."""
        expired = (
            select(WebhookDeliveryRow.delivery_id)
            .where(WebhookDeliveryRow.dead_at < before)
            .order_by(WebhookDeliveryRow.dead_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        dropped = await self._session.scalars(
            delete(WebhookDeliveryRow)
            .where(WebhookDeliveryRow.delivery_id.in_(expired.scalar_subquery()))
            .returning(WebhookDeliveryRow.delivery_id)
        )
        return len(dropped.all())
