"""The wire models agent-runs has beyond the contracts.

A run *is* the contracts' ``RunRecord`` (started from a ``RunStart``, paused with an
``Interrupt``, resumed with an ``InterruptResolution``) and a schedule *is* a ``Schedule``
(created from a ``ScheduleSpec``); those are used as they are. What is here is the service's
own shapes: a listing's summary, a lease and a claim, the resolution history, a schedule's
partial update and a fire's result, webhook subscriptions, the outbox's deliveries and the
delivery envelope, and a page of a listing. ``tests/test_contract.py`` checks every one
against the committed ``docs/openapi.json``.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict
from trellis.contracts.runs import Interrupt, InterruptResolution, RunRecord, RunStatus, Schedule


class RunSummary(BaseModel):
    """A run as a listing shows it: enough to tell runs apart and to work an inbox. The
    question a paused run asks is ``awaiting``; ``assignee`` is whose inbox it is in;
    ``deadline`` is the run's own (the interrupt's is ``awaiting.deadline``). Read the run
    (``RunsClient.get``) for its input, output, error and checkpoint."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    agent_id: str
    status: RunStatus
    awaiting: Interrupt | None = None
    assignee: str | None = None
    deadline: datetime | None = None
    updated_at: datetime


class Lease(BaseModel):
    """A worker's hold on a running run, until ``expires_at`` unless it heartbeats.
    ``remaining_seconds`` is the working time the run had left when the lease was given (its
    ``timeout_seconds`` or the service's maximum, the lesser, less what it worked; ``None``:
    no limit): past it agent-runs ends the run ``TIMEOUT``. ``cancel_requested``: someone
    asked to cancel the run; stop and finish it ``CANCELLED`` (the lease is no longer
    extended, and agent-runs cancels the run when it runs out)."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    worker_id: str
    expires_at: datetime
    remaining_seconds: float | None = None
    cancel_requested: bool = False


class Claimed(BaseModel):
    """What a claim hands a worker: the run (now ``RUNNING``, with its checkpoint and its
    last resolution) and the lease on it."""

    model_config = ConfigDict(frozen=True)

    run: RunRecord
    lease: Lease


class ResolutionEntry(BaseModel):
    """One answered interrupt: what was asked, how it was answered, on which attempt."""

    model_config = ConfigDict(frozen=True)

    interrupt: Interrupt
    resolution: InterruptResolution
    attempt: int
    recorded_at: datetime


class ScheduleUpdate(BaseModel):
    """The fields a schedule's owner may change; only the ones set are sent, and only those
    change. ``enabled=False`` pauses the schedule; ``enabled=True`` resumes it, clearing an
    auto-pause. ``metadata`` is merged into the schedule's."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    agent_id: str | None = None
    name: str | None = None
    cadence: str | None = None
    timezone: str | None = None
    input: Any = None
    workspace_id: str | None = None
    enabled: bool | None = None
    metadata: dict[str, Any] | None = None


class FireResult(BaseModel):
    """A fire: the run it queued (the same one for a repeated tick) and the schedule,
    advanced. ``idempotency_key`` is the run's, ``<schedule_id>@<fire_time UTC>``."""

    model_config = ConfigDict(frozen=True)

    schedule_id: str
    run_id: str
    fire_time: datetime
    idempotency_key: str
    schedule: Schedule


class WebhookEvent(StrEnum):
    """A run event a subscription may hear: a pause, an escalation, an ending."""

    PAUSED = "run.paused"
    ESCALATED = "run.escalated"
    FINISHED = "run.finished"


class Webhook(BaseModel):
    """A subscription as listed and read: never its secret. ``previous_secret_expires_at``
    is, after a rotation, when deliveries stop being signed with the replaced secret too."""

    model_config = ConfigDict(frozen=True)

    webhook_id: str
    url: str
    events: list[WebhookEvent]
    created_by: str
    created_at: datetime
    previous_secret_expires_at: datetime | None = None


class WebhookCreated(Webhook):
    """The answer to a create, the only one that carries ``secret``: it signs every delivery
    to the subscription (``trellis.runs.webhooks.verify_signature`` checks it). Keep it; it
    is never shown again."""

    secret: str


class DeliveryState(StrEnum):
    """Where a delivery is: still owed, or given up on (and kept to be redelivered)."""

    PENDING = "pending"
    DEAD = "dead"


class DeliveryRecord(BaseModel):
    """A delivery in agent-runs' outbox: which event (``event_id``, ``type``, about
    ``run_id``) it carries to which subscription, and how its attempts went. A dead one is
    kept for a while (seven days by default) to be redelivered."""

    model_config = ConfigDict(frozen=True)

    delivery_id: str
    webhook_id: str
    event_id: str
    type: WebhookEvent
    run_id: str
    state: DeliveryState
    attempts: int
    last_error: str | None = None
    next_attempt_at: datetime | None = None
    dead_at: datetime | None = None
    created_at: datetime


class WebhookData(BaseModel):
    """What a delivery is about: the run, as a listing shows it."""

    model_config = ConfigDict(frozen=True)

    run: RunSummary


class WebhookDelivery(BaseModel):
    """The body agent-runs POSTs to a subscription. ``event_id`` is the same on every retry
    of one event (drop repeats by it); read the run for anything the summary lacks."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    type: WebhookEvent
    tenant_id: str
    workspace_id: str | None = None
    occurred_at: datetime
    data: WebhookData


class Page[T](BaseModel):
    """One page of a listing: the items and the cursor of the next page (None on the last),
    read from the answer's ``Link: rel="next"``. Pass ``next_cursor`` back as ``cursor`` to
    continue."""

    model_config = ConfigDict(frozen=True)

    items: list[T]
    next_cursor: str | None = None

    @property
    def has_more(self) -> bool:
        return self.next_cursor is not None
