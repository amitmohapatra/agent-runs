"""The wire models agent-runs has beyond the contracts.

A run *is* the contracts' ``RunRecord`` (started from a ``RunStart``, paused with an
``Interrupt``, resumed with an ``InterruptResolution``) and a schedule *is* a ``Schedule``
(created from a ``ScheduleSpec``); those are used as they are. What is here is the service's
own shapes: a listing's summary, a lease and a claim, the resolution history, a schedule's
partial update and a fire's result, webhook subscriptions and the delivery envelope, and a
page of a listing. ``tests/test_contract.py`` checks every one against the committed
``docs/openapi.json``.
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
    """A worker's hold on a running run, until ``expires_at`` unless it heartbeats."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    worker_id: str
    expires_at: datetime


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
    """A subscription as listed and read: never its secret."""

    model_config = ConfigDict(frozen=True)

    webhook_id: str
    url: str
    events: list[WebhookEvent]
    created_by: str
    created_at: datetime


class WebhookCreated(Webhook):
    """The answer to a create, the only one that carries ``secret``: it signs every delivery
    to the subscription (``trellis.runs.webhooks.verify_signature`` checks it). Keep it; it
    is never shown again."""

    secret: str


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
