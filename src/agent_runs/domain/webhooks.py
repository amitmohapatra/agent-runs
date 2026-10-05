"""Tenant webhook subscriptions: which URL hears which run events, and the deliveries owed to
them.

A subscription belongs to the tenant, not to one run or schedule: whoever wants to know
(an inbox UI, a chat bridge) subscribes once. Its secret is minted here, returned once on
create (and on each rotation), and signs every delivery to it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated
from urllib.parse import urlparse

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, field_validator
from trellis.contracts.runs import RunRecord, RunStatus

from agent_runs.domain.errors import Conflict, Unprocessable


class WebhookEvent(StrEnum):
    """A run event a subscription may hear: a pause, an escalation, an ending."""

    PAUSED = "run.paused"
    ESCALATED = "run.escalated"
    FINISHED = "run.finished"


def event_of(run: RunRecord) -> WebhookEvent | None:
    """What a run's current status announces, if anything: a pause or an ending."""
    if run.status is RunStatus.PAUSED:
        return WebhookEvent.PAUSED
    return WebhookEvent.FINISHED if run.final else None


Url = Annotated[str, StringConstraints(min_length=1, max_length=2048, strip_whitespace=True)]


class WebhookCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    url: Url = Field(description="Where deliveries go: absolute https (http too in dev).")
    events: list[WebhookEvent] = Field(
        min_length=1, description="The run events to deliver (duplicates dropped)."
    )

    @field_validator("events")
    @classmethod
    def _distinct(cls, events: list[WebhookEvent]) -> list[WebhookEvent]:
        return sorted(set(events))

    def check_url(self, *, allow_http: bool) -> None:
        """``https`` with a host; plain ``http`` only in dev (a receiver on a laptop)."""
        parsed = urlparse(self.url)
        schemes = {"https", "http"} if allow_http else {"https"}
        if parsed.scheme not in schemes or not parsed.hostname:
            raise Unprocessable(f"url must be an absolute {' or '.join(sorted(schemes))} URL")


class Webhook(BaseModel):
    """A subscription as listed: never its secret."""

    model_config = ConfigDict(frozen=True)

    webhook_id: str = Field(description="The subscription's id.")
    url: str = Field(description="Where deliveries go.")
    events: list[WebhookEvent] = Field(description="The run events delivered, sorted.")
    created_by: str = Field(description="The principal of the key that created it.")
    created_at: AwareDatetime = Field(description="When it was created.")
    previous_secret_expires_at: AwareDatetime | None = Field(
        default=None,
        description="After a rotation, when deliveries stop being signed with the secret it "
        "replaced as well as with the new one; null before any rotation.",
    )


class WebhookCreated(Webhook):
    """The answer to a create or a rotation, the only ones that carry ``secret``."""

    secret: str = Field(
        description="Signs every delivery (X-Trellis-Signature); shown in this answer only."
    )


@dataclass(frozen=True)
class Attempt:
    """How one delivery attempt went: accepted (no ``error``), or why not and whether
    another attempt is worth making."""

    error: str | None = None
    retry: bool = False

    @property
    def accepted(self) -> bool:
        return self.error is None


class DeliveryState(StrEnum):
    """Where a delivery is: still owed (waiting for its next attempt), or given up on and
    kept for redelivery."""

    PENDING = "pending"
    DEAD = "dead"


class DeliveryRecord(BaseModel):
    """A delivery in the outbox, as listed: which event it carries to which subscription,
    and how its attempts went."""

    model_config = ConfigDict(frozen=True)

    delivery_id: str = Field(description="The delivery's id (`dlv_…`).")
    webhook_id: str = Field(description="The subscription it goes to.")
    event_id: str = Field(description="The event's id, as X-Trellis-Delivery carries it.")
    type: WebhookEvent = Field(description="The event.")
    run_id: str = Field(description="The run the event is about.")
    state: DeliveryState = Field(description="pending (still owed) or dead (given up on).")
    attempts: int = Field(description="Attempts made since it was written or redelivered.")
    last_error: str | None = Field(
        default=None, description="Why the last attempt failed; null before any did."
    )
    next_attempt_at: AwareDatetime | None = Field(
        default=None, description="When a pending delivery is tried next (null when dead)."
    )
    dead_at: AwareDatetime | None = Field(
        default=None, description="When it was given up on (null while pending)."
    )
    created_at: AwareDatetime = Field(description="When the event was written.")


class TooManyWebhooks(Conflict):
    def __init__(self, limit: int) -> None:
        super().__init__(f"a tenant has at most {limit} webhook subscriptions")
