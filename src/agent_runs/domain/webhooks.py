"""Tenant webhook subscriptions: which URL hears which run events.

A subscription belongs to the tenant, not to one run or schedule: whoever wants to know
(an inbox UI, a chat bridge) subscribes once. Its secret is minted here, returned once on
create, and signs every delivery to it.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated
from urllib.parse import urlparse

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, field_validator
from trellis.contracts.runs import RunRecord, RunStatus

from agent_runs.domain.errors import Conflict, Unprocessable


class WebhookEvent(StrEnum):
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

    url: Url
    events: list[WebhookEvent] = Field(min_length=1)

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

    webhook_id: str
    url: str
    events: list[WebhookEvent]
    created_by: str
    created_at: AwareDatetime


class WebhookCreated(Webhook):
    """The answer to a create, the only one that carries ``secret``."""

    secret: str


class TooManyWebhooks(Conflict):
    def __init__(self, limit: int) -> None:
        super().__init__(f"a tenant has at most {limit} webhook subscriptions")
