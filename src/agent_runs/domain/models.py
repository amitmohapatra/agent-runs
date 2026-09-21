"""What a run is, and the transitions it is allowed to make.

A *run* is one execution of one agent, recorded so it can outlive the process that started
it. That is the whole point: a turn that pauses to ask a person a question may wait longer
than any worker stays alive, and without a durable record the answer has nowhere to return
to.

The states are deliberately the contract's own ``AgentStatus`` rather than a private enum —
a run that the harness calls PAUSED and this service calls SUSPENDED is two vocabularies for
one fact, and they drift.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from universal_agent_contracts.messages import AgentStatus


def _now() -> datetime:
    return datetime.now(UTC)


#: States a run can still move on from. Everything else is final.
LIVE = frozenset({AgentStatus.PAUSED})
RUNNING = "RUNNING"

#: Which transitions are legal. A run service that accepts any update becomes a log of
#: whatever the last caller believed, and "why did this run finish twice" is unanswerable.
#:
#: RUNNING is this service's own state rather than a contract one: the contract describes how
#: a turn *ended*, and a turn in flight has not ended.
ALLOWED: dict[str, frozenset[str]] = {
    RUNNING: frozenset(
        {
            str(AgentStatus.SUCCESS),
            str(AgentStatus.PARTIAL),
            str(AgentStatus.ERROR),
            str(AgentStatus.TIMEOUT),
            str(AgentStatus.CANCELLED),
            str(AgentStatus.REJECTED),
            str(AgentStatus.PAUSED),
        }
    ),
    # A paused run resumes (back to RUNNING) or is abandoned. It cannot jump straight to
    # SUCCESS: something has to actually run to produce a result.
    str(AgentStatus.PAUSED): frozenset(
        {RUNNING, str(AgentStatus.CANCELLED), str(AgentStatus.TIMEOUT)}
    ),
}


class RunCreate(BaseModel):
    """Start a run. ``idempotency_key`` makes a retried start return the same run."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    agent_id: str
    run_id: str | None = None
    parent_run_id: str | None = None
    thread_id: str | None = None
    user_id: str | None = None
    #: Who this run acts as. A scheduled run has no one present, so the identity it carries
    #: is the one recorded when the schedule was made — it must never widen at fire time.
    on_behalf_of: str | None = None
    input: Any = None
    deadline: datetime | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunTransition(BaseModel):
    """Move a run to a new state."""

    model_config = ConfigDict(extra="forbid")

    status: str
    output: Any = None
    error: dict[str, Any] | None = None
    #: Why it paused, and what it is waiting for — shown to whoever must answer.
    awaiting: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class Run(BaseModel):
    """A run as stored."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    tenant_id: str
    agent_id: str
    status: str = RUNNING
    parent_run_id: str | None = None
    thread_id: str | None = None
    user_id: str | None = None
    on_behalf_of: str | None = None
    input: Any = None
    output: Any = None
    error: dict[str, Any] | None = None
    awaiting: dict[str, Any] | None = None
    attempt: int = 1
    deadline: datetime | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)

    @property
    def final(self) -> bool:
        return self.status != RUNNING and self.status not in {str(s) for s in LIVE}


class InvalidTransition(Exception):
    """A transition the state machine does not allow."""

    def __init__(self, run_id: str, frm: str, to: str) -> None:
        super().__init__(f"run {run_id} cannot move from {frm} to {to}")
        self.run_id, self.frm, self.to = run_id, frm, to


def check(run_id: str, frm: str, to: str) -> None:
    """Raise unless ``frm -> to`` is legal."""
    if to not in ALLOWED.get(frm, frozenset()):
        raise InvalidTransition(run_id, frm, to)


__all__ = [
    "ALLOWED",
    "LIVE",
    "RUNNING",
    "InvalidTransition",
    "Run",
    "RunCreate",
    "RunTransition",
    "check",
]
