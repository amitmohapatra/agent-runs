"""The schedule requests the contracts cannot express. A schedule *is* the contracts'
``Schedule``, created from a ``ScheduleSpec``; what is here is the partial update, the fire
request and its result, and the errors of firing.

``on_behalf_of`` is set once, at creation, by a credential allowed to act as that principal,
and never accepted again: not by an update, not by a fire. Everything a fired run executes
as comes from the stored row.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict
from trellis.contracts.errors import AgentError
from trellis.contracts.runs import Schedule

from agent_runs.domain.errors import Conflict, Unavailable, Unprocessable


class ScheduleUpdate(BaseModel):
    """The fields a schedule's owner may change; only the ones sent change. There is no
    ``tenant_id`` and no ``on_behalf_of``, and ``extra="forbid"`` makes sending either a 422:
    rotating the identity of a schedule is indistinguishable from stealing it. The merged
    result is validated as a ``ScheduleSpec`` again, exactly as a create is."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    agent_id: str | None = None
    name: str | None = None
    cadence: str | None = None
    timezone: str | None = None
    input: Any = None
    workspace_id: str | None = None
    enabled: bool | None = None
    metadata: dict[str, Any] | None = None


class FireRequest(BaseModel):
    """Fire now, for the tick ``at`` (an instant that has arrived). ``at`` is half of the
    idempotency key: a repeat for the same instant returns the run the first fire queued.
    Without it a fire is for the tick the schedule is due for, else for now (a manual
    "run it again", where two clicks are two runs)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    at: AwareDatetime | None = None


class FireResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    schedule_id: str
    run_id: str
    fire_time: datetime
    idempotency_key: str
    schedule: Schedule


class DuplicateSchedule(Conflict):
    """An update that would give a schedule the identity another one already has."""

    def __init__(self, schedule_id: str) -> None:
        super().__init__(
            f"schedule {schedule_id} would duplicate another schedule of this tenant: the same "
            "agent_id, on_behalf_of, cadence and input"
        )


class ScheduleNotFiring(Conflict):
    def __init__(self, schedule_id: str) -> None:
        super().__init__(f"schedule {schedule_id} is paused and will not fire")


class FireTimeOutOfRange(Unprocessable):
    def __init__(self, at: datetime, now: datetime) -> None:
        super().__init__(
            f"fire time {at.isoformat()} has not arrived (now {now.isoformat()}): a fire is for "
            "a tick that has come due"
        )


class FireFailed(Unavailable):
    """The run could not be queued. The failure is recorded on the schedule (which may have
    paused itself); the caller commits that before answering."""

    def __init__(self, schedule: Schedule, error: AgentError) -> None:
        super().__init__(
            {
                "message": f"schedule {schedule.schedule_id} could not fire: {error.message}",
                "consecutive_failures": schedule.consecutive_failures,
                "auto_paused": not schedule.enabled,
                "error": error.model_dump(mode="json"),
            }
        )


def idempotency_key(schedule_id: str, fire_time: datetime) -> str:
    """One schedule, one instant, one run. UTC first, so one instant spelled in two zones is
    one key."""
    return f"{schedule_id}@{fire_time.astimezone(UTC).isoformat()}"


def input_sha256(value: Any) -> str:
    """Half of a schedule's identity: the SHA-256 (hex) of its input as canonical JSON (keys
    sorted, no whitespace, UTF-8, non-ASCII kept). Two inputs that are equal as JSON are one
    input, however their keys were ordered."""
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(canonical.encode()).hexdigest()
