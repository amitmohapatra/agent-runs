"""The schedule requests the contracts cannot express. A schedule *is* the contracts'
``Schedule``, created from a ``ScheduleSpec``; what is here is the partial update, the fire
request and its result, and the errors of firing.

``on_behalf_of`` is set once, at creation, by a credential allowed to act as that principal,
and never accepted again: not by an update, not by a fire. Everything a fired run executes
as comes from the stored row.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from hashlib import sha256
from typing import Any, Final

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from trellis.contracts.errors import AgentError
from trellis.contracts.runs import Schedule

from agent_runs.domain.errors import Conflict, ErrorCode, Unavailable, Unprocessable


class ScheduleUpdate(BaseModel):
    """The fields a schedule's owner may change; only the ones sent change. There is no
    ``tenant_id`` and no ``on_behalf_of``, and ``extra="forbid"`` makes sending either a 422:
    rotating the identity of a schedule is indistinguishable from stealing it. The merged
    result is validated as a ``ScheduleSpec`` again, exactly as a create is."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    agent_id: str | None = Field(default=None, description="Fire runs of this agent instead.")
    name: str | None = Field(default=None, description="A new label (need not be unique).")
    cadence: str | None = Field(
        default=None,
        description="hourly, daily, weekly, weekdays, manual, or a cron expression firing at "
        "most hourly; a change re-arms the next fire.",
    )
    timezone: str | None = Field(default=None, description="The IANA zone the cadence is in.")
    input: Any = Field(default=None, description="The input each fired run gets.")
    workspace_id: str | None = Field(default=None, description="The workspace runs act in.")
    enabled: bool | None = Field(
        default=None, description="false pauses; true resumes (clearing an auto-pause)."
    )
    timeout_seconds: float | None = Field(
        default=None,
        gt=0,
        description="The working-time limit of the runs it fires, in seconds; null removes it.",
    )
    agent_version: str | None = Field(
        default=None,
        max_length=128,
        description="The agent version its runs record; null removes it.",
    )
    priority: int | None = Field(
        default=None,
        ge=-1000,
        le=1000,
        description="The claim order of the runs it fires, -1000 to 1000 (higher first).",
    )
    concurrency_key: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
        description="The concurrency key of the runs it fires; null removes it.",
    )
    end_at: AwareDatetime | None = Field(
        default=None,
        description="When it ends (with an offset; kept in UTC): nothing fires at or after it, "
        "and an armed fire at or past it is dropped. null removes it, re-arming the next "
        "occurrence.",
    )
    metadata: dict[str, Any] | None = Field(
        default=None, description="Merged into the schedule's metadata."
    )


class FireRequest(BaseModel):
    """Fire now, for the tick ``at`` (an instant that has arrived). ``at`` is half of the
    idempotency key: a repeat for the same instant returns the run the first fire queued.
    Without it a fire is for the tick the schedule is due for, else for now (a manual
    "run it again", where two clicks are two runs)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    at: AwareDatetime | None = Field(
        default=None,
        description="The tick to fire for, an instant that has arrived (with an offset); "
        "half of the idempotency key.",
    )


class FireResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    schedule_id: str = Field(description="The schedule fired.")
    run_id: str = Field(description="The run queued (the same one for a repeated tick).")
    fire_time: datetime = Field(description="The tick this fire was for.")
    idempotency_key: str = Field(description="The run's key: `<schedule_id>@<fire_time UTC>`.")
    schedule: Schedule = Field(description="The schedule, advanced.")


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


class ScheduleEnded(Conflict):
    """A fire at or after the schedule's ``end_at``: for an instant at or past it, or at all
    once it has passed. Nothing fires at or after an end, by hand or by the ticker."""

    def __init__(self, schedule: Schedule) -> None:
        assert schedule.end_at is not None
        super().__init__(
            f"schedule {schedule.schedule_id} ended at {schedule.end_at.isoformat()} and fires "
            "no more"
        )
        self.schedule = schedule


class TooManySchedules(Conflict):
    """A create past the tenant's ``RUNS__SCHEDULES__MAX_PER_TENANT``: ``SCHEDULE_LIMIT``,
    with the limit and the schedules the tenant has (``count``) in ``details``, so a caller
    tells it from a lost race (``CONFLICT``) and says the numbers without reading the text.
    Still 409: the tenant's state, not its pace, refuses it, and only a delete makes room."""

    code = ErrorCode.SCHEDULE_LIMIT

    def __init__(self, limit: int, count: int) -> None:
        super().__init__(
            f"a tenant has at most {limit} schedules",
            details={"max_per_tenant": limit, "schedule_count": count},
        )


#: ``AgentError.code`` of a fire refused by ``RUNS__SCHEDULES__MAX_FIRES_PER_DAY``, and the
#: code of the ``409`` a fire by hand answers for it: one code wherever the refusal is read.
FIRES_PER_DAY: Final = ErrorCode.FIRES_PER_DAY.value


def fires_per_day_details(limit: int, fires: int, day: date) -> dict[str, Any]:
    """What a refusal by the daily fire limit says: the limit, the fires the tenant's
    schedules used on the UTC day, and that day."""
    return {"max_fires_per_day": limit, "fires_today": fires, "day": day.isoformat()}


class FireRefused(Conflict):
    """A fire past the tenant's ``RUNS__SCHEDULES__MAX_FIRES_PER_DAY`` for the UTC day: it
    queued nothing. Recorded on the schedule (``last_error``) and the tick skipped, as a
    failed fire is recorded and an overlapping tick skipped; the schedule is not paused for
    it. The caller commits that before answering. ``FIRES_PER_DAY``: ``details`` holds the
    numbers the recorded error does, and that error."""

    code = ErrorCode.FIRES_PER_DAY

    def __init__(self, schedule: Schedule, error: AgentError) -> None:
        super().__init__(
            error.message, details={**error.details, "error": error.model_dump(mode="json")}
        )
        self.schedule = schedule
        self.error = error


class SchedulePolicy(BaseModel):
    """How this deployment's schedules fire, and where the caller's tenant stands against
    its limits: what a harness tells a model as facts before it sets a schedule."""

    model_config = ConfigDict(frozen=True)

    min_interval_seconds: int = Field(
        description="The shortest gap a cadence's occurrences may have, in seconds "
        "(RUNS__SCHEDULES__MIN_INTERVAL_SECONDS); a faster one is 422 CADENCE_TOO_FREQUENT."
    )
    jitter_seconds: int = Field(
        description="Each schedule fires a stable 0 to this many seconds after its tick."
    )
    max_failed_fires: int = Field(
        description="Fires in a row that could not queue a run before a schedule pauses itself."
    )
    max_per_tenant: int | None = Field(
        description="The most schedules a tenant may have (409 SCHEDULE_LIMIT past it); null: "
        "no limit."
    )
    max_fires_per_day: int | None = Field(
        description="The most runs a tenant's schedules may fire in a UTC day (FIRES_PER_DAY "
        "past it); null: no limit."
    )
    schedule_count: int = Field(description="The schedules the tenant has now.")
    fires_today: int | None = Field(
        description="The fires the tenant's schedules used on `day`; null while "
        "max_fires_per_day is off (nothing is counted)."
    )
    day: date = Field(description="The UTC day `fires_today` counts.")


class FireTimeOutOfRange(Unprocessable):
    def __init__(self, at: datetime, now: datetime) -> None:
        super().__init__(
            f"fire time {at.isoformat()} has not arrived (now {now.isoformat()}): a fire is for "
            "a tick that has come due"
        )


class FireSkipped(Exception):
    """A due tick was skipped because the schedule's last run has not ended (Kubernetes'
    ``concurrencyPolicy: Forbid``, Temporal's overlap policy ``SKIP``); the schedule was armed
    for its next occurrence."""

    def __init__(self, schedule: Schedule, active_run_id: str) -> None:
        super().__init__(f"schedule {schedule.schedule_id} skipped: {active_run_id} has not ended")
        self.schedule = schedule
        self.active_run_id = active_run_id


class FireFailed(Unavailable):
    """The run could not be queued. The failure is recorded on the schedule (which may have
    paused itself); the caller commits that before answering."""

    def __init__(self, schedule: Schedule, error: AgentError) -> None:
        super().__init__(
            # the database's own words stay in details.error (and the schedule's last_error)
            f"schedule {schedule.schedule_id} could not queue its run",
            details={
                "consecutive_failures": schedule.consecutive_failures,
                "auto_paused": not schedule.enabled,
                "error": error.model_dump(mode="json"),
            },
            # worth repeating only while the schedule still fires and the cause may pass
            retryable=error.retryable and schedule.enabled,
        )


def input_sha256(value: Any) -> str:
    """Half of a schedule's identity: the SHA-256 (hex) of its input as canonical JSON (keys
    sorted, no whitespace, UTF-8, non-ASCII kept). Two inputs that are equal as JSON are one
    input, however their keys were ordered."""
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(canonical.encode()).hexdigest()
