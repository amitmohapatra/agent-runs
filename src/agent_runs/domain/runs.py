"""The run requests the contracts cannot express. A run *is* the contracts' ``RunRecord``,
started from a ``RunStart``, paused with an ``Interrupt`` and resumed with an
``InterruptResolution``; what is here is only the service's own verbs: queueing, pausing with
a checkpoint, finishing, claiming and heartbeating a lease.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, model_validator
from trellis.contracts.errors import AgentError
from trellis.contracts.runs import (
    Interrupt,
    InterruptResolution,
    RunEvent,
    RunRecord,
    RunStart,
    RunStatus,
)

from agent_runs.config.constants import (
    DEFAULT_LEASE_SECONDS,
    MAX_CHECKPOINT_BYTES,
    MAX_EVENTS_PER_APPEND,
    MAX_LEASE_SECONDS,
    MIN_LEASE_SECONDS,
)
from agent_runs.domain.errors import TooLarge

WorkerId = Annotated[str, StringConstraints(min_length=1, max_length=200, strip_whitespace=True)]
LeaseSeconds = Annotated[int, Field(ge=MIN_LEASE_SECONDS, le=MAX_LEASE_SECONDS)]


class RunCreate(RunStart):
    """A ``RunStart``; ``queue=true`` puts it on the queue (``QUEUED``) for a worker to claim
    instead of recording it as already ``RUNNING`` in the caller's process."""

    queue: bool = Field(
        default=False,
        description="true: put the run on the queue (QUEUED) for a worker to claim; false: "
        "record it as already RUNNING in the caller's process.",
    )

    def start(self) -> RunStart:
        return RunStart.model_validate(self.model_dump(exclude={"queue"}))


def _json_size(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode())


def bounded_payload(value: Any, *, name: str, limit: int) -> None:
    """A run's ``input`` or ``output``, refused (413) past ``limit`` bytes of compact JSON
    (``RUNS__SERVICE__MAX_PAYLOAD_BYTES``): it lives in the run's row and in every read."""
    if value is not None and (size := _json_size(value)) > limit:
        raise TooLarge(f"{name} is {size} bytes; the most a run keeps is {limit}")


def bounded_checkpoint(checkpoint: dict[str, Any] | None) -> dict[str, Any] | None:
    """The checkpoint, refused (413) past ``MAX_CHECKPOINT_BYTES`` of compact JSON."""
    if checkpoint is None:
        return None
    size = _json_size(checkpoint)
    if size > MAX_CHECKPOINT_BYTES:
        raise TooLarge(
            f"checkpoint is {size} bytes; the most a run keeps is {MAX_CHECKPOINT_BYTES}"
        )
    return checkpoint


class RunPause(BaseModel):
    """What a run waits on, and the executor's opaque ``checkpoint`` (its resume journal and
    the framework's own resume state) for whichever worker resumes it. The checkpoint
    replaces any earlier one and is cleared when the run finishes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    interrupt: Interrupt = Field(
        description="What the run waits on; its tenant_id and run_id are this run's."
    )
    checkpoint: dict[str, Any] | None = Field(
        default=None,
        description="The executor's opaque resume state (journal, framework state), at most "
        "1 MiB of compact JSON; replaces any earlier one, cleared when the run ends.",
    )

    def bounded_checkpoint(self) -> dict[str, Any] | None:
        return bounded_checkpoint(self.checkpoint)


class RunFinish(BaseModel):
    """How a run ended. The contracts decide which endings may carry an ``error``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: RunStatus = Field(
        description="An ending: SUCCESS, PARTIAL, ERROR, TIMEOUT, CANCELLED or REJECTED."
    )
    output: Any = Field(
        default=None, description="What the run produced, at most 1 MiB of compact JSON."
    )
    error: AgentError | None = Field(
        default=None, description="Why it failed; only with ERROR, TIMEOUT or REJECTED."
    )

    @model_validator(mode="after")
    def _is_an_ending(self) -> Self:
        if not self.status.final:
            raise ValueError(f"{self.status.value} is not how a run ends")
        RunRecord(run_id="-", tenant_id="-", agent_id="-", status=self.status, error=self.error)
        return self


class RunCancel(BaseModel):
    """Why a run is cancelled: kept with the run, with the principal of the key that asked."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    reason: str | None = Field(
        default=None, max_length=1000, description="Why the run is cancelled (optional)."
    )


class ClaimRequest(BaseModel):
    """A worker asking for the oldest queued run of one of its agents."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    worker_id: WorkerId = Field(description="The claiming worker, 1 to 200 characters.")
    agent_ids: list[str] = Field(
        min_length=1, max_length=100, description="The agents this worker runs (1 to 100)."
    )
    lease_seconds: LeaseSeconds = Field(
        default=DEFAULT_LEASE_SECONDS, description="How long the lease lasts, 5 to 3600 s."
    )


class HeartbeatRequest(BaseModel):
    """Extend the lease, and optionally save a progress ``checkpoint``: the executor's resume
    journal as it stands (tool calls done and their outputs), so the attempt after a worker
    crash resumes from it instead of repeating side effects. It replaces the run's
    checkpoint (absent: kept as it is), has the pause checkpoint's bound, and comes back on
    the next claim and every read."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    worker_id: WorkerId = Field(description="The worker holding the lease.")
    lease_seconds: LeaseSeconds = Field(
        default=DEFAULT_LEASE_SECONDS, description="The new lease, from now, 5 to 3600 s."
    )
    checkpoint: dict[str, Any] | None = Field(
        default=None,
        description="Progress to save: the executor's resume journal as it stands, at most "
        "1 MiB of compact JSON. Replaces the run's checkpoint; absent, it is kept.",
    )


class ReleaseRequest(BaseModel):
    """A worker letting go of a run it holds (it is stopping): the run goes back on the queue
    at once, for another worker, optionally with the progress made so far."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    worker_id: WorkerId = Field(description="The worker holding the lease.")
    checkpoint: dict[str, Any] | None = Field(
        default=None,
        description="Progress to save first, as a heartbeat saves it (at most 1 MiB of "
        "compact JSON); absent, the run's checkpoint is kept.",
    )


class Lease(BaseModel):
    """A worker's hold on a running run, until ``expires_at`` unless it heartbeats, and what
    the worker must know to stop in time: the working time the run has left."""

    model_config = ConfigDict(frozen=True)

    run_id: str = Field(description="The leased run.")
    worker_id: str = Field(description="The worker holding it.")
    expires_at: AwareDatetime = Field(description="When the lease lapses without a heartbeat.")
    remaining_seconds: float | None = Field(
        default=None,
        description="Working time the run has left as of this answer, in seconds: the lesser "
        "of its timeout_seconds and the service's maximum, less what it has worked. Past it "
        "the ticker ends the run TIMEOUT (run_timeout). Null: no limit.",
    )
    cancel_requested: bool = Field(
        default=False,
        description="Someone asked to cancel the run: stop working it and finish it CANCELLED. "
        "The lease is no longer extended; when it runs out the ticker cancels the run.",
    )


class RunSummary(BaseModel):
    """A run as a listing shows it: enough to tell runs apart and to work an inbox. The
    question a paused run asks is ``awaiting``; ``assignee`` is whose inbox it is in;
    ``deadline`` is the run's own deadline (``RunStart.deadline``), not the interrupt's
    (that one is ``awaiting.deadline``). Input, output, error and the checkpoint are only on
    the full record, ``GET /v1/runs/{run_id}``."""

    model_config = ConfigDict(frozen=True)

    run_id: str = Field(description="The run's id.")
    agent_id: str = Field(description="The agent the run is of.")
    status: RunStatus = Field(description="Where the run is in its lifecycle.")
    awaiting: Interrupt | None = Field(
        default=None, description="The question a paused run waits on (null otherwise)."
    )
    assignee: str | None = Field(
        default=None, description="Whose inbox a paused run is in: a person or a role."
    )
    deadline: AwareDatetime | None = Field(
        default=None, description="The run's own deadline (not the interrupt's)."
    )
    updated_at: AwareDatetime = Field(description="When the run last changed.")


class Claimed(BaseModel):
    """What a claim hands a worker: the run (now ``RUNNING``) and its lease."""

    model_config = ConfigDict(frozen=True)

    run: RunRecord = Field(description="The claimed run, now RUNNING, with its checkpoint.")
    lease: Lease = Field(description="The worker's lease on it.")


class ResolutionEntry(BaseModel):
    """One answered interrupt: what was asked, how it was answered, on which attempt."""

    model_config = ConfigDict(frozen=True)

    interrupt: Interrupt = Field(description="The interrupt as it was asked.")
    resolution: InterruptResolution = Field(description="How it was answered.")
    attempt: int = Field(description="The attempt that paused.")
    recorded_at: AwareDatetime = Field(description="When the answer took effect here.")


class EventsAppend(BaseModel):
    """Events of the run, to add to its log in this order. Each names the run and its tenant;
    one already in the log (the same ``attempt`` and ``sequence``) is not added again, so a
    retried append is harmless."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    events: list[RunEvent] = Field(
        min_length=1,
        max_length=MAX_EVENTS_PER_APPEND,
        description=f"The events, 1 to {MAX_EVENTS_PER_APPEND}, in the order they happened.",
    )


class EventsAppended(BaseModel):
    """What an append did."""

    model_config = ConfigDict(frozen=True)

    appended: int = Field(description="Events added (repeats of logged ones are not).")
    position: int = Field(description="The position of the run's last event now; 0 for none.")


class RunEventEntry(BaseModel):
    """One event of a run's log and its place in it."""

    model_config = ConfigDict(frozen=True)

    position: int = Field(
        description="The event's place in the run's log, from 1: what `after` and "
        "`Last-Event-ID` name."
    )
    event: RunEvent = Field(description="The event, as it was appended.")
