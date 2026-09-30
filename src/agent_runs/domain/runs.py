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
from trellis.contracts.runs import Interrupt, RunRecord, RunStart, RunStatus

from agent_runs.config.constants import (
    DEFAULT_LEASE_SECONDS,
    MAX_CHECKPOINT_BYTES,
    MAX_LEASE_SECONDS,
    MIN_LEASE_SECONDS,
)
from agent_runs.domain.errors import TooLarge

WorkerId = Annotated[str, StringConstraints(min_length=1, max_length=200, strip_whitespace=True)]
LeaseSeconds = Annotated[int, Field(ge=MIN_LEASE_SECONDS, le=MAX_LEASE_SECONDS)]


class RunCreate(RunStart):
    """A ``RunStart``; ``queue=true`` puts it on the queue (``QUEUED``) for a worker to claim
    instead of recording it as already ``RUNNING`` in the caller's process."""

    queue: bool = False

    def start(self) -> RunStart:
        return RunStart.model_validate(self.model_dump(exclude={"queue"}))


class RunPause(BaseModel):
    """What a run waits on, and the executor's opaque ``checkpoint`` (its resume journal and
    the framework's own resume state) for whichever worker resumes it. The checkpoint
    replaces any earlier one and is cleared when the run finishes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    interrupt: Interrupt
    checkpoint: dict[str, Any] | None = None

    def bounded_checkpoint(self) -> dict[str, Any] | None:
        """The checkpoint, refused (413) past ``MAX_CHECKPOINT_BYTES`` of compact JSON."""
        if self.checkpoint is None:
            return None
        size = len(json.dumps(self.checkpoint, separators=(",", ":"), ensure_ascii=False).encode())
        if size > MAX_CHECKPOINT_BYTES:
            raise TooLarge(
                f"checkpoint is {size} bytes; the most a pause carries is {MAX_CHECKPOINT_BYTES}"
            )
        return self.checkpoint


class RunFinish(BaseModel):
    """How a run ended. The contracts decide which endings may carry an ``error``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: RunStatus
    output: Any = None
    error: AgentError | None = None

    @model_validator(mode="after")
    def _is_an_ending(self) -> Self:
        if not self.status.final:
            raise ValueError(f"{self.status.value} is not how a run ends")
        RunRecord(run_id="-", tenant_id="-", agent_id="-", status=self.status, error=self.error)
        return self


class ClaimRequest(BaseModel):
    """A worker asking for the oldest queued run of one of its agents."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    worker_id: WorkerId
    agent_ids: list[str] = Field(min_length=1, max_length=100)
    lease_seconds: LeaseSeconds = DEFAULT_LEASE_SECONDS


class HeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    worker_id: WorkerId
    lease_seconds: LeaseSeconds = DEFAULT_LEASE_SECONDS


class Lease(BaseModel):
    """A worker's hold on a running run, until ``expires_at`` unless it heartbeats."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    worker_id: str
    expires_at: AwareDatetime


class RunSummary(BaseModel):
    """A run as a listing shows it: enough to tell runs apart and to work an inbox. The
    question a paused run asks is ``awaiting``; ``assignee`` is whose inbox it is in;
    ``deadline`` is the run's own deadline (``RunStart.deadline``), not the interrupt's
    (that one is ``awaiting.deadline``). Input, output, error and the checkpoint are only on
    the full record, ``GET /v1/runs/{run_id}``."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    agent_id: str
    status: RunStatus
    awaiting: Interrupt | None = None
    assignee: str | None = None
    deadline: AwareDatetime | None = None
    updated_at: AwareDatetime


class Claimed(BaseModel):
    """What a claim hands a worker: the run (now ``RUNNING``) and its lease."""

    model_config = ConfigDict(frozen=True)

    run: RunRecord
    lease: Lease
