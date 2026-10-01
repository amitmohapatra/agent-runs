"""Run routes: recording runs, the worker queue and the human inbox. This service never
executes an agent; a harness (in process, or a ``trellis worker`` claiming from the queue)
does, and records here what happened."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Response
from trellis.contracts.ids import now
from trellis.contracts.runs import InterruptResolution, RunRecord, RunStatus

from agent_runs.api.deps import Session, Who
from agent_runs.config.constants import DEFAULT_PAGE, MAX_PAGE
from agent_runs.domain.runs import (
    Claimed,
    ClaimRequest,
    HeartbeatRequest,
    Lease,
    ResolutionEntry,
    RunCreate,
    RunFinish,
    RunPause,
    RunSummary,
)
from agent_runs.store.runs import RunStore
from agent_runs.store.webhooks import WebhookStore

router = APIRouter(prefix="/v1/runs", tags=["runs"])

_CREATED = 201
_NO_CONTENT = 204

#: A worker fencing its write: refused (409) unless it still holds the run's lease.
WorkerId = Annotated[str | None, Query(max_length=200)]


@router.post("", status_code=_CREATED, responses={200: {"model": RunRecord}})
async def start(body: RunCreate, db: Session, who: Who, response: Response) -> RunRecord:
    """Record a run (``RUNNING``), or queue it for a worker (``queue: true`` → ``QUEUED``).
    A repeated run id or ``idempotency_key`` answers 200 with the run the first start made."""
    who.require_tenant(body.tenant_id)
    who.require_may_act_for(body.on_behalf_of)
    run, created = await RunStore(db).start(body.start(), queue=body.queue, now=now())
    await db.commit()
    if not created:
        response.status_code = 200
    return run


@router.post(
    "/claim", response_model=Claimed, responses={_NO_CONTENT: {"description": "nothing queued"}}
)
async def claim(body: ClaimRequest, db: Session, who: Who) -> Claimed | Response:
    """Lease the oldest queued run of ``agent_ids`` to ``worker_id``, or 204 when there is
    none. The run is ``RUNNING`` and the worker holds it until ``lease.expires_at``."""
    claimed = await RunStore(db).claim(who.tenant_id, body, now=now())
    await db.commit()
    return claimed if claimed is not None else Response(status_code=_NO_CONTENT)


@router.post("/{run_id}/heartbeat")
async def heartbeat(run_id: str, body: HeartbeatRequest, db: Session, who: Who) -> Lease:
    """Extend the lease. 409 means it is no longer the worker's; stop working the run."""
    lease = await RunStore(db).heartbeat(who.tenant_id, run_id, body, now=now())
    await db.commit()
    return lease


@router.post("/{run_id}/pause")
async def pause(
    run_id: str, body: RunPause, db: Session, who: Who, worker_id: WorkerId = None
) -> RunRecord:
    """The run waits on ``body.interrupt`` (``awaiting``); its ``assignee`` puts it in that
    inbox. ``body.checkpoint`` is kept for the worker that resumes it (413 past the bound)."""
    at = now()
    run = await RunStore(db).pause(who.tenant_id, run_id, body, worker_id=worker_id, now=at)
    await WebhookStore(db).announce(run, now=at)
    await db.commit()
    return run


@router.post("/{run_id}/resume")
async def resume(run_id: str, body: InterruptResolution, db: Session, who: Who) -> RunRecord:
    """Answer the interrupt the run waits on. ``CANCEL`` ends it; anything else continues it
    as the next attempt: ``QUEUED`` for a worker when the run came from the queue, else
    ``RUNNING``. The resolution is kept as ``last_resolution`` and appended to the run's
    ``resolutions``, in the same transaction."""
    at = now()
    run = await RunStore(db).resume(who.tenant_id, run_id, body, now=at)
    await WebhookStore(db).announce(run, now=at)
    await db.commit()
    return run


@router.post("/{run_id}/finish")
async def finish(
    run_id: str, body: RunFinish, db: Session, who: Who, worker_id: WorkerId = None
) -> RunRecord:
    """End the run. Cancelling a queued or paused run is a finish with ``CANCELLED``."""
    at = now()
    run = await RunStore(db).finish(who.tenant_id, run_id, body, worker_id=worker_id, now=at)
    await WebhookStore(db).announce(run, now=at)
    await db.commit()
    return run


@router.get("/{run_id}")
async def get(run_id: str, db: Session, who: Who) -> RunRecord:
    return await RunStore(db).get(who.tenant_id, run_id)


@router.get("/{run_id}/resolutions")
async def resolutions(run_id: str, db: Session, who: Who) -> list[ResolutionEntry]:
    """Every interrupt the run paused on and how a person answered it, oldest first: the
    audit trail ``last_resolution`` is only the end of."""
    return await RunStore(db).resolutions(who.tenant_id, run_id)


@router.get("")
async def listing(
    db: Session,
    who: Who,
    status: RunStatus | None = None,
    assignee: str | None = None,
    agent_id: str | None = None,
    thread_id: str | None = None,
    parent_run_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = DEFAULT_PAGE,
) -> list[RunSummary]:
    """This tenant's runs, newest first, as summaries; the full record is
    ``GET /v1/runs/{run_id}``. ``status=PAUSED&assignee=…`` is an inbox."""
    return await RunStore(db).list(
        who.tenant_id,
        status=status,
        assignee=assignee,
        agent_id=agent_id,
        thread_id=thread_id,
        parent_run_id=parent_run_id,
        limit=limit,
    )
