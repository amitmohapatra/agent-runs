"""Run routes: recording runs, the worker queue and the human inbox. This service never
executes an agent; a harness (in process, or a ``trellis worker`` claiming from the queue)
does, and records here what happened."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Body, Header, Path, Query, Request, Response
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from trellis.contracts.ids import now
from trellis.contracts.runs import InterruptResolution, RunRecord, RunStatus

from agent_runs.api import examples
from agent_runs.api.deps import Claiming, Session, Who
from agent_runs.api.openapi import conflict
from agent_runs.api.pagination import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    decode_cursor,
    link_next,
)
from agent_runs.config.constants import (
    EVENT_KEEPALIVE_SECONDS,
    EVENT_POLL_SECONDS,
    EVENT_STREAM_SECONDS,
    MAX_PAGE,
)
from agent_runs.domain.runs import (
    Claimed,
    ClaimRequest,
    EventsAppend,
    EventsAppended,
    HeartbeatRequest,
    Lease,
    ReleaseRequest,
    ResolutionEntry,
    RunCancel,
    RunCreate,
    RunEventEntry,
    RunFinish,
    RunPause,
    RunSummary,
    bounded_payload,
)
from agent_runs.observability.metrics import claims_total
from agent_runs.store.runs import RunStore
from agent_runs.store.webhooks import WebhookStore

router = APIRouter(prefix="/v1/runs", tags=["runs"])

_CREATED = 201
_NO_CONTENT = 204


def _payload_limit(request: Request) -> int:
    return request.app.state.settings.service.max_payload_bytes


def _leasing(request: Request, db: AsyncSession) -> RunStore:
    """The store for a route that hands out a lease, which tells the worker the working time
    the run has left under the service's maximum too, and claims within the deployment's
    limits."""
    limits = request.app.state.settings.runs
    return RunStore(
        db,
        max_run_seconds=limits.max_run_seconds,
        concurrency_per_key=limits.concurrency_per_key,
        max_running_per_tenant=limits.max_running_per_tenant,
    )


#: A worker fencing its write: refused (409) unless it still holds the run's lease.
WorkerId = Annotated[
    str | None,
    Query(
        max_length=200,
        description="The worker writing, when the run is leased (a worker always sends it): "
        "refused with 409 LEASE_LOST unless it still holds the run's lease. Omitted by a run "
        "recorded in the caller's own process.",
    ),
]
RunId = Annotated[str, Path(description="The run's id (`run_…`, or the caller's own).")]
_LEASE_LOST = (
    "LEASE_LOST: the worker no longer holds the run's lease (it lapsed and the run was "
    "requeued, or the run was paused, cancelled or finished): stop working the run. "
    "CONFLICT: the run is not in a state that allows this."
)


@router.post(
    "",
    status_code=_CREATED,
    summary="Record or queue a run",
    response_description="Created: the new run (`RUNNING`, or `QUEUED` with `queue: true`); "
    "`Location` names it.",
    responses={
        200: {
            "model": RunRecord,
            "description": "A repeat: the run with this `run_id` or `idempotency_key` already "
            "exists in this tenant, unchanged.",
        },
        **conflict(
            "CONFLICT: the `run_id` cannot be used, or the `idempotency_key` started a "
            "different run (`details.differing` names the fields)."
        ),
    },
)
async def start(
    body: Annotated[RunCreate, Body(openapi_examples=examples.START)],
    request: Request,
    db: Session,
    who: Who,
    response: Response,
) -> RunRecord:
    """Record a run (``RUNNING``), or queue it for a worker (``queue: true`` → ``QUEUED``).
    A repeated run id or ``idempotency_key`` answers 200 with the run the first start made;
    the same key with a different start is a 409. ``input`` is at most
    ``RUNS__SERVICE__MAX_PAYLOAD_BYTES`` (1 MiB) of compact JSON."""
    who.require_tenant(body.tenant_id)
    who.require_may_act_for(body.on_behalf_of)
    bounded_payload(body.input, name="input", limit=_payload_limit(request))
    run, created = await RunStore(db).start(body.start(), queue=body.queue, now=now())
    await db.commit()
    if created:
        response.headers["Location"] = f"{router.prefix}/{run.run_id}"
    else:
        response.status_code = 200
    return run


@router.post(
    "/claim",
    response_model=Claimed,
    summary="Claim the next queued run",
    response_description="The run, now `RUNNING`, and the worker's lease on it.",
    responses={
        _NO_CONTENT: {
            "description": "Nothing is queued for these agents that may run now; poll later."
        }
    },
)
async def claim(
    body: Annotated[ClaimRequest, Body(openapi_examples=examples.CLAIM)],
    request: Request,
    db: Session,
    who: Claiming,
) -> Claimed | Response:
    """Lease the next queued run of ``agent_ids`` to ``worker_id``, or 204 when there is
    none that may run now. The run is ``RUNNING`` and the worker holds it until
    ``lease.expires_at``; ``lease.remaining_seconds`` is the working time it has left.

    The next run is the one with the highest ``priority``, then the oldest, among those
    with room: fewer than ``RUNS__RUNS__CONCURRENCY_PER_KEY`` (1) runs of the tenant
    sharing its ``concurrency_key`` are ``RUNNING``, and the tenant's workers hold fewer
    than ``RUNS__RUNS__MAX_RUNNING_PER_TENANT`` runs (no cap unless set). A platform key
    that sends no ``X-Trellis-Tenant`` claims from every tenant's queue, the tenant whose
    workers hold the fewest runs of these agents first: a fair share of the fleet, with
    nothing to set. The claimed run names its tenant (``run.tenant_id``), which the
    worker's later calls send."""
    claimed = await _leasing(request, db).claim(who.tenant_id, body, now=now())
    await db.commit()
    claims_total.labels("empty" if claimed is None else "claimed").inc()
    return claimed if claimed is not None else Response(status_code=_NO_CONTENT)


@router.post(
    "/{run_id}/heartbeat",
    summary="Extend a lease, optionally saving progress",
    response_description="The extended lease.",
    responses=conflict(
        "LEASE_LOST: the lease is no longer this worker's, or the run no longer runs: stop "
        "working the run. Nothing is saved."
    ),
)
async def heartbeat(
    run_id: RunId,
    body: Annotated[HeartbeatRequest, Body(openapi_examples=examples.HEARTBEAT)],
    request: Request,
    db: Session,
    who: Who,
) -> Lease:
    """Extend the lease to ``now + lease_seconds``; every third of the lease is a good
    rhythm. With ``checkpoint``, also save the executor's progress (its resume journal) on
    the run, replacing the one there: the next attempt's claim gets it, so a worker crash
    repeats no checkpointed side effect. The lease says the working time the run has left
    (``remaining_seconds``). 409 ``LEASE_LOST`` means the lease is no longer the worker's;
    stop working the run."""
    lease = await _leasing(request, db).heartbeat(who.tenant_id, run_id, body, now=now())
    await db.commit()
    return lease


@router.post(
    "/{run_id}/release",
    summary="Let go of a run: back on the queue",
    response_description="The run, `QUEUED` as its next attempt (or `CANCELLED`, when its "
    "cancel was asked for); for a repeat, the run as it is.",
    responses=conflict(
        "LEASE_LOST: the lease is no longer this worker's, or the run no longer runs: there "
        "is nothing to let go of. Nothing is saved."
    ),
)
async def release(
    run_id: RunId,
    body: Annotated[ReleaseRequest, Body(openapi_examples=examples.RELEASE)],
    db: Session,
    who: Who,
) -> RunRecord:
    """The worker holding the run lets go of it, as a worker that is stopping does with the
    runs it could not finish: the run goes back on the queue at once as its next attempt,
    for another worker, without counting a lapsed lease (nothing crashed). With
    ``checkpoint``, the progress made so far is saved first, as a heartbeat saves it. A run
    whose cancel was asked for ends ``CANCELLED`` instead. Repeated by the same worker, it
    answers the run as it is."""
    at = now()
    run, released = await RunStore(db).release(who.tenant_id, run_id, body, now=at)
    if released:
        await WebhookStore(db).announce(run, now=at)
    await db.commit()
    return run


@router.post(
    "/{run_id}/pause",
    summary="Pause a run for a person",
    response_description="The run, `PAUSED` on the interrupt (or as stored, for a repeat).",
    responses=conflict(_LEASE_LOST),
)
async def pause(
    run_id: RunId,
    body: Annotated[RunPause, Body(openapi_examples=examples.PAUSE)],
    db: Session,
    who: Who,
    worker_id: WorkerId = None,
) -> RunRecord:
    """The run waits on ``body.interrupt`` (``awaiting``); its ``assignee`` puts it in that
    inbox, and its ``expects``, when given, must be a JSON Schema (422 otherwise, saying
    why). ``body.checkpoint`` is kept for the worker that resumes it (413 past the bound).
    The lease ends. Repeated by the same caller on the same interrupt, it answers the stored
    run and changes nothing."""
    at = now()
    run, paused = await RunStore(db).pause(who.tenant_id, run_id, body, worker_id=worker_id, now=at)
    if paused:
        await WebhookStore(db).announce(run, now=at)
    await db.commit()
    return run


@router.post(
    "/{run_id}/resume",
    summary="Answer the interrupt a run waits on",
    response_description="The run: `CANCELLED`, or continuing as the next attempt "
    "(`QUEUED` or `RUNNING`); for a repeat of the same resolution, the run as it is now.",
    responses={
        403: {
            "description": "AUTHORIZATION: the key may not answer this run. A key restricted "
            "to listed people (`may_act_as`) answers only as one of them (`reviewer`), and "
            "only a run assigned to that person or to nobody, never one assigned to a group "
            "(`role:…`); the detail says which, and what would. Or the key registry refuses "
            "the key, or X-Trellis-Tenant names a tenant the key may not act for."
        },
        **conflict(
            "CONFLICT: the interrupt was already answered by another resolution (a second "
            "answer), or the run is not paused, or waits on another interrupt."
        ),
    },
)
async def resume(
    run_id: RunId,
    body: Annotated[InterruptResolution, Body(openapi_examples=examples.RESUME)],
    db: Session,
    who: Who,
) -> RunRecord:
    """Answer the interrupt the run waits on. ``CANCEL`` ends it; anything else continues it
    as the next attempt: ``QUEUED`` for a worker when the run came from the queue, else
    ``RUNNING``. The resolution is kept as ``last_resolution`` and appended to the run's
    ``resolutions``, in the same transaction.

    Who may answer: an admin or platform key, and a key that may act for anyone (``*`` in
    ``may_act_as``, the default), any run; a key restricted to listed people, only as one of
    them (``reviewer``, a bare id being ``user:<id>``; none is the key itself) and only a run
    assigned to that person or to nobody, checked against the assignee now (after any
    escalation). Anything else is 403 ``AUTHORIZATION``, before anything is written.

    The answer must fit the question, also before anything is written: an ``ANSWER`` fits
    the interrupt's ``expects`` (a JSON Schema), or else is one of its ``options`` when it
    has some; an ``EDIT`` of a question (no ``tool_call``) carries a ``payload`` that fits
    ``expects``. Anything else is 422 ``VALIDATION``, the detail saying what does not fit.

    Repeated with the very same resolution (its ``resolved_at`` included), as a client
    retries it after losing the answer, it answers the run as it is now and changes nothing.
    Any other answer to an interrupt already answered is 409."""
    at = now()
    run, resumed = await RunStore(db).resume(
        who.tenant_id, run_id, body, answerer=who.credential, now=at
    )
    if resumed:
        await WebhookStore(db).announce(run, now=at)
    await db.commit()
    return run


@router.post(
    "/{run_id}/cancel",
    summary="Cancel a run, whatever its status",
    response_description="The run: `CANCELLED`, or `RUNNING` with its cancel asked of the "
    "worker holding it; for a repeat, the run as it is.",
    responses={
        403: {
            "description": "AUTHORIZATION: the key may not cancel this run: the keys that may "
            "answer it may cancel it (a key restricted to listed people, only a run assigned "
            "to one of them or to nobody). Or the key registry refuses the key, or "
            "X-Trellis-Tenant names a tenant the key may not act for."
        },
        **conflict("CONFLICT: the run already ended (other than by this very cancel)."),
    },
)
async def cancel(
    run_id: RunId,
    body: Annotated[RunCancel, Body(openapi_examples=examples.CANCEL)],
    db: Session,
    who: Who,
) -> RunRecord:
    """Cancel the run, keeping ``reason`` and the key's principal with it. A ``QUEUED`` or
    ``PAUSED`` run, and a ``RUNNING`` one no worker holds (kept in its caller's process),
    ends ``CANCELLED`` at once, announced as ``run.finished``. A ``RUNNING`` run a worker
    holds is asked to stop: the worker's next heartbeat answers ``cancel_requested: true``
    and no longer extends its lease; the worker finishes the run ``CANCELLED``, and if it
    has not when the lease runs out, the ticker cancels the run.

    Who may cancel: the keys that may answer the run (``resume``), checked against its
    assignee now, before anything is written; 403 otherwise. A cancel already asked for, or
    repeated after the run was cancelled by the same principal for the same reason, answers
    the run as it is; an ended run is otherwise 409."""
    at = now()
    run, changed = await RunStore(db).cancel(
        who.tenant_id, run_id, body, canceller=who.credential, now=at
    )
    if changed:
        await WebhookStore(db).announce(run, now=at)
    await db.commit()
    return run


@router.post(
    "/{run_id}/finish",
    summary="End a run",
    response_description="The ended run (or as stored, for a repeat).",
    responses=conflict(_LEASE_LOST),
)
async def finish(
    run_id: RunId,
    body: Annotated[RunFinish, Body(openapi_examples=examples.FINISH)],
    request: Request,
    db: Session,
    who: Who,
    worker_id: WorkerId = None,
) -> RunRecord:
    """End the run: from ``RUNNING`` any ending; from ``QUEUED`` or ``PAUSED`` only
    ``CANCELLED`` or ``TIMEOUT`` (cancelling a queued or waiting run). Repeated by the same
    caller with the same status, it answers the stored run and changes nothing. ``output``
    is at most ``RUNS__SERVICE__MAX_PAYLOAD_BYTES`` (1 MiB) of compact JSON."""
    bounded_payload(body.output, name="output", limit=_payload_limit(request))
    at = now()
    run, ended = await RunStore(db).finish(who.tenant_id, run_id, body, worker_id=worker_id, now=at)
    if ended:
        await WebhookStore(db).announce(run, now=at)
    await db.commit()
    return run


@router.get(
    "/{run_id}",
    summary="Read a run",
    response_description="The full record: input, output, error, checkpoint, awaiting.",
)
async def get(run_id: RunId, db: Session, who: Who) -> RunRecord:
    """One run of this tenant, the whole record, its ``worked_seconds`` counting the stretch
    it is running now. Another tenant's run is 404."""
    return await RunStore(db).get(who.tenant_id, run_id, now=now())


AfterQuery = Annotated[
    int,
    Query(
        ge=0,
        description="Only the events past this position: the last one seen (0, the default: "
        "from the first).",
    ),
]


@router.post(
    "/{run_id}/events",
    summary="Append events to a run's log",
    response_description="How many were added, and the position of the log's last event.",
    responses=conflict(_LEASE_LOST),
)
async def append_events(
    run_id: RunId,
    body: Annotated[EventsAppend, Body(openapi_examples=examples.EVENTS)],
    db: Session,
    who: Who,
    worker_id: WorkerId = None,
) -> EventsAppended:
    """Add the run's events (``RunEvent``: AG-UI's vocabulary, each naming this run and
    tenant, else 422) to its log, in order, each at the next position. Only while the run
    runs, fenced as a heartbeat is: by the worker holding its lease (``worker_id``), or by
    the caller it runs in when no worker holds it; 409 otherwise. An event already logged
    (the same ``attempt`` and ``sequence``) is not added again, so a retried append is safe.
    Append before pausing or finishing: the log takes nothing after. The log goes when the
    run is purged."""
    appended = await RunStore(db).append_events(who.tenant_id, run_id, body, worker_id=worker_id)
    await db.commit()
    return appended


@router.get(
    "/{run_id}/events",
    summary="Read a run's events",
    response_description="The run's events past `after`, oldest first, at most `limit`.",
)
async def events(
    run_id: RunId,
    db: Session,
    who: Who,
    after: AfterQuery = 0,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> list[RunEventEntry]:
    """The run's log from position ``after`` on, from any replica. The log is read by
    position rather than by cursor: pass the last ``position`` seen as ``after`` for the
    next page; the stream (``/events/stream``) counts the same positions."""
    entries, _ = await RunStore(db).events(who.tenant_id, run_id, after=after, limit=limit)
    return entries


@router.get(
    "/{run_id}/events/stream",
    summary="Follow a run's events (server-sent events)",
    response_class=StreamingResponse,
    response_description="A `text/event-stream`: each event as `id: <position>`, `event: "
    '<type>` and `data: <the entry as JSON>`; `event: end` with `data: {"status": …}` once '
    "the run has ended and its last event was sent.",
    responses={200: {"content": {"text/event-stream": {"schema": {"type": "string"}}}}},
)
async def stream_events(
    run_id: RunId,
    request: Request,
    who: Who,
    after: AfterQuery = 0,
    last_event_id: Annotated[
        int | None,
        Header(
            alias="Last-Event-ID",
            ge=0,
            description="The last position the client saw, as a reconnecting EventSource "
            "sends it; wins over a smaller `after`.",
        ),
    ] = None,
) -> StreamingResponse:
    """The run's events past ``after`` (or ``Last-Event-ID``), then each new one as it is
    appended, whichever replica it was appended to. The stream ends with ``event: end``
    once the run has ended and every event was sent; a paused run's stream stays open. A
    comment line every 15 s keeps idle connections open, and after five minutes the
    service ends the stream without ``end``: reconnect with the last position seen."""
    async with request.app.state.sessions() as db:  # a 404 before the stream opens
        await RunStore(db).get(who.tenant_id, run_id, now=now())
    start = max(after, last_event_id or 0)
    return StreamingResponse(
        _follow(request, who.tenant_id, run_id, start),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _sse(event: str, data: str, *, position: int | None = None) -> str:
    """One server-sent event (data is one line of JSON)."""
    head = "" if position is None else f"id: {position}\n"
    return f"{head}event: {event}\ndata: {data}\n\n"


async def _follow(request: Request, tenant_id: str, run_id: str, after: int) -> AsyncIterator[str]:
    """The stream's body: each read in a short transaction of its own, so a stream holds no
    connection while it waits."""
    ends_at = time.monotonic() + EVENT_STREAM_SECONDS
    quiet = 0.0
    while True:
        async with request.app.state.sessions() as db:
            entries, status = await RunStore(db).events(
                tenant_id, run_id, after=after, limit=MAX_PAGE
            )
        for entry in entries:
            after = entry.position
            yield _sse(entry.event.type.value, entry.model_dump_json(), position=after)
        if len(entries) == MAX_PAGE:
            continue
        if status.final:
            yield _sse("end", json.dumps({"status": status.value}))
            return
        if time.monotonic() >= ends_at or await request.is_disconnected():
            return
        await asyncio.sleep(EVENT_POLL_SECONDS)
        quiet = 0.0 if entries else quiet + EVENT_POLL_SECONDS
        if quiet >= EVENT_KEEPALIVE_SECONDS:
            quiet = 0.0
            yield ": keepalive\n\n"


_RESOLUTIONS_CURSOR = {"recorded_at": datetime, "resolution_id": str}
_RUNS_CURSOR = {"created_at": datetime, "run_id": str}


@router.get(
    "/{run_id}/resolutions",
    summary="List a run's answered interrupts",
    response_description="A page of the run's resolutions, oldest first.",
)
async def resolutions(
    run_id: RunId,
    request: Request,
    response: Response,
    db: Session,
    who: Who,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> list[ResolutionEntry]:
    """Every interrupt the run paused on and how a person answered it, oldest first: the
    audit trail ``last_resolution`` is only the end of."""
    page = await RunStore(db).resolutions(
        who.tenant_id,
        run_id,
        limit=limit,
        after=decode_cursor(cursor, fields=_RESOLUTIONS_CURSOR),
    )
    link_next(request, response, page.after)
    return page.items


@router.get(
    "",
    name="list",
    summary="List runs, or an inbox",
    response_description="A page of run summaries, newest first.",
)
async def listing(
    request: Request,
    response: Response,
    db: Session,
    who: Who,
    status: Annotated[RunStatus | None, Query(description="Only runs in this status.")] = None,
    assignee: Annotated[
        str | None,
        Query(description="Only paused runs assigned to this person or role: an inbox."),
    ] = None,
    agent_id: Annotated[str | None, Query(description="Only runs of this agent.")] = None,
    thread_id: Annotated[str | None, Query(description="Only runs in this thread.")] = None,
    parent_run_id: Annotated[
        str | None, Query(description="Only the child runs of this run.")
    ] = None,
    top_level: Annotated[
        bool,
        Query(
            description="true: only top-level runs (no parent_run_id), so an inbox lists a "
            "paused child's parent, not the child too."
        ),
    ] = False,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> list[RunSummary]:
    """This tenant's runs, newest first, as summaries; the full record is
    ``GET /v1/runs/{run_id}``. ``status=PAUSED&assignee=…`` is an inbox, and
    ``&top_level=true`` leaves out the runs other runs started (sub-agents)."""
    page = await RunStore(db).list(
        who.tenant_id,
        status=status,
        assignee=assignee,
        agent_id=agent_id,
        thread_id=thread_id,
        parent_run_id=parent_run_id,
        top_level=top_level,
        limit=limit,
        after=decode_cursor(cursor, fields=_RUNS_CURSOR),
    )
    link_next(request, response, page.after)
    return page.items
