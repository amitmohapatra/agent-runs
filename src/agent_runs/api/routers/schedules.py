"""Schedule routes: when a run should start, never what it then does.

Two halves of authorization, both needed. The credential fixes the tenant. Within it, the
credential also fixes which principals the caller may make runs execute as: creating a
schedule as someone else, or editing, firing or deleting one that runs as someone else,
needs a key that may act for them. Otherwise repointing a colleague's schedule at another
agent would do everything editing ``on_behalf_of`` would.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Request, Response
from trellis.contracts.ids import now
from trellis.contracts.runs import Schedule, ScheduleSpec

from agent_runs.api.deps import Session, Who
from agent_runs.api.pagination import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    decode_cursor,
    link_next,
)
from agent_runs.domain.schedules import FireFailed, FireRequest, FireResult, ScheduleUpdate
from agent_runs.firing import Firing
from agent_runs.store.schedules import ScheduleStore

router = APIRouter(prefix="/v1/schedules", tags=["schedules"])

_CREATED = 201
_NO_CONTENT = 204


@router.post("", status_code=_CREATED, responses={200: {"model": Schedule}})
async def create(spec: ScheduleSpec, db: Session, who: Who, response: Response) -> Schedule:
    """An upsert on the schedule's identity ``(agent_id, on_behalf_of, cadence, input)`` in
    this tenant: ``201`` with a new schedule armed for its next occurrence (``created_by`` is
    the key's principal), or ``200`` with the existing one, unchanged."""
    who.require_tenant(spec.tenant_id)
    who.require_may_act_for(spec.on_behalf_of)
    schedule, created = await ScheduleStore(db).upsert(spec, created_by=who.principal, now=now())
    await db.commit()
    if created:
        response.headers["Location"] = f"{router.prefix}/{schedule.schedule_id}"
    else:
        response.status_code = 200
    return schedule


_CURSOR = {"created_at": datetime, "schedule_id": str}


@router.get("", name="list")
async def listing(
    request: Request,
    response: Response,
    db: Session,
    who: Who,
    enabled: bool | None = None,
    agent_id: str | None = None,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> list[Schedule]:
    """This tenant's schedules, newest first."""
    page = await ScheduleStore(db).list(
        who.tenant_id,
        enabled=enabled,
        agent_id=agent_id,
        limit=limit,
        after=decode_cursor(cursor, fields=_CURSOR),
    )
    link_next(request, response, page.after)
    return page.items


@router.get("/{schedule_id}")
async def get(schedule_id: str, db: Session, who: Who) -> Schedule:
    return await ScheduleStore(db).get(who.tenant_id, schedule_id)


@router.patch("/{schedule_id}")
async def update(schedule_id: str, change: ScheduleUpdate, db: Session, who: Who) -> Schedule:
    """Change the fields sent; ``on_behalf_of`` is not one of them. ``{"enabled": false}``
    pauses, ``{"enabled": true}`` resumes (clearing an auto-pause)."""
    await _administered(schedule_id, db, who)
    schedule = await ScheduleStore(db).update(who.tenant_id, schedule_id, change, now=now())
    await db.commit()
    return schedule


@router.delete("/{schedule_id}", status_code=_NO_CONTENT)
async def delete(schedule_id: str, db: Session, who: Who) -> Response:
    await _administered(schedule_id, db, who)
    await ScheduleStore(db).delete(who.tenant_id, schedule_id)
    await db.commit()
    return Response(status_code=_NO_CONTENT)


@router.post("/{schedule_id}/fire")
async def fire(
    schedule_id: str, db: Session, who: Who, body: FireRequest | None = None
) -> FireResult:
    """Queue the run for this schedule's tick now. Repeating it for the same ``at`` returns
    the same run. 409 when paused, 422 for an ``at`` that has not arrived, 503 when the run
    could not be queued (recorded on the schedule, which may pause itself)."""
    await _administered(schedule_id, db, who)
    at = body.at if body is not None else None
    try:
        result = await Firing(db).fire(who.tenant_id, schedule_id, at=at, now=now())
    except FireFailed:
        await db.commit()
        raise
    await db.commit()
    return result


async def _administered(schedule_id: str, db: Session, who: Who) -> None:
    """404 before 403: "exists but is not yours" is itself an answer about someone else."""
    schedule = await ScheduleStore(db).get(who.tenant_id, schedule_id)
    who.require_may_act_for(schedule.on_behalf_of)
