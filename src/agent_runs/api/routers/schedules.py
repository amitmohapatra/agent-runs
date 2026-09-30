"""Schedule routes: when a run should start, never what it then does.

Two halves of authorization, both needed. The credential fixes the tenant. Within it, the
credential also fixes which principals the caller may make runs execute as: creating a
schedule as someone else, or editing, firing or deleting one that runs as someone else,
needs a key that may act for them. Otherwise repointing a colleague's schedule at another
agent would do everything editing ``on_behalf_of`` would.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Response
from trellis.contracts.ids import now
from trellis.contracts.runs import Schedule, ScheduleSpec

from agent_runs.api.deps import Session, Who
from agent_runs.config.constants import DEFAULT_PAGE, MAX_PAGE
from agent_runs.domain.schedules import FireFailed, FireRequest, FireResult, ScheduleUpdate
from agent_runs.firing import Firing
from agent_runs.store.schedules import ScheduleStore

router = APIRouter(prefix="/v1/schedules", tags=["schedules"])

_CREATED = 201
_NO_CONTENT = 204


@router.post("", status_code=_CREATED)
async def create(spec: ScheduleSpec, db: Session, who: Who) -> Schedule:
    """A schedule armed for its next occurrence. ``created_by`` is the key's principal."""
    who.require_tenant(spec.tenant_id)
    who.require_may_act_for(spec.on_behalf_of)
    schedule = await ScheduleStore(db).create(spec, created_by=who.principal, now=now())
    await db.commit()
    return schedule


@router.get("")
async def listing(
    db: Session,
    who: Who,
    enabled: bool | None = None,
    agent_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = DEFAULT_PAGE,
) -> list[Schedule]:
    """This tenant's schedules, newest first."""
    return await ScheduleStore(db).list(
        who.tenant_id, enabled=enabled, agent_id=agent_id, limit=limit
    )


@router.get("/{schedule_id}")
async def get(schedule_id: str, db: Session, who: Who) -> Schedule:
    return await ScheduleStore(db).get(who.tenant_id, schedule_id)


@router.patch("/{schedule_id}")
async def update(schedule_id: str, change: ScheduleUpdate, db: Session, who: Who) -> Schedule:
    """Change the fields sent; ``on_behalf_of`` is not one of them."""
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


@router.post("/{schedule_id}/pause")
async def pause(schedule_id: str, db: Session, who: Who) -> Schedule:
    return await _set_enabled(schedule_id, db, who, enabled=False)


@router.post("/{schedule_id}/resume")
async def resume(schedule_id: str, db: Session, who: Who) -> Schedule:
    """Fire again from the next occurrence it can still honour; clears an auto-pause."""
    return await _set_enabled(schedule_id, db, who, enabled=True)


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


async def _set_enabled(schedule_id: str, db: Session, who: Who, *, enabled: bool) -> Schedule:
    await _administered(schedule_id, db, who)
    schedule = await ScheduleStore(db).set_enabled(
        who.tenant_id, schedule_id, enabled=enabled, now=now()
    )
    await db.commit()
    return schedule
