"""Run routes. Recording what happened to a run — never executing one."""

from __future__ import annotations

from typing import Annotated, Any

import structlog
from fastapi import APIRouter, HTTPException, Query

from agent_runs.api.deps import Session, Tenant
from agent_runs.domain.models import RUNNING, InvalidTransition, Run, RunCreate, RunTransition
from agent_runs.store.runs import RunStore

log = structlog.get_logger(__name__)

_CONFLICT = 409
_NOT_FOUND = 404
_FORBIDDEN = 403

router = APIRouter(prefix="/v1/runs", tags=["runs"])


@router.post("", status_code=201)
async def start(spec: RunCreate, db: Session, tenant_id: Tenant) -> Run:
    """Start a run, or return the existing one for a repeated idempotency key."""
    if spec.tenant_id != tenant_id:
        raise HTTPException(_FORBIDDEN, "tenant_id does not match the authenticated tenant")
    run, created = await RunStore(db).start(spec)
    await db.commit()
    log.info("run.started", run_id=run.run_id, agent_id=run.agent_id, created=created)
    return run


@router.get("/{run_id}")
async def get(run_id: str, db: Session, tenant_id: Tenant) -> Run:
    run = await RunStore(db).get(tenant_id, run_id)
    if run is None:
        raise HTTPException(_NOT_FOUND, f"no run {run_id}")
    return run


@router.post("/{run_id}/transition")
async def transition(run_id: str, change: RunTransition, db: Session, tenant_id: Tenant) -> Run:
    try:
        run = await RunStore(db).transition(tenant_id, run_id, change)
    except InvalidTransition as exc:
        # 409, not 400: the request is well formed, it lost a race or arrived twice.
        raise HTTPException(_CONFLICT, str(exc)) from exc
    if run is None:
        raise HTTPException(_NOT_FOUND, f"no run {run_id}")
    await db.commit()
    log.info("run.transitioned", run_id=run_id, status=run.status)
    return run


@router.post("/{run_id}/resume")
async def resume(run_id: str, db: Session, tenant_id: Tenant, answer: Any = None) -> Run:
    """What a human reply does to a paused run."""
    change = RunTransition(status=RUNNING, metadata={"answer": answer} if answer else {})
    return await transition(run_id, change, db, tenant_id)


@router.get("")
async def listing(
    db: Session,
    tenant_id: Tenant,
    status: str | None = None,
    agent_id: str | None = None,
    thread_id: str | None = None,
    parent_run_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> list[Run]:
    """Runs for this tenant, newest first. ``status=PAUSED`` is the human-inbox query."""
    return await RunStore(db).list(
        tenant_id,
        status=status,
        agent_id=agent_id,
        thread_id=thread_id,
        parent_run_id=parent_run_id,
        limit=limit,
    )


@router.get("/{run_id}/lineage")
async def lineage(run_id: str, db: Session, tenant_id: Tenant) -> list[Run]:
    """The run and its ancestors, nearest first."""
    chain = await RunStore(db).lineage(tenant_id, run_id)
    if not chain:
        raise HTTPException(_NOT_FOUND, f"no run {run_id}")
    return chain
