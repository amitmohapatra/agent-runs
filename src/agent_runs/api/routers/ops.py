"""Operations routes, with no key: liveness, readiness and Prometheus metrics."""

from __future__ import annotations

from fastapi import APIRouter, Request, Response

from agent_runs.domain.errors import Unavailable
from agent_runs.observability.metrics import CONTENT_TYPE, render
from agent_runs.store.database import ping

router = APIRouter(tags=["ops"])


@router.get("/health/live")
async def live() -> dict[str, str]:
    """The process is up. Asks nothing of any dependency, so a database outage never gets
    the API process restarted."""
    return {"status": "ok"}


@router.get("/health/ready")
async def ready(request: Request) -> dict[str, str]:
    """Ready means the database answers, the only dependency every request has; 503
    (a problem, with ``Retry-After``) while it does not."""
    if not await ping(request.app.state.engine):
        raise Unavailable("the database does not answer")
    return {"status": "ok"}


@router.get("/metrics")
async def metrics(request: Request) -> Response:
    """Prometheus metrics of this worker process: requests by route and status, latency,
    queue claims, rate-limit refusals, the database pool."""
    return Response(render(request.app.state.engine), media_type=CONTENT_TYPE)
