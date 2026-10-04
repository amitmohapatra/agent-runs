"""Operations routes, with no key: liveness, readiness and Prometheus metrics."""

from __future__ import annotations

from fastapi import APIRouter, Request, Response

from agent_runs.domain.errors import Unavailable
from agent_runs.observability.metrics import CONTENT_TYPE, render
from agent_runs.store.database import ping

router = APIRouter(tags=["ops"])


@router.get("/health/live", summary="Liveness", response_description="The process is up.")
async def live() -> dict[str, str]:
    """The process is up. Asks nothing of any dependency, so a database outage never gets
    the API process restarted."""
    return {"status": "ok"}


@router.get(
    "/health/ready",
    summary="Readiness",
    response_description="The database answers.",
    responses={
        503: {"description": "DEPENDENCY_UNAVAILABLE: the database does not answer within 3 s."}
    },
)
async def ready(request: Request) -> dict[str, str]:
    """Ready means the database answers, the only dependency every request has; 503
    (a problem, with ``Retry-After``) while it does not."""
    if not await ping(request.app.state.engine):
        raise Unavailable("the database does not answer")
    return {"status": "ok"}


@router.get(
    "/metrics",
    summary="Prometheus metrics",
    response_class=Response,
    responses={
        200: {
            "description": "This worker process's metrics, in the Prometheus text format.",
            "content": {"text/plain": {"schema": {"type": "string"}}},
        }
    },
)
async def metrics(request: Request) -> Response:
    """Prometheus metrics of this worker process: requests by route and status, latency,
    queue claims, rate-limit refusals, the database pool."""
    return Response(render(request.app.state.engine), media_type=CONTENT_TYPE)
