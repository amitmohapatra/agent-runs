"""Application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from agent_runs.api.deps import Session
from agent_runs.api.routers import runs, schedules, webhooks
from agent_runs.config.settings import Settings, get_settings
from agent_runs.domain.errors import ServiceError
from agent_runs.observability.logging import configure_logging
from agent_runs.store.database import connect

log = structlog.get_logger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(
        level=settings.observability.log_level, json_output=settings.observability.log_json
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = await connect(settings.database)
        app.state.engine = engine
        app.state.sessions = async_sessionmaker(engine, expire_on_commit=False)
        log.info("agent_runs.started", environment=settings.service.environment)
        try:
            yield
        finally:
            await engine.dispose()
            log.info("agent_runs.stopped")

    app = FastAPI(title="agent-runs", version=version("agent-runs"), lifespan=lifespan)
    app.state.settings = settings
    app.include_router(runs.router)
    app.include_router(schedules.router)
    app.include_router(webhooks.router)

    @app.exception_handler(ServiceError)
    async def service_error(_: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

    @app.get("/health/live", tags=["ops"])
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["ops"])
    async def ready(db: Session) -> dict[str, str]:
        """Ready means the database answers, the only dependency this service has."""
        await db.execute(text("SELECT 1"))
        return {"status": "ok"}

    return app
