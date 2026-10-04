"""Application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version

import structlog
from fastapi import FastAPI, Request
from sqlalchemy.ext.asyncio import async_sessionmaker

from agent_runs.api.errors import install_error_handlers
from agent_runs.api.middleware import RequestContextMiddleware
from agent_runs.api.routers import artifacts, runs, schedules, webhooks
from agent_runs.blob import open_blob_store
from agent_runs.config.settings import Settings, get_settings
from agent_runs.domain.errors import Unavailable
from agent_runs.keys import KeyRegistry
from agent_runs.observability.logging import configure_logging
from agent_runs.store.database import connect, ping

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
        app.state.keys = KeyRegistry(settings.memory.url)
        app.state.blobs = open_blob_store(settings.blob)
        log.info("agent_runs.started", environment=settings.service.environment)
        try:
            yield
        finally:
            await app.state.keys.aclose()
            await app.state.blobs.aclose()
            await engine.dispose()
            log.info("agent_runs.stopped")

    app = FastAPI(title="agent-runs", version=version("agent-runs"), lifespan=lifespan)
    app.state.settings = settings
    app.add_middleware(RequestContextMiddleware)
    install_error_handlers(app)
    app.include_router(runs.router)
    app.include_router(artifacts.router)
    app.include_router(schedules.router)
    app.include_router(webhooks.router)

    @app.get("/health/live", tags=["ops"])
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["ops"])
    async def ready(request: Request) -> dict[str, str]:
        """Ready means the database answers, the only dependency every request has; 503
        (a problem, with ``Retry-After``) while it does not."""
        if not await ping(request.app.state.engine):
            raise Unavailable("the database does not answer")
        return {"status": "ok"}

    return app
