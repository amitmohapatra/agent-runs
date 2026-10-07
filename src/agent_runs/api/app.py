"""Application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version
from typing import Any

import structlog
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker

from agent_runs.api.errors import install_error_handlers
from agent_runs.api.middleware import (
    BodyLimitMiddleware,
    CompressionMiddleware,
    RequestContextMiddleware,
)
from agent_runs.api.openapi import TITLE, custom_openapi, operation_id
from agent_runs.api.ratelimit import TenantRateLimiter
from agent_runs.api.routers import artifacts, ops, runs, schedules, webhooks
from agent_runs.blob import open_blob_store
from agent_runs.config.settings import Settings, get_settings
from agent_runs.keys import KeyRegistry
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
        engine = await connect(settings.database, settings.service.worker_count)
        app.state.engine = engine
        app.state.sessions = async_sessionmaker(engine, expire_on_commit=False)
        app.state.limiter = TenantRateLimiter(settings.rate_limit, engine)
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

    app = FastAPI(
        title=TITLE,
        version=version("agent-runs"),
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        generate_unique_id_function=operation_id,
    )
    app.state.settings = settings
    # Added innermost first: a request meets the request context (id, metrics), then the
    # body limit, then compression, which acts on what the route produced.
    app.add_middleware(CompressionMiddleware)
    app.add_middleware(BodyLimitMiddleware, max_body_bytes=settings.service.max_body_bytes)
    app.add_middleware(RequestContextMiddleware)
    install_error_handlers(app)
    app.include_router(ops.router)
    app.include_router(runs.router)
    app.include_router(artifacts.router)
    app.include_router(schedules.router)
    app.include_router(webhooks.router)

    def openapi() -> dict[str, Any]:
        return custom_openapi(app)

    app.openapi = openapi  # type: ignore[method-assign]
    return app
