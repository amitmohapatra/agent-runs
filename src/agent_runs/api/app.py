"""Application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from agent_runs.api.deps import Session
from agent_runs.api.routers import runs
from agent_runs.config.settings import Settings, get_settings
from agent_runs.store.tables import Base

log = structlog.get_logger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(settings.database.url, echo=settings.database.echo)
        async with engine.begin() as conn:
            # One table, owned entirely by this service: a migration tool would be ceremony.
            # That judgement changes the day a second table appears.
            await conn.run_sync(Base.metadata.create_all)
        app.state.engine = engine
        app.state.sessions = async_sessionmaker(engine, expire_on_commit=False)
        log.info("agent_runs.started", environment=settings.service.environment)
        try:
            yield
        finally:
            await engine.dispose()
            log.info("agent_runs.stopped")

    app = FastAPI(title="Agent Runs", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.include_router(runs.router)

    @app.get("/health/live", tags=["ops"])
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["ops"])
    async def ready(db: Session) -> dict[str, str]:
        """Ready means the database answers — the only dependency this service has."""
        await db.execute(text("SELECT 1"))
        return {"status": "ok"}

    return app
