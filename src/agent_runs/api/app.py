"""Application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import structlog
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from agent_runs.api.deps import Session
from agent_runs.api.routers import runs
from agent_runs.config.settings import Settings, get_settings
from agent_runs.observability.logging import configure_logging
from agent_runs.store.tables import Base  # noqa: F401 - imported for metadata
from agent_runs.webhooks import WebhookSender

log = structlog.get_logger(__name__)


#: Where alembic.ini lives relative to this module. Resolved once, at import: reading the
#: filesystem inside the startup coroutine blocks the event loop for no reason.
_ALEMBIC_INI = Path(__file__).resolve().parents[3] / "alembic.ini"


def _expected_revision() -> str | None:
    """The migration this build was written against."""
    return ScriptDirectory.from_config(Config(str(_ALEMBIC_INI))).get_current_head()


async def _require_current_schema(engine: AsyncEngine, settings: Settings) -> None:
    """Refuse to start against a schema this code cannot use."""
    head = _expected_revision()

    async with engine.begin() as conn:
        current = await conn.run_sync(
            lambda sync_conn: MigrationContext.configure(sync_conn).get_current_revision()
        )
    if current == head:
        return
    raise RuntimeError(
        f"database schema is at {current or 'nothing'}, this build needs {head}: "
        f"run `alembic upgrade head` (or `make migrate`) before starting the service"
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    # Before anything else logs: settings that nothing reads are not configuration.
    configure_logging(
        level=settings.observability.log_level, json_output=settings.observability.log_json
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(settings.database.url, echo=settings.database.echo)
        # Schema changes are migrations now, not create_all. create_all creates a missing
        # *table* and silently ignores a missing *column*, so adding webhook_url left every
        # already-deployed database answering UndefinedColumn on insert — a deploy-time
        # problem discovered at runtime, one request at a time.
        #
        # Startup verifies rather than migrates: two replicas racing to apply the same
        # migration is a worse failure than a clear refusal to start. `make migrate`, or
        # the one-shot `migrate` service in compose, is what moves the schema.
        await _require_current_schema(engine, settings)
        app.state.engine = engine
        app.state.sessions = async_sessionmaker(engine, expire_on_commit=False)
        app.state.webhooks = WebhookSender(settings.webhooks)
        log.info("agent_runs.started", environment=settings.service.environment)
        try:
            yield
        finally:
            # Deliveries in flight get their timeout to finish: a notification dropped by a
            # deploy is one a UI never hears about, and the run is already terminal.
            await app.state.webhooks.aclose()
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
