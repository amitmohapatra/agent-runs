"""The engines both processes (API and ticker) open, and the schema check they start with.

Startup verifies rather than migrates: two replicas racing to apply one migration is worse
than a clear refusal. The one-shot ``migrate`` service (``alembic upgrade head``) moves the
schema.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from agent_runs.config.constants import (
    DB_CONNECT_TIMEOUT_SECONDS,
    DB_POOL_RECYCLE_SECONDS,
    DB_POOL_TIMEOUT_SECONDS,
    DB_STATEMENT_TIMEOUT_MS,
    READY_TIMEOUT_SECONDS,
)
from agent_runs.config.settings import DatabaseSettings

#: Resolved at import: reading the filesystem inside a coroutine blocks the loop.
ALEMBIC_INI = Path(__file__).resolve().parents[3] / "alembic.ini"
_HEAD = ScriptDirectory.from_config(Config(str(ALEMBIC_INI))).get_current_head()


def _engine(config: DatabaseSettings, *, size: int, overflow: int) -> AsyncEngine:
    """An engine with the protections ``DatabaseSettings`` describes; it connects when first
    used."""
    return create_async_engine(
        config.url,
        pool_size=size,
        max_overflow=overflow,
        pool_timeout=DB_POOL_TIMEOUT_SECONDS,
        pool_recycle=DB_POOL_RECYCLE_SECONDS,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": DB_CONNECT_TIMEOUT_SECONDS,
            "options": f"-c statement_timeout={DB_STATEMENT_TIMEOUT_MS}",
        },
    )


async def connect(config: DatabaseSettings, processes: int = 1) -> AsyncEngine:
    """An engine on a database at the schema this build was written against, its pool this
    process's share of the budget (one of ``processes`` in the container), with the
    protections ``DatabaseSettings`` describes."""
    plan = config.pool_plan(processes)
    engine = _engine(config, size=plan.size, overflow=plan.overflow)
    async with engine.connect() as conn:
        current = await conn.run_sync(
            lambda sync: MigrationContext.configure(sync).get_current_revision()
        )
    if current != _HEAD:
        await engine.dispose()
        raise RuntimeError(
            f"database schema is at {current or 'nothing'}, this build needs {_HEAD}: "
            "run `alembic upgrade head` (make migrate) first"
        )
    return engine


def registry_engine(config: DatabaseSettings, processes: int = 1) -> AsyncEngine:
    """The engine the payload registry is written through (``blob.payloads.bind``): a small
    pool of its own, out of the same budget. A write registers its object in a short
    transaction while the session of the request that makes it may hold a connection and a
    row lock; from one pool, a burst of such requests would hold every connection while each
    waits for one more."""
    return _engine(config, size=config.pool_plan(processes).registry, overflow=0)


async def ping(engine: AsyncEngine, *, within: float = READY_TIMEOUT_SECONDS) -> bool:
    """Whether the database answers ``SELECT 1`` within ``within`` seconds: the readiness probe.
    Bounded as a whole (a pooled connection that turns out dead, a server that accepts the
    query and never answers), and it never raises: "not ready" is an answer."""
    try:
        async with asyncio.timeout(within):
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
    except (DBAPIError, PoolTimeout, TimeoutError, OSError):
        return False
    return True
