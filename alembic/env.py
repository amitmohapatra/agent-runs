"""Alembic's entry point.

The URL comes from the service's own settings rather than alembic.ini, so there is exactly
one answer to "which database" and a migration cannot be run against a different one than
the service uses.
"""

from __future__ import annotations

import asyncio

from sqlalchemy.ext.asyncio import create_async_engine

from agent_runs.config.settings import get_settings
from agent_runs.store.tables import Base
from alembic import context

config = context.config
target_metadata = Base.metadata


def _url() -> str:
    return get_settings().database.url


def run_migrations_offline() -> None:
    context.configure(url=_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def _run(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_async_engine(_url(), poolclass=None)
    async with engine.connect() as connection:
        await connection.run_sync(_run)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
