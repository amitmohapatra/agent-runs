"""The migrations are the schema; the mappings must describe exactly what they build."""

from __future__ import annotations

from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext

from agent_runs.store.tables import Base


async def test_the_migrations_build_the_schema_the_code_maps(app) -> None:
    async with app.state.engine.connect() as conn:
        diff = await conn.run_sync(
            lambda sync: compare_metadata(MigrationContext.configure(sync), Base.metadata)
        )
    assert diff == []
