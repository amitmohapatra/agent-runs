"""The migration chain on an empty database: up to head, down to base, up again. Payloads are
stored by reference only, so head has no inline payload column."""

from __future__ import annotations

import psycopg
import pytest
from alembic.config import Config
from psycopg import sql

from agent_runs.config.settings import reset_settings_cache
from agent_runs.store.database import ALEMBIC_INI
from alembic import command
from tests.conftest import ADMIN_URL, DB_URL, PG

EMPTY = "agent_runs_migrations"
INLINE = {
    "agent_runs": {"input", "output", "last_resolution", "checkpoint"},
    "run_resolutions": {"interrupt", "resolution"},
}


def _columns(conn: psycopg.Connection, table: str) -> dict[str, bool]:
    rows = conn.execute(
        "SELECT column_name, is_nullable = 'YES' FROM information_schema.columns "
        "WHERE table_name = %s",
        (table,),
    )
    return dict(rows.fetchall())


def _tables(conn: psycopg.Connection) -> set[str]:
    rows = conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    return {name for (name,) in rows}


@pytest.fixture
def empty(monkeypatch: pytest.MonkeyPatch):
    if not PG:
        pytest.skip(f"no PostgreSQL at {ADMIN_URL}")
    name = sql.Identifier(EMPTY)
    with psycopg.connect(ADMIN_URL, autocommit=True) as admin:
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(name))
        admin.execute(sql.SQL("CREATE DATABASE {}").format(name))
    monkeypatch.setenv("RUNS__DATABASE__URL", DB_URL.rsplit("/", 1)[0] + f"/{EMPTY}")
    reset_settings_cache()
    try:
        with psycopg.connect(ADMIN_URL.rsplit("/", 1)[0] + f"/{EMPTY}", autocommit=True) as conn:
            yield conn
    finally:
        monkeypatch.undo()
        reset_settings_cache()
        with psycopg.connect(ADMIN_URL, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(name))


def test_the_chain_applies_to_an_empty_database_and_back(empty: psycopg.Connection) -> None:
    config = Config(str(ALEMBIC_INI))
    command.upgrade(config, "head")
    for table, inline in INLINE.items():
        assert not inline & _columns(empty, table).keys()
    resolutions = _columns(empty, "run_resolutions")
    assert not resolutions["interrupt_ref"] and not resolutions["resolution_ref"]

    command.downgrade(config, "-1")
    command.upgrade(config, "head")
    command.downgrade(config, "base")
    assert _tables(empty) == {"alembic_version"}
    command.upgrade(config, "head")
    assert not INLINE["agent_runs"] & _columns(empty, "agent_runs").keys()
