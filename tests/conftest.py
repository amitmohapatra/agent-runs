"""Fixtures. A real PostgreSQL when one is reachable, skipped with a reason when not —
never silently passed against a stand-in that cannot reproduce a race."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from agent_runs.api.app import create_app
from agent_runs.config.settings import Credential, DatabaseSettings, ServiceSettings, Settings
from alembic import command

ADMIN_URL = os.environ.get(
    "RUNS_TEST_ADMIN_URL", "postgresql://memory:memory@localhost:5432/postgres"
)
DB_NAME = os.environ.get("RUNS_TEST_DB", "agent_runs_tests")
DB_URL = f"postgresql+psycopg://memory:memory@localhost:5432/{DB_NAME}"

H = {"X-Api-Key": "dev-key", "X-Tenant-Id": "acme"}


def _pg_ready() -> bool:
    import psycopg

    try:
        with psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=2) as conn:
            # Dropped and recreated, not reused. The schema comes from migrations, and a
            # database left over from an older build carries its tables *and* no alembic
            # version — so the first migration tries to create a table that is already
            # there and every test errors on a DuplicateTable that has nothing to do with
            # the code under test.
            conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s",
                (DB_NAME,),
            )
            conn.execute(f'DROP DATABASE IF EXISTS "{DB_NAME}"')
            conn.execute(f'CREATE DATABASE "{DB_NAME}"')
        return True
    except Exception:
        return False


PG = _pg_ready()


#: Resolved at import: touching the filesystem inside an async fixture blocks the loop.
ROOT = Path(__file__).resolve().parents[1]


async def _migrate(url: str) -> None:
    """Bring the test database to head. Runs in a thread: alembic is synchronous."""

    def upgrade() -> None:
        config = Config(str(ROOT / "alembic.ini"))
        config.set_main_option("script_location", str(ROOT / "alembic"))
        os.environ["RUNS__DATABASE__URL"] = url
        from agent_runs.config.settings import reset_settings_cache

        reset_settings_cache()
        command.upgrade(config, "head")

    await asyncio.to_thread(upgrade)


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    if not PG:
        pytest.skip(f"no PostgreSQL at {ADMIN_URL}")
    # Two credentials, each bound to its own tenant. A single key with the tenant taken
    # from a header is not authentication: the isolation tests below would pass by
    # impersonation rather than by isolation, which is how they passed before.
    settings = Settings(
        database=DatabaseSettings(url=DB_URL),
        service=ServiceSettings(
            api_keys={
                "dev-key": Credential(tenant_id="acme", name="tests"),
                "other-key": Credential(tenant_id="globex", name="tests: second tenant"),
            }
        ),
    )
    # The schema comes from migrations now, exactly as it does in production: a fixture
    # that built tables with create_all would test a schema no deployment ever has.
    await _migrate(DB_URL)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        # Each test starts from an empty table: a state-machine test that inherits another
        # test's paused run passes for the wrong reason.
        async with app.state.engine.begin() as conn:
            await conn.execute(text("TRUNCATE agent_runs"))
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://runs", headers=H) as c:
            yield c


def started(**over):
    body = {"tenant_id": "acme", "agent_id": "triage", **over}
    return body


@pytest.fixture
async def other_tenant(client: AsyncClient) -> AsyncIterator[AsyncClient]:
    """A second tenant against the same app.

    A whole client rather than per-request headers: httpx merges request headers *into* the
    client's defaults, so an override arrives as a second spelling of the same header and
    the server reads whichever comes first. Tenant isolation is the thing under test here,
    so the test must not depend on that resolution order.
    """
    async with AsyncClient(
        transport=ASGITransport(app=client._transport.app),
        base_url="http://runs",
        headers={"X-Api-Key": "other-key", "X-Tenant-Id": "globex"},
    ) as c:
        yield c
