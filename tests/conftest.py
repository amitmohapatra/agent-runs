"""Fixtures. A real PostgreSQL when one is reachable, skipped with a reason when not —
never silently passed against a stand-in that cannot reproduce a race."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from agent_runs.api.app import create_app
from agent_runs.config.settings import Credential, DatabaseSettings, ServiceSettings, Settings

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
            exists = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,)
            ).fetchone()
            if not exists:
                conn.execute(f'CREATE DATABASE "{DB_NAME}"')
        return True
    except Exception:
        return False


PG = _pg_ready()


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
