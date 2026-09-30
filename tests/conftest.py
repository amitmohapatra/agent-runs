"""Fixtures. A real PostgreSQL (the local one, in a database of its own that this suite
drops and recreates), skipped with a reason when there is none: a race or a ``SKIP LOCKED``
claim cannot be reproduced against a stand-in."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from itertools import count
from typing import Any

import httpx
import pytest
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.exc import DataError, OperationalError
from trellis.contracts.runs import Interrupt, InterruptDecision, InterruptResolution

from agent_runs.api.app import create_app
from agent_runs.config.settings import (
    Credential,
    DatabaseSettings,
    ServiceSettings,
    Settings,
    reset_settings_cache,
)
from agent_runs.store.database import ALEMBIC_INI
from agent_runs.store.runs import RunStore
from agent_runs.ticker import Ticker
from agent_runs.webhooks import WebhookSender
from alembic import command

ADMIN_URL = os.environ.get(
    "RUNS_TEST_ADMIN_URL", "postgresql://memory:memory@localhost:5432/postgres"
)
DB_NAME = os.environ.get("RUNS_TEST_DB", "agent_runs_tests")
DB_URL = f"postgresql+psycopg://memory:memory@localhost:5432/{DB_NAME}"

#: Each credential a different shape: a tenant service key that may act as anyone in acme,
#: the same for globex (a different secret: one key for both tenants would let isolation
#: tests pass by impersonation), an ordinary acme user, and a platform key with no tenant.
CREDENTIALS = {
    "dev-key": Credential(tenant_id="acme", principal="user_ada", may_act_as=("*",)),
    "other-key": Credential(tenant_id="globex", principal="user_ada", may_act_as=("*",)),
    "narrow-key": Credential(tenant_id="acme", principal="user_bob"),
    "platform-key": Credential(tenant_id=None, principal="svc_worker", may_act_as=("*",)),
}
SETTINGS = Settings(
    database=DatabaseSettings(url=DB_URL), service=ServiceSettings(api_keys=CREDENTIALS)
)


def _fresh_database() -> bool:
    import psycopg
    from psycopg import sql

    database = sql.Identifier(DB_NAME)
    try:
        with psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=2) as conn:
            conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s",
                (DB_NAME,),
            )
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(database))
            conn.execute(sql.SQL("CREATE DATABASE {}").format(database))
    except psycopg.OperationalError:
        return False
    return True


PG = _fresh_database()


@pytest.fixture(scope="session")
def migrated() -> Iterator[None]:
    """The schema comes from the migrations, exactly as in a deployment."""
    if not PG:
        pytest.skip(f"no PostgreSQL at {ADMIN_URL}")
    os.environ["RUNS__DATABASE__URL"] = DB_URL
    reset_settings_cache()
    command.upgrade(Config(str(ALEMBIC_INI)), "head")
    yield


class Receiver:
    """A webhook receiver behind ``httpx.MockTransport``: records what it is sent."""

    def __init__(self) -> None:
        self.received: list[httpx.Request] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.received.append(request)
        return httpx.Response(200)

    def events(self) -> list[dict[str, Any]]:
        import json

        return [json.loads(r.content) for r in self.received]


@pytest.fixture
def receiver() -> Receiver:
    return Receiver()


@pytest.fixture
async def app(migrated: None, receiver: Receiver) -> AsyncIterator[Any]:
    application = create_app(SETTINGS)
    async with application.router.lifespan_context(application):
        async with application.state.engine.begin() as conn:
            await conn.execute(text("TRUNCATE agent_runs, agent_schedules"))
        await application.state.webhooks.aclose()
        application.state.webhooks = sender(receiver, secret="s3cret")
        yield application


def sender(receiver: Receiver, **kwargs: Any) -> WebhookSender:
    """The service's sender, delivering to ``receiver`` without waiting between retries."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(receiver.handle))
    options = {"secret": "", "allow_http": True, "retry_base": timedelta(0), **kwargs}
    return WebhookSender(client=client, **options)


def _client(app: Any, key: str, **headers: str) -> AsyncClient:
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://runs",
        headers={"X-Api-Key": key, **headers},
    )


@pytest.fixture
async def client(app: Any) -> AsyncIterator[AsyncClient]:
    async with _client(app, "dev-key") as c:
        yield c


@pytest.fixture
async def other_tenant(app: Any) -> AsyncIterator[AsyncClient]:
    async with _client(app, "other-key") as c:
        yield c


@pytest.fixture
async def narrow(app: Any) -> AsyncIterator[AsyncClient]:
    """An ordinary acme user: may act only as themself."""
    async with _client(app, "narrow-key") as c:
        yield c


@pytest.fixture
async def platform(app: Any) -> AsyncIterator[AsyncClient]:
    """A platform key acting for acme."""
    async with _client(app, "platform-key", **{"X-Trellis-Tenant": "acme"}) as c:
        yield c


# ------------------------------------------------------------------------------ builders


def started(**over: Any) -> dict[str, Any]:
    return {"tenant_id": "acme", "agent_id": "triage", **over}


def interrupt(run_id: str, **over: Any) -> dict[str, Any]:
    return Interrupt(tenant_id="acme", run_id=run_id, question="Approve?", **over).awaiting()


def resolution(run: dict[str, Any], decision: str = "APPROVE", **over: Any) -> dict[str, Any]:
    return InterruptResolution(
        interrupt_id=run["awaiting"]["interrupt_id"],
        run_id=run["run_id"],
        decision=InterruptDecision(decision),
        **over,
    ).model_dump(mode="json")


async def paused(client: AsyncClient, **over: Any) -> dict[str, Any]:
    """A run that is waiting on a person."""
    run = (await client.post("/v1/runs", json=started(**over))).json()
    response = await client.post(f"/v1/runs/{run['run_id']}/pause", json=interrupt(run["run_id"]))
    assert response.status_code == 200, response.text
    return response.json()


def at(minutes: float) -> datetime:
    """An instant relative to now, for a sweep that should (or should not) find something."""
    return datetime.now(UTC) + timedelta(minutes=minutes)


# ------------------------------------------------------------------------------ schedules

_names = count(1)


def scheduled(**over: Any) -> dict[str, Any]:
    """A create body; the name is unique per call because (tenant, name) is unique."""
    return {
        "tenant_id": "acme",
        "agent_id": "briefing",
        "name": f"schedule-{next(_names)}",
        "cadence": "daily",
        "timezone": "UTC",
        "on_behalf_of": "user_ada",
        **over,
    }


@pytest.fixture
def ticker(app: Any, tmp_path: Any) -> Ticker:
    """The real ticker over the app's database and webhook sender."""
    return Ticker(app.state.sessions, app.state.webhooks, heartbeat=tmp_path / "beat")


async def queued_runs(app: Any) -> list[dict[str, Any]]:
    """Every queued run, as rows: what the fires produced."""
    async with app.state.engine.connect() as conn:
        rows = await conn.execute(
            text("SELECT * FROM agent_runs WHERE status = 'QUEUED' ORDER BY created_at")
        )
        return [dict(row._mapping) for row in rows]


class BrokenQueue:
    """Makes queueing a run fail the way the database would: ``retryable`` is an
    OperationalError (a connection lost, a lock timeout), else a DataError (a row the
    database will never take)."""

    def __init__(self, monkeypatch: Any) -> None:
        self._monkeypatch = monkeypatch
        self.calls = 0

    def fail(self, *, retryable: bool = True) -> None:
        error = OperationalError if retryable else DataError

        async def refuse(*args: Any, **kwargs: Any) -> Any:
            self.calls += 1
            raise error("INSERT INTO agent_runs", {}, Exception("the database refused"))

        self._monkeypatch.setattr(RunStore, "start", refuse)

    def heal(self) -> None:
        self._monkeypatch.undo()


@pytest.fixture
def broken(monkeypatch: Any) -> BrokenQueue:
    return BrokenQueue(monkeypatch)


async def arm(app: Any, schedule_id: str, due: datetime) -> datetime:
    """Put a schedule's clock where a test needs it, straight on the row: no API moves
    next_fire_at backwards, which is a property several tests exist to keep."""
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE agent_schedules SET next_fire_at = :due WHERE schedule_id = :sid"),
            {"due": due, "sid": schedule_id},
        )
    return due
