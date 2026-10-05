"""Fixtures. A real PostgreSQL (the local one, in a database of its own that this suite
drops and recreates), skipped with a reason when there is none: a race or a ``SKIP LOCKED``
claim cannot be reproduced against a stand-in."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
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
from agent_runs.blob.filesystem import FilesystemBlobStore
from agent_runs.config.settings import DatabaseSettings, Settings, reset_settings_cache
from agent_runs.keys import KeyRegistry
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

#: What the fake key registry knows, as ``GET /v1/keys/self`` answers it. Each a different
#: shape: a tenant service key that may act as anyone in acme, the same for globex (a
#: different secret: one key for both tenants would let isolation tests pass by
#: impersonation), an ordinary acme user, a platform key with no tenant, an approvals UI's
#: key restricted to one person (priya), and an acme admin key restricted to nobody in
#: particular (its role, not its list, is what lets it answer any run).
KEYS: dict[str, dict[str, Any]] = {
    "dev-key": {
        "key_id": "key_dev",
        "tenant_id": "acme",
        "principal": "user_ada",
        "role": "service",
        "may_act_as": ["*"],
    },
    "other-key": {
        "key_id": "key_other",
        "tenant_id": "globex",
        "principal": "user_ada",
        "role": "service",
        "may_act_as": ["*"],
    },
    "narrow-key": {
        "key_id": "key_narrow",
        "tenant_id": "acme",
        "principal": "user_bob",
        "role": "service",
        "may_act_as": [],
    },
    "platform-key": {
        "key_id": "key_platform",
        "tenant_id": None,
        "principal": "svc_worker",
        "role": "platform",
        "may_act_as": ["*"],
    },
    "priya-key": {
        "key_id": "key_priya",
        "tenant_id": "acme",
        "principal": "key:key_priya",
        "role": "service",
        "may_act_as": ["user:priya"],
    },
    "admin-key": {
        "key_id": "key_admin",
        "tenant_id": "acme",
        "principal": "key:key_admin",
        "role": "admin",
        "may_act_as": [],
    },
}
#: A key the registry knows but refuses (its tenant is suspended).
SUSPENDED_KEY = "suspended-key"
SETTINGS = Settings(database=DatabaseSettings(url=DB_URL))


class FakeMemory:
    """The Memory Service's key registry, as a tiny ASGI app: ``GET /v1/keys/self``
    answers the ``X-Api-Key`` it is sent, and counts what it was asked."""

    def __init__(self, keys: dict[str, dict[str, Any]] | None = None) -> None:
        self.keys = dict(KEYS if keys is None else keys)
        self.asked: list[str] = []
        self.status: int | None = None  # force an answer (a registry that is down)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        import json

        headers = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
        key = headers.get("x-api-key", "")
        self.asked.append(key)
        if scope["path"] != "/v1/keys/self" or scope["method"] != "GET":
            status, body = 404, {"detail": "not found"}
        elif self.status is not None:
            status, body = self.status, {"detail": "forced"}
        elif key == SUSPENDED_KEY:
            status, body = 403, {"detail": "tenant is suspended"}
        elif key in self.keys:
            status, body = 200, self.keys[key]
        else:
            status, body = 401, {"detail": "unknown key"}
        payload = json.dumps(body).encode()
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": payload})

    def registry(self, **kwargs: Any) -> KeyRegistry:
        client = httpx.AsyncClient(transport=ASGITransport(app=self), base_url="http://memory")
        return KeyRegistry("http://memory", client=client, **kwargs)


@pytest.fixture
def memory() -> FakeMemory:
    return FakeMemory()


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
def blobs(tmp_path: Any) -> FilesystemBlobStore:
    """The filesystem blob store, in this test's own directory."""
    return FilesystemBlobStore(tmp_path / "blobs")


@asynccontextmanager
async def serving(
    settings: Settings, memory: FakeMemory, blobs: FilesystemBlobStore
) -> AsyncIterator[Any]:
    """The app on ``settings``, started, with the fake registry and this test's blob store,
    over empty tables."""
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        await application.state.keys.aclose()
        application.state.keys = memory.registry()
        application.state.blobs = blobs
        async with application.state.engine.begin() as conn:
            await conn.execute(
                text(
                    "TRUNCATE run_resolutions, run_artifacts, agent_runs, agent_schedules, "
                    "webhooks, webhook_deliveries, rate_limit_buckets"
                )
            )
        yield application


@pytest.fixture
async def app(migrated: None, memory: FakeMemory, blobs: FilesystemBlobStore) -> AsyncIterator[Any]:
    async with serving(SETTINGS, memory, blobs) as application:
        yield application


def sender(receiver: Receiver, *, allow_http: bool = True) -> WebhookSender:
    """The ticker's sender, delivering to ``receiver``."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(receiver.handle))
    return WebhookSender(client=client, allow_http=allow_http, allow_private=True)


def client_of(app: Any, key: str = "dev-key", **headers: str) -> AsyncClient:
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://runs",
        headers={"X-Api-Key": key, **headers},
    )


@pytest.fixture
async def client(app: Any) -> AsyncIterator[AsyncClient]:
    async with client_of(app, "dev-key") as c:
        yield c


@pytest.fixture
async def other_tenant(app: Any) -> AsyncIterator[AsyncClient]:
    async with client_of(app, "other-key") as c:
        yield c


@pytest.fixture
async def narrow(app: Any) -> AsyncIterator[AsyncClient]:
    """An ordinary acme user: may act only as themself."""
    async with client_of(app, "narrow-key") as c:
        yield c


@pytest.fixture
async def platform(app: Any) -> AsyncIterator[AsyncClient]:
    """A platform key acting for acme."""
    async with client_of(app, "platform-key", **{"X-Trellis-Tenant": "acme"}) as c:
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


def pause(run_id: str, checkpoint: dict[str, Any] | None = None, **over: Any) -> dict[str, Any]:
    """A pause body: the interrupt, and the executor's checkpoint when there is one."""
    body: dict[str, Any] = {"interrupt": interrupt(run_id, **over)}
    if checkpoint is not None:
        body["checkpoint"] = checkpoint
    return body


async def paused(client: AsyncClient, **over: Any) -> dict[str, Any]:
    """A run that is waiting on a person."""
    run = (await client.post("/v1/runs", json=started(**over))).json()
    response = await client.post(f"/v1/runs/{run['run_id']}/pause", json=pause(run["run_id"]))
    assert response.status_code == 200, response.text
    return response.json()


def at(minutes: float) -> datetime:
    """An instant relative to now, for a sweep that should (or should not) find something."""
    return datetime.now(UTC) + timedelta(minutes=minutes)


# ------------------------------------------------------------------------------ schedules

_names = count(1)


def scheduled(**over: Any) -> dict[str, Any]:
    """A create body. Each call is a different schedule (its input differs), because a
    create with the identity of an existing schedule returns that one."""
    n = next(_names)
    return {
        "tenant_id": "acme",
        "agent_id": "briefing",
        "name": f"schedule-{n}",
        "input": {"n": n},
        "cadence": "daily",
        "timezone": "UTC",
        "on_behalf_of": "user_ada",
        **over,
    }


@pytest.fixture
async def ticker(app: Any, receiver: Receiver, tmp_path: Any) -> AsyncIterator[Ticker]:
    """The real ticker over the app's database, sending webhooks to ``receiver``."""
    hooks = sender(receiver)
    yield Ticker(app.state.sessions, hooks, app.state.blobs, heartbeat_path=tmp_path / "beat")
    await hooks.aclose()


async def backoff_passed(app: Any) -> None:
    """Let every queued run's retry backoff (``available_at``) pass, straight on the row: what
    the clock would do, without the wait."""
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE agent_runs SET available_at = NULL WHERE status = 'QUEUED'")
        )


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
