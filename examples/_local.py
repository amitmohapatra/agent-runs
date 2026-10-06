"""agent-runs in this process, for the examples: the real app, the real ticker, the real SDK.

``local_service()`` gives a :class:`Local` with:

* ``app``: the FastAPI app, started, over a fresh database of its own (``agent_runs_examples``
  on the local PostgreSQL, dropped and recreated, migrated to head: the schema a deployment
  has);
* ``runs(key)``: a ``trellis.runs.RunsClient`` that reaches the app through
  ``httpx.ASGITransport``, with no port and no network;
* ``ticker``: the real ``Ticker`` over the same database, delivering webhooks to
  ``receiver`` (an ``httpx.MockTransport``) instead of the internet; ``tick(at)`` runs one
  tick as if the clock read ``at``;
* a fake key registry: the Memory Service's ``GET /v1/keys/self``, answering the keys below.

What it needs: a PostgreSQL the admin URL reaches, the same one the test suite uses
(``RUNS_TEST_ADMIN_URL``, default ``postgresql://memory:memory@localhost:5432/postgres``;
``make up`` or CI's service provides one). It needs no Memory Service, no blob bucket and no
other network. The keys are placeholders, never printed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import psycopg
from alembic.config import Config
from fastapi import FastAPI
from psycopg import sql
from sqlalchemy import text
from trellis.runs import RunsClient

from agent_runs.api.app import create_app
from agent_runs.config.settings import (
    BlobSettings,
    DatabaseSettings,
    ObservabilitySettings,
    Settings,
    reset_settings_cache,
)
from agent_runs.keys import KeyRegistry
from agent_runs.observability.logging import configure_logging
from agent_runs.store.database import ALEMBIC_INI
from agent_runs.ticker import Ticker
from agent_runs.webhooks import WebhookSender
from alembic import command

ADMIN_URL = os.environ.get(
    "RUNS_TEST_ADMIN_URL", "postgresql://memory:memory@localhost:5432/postgres"
)
DB_NAME = os.environ.get("RUNS_EXAMPLES_DB", "agent_runs_examples")

#: What the key registry answers, by key. ``SERVICE_KEY`` is an application's key that may
#: act for anyone in acme; ``PRIYA_KEY`` is an approvals UI's key restricted to one person.
SERVICE_KEY = "service-key-placeholder"
PRIYA_KEY = "priya-key-placeholder"
KEYS: dict[str, dict[str, Any]] = {
    SERVICE_KEY: {
        "key_id": "key_app",
        "tenant_id": "acme",
        "principal": "user:ada",
        "role": "service",
        "may_act_as": ["*"],
    },
    PRIYA_KEY: {
        "key_id": "key_priya",
        "tenant_id": "acme",
        "principal": "key:key_priya",
        "role": "service",
        "may_act_as": ["user:priya"],
    },
}


def _registry() -> KeyRegistry:
    """The Memory Service's key introspection, answered here."""

    def introspect(request: httpx.Request) -> httpx.Response:
        info = KEYS.get(request.headers.get("x-api-key", ""))
        if request.url.path != "/v1/keys/self" or info is None:
            return httpx.Response(401, json={"detail": "unknown key"})
        return httpx.Response(200, json=info)

    client = httpx.AsyncClient(transport=httpx.MockTransport(introspect), base_url="http://memory")
    return KeyRegistry("http://memory", client=client)


@dataclass
class Receiver:
    """A webhook receiver: records each delivery; ``answer`` is the status it replies."""

    answer: int = 200
    received: list[httpx.Request] = field(default_factory=list)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.received.append(request)
        return httpx.Response(self.answer)

    def bodies(self) -> list[dict[str, Any]]:
        return [json.loads(r.content) for r in self.received]


@dataclass
class Local:
    app: FastAPI
    ticker: Ticker
    receiver: Receiver
    _clients: list[tuple[RunsClient, httpx.AsyncClient]] = field(default_factory=list)

    def runs(self, key: str = SERVICE_KEY) -> RunsClient:
        """An SDK client of the in-process app, authenticated with ``key``."""
        http = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://runs"
        )
        client = RunsClient("http://runs", api_key=key, http_client=http)
        self._clients.append((client, http))
        return client

    async def tick(self, at: datetime | None = None) -> Any:
        """One ticker pass, as if the clock read ``at`` (now when not given)."""
        return await self.ticker.tick(now=at)

    async def let_backoff_pass(self) -> None:
        """Make every queued run's retry backoff (``available_at``) pass now, straight on the
        row: what the clock would do in a few seconds, without the wait."""
        async with self.app.state.engine.begin() as conn:
            await conn.execute(
                text("UPDATE agent_runs SET available_at = NULL WHERE status = 'QUEUED'")
            )


def _fresh_database() -> str:
    """Drop and recreate the examples' own database; its SQLAlchemy URL."""
    database = sql.Identifier(DB_NAME)
    try:
        with psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=3) as conn:
            conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s",
                (DB_NAME,),
            )
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(database))
            conn.execute(sql.SQL("CREATE DATABASE {}").format(database))
    except psycopg.OperationalError as exc:
        raise SystemExit(
            f"the examples need a PostgreSQL at {ADMIN_URL} (RUNS_TEST_ADMIN_URL): {exc}"
        ) from exc
    server = ADMIN_URL.rsplit("/", 1)[0].replace("postgresql://", "postgresql+psycopg://", 1)
    return f"{server}/{DB_NAME}"


@asynccontextmanager
async def local_service() -> AsyncIterator[Local]:
    """agent-runs, migrated and started in this process, with its ticker."""
    configure_logging("ERROR", json_output=False)  # the examples print what they show
    logging.getLogger("alembic").setLevel(logging.WARNING)
    url = await asyncio.to_thread(_fresh_database)
    os.environ["RUNS__DATABASE__URL"] = url  # what the migrations read
    reset_settings_cache()
    # alembic's env.py runs its own event loop, so the migration runs in a thread of its own
    await asyncio.to_thread(command.upgrade, Config(str(ALEMBIC_INI)), "head")
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(
            database=DatabaseSettings(url=url),
            blob=BlobSettings(root=Path(tmp) / "blobs"),
            observability=ObservabilitySettings(log_level="ERROR", log_json=False),
        )
        app = create_app(settings)
        async with app.router.lifespan_context(app):
            await app.state.keys.aclose()
            app.state.keys = _registry()
            receiver = Receiver()
            hooks = WebhookSender(
                allow_http=True,
                allow_private=True,
                client=httpx.AsyncClient(transport=httpx.MockTransport(receiver.handle)),
            )
            ticker = Ticker(
                app.state.sessions, hooks, app.state.blobs, heartbeat_path=Path(tmp) / "beat"
            )
            local = Local(app, ticker, receiver)
            try:
                yield local
            finally:
                for client, http in local._clients:
                    await client.aclose()
                    await http.aclose()
                await hooks.aclose()
