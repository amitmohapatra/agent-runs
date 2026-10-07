"""Deployment facts: what the settings read, what they refuse, and the engine and the key
registry client they build."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import text

from agent_runs.api.errors import is_unavailable
from agent_runs.config.settings import (
    BlobSettings,
    DatabaseSettings,
    PoolPlan,
    ServiceSettings,
    Settings,
    get_settings,
)
from agent_runs.keys import KeyRegistry
from agent_runs.store.database import connect
from tests.conftest import SETTINGS


@pytest.fixture
def fresh_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MEMORY_URL", raising=False)
    monkeypatch.delenv("RUNS__MEMORY__URL", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_the_memory_url_falls_back_to_the_platform_name(fresh_settings, monkeypatch) -> None:
    assert get_settings().memory.url == "http://localhost:8080"
    monkeypatch.setenv("MEMORY_URL", "http://memory.internal:8080")
    get_settings.cache_clear()
    assert get_settings().memory.url == "http://memory.internal:8080"
    monkeypatch.setenv("RUNS__MEMORY__URL", "http://runs-own-memory:8080")
    get_settings.cache_clear()
    assert get_settings().memory.url == "http://runs-own-memory:8080", "the service's own wins"


@pytest.mark.parametrize("environment", ["staging", "prod", "production"])
def test_a_deployment_needs_a_bucket(environment) -> None:
    with pytest.raises(ValidationError, match="RUNS__BLOB__BUCKET is required"):
        Settings(service=ServiceSettings(environment=environment))
    deployed = Settings(
        service=ServiceSettings(environment=environment), blob=BlobSettings(bucket="artifacts")
    )
    assert deployed.blob.bucket == "artifacts"


@pytest.mark.parametrize("environment", ["dev", "test"])
def test_the_filesystem_blob_store_runs_on_a_laptop_and_in_the_suite(environment) -> None:
    assert Settings(service=ServiceSettings(environment=environment)).blob.bucket is None


def test_settings_that_were_removed_are_ignored_when_left_set(fresh_settings, monkeypatch) -> None:
    for name, value in {
        "RUNS__BLOB__PROVIDER": "filesystem",
        "RUNS__DATABASE__POOL_SIZE": "3",
        "RUNS__DATABASE__POOL_PRE_PING": "false",
        "RUNS__DATABASE__STATEMENT_TIMEOUT_MS": "100",
    }.items():
        monkeypatch.setenv(name, value)
    assert get_settings().database.pool_plan(1) == PoolPlan(size=10, overflow=10)


@pytest.mark.parametrize(
    ("budget", "processes", "plan"),
    [
        (None, 1, PoolPlan(10, 10)),
        (None, 8, PoolPlan(10, 10)),
        (40, 4, PoolPlan(5, 5)),
        (25, 2, PoolPlan(6, 6)),
        (9, 1, PoolPlan(5, 4)),
        (2, 8, PoolPlan(1, 1)),
    ],
)
def test_the_connection_budget_is_split_between_the_processes(budget, processes, plan) -> None:
    assert DatabaseSettings(connection_budget=budget).pool_plan(processes) == plan


def test_a_budget_below_one_pool_is_refused() -> None:
    with pytest.raises(ValidationError):
        DatabaseSettings(connection_budget=1)


async def test_the_engine_carries_its_pool_and_timeout_protections(migrated) -> None:
    engine = await connect(SETTINGS.database.model_copy(update={"connection_budget": 12}), 2)
    try:
        pool = engine.pool
        assert (pool.timeout(), pool._recycle, pool._pre_ping) == (5.0, 300, True)  # type: ignore[attr-defined]
        assert (pool.size(), pool._max_overflow) == (3, 3)  # type: ignore[attr-defined]
        async with engine.connect() as conn:
            assert await conn.scalar(text("SHOW statement_timeout")) == "15s"
    finally:
        await engine.dispose()


async def test_a_statement_past_its_timeout_is_cancelled_as_unavailable(
    migrated, monkeypatch
) -> None:
    monkeypatch.setattr("agent_runs.store.database.DB_STATEMENT_TIMEOUT_MS", 100)
    engine = await connect(SETTINGS.database)
    try:
        async with engine.connect() as conn:
            with pytest.raises(Exception) as cancelled:
                await conn.execute(text("SELECT pg_sleep(2)"))
        assert is_unavailable(cancelled.value), "a 503, retryable, not a 500"
    finally:
        await engine.dispose()


async def test_the_registry_client_bounds_its_waits_and_keeps_connections_alive() -> None:
    registry = KeyRegistry("http://memory")
    try:
        client = registry._client
        assert client.timeout == httpx.Timeout(3.0, connect=2.0)
        pool = client._transport._pool  # type: ignore[attr-defined]
        assert (pool._max_connections, pool._max_keepalive_connections) == (100, 20)
        assert pool._keepalive_expiry == 30.0
    finally:
        await registry.aclose()
