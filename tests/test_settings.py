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
    BlobProvider,
    BlobSettings,
    DatabaseSettings,
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
def test_the_filesystem_blob_store_is_refused_outside_dev_and_test(environment) -> None:
    with pytest.raises(ValidationError, match="filesystem is for dev and test only"):
        Settings(service=ServiceSettings(environment=environment))
    deployed = Settings(
        service=ServiceSettings(environment=environment),
        blob=BlobSettings(provider=BlobProvider.GCS, bucket="artifacts"),
    )
    assert deployed.blob.provider is BlobProvider.GCS


@pytest.mark.parametrize("environment", ["dev", "test"])
def test_the_filesystem_blob_store_runs_on_a_laptop_and_in_the_suite(environment) -> None:
    assert Settings(service=ServiceSettings(environment=environment)).blob.provider is (
        BlobProvider.FILESYSTEM
    )


async def test_the_engine_carries_its_pool_and_timeout_protections(migrated) -> None:
    config = SETTINGS.database.model_copy(
        update={"pool_timeout_seconds": 2.5, "pool_recycle_seconds": 120, "max_overflow": 3}
    )
    engine = await connect(config)
    try:
        pool = engine.pool
        assert (pool.timeout(), pool._recycle, pool._pre_ping) == (2.5, 120, True)  # type: ignore[attr-defined]
        assert pool._max_overflow == 3  # type: ignore[attr-defined]
        async with engine.connect() as conn:
            assert await conn.scalar(text("SHOW statement_timeout")) == "15s"
    finally:
        await engine.dispose()


async def test_a_statement_past_its_timeout_is_cancelled_as_unavailable(migrated) -> None:
    engine = await connect(SETTINGS.database.model_copy(update={"statement_timeout_ms": 100}))
    try:
        async with engine.connect() as conn:
            with pytest.raises(Exception) as cancelled:
                await conn.execute(text("SELECT pg_sleep(2)"))
        assert is_unavailable(cancelled.value), "a 503, retryable, not a 500"
    finally:
        await engine.dispose()


def test_the_database_defaults_are_the_documented_ones() -> None:
    config = DatabaseSettings()
    assert (config.pool_size, config.max_overflow, config.pool_pre_ping) == (10, 10, True)
    assert (config.pool_timeout_seconds, config.pool_recycle_seconds) == (5.0, 300)
    assert (config.connect_timeout_seconds, config.statement_timeout_ms) == (5, 15_000)


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
