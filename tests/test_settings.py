"""Deployment facts: what the settings read, what they refuse, and the engine and the key
registry client they build."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import text

from agent_runs.api.errors import is_unavailable
from agent_runs.config.constants import FIRE_RETRY_BASE, FIRE_RETRY_CAP, MAX_CONSECUTIVE_FAILURES
from agent_runs.config.settings import (
    BlobSettings,
    DatabaseSettings,
    LimitsSettings,
    PoolPlan,
    SchedulesSettings,
    ServiceSettings,
    Settings,
    get_settings,
)
from agent_runs.domain.cadence import JITTER_MAX, MIN_INTERVAL
from agent_runs.keys import KeyRegistry
from agent_runs.store.database import connect, registry_engine
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
    assert get_settings().database.pool_plan(1) == PoolPlan(size=9, overflow=9, registry=2)


def test_the_limits_group_is_read_and_its_old_name_still_works_with_a_warning(
    fresh_settings, monkeypatch, caplog
) -> None:
    """``RUNS__LIMITS__*`` (renamed from ``RUNS__RUNS__*``): the new names are read; for one
    release an old one still is, with one warning line naming the new one; the new wins."""
    monkeypatch.setenv("RUNS__LIMITS__MAX_RUN_SECONDS", "30")
    monkeypatch.setenv("RUNS__LIMITS__CONCURRENCY_PER_KEY", "2")
    limits = get_settings().limits
    assert (limits.max_run_seconds, limits.concurrency_per_key) == (30, 2)
    assert "renamed" not in caplog.text
    monkeypatch.delenv("RUNS__LIMITS__MAX_RUN_SECONDS")
    for name, value in {
        "RUNS__RUNS__MAX_RUN_SECONDS": "40",
        "RUNS__RUNS__MAX_RUNNING_PER_TENANT": "5",
        "RUNS__RUNS__RETENTION_DAYS": "90",
        "RUNS__RUNS__CONCURRENCY_PER_KEY": "3",
    }.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    with caplog.at_level("WARNING"):
        limits = get_settings().limits
    assert (limits.max_run_seconds, limits.max_running_per_tenant) == (40, 5)
    assert limits.retention_days == 90 and limits.concurrency_per_key == 2, "the new name wins"
    said = [r.getMessage() for r in caplog.records if "renamed" in r.getMessage()]
    assert said == [
        "RUNS__RUNS__MAX_RUN_SECONDS is renamed RUNS__LIMITS__MAX_RUN_SECONDS; the old name "
        "stops working in the next release",
        "RUNS__RUNS__CONCURRENCY_PER_KEY is renamed RUNS__LIMITS__CONCURRENCY_PER_KEY (set "
        "too: the new name wins); the old name stops working in the next release",
        "RUNS__RUNS__MAX_RUNNING_PER_TENANT is renamed RUNS__LIMITS__MAX_RUNNING_PER_TENANT; "
        "the old name stops working in the next release",
        "RUNS__RUNS__RETENTION_DAYS is renamed RUNS__LIMITS__RETENTION_DAYS; the old name "
        "stops working in the next release",
    ]


def test_one_old_limit_name_moves_that_limit_only(fresh_settings, monkeypatch, caplog) -> None:
    """An old group with one name set moves that one value and warns once: the limits it
    does not name keep their defaults and draw no warning."""
    monkeypatch.setenv("RUNS__RUNS__RETENTION_DAYS", "30")
    with caplog.at_level("WARNING"):
        limits = get_settings().limits
    assert limits == LimitsSettings(retention_days=30)
    said = [r.getMessage() for r in caplog.records if "renamed" in r.getMessage()]
    assert said == [
        "RUNS__RUNS__RETENTION_DAYS is renamed RUNS__LIMITS__RETENTION_DAYS; the old name "
        "stops working in the next release"
    ]


@pytest.mark.parametrize(
    ("budget", "processes", "plan"),
    [
        (None, 1, PoolPlan(9, 9, registry=2)),
        (None, 8, PoolPlan(9, 9, registry=2)),
        (40, 4, PoolPlan(5, 4, registry=1)),
        (25, 2, PoolPlan(6, 5, registry=1)),
        (9, 1, PoolPlan(4, 4, registry=1)),
        (2, 8, PoolPlan(1, 0, registry=1)),
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
        assert (pool.size(), pool._max_overflow) == (3, 2)  # type: ignore[attr-defined]
        async with engine.connect() as conn:
            assert await conn.scalar(text("SHOW statement_timeout")) == "15s"
    finally:
        await engine.dispose()


async def test_the_registry_has_a_pool_of_its_own_with_the_same_protections(migrated) -> None:
    engine = registry_engine(SETTINGS.database.model_copy(update={"connection_budget": 12}), 2)
    try:
        pool = engine.pool
        assert (pool.timeout(), pool._recycle, pool._pre_ping) == (5.0, 300, True)  # type: ignore[attr-defined]
        assert (pool.size(), pool._max_overflow) == (1, 0)  # type: ignore[attr-defined]
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


def test_the_schedule_policy_defaults_to_what_was_hard_coded(fresh_settings) -> None:
    """``RUNS__SCHEDULES__*`` unset is exactly the policy before the group existed: the
    one-hour floor, the minute of jitter, three failed fires, 5 min doubling to 1 h. The two
    count limits are on: 50 schedules per tenant, 500 fires per tenant per UTC day."""
    schedules = get_settings().schedules
    assert schedules == SchedulesSettings()
    assert schedules.min_interval == MIN_INTERVAL == timedelta(hours=1)
    assert schedules.jitter == JITTER_MAX == timedelta(seconds=60)
    assert schedules.max_failed_fires == MAX_CONSECUTIVE_FAILURES == 3
    assert (schedules.retry_base, schedules.retry_cap) == (FIRE_RETRY_BASE, FIRE_RETRY_CAP)
    assert (schedules.retry_base, schedules.retry_cap) == (timedelta(minutes=5), timedelta(hours=1))
    assert (schedules.max_per_tenant, schedules.max_fires_per_day) == (50, 500)
    assert (schedules.per_tenant_limit, schedules.fires_per_day_limit) == (50, 500)


def test_zero_turns_either_count_limit_off(fresh_settings, monkeypatch) -> None:
    """``RUNS__SCHEDULES__MAX_PER_TENANT=0`` and ``MAX_FIRES_PER_DAY=0``: no limit."""
    monkeypatch.setenv("RUNS__SCHEDULES__MAX_PER_TENANT", "0")
    monkeypatch.setenv("RUNS__SCHEDULES__MAX_FIRES_PER_DAY", "0")
    schedules = get_settings().schedules
    assert (schedules.max_per_tenant, schedules.max_fires_per_day) == (0, 0)
    assert schedules.per_tenant_limit is None and schedules.fires_per_day_limit is None


def test_the_schedule_policy_is_read_from_its_group(fresh_settings, monkeypatch) -> None:
    for name, value in {
        "RUNS__SCHEDULES__MIN_INTERVAL_SECONDS": "900",
        "RUNS__SCHEDULES__JITTER_SECONDS": "0",
        "RUNS__SCHEDULES__MAX_FAILED_FIRES": "5",
        "RUNS__SCHEDULES__RETRY_BASE_SECONDS": "60",
        "RUNS__SCHEDULES__RETRY_CAP_SECONDS": "600",
        "RUNS__SCHEDULES__MAX_PER_TENANT": "100",
        "RUNS__SCHEDULES__MAX_FIRES_PER_DAY": "1000",
    }.items():
        monkeypatch.setenv(name, value)
    schedules = get_settings().schedules
    assert (schedules.min_interval, schedules.jitter) == (timedelta(minutes=15), timedelta(0))
    assert schedules.max_failed_fires == 5
    assert (schedules.retry_base, schedules.retry_cap) == (
        timedelta(minutes=1),
        timedelta(minutes=10),
    )
    assert (schedules.max_per_tenant, schedules.max_fires_per_day) == (100, 1000)


@pytest.mark.parametrize(
    "fields",
    [
        {"min_interval_seconds": 60},  # per-minute stays refused, whatever the floor
        {"min_interval_seconds": 900, "jitter_seconds": 900},  # a fire past the next tick
        {"jitter_seconds": -1},
        {"max_failed_fires": 0},
        {"retry_base_seconds": 0},
        {"retry_base_seconds": 600, "retry_cap_seconds": 300},
        {"max_per_tenant": -1},
        {"max_fires_per_day": -1},
    ],
)
def test_a_schedule_policy_that_cannot_hold_is_refused(fields) -> None:
    with pytest.raises(ValidationError):
        SchedulesSettings(**fields)
