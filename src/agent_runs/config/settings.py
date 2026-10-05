"""Deployment facts, and nothing else. One prefix, ``RUNS__``, with ``__`` between levels
(the Memory Service's shape). Every variable is documented in ``.env.example``; design
decisions are constants in ``constants.py``.
"""

from __future__ import annotations

import os
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Final, Self

from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEV = "dev"
TEST = "test"
#: Environments that run on one machine: the filesystem blob store is for them only.
LOCAL_ENVIRONMENTS: Final = frozenset({DEV, TEST})
#: The platform-wide name of the Memory Service's URL (the harness and the memory SDK read
#: it too); ``RUNS__MEMORY__URL`` wins when both are set.
MEMORY_URL_ENV: Final = "MEMORY_URL"
#: Uvicorn processes when ``RUNS__SERVICE__WORKERS`` is unset: one per CPU, within these.
MIN_WORKERS: Final = 1
MAX_WORKERS: Final = 8


def default_workers(cpus: int | None = None) -> int:
    """One worker per CPU this process may use, at least 1 and at most 8."""
    available = cpus if cpus is not None else (os.cpu_count() or MIN_WORKERS)
    return max(MIN_WORKERS, min(MAX_WORKERS, available))


class ServiceSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8090
    #: "dev" also accepts plain-http webhook URLs (a receiver on a laptop); "dev" and "test"
    #: are the only environments the filesystem blob store may run in.
    environment: str = DEV
    #: Uvicorn worker processes; unset, one per CPU (1 to 8). Each worker keeps its own key
    #: cache, rate-limit buckets and metrics.
    workers: int | None = Field(default=None, ge=1)
    #: How long a stopping worker lets requests in flight finish before closing them.
    graceful_shutdown_seconds: int = Field(default=20, ge=0)
    #: Request bodies of the JSON routes are refused (413) past this many bytes, counted as
    #: they arrive (chunked bodies too). Artifact uploads have their own bound.
    max_body_bytes: int = Field(default=4 * 1024 * 1024, ge=1024)
    #: The most a run's ``input`` (POST /v1/runs) or ``output`` (finish) may be, as compact
    #: JSON (413 past it): they live in the run's row, and every read of the run carries them.
    max_payload_bytes: int = Field(default=1024 * 1024, ge=1024)

    @property
    def is_dev(self) -> bool:
        return self.environment == DEV

    @property
    def worker_count(self) -> int:
        return self.workers if self.workers is not None else default_workers()


def _memory_url() -> str:
    return os.environ.get(MEMORY_URL_ENV) or "http://localhost:8080"


class MemorySettings(BaseModel):
    #: The Memory Service, the one key registry: every X-API-Key is introspected there.
    #: ``RUNS__MEMORY__URL``, else ``MEMORY_URL``, else a local one.
    url: str = Field(default_factory=_memory_url)


class DatabaseSettings(BaseModel):
    """The engine both processes use, with the protections a pooled connection needs: a
    pre-ping on checkout (a connection the server or a proxy closed while pooled is replaced,
    not handed to a request), a recycle window shorter than the idle timeouts in between, a
    bounded wait for a pooled connection, a connect timeout, and a statement timeout so one
    slow query cannot hold a connection (and a row lock) indefinitely."""

    url: str = "postgresql+psycopg://memory:memory@localhost:5432/agent_runs"
    pool_size: int = Field(default=10, ge=1)
    #: Connections opened past ``pool_size`` under a burst, closed when returned.
    max_overflow: int = Field(default=10, ge=0)
    #: Seconds a request waits for a pooled connection before a 503.
    pool_timeout_seconds: float = Field(default=5.0, gt=0)
    #: Seconds after which a pooled connection is replaced rather than reused.
    pool_recycle_seconds: int = Field(default=300, ge=1)
    pool_pre_ping: bool = True
    #: Seconds to open a connection to PostgreSQL.
    connect_timeout_seconds: int = Field(default=5, ge=1)
    #: Milliseconds after which PostgreSQL cancels a statement (a 503 here); 0 is no limit.
    statement_timeout_ms: int = Field(default=15_000, ge=0)


class BlobProvider(StrEnum):
    FILESYSTEM = "filesystem"
    GCS = "gcs"


class BlobSettings(BaseModel):
    """Where run artifacts' bytes live. ``filesystem`` writes under ``root`` (the API and the
    ticker must share it); ``gcs`` writes to ``bucket`` with the environment's credentials."""

    provider: BlobProvider = BlobProvider.FILESYSTEM
    root: Path = Path(".blob")
    bucket: str | None = None

    @model_validator(mode="after")
    def _gcs_names_a_bucket(self) -> Self:
        if self.provider is BlobProvider.GCS and not self.bucket:
            raise ValueError("RUNS__BLOB__BUCKET is required when RUNS__BLOB__PROVIDER=gcs")
        return self


class TickerSettings(BaseModel):
    #: The file the ticker touches every tick and ``python -m agent_runs.heartbeat`` reads.
    #: Unset: a per-process file in the temp directory (and no probe).
    heartbeat_file: Path | None = None
    #: The port the ticker serves Prometheus metrics on (``/metrics``); unset, none.
    metrics_port: int | None = Field(default=None, ge=1, le=65535)


class RunsSettings(BaseModel):
    #: The most working time any run may take, in seconds (time RUNNING, across attempts): a
    #: run's own ``timeout_seconds`` may only be shorter. Unset: no platform maximum.
    max_run_seconds: float | None = Field(default=None, gt=0)


class RateLimitSettings(BaseModel):
    """Each tenant's request budget on the ``/v1`` routes: a token bucket refilled at
    ``per_minute`` and holding at most ``burst`` requests. Kept in each worker process's
    memory, so the budget a tenant really gets is about this times the number of workers and
    replicas: a guard against a runaway client, not a quota. ``per_minute`` 0 turns it off."""

    per_minute: int = Field(default=3000, ge=0)
    burst: int = Field(default=500, ge=1)


class ObservabilitySettings(BaseModel):
    log_level: str = "INFO"
    log_json: bool = True


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RUNS__", env_nested_delimiter="__", env_file=".env", extra="ignore"
    )

    service: ServiceSettings = Field(default_factory=ServiceSettings)
    memory: MemorySettings = Field(default_factory=MemorySettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    blob: BlobSettings = Field(default_factory=BlobSettings)
    ticker: TickerSettings = Field(default_factory=TickerSettings)
    runs: RunsSettings = Field(default_factory=RunsSettings)
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)

    @model_validator(mode="after")
    def _filesystem_blobs_are_local(self) -> Self:
        """A directory on one machine is not where a deployment keeps artifacts: replicas
        would not share it, and a redeploy would lose it."""
        environment = self.service.environment
        if self.blob.provider is BlobProvider.FILESYSTEM and environment not in LOCAL_ENVIRONMENTS:
            raise ValueError(
                f"RUNS__BLOB__PROVIDER=filesystem is for dev and test only, not {environment!r}: "
                "use gcs (RUNS__BLOB__BUCKET)"
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()
