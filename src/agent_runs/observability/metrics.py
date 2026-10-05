"""Prometheus metrics: one registry per process, exposed on the API's ``/metrics`` and, when
``RUNS__TICKER__METRICS_PORT`` is set, on the ticker's own port.

One registry per process is what a scrape sees. The API runs ``RUNS__SERVICE__WORKERS``
uvicorn processes sharing one socket, so a scrape is answered by whichever worker accepts
it: each counter is one worker's share of the traffic. Scrape with one worker (or sum over
pods with one worker each) when the totals matter.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Final

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    start_http_server,
)
from prometheus_client.metrics_core import Metric
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import QueuePool

REGISTRY: Final = CollectorRegistry(auto_describe=True)
CONTENT_TYPE: Final = CONTENT_TYPE_LATEST

POOL_STATES: Final = ("size", "checked_out", "idle", "overflow")
_LATENCY_BUCKETS: Final = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

http_requests_total = Counter(
    "runs_http_requests_total",
    "HTTP requests by route template and status",
    ["method", "route", "status"],
    registry=REGISTRY,
)
http_request_seconds = Histogram(
    "runs_http_request_seconds",
    "HTTP request latency by route template",
    ["method", "route"],
    buckets=_LATENCY_BUCKETS,
    registry=REGISTRY,
)
claims_total = Counter(
    "runs_claims_total",
    "Queue claims by outcome: claimed (a run leased) or empty (nothing queued)",
    ["outcome"],
    registry=REGISTRY,
)
rate_limited_total = Counter(
    "runs_rate_limited_total",
    "Requests refused with 429 by the per-tenant rate limiter",
    registry=REGISTRY,
)
ticks_total = Counter(
    "runs_ticker_ticks_total",
    "Ticker passes by outcome: ok, failed (the database) or skipped (the breaker is open)",
    ["outcome"],
    registry=REGISTRY,
)
swept_total = Counter(
    "runs_ticker_swept_total",
    "Rows each ticker step handled: fired, timed_out, requeued, escalated, sent, purged",
    ["step"],
    registry=REGISTRY,
)
db_pool_connections = Gauge(
    "runs_db_pool_connections",
    "Database pool connections by state: size (configured), checked_out, idle, overflow",
    ["state"],
    registry=REGISTRY,
)


def observe_pool(engine: AsyncEngine | None) -> None:
    """Set the pool gauges from the engine's pool as it is now."""
    if engine is None:
        return
    for state in POOL_STATES:
        db_pool_connections.labels(state).set(_pool_reading(engine, state))


def render(engine: AsyncEngine | None = None) -> bytes:
    """The text exposition of every metric, with the pool gauges read fresh."""
    observe_pool(engine)
    return generate_latest(REGISTRY)


class _Scrape:
    """The process registry, with the pool gauges read at the moment of the scrape."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    def collect(self) -> Iterator[Metric]:
        observe_pool(self._engine)
        yield from REGISTRY.collect()


def serve(port: int, engine: AsyncEngine) -> None:
    """The ticker's metrics on ``port``, from a daemon thread."""
    scraped = CollectorRegistry()
    scraped.register(_Scrape(engine))
    start_http_server(port, registry=scraped)


def _pool_reading(engine: AsyncEngine, state: str) -> float:
    pool = engine.pool
    if not isinstance(pool, QueuePool):
        return 0.0
    readings = {
        "size": pool.size(),
        "checked_out": pool.checkedout(),
        "idle": pool.checkedin(),
        "overflow": max(0, pool.overflow()),
    }
    return float(readings[state])
