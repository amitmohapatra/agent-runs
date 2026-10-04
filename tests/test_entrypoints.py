"""The process edges: the two console scripts, the liveness probe run as a module, the schema
check both processes start with, and the logging they configure. Nothing here serves or
ticks for real; each test stops at the call that would."""

from __future__ import annotations

import asyncio
import importlib
import runpy
import time
import warnings
from collections.abc import Coroutine, Iterator
from pathlib import Path
from typing import Any

import pytest
import structlog
import uvicorn
from fastapi import FastAPI

from agent_runs import __main__ as api_main
from agent_runs import ticker as ticker_module
from agent_runs.config.settings import default_workers, get_settings
from agent_runs.observability.logging import configure_logging
from agent_runs.store import database
from agent_runs.store.database import connect
from tests.conftest import SETTINGS
from tests.test_ticker import _dead_ticker


@pytest.fixture
def fresh_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Settings read anew from this test's environment, with no ``.env`` in sight."""
    monkeypatch.chdir(tmp_path)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _run_as_main(module: str) -> None:
    """``python -m <module>``, in this process (the module is already imported, which
    runpy warns about)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        runpy.run_module(module, run_name="__main__")


# ------------------------------------------------------------------ agent-runs (the API)


def test_the_api_entry_point_serves_the_app_where_the_settings_say(
    fresh_settings, monkeypatch
) -> None:
    served: dict[str, Any] = {}

    def serve(app: str, **kwargs: Any) -> None:
        served.update(app=app, **kwargs)

    monkeypatch.setattr(uvicorn, "run", serve)
    monkeypatch.setenv("RUNS__SERVICE__HOST", "127.0.0.1")
    monkeypatch.setenv("RUNS__SERVICE__PORT", "9123")
    monkeypatch.setenv("RUNS__SERVICE__WORKERS", "3")
    monkeypatch.setenv("RUNS__SERVICE__GRACEFUL_SHUTDOWN_SECONDS", "7")
    api_main.main()
    assert (served["host"], served["port"], served["workers"]) == ("127.0.0.1", 9123, 3)
    assert served["timeout_graceful_shutdown"] == 7
    assert served["log_config"] is None, "the service configures its own logging"
    assert served["factory"] is True, "each worker builds its own app"
    module, _, factory = served["app"].partition(":")
    app: FastAPI = getattr(importlib.import_module(module), factory)()
    assert app.title == "agent-runs"
    assert {"/v1/runs", "/health/ready"} <= set(app.openapi()["paths"])

    served.clear()
    _run_as_main("agent_runs.__main__")
    assert served["port"] == 9123


def test_the_worker_count_defaults_to_the_cpus_within_bounds(fresh_settings, monkeypatch) -> None:
    assert [default_workers(n) for n in (0, 1, 4, 8, 64)] == [1, 1, 4, 8, 8]
    monkeypatch.setattr("os.cpu_count", lambda: None)
    assert default_workers() == 1
    monkeypatch.setattr("os.cpu_count", lambda: 6)
    assert get_settings().service.worker_count == 6


# ------------------------------------------------------------------ agent-runs-ticker


async def test_the_ticker_process_wires_the_loop_and_its_stop_signals(
    app, fresh_settings, monkeypatch, tmp_path
) -> None:
    """``app`` empties the tables, so the one real tick this makes finds nothing to do."""
    beat = tmp_path / "beat"
    monkeypatch.setenv("RUNS__DATABASE__URL", SETTINGS.database.url)
    monkeypatch.setenv("RUNS__TICKER__HEARTBEAT_FILE", str(beat))
    monkeypatch.setenv("RUNS__BLOB__ROOT", str(tmp_path / "blobs"))
    monkeypatch.setenv("RUNS__TICKER__METRICS_PORT", "9464")
    seen: dict[str, Any] = {}
    monkeypatch.setattr(
        ticker_module.metrics, "serve", lambda port, engine: seen.update(metrics_port=port)
    )

    async def one_pass(self: ticker_module.Ticker, stop: asyncio.Event) -> None:
        seen["heartbeat"] = self._heartbeat
        seen["report"] = await self.tick()
        seen["stop"] = stop

    monkeypatch.setattr(ticker_module.Ticker, "run_forever", one_pass)
    await ticker_module.run()
    assert seen["heartbeat"] == beat
    assert seen["metrics_port"] == 9464, "the ticker serves its metrics when asked to"
    assert seen["report"] == ticker_module.TickReport()
    assert not seen["stop"].is_set()
    loop = asyncio.get_running_loop()
    assert loop.remove_signal_handler(2) and loop.remove_signal_handler(15), (
        "SIGINT and SIGTERM stop the loop"
    )


def test_the_ticker_entry_point_runs_the_loop(monkeypatch) -> None:
    ran: list[str] = []

    async def run() -> None:
        ran.append("run")

    monkeypatch.setattr(ticker_module, "run", run)
    ticker_module.main()
    assert ran == ["run"]

    def no_loop(coro: Coroutine[Any, Any, None]) -> None:
        ran.append(coro.__qualname__)
        coro.close()

    monkeypatch.setattr(asyncio, "run", no_loop)
    _run_as_main("agent_runs.ticker")
    assert ran == ["run", "run"]


async def test_a_heartbeat_that_cannot_be_written_does_not_stop_the_ticker(tmp_path) -> None:
    ticker, _ = _dead_ticker(tmp_path)
    ticker._heartbeat = tmp_path  # a directory: writing it raises IsADirectoryError
    ticker.beat()
    assert tmp_path.is_dir()


# ------------------------------------------------------------------ the liveness probe


def test_the_probe_runs_as_a_module_and_exits_with_the_answer(
    fresh_settings, monkeypatch, tmp_path
) -> None:
    beat = tmp_path / "beat"
    beat.write_text(str(time.time()))
    monkeypatch.setenv("RUNS__TICKER__HEARTBEAT_FILE", str(beat))
    with pytest.raises(SystemExit) as alive:
        _run_as_main("agent_runs.heartbeat")
    assert alive.value.code == 0
    beat.write_text(str(time.time() - 3600))
    with pytest.raises(SystemExit) as stale:
        _run_as_main("agent_runs.heartbeat")
    assert stale.value.code == 1


# ------------------------------------------------------------------ the schema check


async def test_a_database_at_another_revision_is_refused(migrated, monkeypatch) -> None:
    engine = await connect(SETTINGS.database)
    await engine.dispose()
    monkeypatch.setattr(database, "_HEAD", "f00dfeedbeef")
    with pytest.raises(RuntimeError, match=r"needs f00dfeedbeef: run `alembic upgrade head`"):
        await connect(SETTINGS.database)


# ------------------------------------------------------------------ logging


def test_logging_is_json_in_a_deployment_and_readable_on_a_laptop() -> None:
    configure_logging(level="DEBUG", json_output=False)
    processors = structlog.get_config()["processors"]
    assert isinstance(processors[-1], structlog.dev.ConsoleRenderer)
    assert structlog.processors.format_exc_info not in processors

    configure_logging(level="INFO", json_output=True)
    processors = structlog.get_config()["processors"]
    assert isinstance(processors[-1], structlog.processors.JSONRenderer)
    assert structlog.processors.format_exc_info in processors
