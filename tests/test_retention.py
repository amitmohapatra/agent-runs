"""Retention: with RUNS__RUNS__RETENTION_DAYS set, the ticker deletes the runs that ended
before it, with their resolutions and events, after their artifacts; unset, nothing goes."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from sqlalchemy import text
from trellis.contracts.ids import now
from trellis.contracts.runs import RunEvent, RunEventType

from agent_runs.config.constants import ARTIFACT_RETENTION
from agent_runs.config.settings import RunsSettings
from agent_runs.ticker import Ticker
from tests.conftest import pause, resolution, sender, started

_KEPT = timedelta(days=30)


async def _ended(client, *, answered: bool = True) -> str:
    """A run that paused, was answered, logged an event and succeeded."""
    run_id = (await client.post("/v1/runs", json=started())).json()["run_id"]
    if answered:
        paused = (await client.post(f"/v1/runs/{run_id}/pause", json=pause(run_id))).json()
        await client.post(f"/v1/runs/{run_id}/resume", json=resolution(paused))
    event = RunEvent(type=RunEventType.STEP_STARTED, tenant_id="acme", run_id=run_id, sequence=0)
    await client.post(f"/v1/runs/{run_id}/events", json={"events": [event.model_dump(mode="json")]})
    await client.post(f"/v1/runs/{run_id}/finish", json={"status": "SUCCESS"})
    return run_id


async def _rows(app: Any, table: str, run_id: str) -> int:
    async with app.state.engine.connect() as conn:
        found = await conn.execute(
            text(f"SELECT count(*) FROM {table} WHERE run_id = :r"),
            {"r": run_id},
        )
        return int(found.scalar_one())


def _ticker(app: Any, receiver: Any, tmp_path: Any, **over: Any) -> Ticker:
    return Ticker(
        app.state.sessions,
        sender(receiver),
        app.state.blobs,
        heartbeat_path=tmp_path / "beat",
        **over,
    )


async def test_runs_ended_before_the_retention_go_with_their_records(
    app, client, receiver, tmp_path
) -> None:
    old = await _ended(client)
    running = (await client.post("/v1/runs", json=started())).json()["run_id"]
    ticker = _ticker(app, receiver, tmp_path, run_retention=_KEPT)
    assert (await ticker.tick(now=now() + _KEPT - timedelta(hours=1))).runs_purged == 0
    assert (await ticker.tick(now=now() + _KEPT + timedelta(hours=1))).runs_purged == 1
    for table in ("agent_runs", "run_resolutions", "run_events"):
        assert await _rows(app, table, old) == 0, table
    assert (await client.get(f"/v1/runs/{old}")).status_code == 404
    assert (await client.get(f"/v1/runs/{running}")).status_code == 200, "a run not ended stays"


async def test_without_a_retention_every_run_is_kept(app, client, receiver, tmp_path) -> None:
    old = await _ended(client, answered=False)
    ticker = _ticker(app, receiver, tmp_path)
    assert (await ticker.tick(now=now() + timedelta(days=3650))).runs_purged == 0
    assert await _rows(app, "agent_runs", old) == 1


async def test_a_run_waits_for_its_artifacts_to_go_first(app, client, receiver, tmp_path) -> None:
    run_id = (await client.post("/v1/runs", json=started())).json()["run_id"]
    uploaded = await client.post(
        f"/v1/runs/{run_id}/artifacts", content=b"{}", headers={"Content-Type": "application/json"}
    )
    assert uploaded.status_code == 201, uploaded.text
    await client.post(f"/v1/runs/{run_id}/finish", json={"status": "SUCCESS"})
    ticker = _ticker(app, receiver, tmp_path, run_retention=timedelta(days=1))
    later = now() + timedelta(days=2)
    assert (await ticker.tick(now=later)).runs_purged == 0, "its artifact is kept 7 days"
    report = await ticker.tick(now=later + ARTIFACT_RETENTION)
    assert (report.purged, report.runs_purged) == (1, 1), "the artifact, then its run"


def test_the_retention_is_off_unless_the_deployment_sets_it() -> None:
    assert RunsSettings().retention is None
    assert RunsSettings(retention_days=90).retention == timedelta(days=90)
