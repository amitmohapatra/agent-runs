"""Moving the payloads an earlier release kept inline (``agent-runs-payloads``): copied in
batches and resumable, verified against what each object reads back as, cleared only once
everything matches; every read of every run answers the same bytes before, during and after."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from sqlalchemy import delete, select, text, update

from agent_runs.blob.payloads import payloads_of
from agent_runs.store.tables import (
    ResolutionRow,
    RunPayloadRow,
    RunRow,
    WebhookDeliveryRow,
)
from agent_runs.tools import move_payloads
from agent_runs.tools.move_payloads import MOVED, Counts, run
from tests.conftest import SETTINGS, pause, resolution, started


async def _as_written_before(app: Any) -> None:
    """Every row as the release before references wrote it: payloads inline, no references."""
    async with app.state.sessions() as db:
        payloads = payloads_of(db)
        for row in (await db.scalars(select(RunRow))).all():
            for name in ("input", "output", "last_resolution", "checkpoint"):
                ref = getattr(row, f"{name}_ref")
                if ref is not None:
                    setattr(row, name, await payloads.get(ref))
                    setattr(row, f"{name}_ref", None)
            if row.awaiting_ref is not None:
                row.awaiting = {
                    **(row.awaiting or {}),
                    "payload": await payloads.get(row.awaiting_ref),
                }
                row.awaiting_ref = None
        for res in (await db.scalars(select(ResolutionRow))).all():
            res.interrupt = await payloads.get(res.interrupt_ref)
            res.resolution = await payloads.get(res.resolution_ref)
            res.interrupt_ref = res.resolution_ref = None
        for dlv in (await db.scalars(select(WebhookDeliveryRow))).all():
            if dlv.payload_ref is not None:
                body = json.loads(json.dumps(dlv.payload))
                body["data"]["run"]["awaiting"]["payload"] = await payloads.get(dlv.payload_ref)
                dlv.payload, dlv.payload_ref = body, None
        await db.execute(delete(RunPayloadRow))
        await db.commit()


async def _old_runs(client: Any, n: int = 3) -> list[str]:
    await client.post("/v1/webhooks", json={"url": "http://hooks.test/r", "events": ["run.paused"]})
    ids = []
    for i in range(n):
        run_ = (await client.post("/v1/runs", json=started(input={"n": i, "ü": "x" * i}))).json()
        rid = run_["run_id"]
        waiting = (
            await client.post(
                f"/v1/runs/{rid}/pause", json=pause(rid, {"j": i}, payload={"t": [i]})
            )
        ).json()
        if i % 2 == 0:
            await client.post(
                f"/v1/runs/{rid}/resume", json=resolution(waiting, "ANSWER", answer={"v": i})
            )
            await client.post(
                f"/v1/runs/{rid}/finish", json={"status": "SUCCESS", "output": {"a": i}}
            )
        ids.append(rid)
    return ids


async def _views(client: Any, ids: list[str]) -> list[bytes]:
    paths = [
        "/v1/runs",
        *(f"/v1/runs/{i}" for i in ids),
        *(f"/v1/runs/{i}/resolutions" for i in ids),
    ]
    return [(await client.get(p)).content for p in paths]


async def _inline_left(app: Any) -> int:
    left = 0
    async with app.state.sessions() as db:
        for m in MOVED:
            left += len((await db.scalars(select(m.model).where(m.inline))).all())
    return left


def _total(counts: list[Counts], what: str) -> int:
    return sum(getattr(c, what) for c in counts)


async def test_old_runs_move_and_read_the_same_bytes_at_every_phase(app: Any, client: Any) -> None:
    ids = await _old_runs(client)
    await _as_written_before(app)
    before = await _views(client, ids)
    inline = await _inline_left(app)
    assert inline > 0
    copied = await run(app.state.sessions, "copy")
    assert _total(copied, "copied") == inline
    assert await _views(client, ids) == before
    checked = await run(app.state.sessions, "verify")
    assert _total(checked, "verified") == inline and _total(checked, "mismatched") == 0
    cleared = await run(app.state.sessions, "clear")
    assert _total(cleared, "cleared") == inline
    assert await _inline_left(app) == 0
    assert await _views(client, ids) == before
    assert _total(await run(app.state.sessions, "copy"), "copied") == 0, "nothing left"


async def test_a_copy_stopped_part_way_goes_on_where_it_stopped(app: Any, client: Any) -> None:
    ids = await _old_runs(client, 4)
    await _as_written_before(app)
    before = await _views(client, ids)
    first = await run(app.state.sessions, "copy", batch=1, batches=1)
    assert all(c.copied <= 1 for c in first)
    rest = await run(app.state.sessions, "copy", batch=2)
    async with app.state.sessions() as db:
        pending = [
            m.name
            for m in MOVED
            if (
                await db.scalars(
                    select(m.model).where(m.inline, move_payloads._absent(getattr(m.model, m.ref)))
                )
            ).first()
        ]
    assert pending == []
    assert _total(first, "copied") + _total(rest, "copied") == await _inline_left(app)
    assert await _views(client, ids) == before


async def test_nothing_is_cleared_unless_every_copy_matches(app: Any, client: Any) -> None:
    ids = await _old_runs(client)
    await _as_written_before(app)
    await run(app.state.sessions, "copy")
    async with app.state.engine.begin() as conn:  # an inline value changed after its copy
        await conn.execute(
            text("UPDATE agent_runs SET input = CAST(:v AS jsonb) WHERE run_id = :r"),
            {"v": json.dumps({"changed": True}), "r": ids[0]},
        )
    inline = await _inline_left(app)
    refused = await run(app.state.sessions, "clear")
    assert _total(refused, "mismatched") == 1 and _total(refused, "cleared") == 0
    assert await _inline_left(app) == inline, "nothing cleared"
    assert move_payloads._failed(refused)


async def test_a_lost_object_is_counted_and_blocks_the_clear(
    app: Any, client: Any, blobs: Any
) -> None:
    await _old_runs(client, 1)
    await _as_written_before(app)
    await run(app.state.sessions, "copy")
    async with app.state.sessions() as db:
        row = (await db.scalars(select(RunRow))).one()
        await blobs.delete(row.input_ref["ref"])
    checked = await run(app.state.sessions, "clear")
    assert _total(checked, "lost") == 1 and _total(checked, "cleared") == 0


async def test_the_command_reports_each_column_and_fails_on_a_mismatch(
    app: Any,
    client: Any,
    blobs: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    blob = SETTINGS.blob.model_copy(update={"root": blobs._root})
    monkeypatch.setattr(
        move_payloads, "get_settings", lambda: SETTINGS.model_copy(update={"blob": blob})
    )
    await _old_runs(client, 1)
    await _as_written_before(app)
    assert await asyncio.to_thread(move_payloads.main, ["copy", "--batch", "5"]) == 0
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert {line["column"] for line in lines} == {m.name for m in MOVED}
    async with app.state.engine.begin() as conn:
        await conn.execute(update(RunRow).values(input={"changed": 1}))
    assert await asyncio.to_thread(move_payloads.main, ["verify"]) == 1
