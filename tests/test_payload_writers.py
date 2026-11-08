"""Run payloads are written by reference: a run's rows hold no payload, reads answer exactly
what they answered inline, a checkpoint may be a list of segments the worker uploaded, an
unchanged save stores nothing new, a write that never commits leaves only an orphan the sweep
deletes, an ended run's events become one object, and a run written before the change-over
goes on (resumes, finishes) beside the ones written after."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import event, func, select, update
from trellis.contracts.ids import now
from trellis.runs.payloads import FRAMED_MEDIA_TYPE, SEGMENTS, decode, encode, frames, ref_of

from agent_runs.api.routers.artifacts import MAX_READ_OBJECTS
from agent_runs.blob.payloads import ORPHAN_AGE, payloads_of
from agent_runs.config.settings import BlobSettings
from agent_runs.store.runs import RunStore
from agent_runs.store.tables import (
    PayloadBlobRow,
    ResolutionRow,
    RunEventRow,
    RunPayloadRow,
    RunRow,
    WebhookDeliveryRow,
)
from tests.conftest import client_of, pause, resolution, started
from tests.test_events import _event

TENANT = "acme"
BIG = {"rows": [{"id": i, "note": "ü" * 20} for i in range(5_000)]}


async def _row(app: Any, run_id: str) -> RunRow:
    async with app.state.sessions() as db:
        row = await db.get(RunRow, run_id)
        assert row is not None
        return row


async def _objects(app: Any) -> int:
    async with app.state.sessions() as db:
        return int(await db.scalar(select(func.count()).select_from(PayloadBlobRow)) or 0)


async def _links(app: Any, run_id: str) -> dict[str, int]:
    async with app.state.sessions() as db:
        rows = await db.execute(
            select(RunPayloadRow.kind, func.count())
            .where(RunPayloadRow.run_id == run_id)
            .group_by(RunPayloadRow.kind)
        )
        return dict(rows.tuples().all())


async def _claimed(client: Any, **over: Any) -> tuple[str, str]:
    """A queued run, claimed: ``(run_id, worker_id)``."""
    run = (await client.post("/v1/runs", json={**started(**over), "queue": True})).json()
    claim = await client.post(
        "/v1/runs/claim", json={"worker_id": "w1", "agent_ids": [run["agent_id"]]}
    )
    assert claim.status_code == 200, claim.text
    return run["run_id"], "w1"


# ------------------------------------------------------------------ rows hold references
async def test_a_run_keeps_no_payload_in_its_rows(app: Any, client: Any) -> None:
    run = (await client.post("/v1/runs", json=started(input=BIG))).json()
    run_id = run["run_id"]
    waiting = (
        await client.post(
            f"/v1/runs/{run_id}/pause", json=pause(run_id, {"journal": [1]}, payload={"t": BIG})
        )
    ).json()
    await client.post(f"/v1/runs/{run_id}/resume", json=resolution(waiting, "EDIT", payload=BIG))
    done = await client.post(
        f"/v1/runs/{run_id}/finish", json={"status": "SUCCESS", "output": {"answer": BIG}}
    )
    assert done.status_code == 200, done.text
    row = await _row(app, run_id)
    assert (row.input, row.output, row.last_resolution, row.checkpoint) == (None,) * 4
    assert row.input_ref and row.output_ref and row.last_resolution_ref
    async with app.state.sessions() as db:
        kept = (await db.scalars(select(ResolutionRow).where(ResolutionRow.run_id == run_id))).one()
    assert (kept.interrupt, kept.resolution) == (None, None)
    got = (await client.get(f"/v1/runs/{run_id}")).json()
    assert got["input"] == BIG and got["output"] == {"answer": BIG}
    assert got["last_resolution"]["payload"] == BIG
    history = (await client.get(f"/v1/runs/{run_id}/resolutions")).json()
    assert history[0]["interrupt"]["payload"] == {"t": BIG}
    assert await _links(app, run_id) == {"input": 1, "output": 1, "answer": 2}, (
        "the ended run let go of its checkpoint and its question's payload"
    )


@pytest.mark.parametrize("value", [None, {}, "", "ü😀", [None]])
async def test_empty_null_and_tiny_payloads_read_back_as_sent(client: Any, value: Any) -> None:
    run = (await client.post("/v1/runs", json=started(input=value))).json()
    await client.post(
        f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS", "output": value}
    )
    got = (await client.get(f"/v1/runs/{run['run_id']}")).json()
    assert (got["input"], got["output"]) == (value, value)


async def test_the_size_bounds_stand_at_and_past_the_limit(client: Any) -> None:
    run_id, worker = await _claimed(client)
    fill = 1024 * 1024 - len(json.dumps({"x": ""}, separators=(",", ":")))
    at = {"x": "a" * fill}
    beat = f"/v1/runs/{run_id}/heartbeat"
    assert (
        await client.post(beat, json={"worker_id": worker, "checkpoint": at})
    ).status_code == 200
    over = {"x": "a" * (fill + 1)}
    assert (
        await client.post(beat, json={"worker_id": worker, "checkpoint": over})
    ).status_code == 413


# ------------------------------------------------------------------ checkpoints
async def test_an_unchanged_save_stores_nothing_new(app: Any, client: Any) -> None:
    run_id, worker = await _claimed(client)
    beat = f"/v1/runs/{run_id}/heartbeat"
    for _ in range(3):
        sent = await client.post(beat, json={"worker_id": worker, "checkpoint": {"calls": BIG}})
        assert sent.status_code == 200, sent.text
    assert await _objects(app) == 1, "one checkpoint, however often it is saved"


async def test_a_segmented_checkpoint_is_kept_and_read_as_sent(app: Any, client: Any) -> None:
    run_id, worker = await _claimed(client)
    refs = []
    for n in range(3):
        put = await client.post(
            f"/v1/runs/{run_id}/payloads",
            params={"worker_id": worker},
            content=encode({"segment": n}),
        )
        assert put.status_code == 201, put.text
        refs.append(put.json())
    again = await client.post(
        f"/v1/runs/{run_id}/payloads", params={"worker_id": worker}, content=encode({"segment": 0})
    )
    assert again.json() == refs[0], "the same segment is the same object"
    checkpoint = {SEGMENTS: refs}
    beat = await client.post(
        f"/v1/runs/{run_id}/heartbeat", json={"worker_id": worker, "checkpoint": checkpoint}
    )
    assert beat.status_code == 200, beat.text
    assert (await client.get(f"/v1/runs/{run_id}")).json()["checkpoint"] == checkpoint
    for n, ref in enumerate(refs):
        got = await client.get(f"/v1/payloads/{ref['sha256']}")
        assert got.status_code == 200 and got.content == encode({"segment": n})
    assert await _links(app, run_id) == {"checkpoint": 3}
    fewer = await client.post(
        f"/v1/runs/{run_id}/heartbeat",
        json={"worker_id": worker, "checkpoint": {SEGMENTS: refs[:1]}},
    )
    assert fewer.status_code == 200
    assert await _links(app, run_id) == {"checkpoint": 1}, "the rest let go"


async def test_a_checkpoint_names_only_this_tenants_uploaded_objects(app: Any, client: Any) -> None:
    run_id, worker = await _claimed(client)
    beat = f"/v1/runs/{run_id}/heartbeat"
    foreign = ref_of("globex", encode({"a": 1})).model_dump()
    never = ref_of(TENANT, encode({"never": "uploaded"})).model_dump()
    for refs in ([foreign], [never], ["not a reference"]):
        sent = await client.post(beat, json={"worker_id": worker, "checkpoint": {SEGMENTS: refs}})
        assert sent.status_code == 422, sent.text
    async with client_of(app, "other-key") as other:
        put = await client.post(
            f"/v1/runs/{run_id}/payloads", params={"worker_id": worker}, content=encode([1])
        )
        sha = put.json()["sha256"]
        assert (await other.get(f"/v1/payloads/{sha}")).status_code == 404, "another tenant's"
    assert (await client.get(f"/v1/payloads/{sha}")).status_code == 200


async def test_an_upload_is_a_payload_object_from_the_lease_holder(client: Any) -> None:
    run_id, worker = await _claimed(client)
    url = f"/v1/runs/{run_id}/payloads"
    bad = await client.post(url, params={"worker_id": worker}, content=b"not zstd json")
    assert bad.status_code == 422
    lost = await client.post(url, params={"worker_id": "w-old"}, content=encode([1]))
    assert lost.status_code == 409 and lost.json()["code"] == "LEASE_LOST"


async def test_a_save_that_never_commits_leaves_an_orphan_the_sweep_deletes(
    app: Any, client: Any, ticker: Any
) -> None:
    run_id, _ = await _claimed(client)
    sent = await client.post(
        f"/v1/runs/{run_id}/heartbeat", json={"worker_id": "w-old", "checkpoint": {"lost": 1}}
    )
    assert sent.status_code == 409, "the lease is lost: nothing refers to what was written"
    assert (await _row(app, run_id)).checkpoint_ref is None
    assert await _objects(app) == 1
    assert (await ticker.tick(now=now() + ORPHAN_AGE - timedelta(minutes=1))).orphans == 0
    assert (await ticker.tick(now=now() + ORPHAN_AGE + timedelta(minutes=1))).orphans == 1
    assert await _objects(app) == 0, "nothing referred to it"


async def test_a_store_that_does_not_answer_refuses_the_save_and_keeps_the_last(
    app: Any, client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, worker = await _claimed(client)
    beat = f"/v1/runs/{run_id}/heartbeat"
    await client.post(beat, json={"worker_id": worker, "checkpoint": {"n": 1}})

    async def down(*args: Any, **kwargs: Any) -> Any:
        raise OSError("the disk is gone")

    monkeypatch.setattr(app.state.blobs, "put", down)
    refused = await client.post(beat, json={"worker_id": worker, "checkpoint": {"n": 2}})
    assert refused.status_code == 503 and refused.json()["retryable"]
    assert (await client.get(f"/v1/runs/{run_id}")).json()["checkpoint"] == {"n": 1}


# ------------------------------------------------------------------ before and after
async def test_a_run_written_before_the_change_over_resumes_and_finishes(
    app: Any, client: Any
) -> None:
    """A paused run whose row holds its payloads inline (written by the last release) is
    answered, claimed, resumed from its inline checkpoint and finished by this one."""
    run = (await client.post("/v1/runs", json={**started(input={"q": 1}), "queue": True})).json()
    run_id = run["run_id"]
    await client.post("/v1/runs/claim", json={"worker_id": "w1", "agent_ids": [run["agent_id"]]})
    waiting = (
        await client.post(
            f"/v1/runs/{run_id}/pause",
            params={"worker_id": "w1"},
            json=pause(run_id, {"journal": "old"}, payload={"p": 1}),
        )
    ).json()
    async with app.state.sessions() as db:  # the row as the last release wrote it
        row = await db.get(RunRow, run_id)
        assert row is not None
        await db.execute(
            update(RunRow)
            .where(RunRow.run_id == run_id)
            .values(
                input={"q": 1},
                input_ref=None,
                checkpoint={"journal": "old"},
                checkpoint_ref=None,
                awaiting={**(row.awaiting or {}), "payload": {"p": 1}},
                awaiting_ref=None,
            )
        )
        await db.commit()
    resumed = await client.post(f"/v1/runs/{run_id}/resume", json=resolution(waiting, "APPROVE"))
    assert resumed.status_code == 200, resumed.text
    claim = (
        await client.post(
            "/v1/runs/claim", json={"worker_id": "w2", "agent_ids": [run["agent_id"]]}
        )
    ).json()
    assert claim["run"]["checkpoint"] == {"journal": "old"} and claim["run"]["input"] == {"q": 1}
    history = (await client.get(f"/v1/runs/{run_id}/resolutions")).json()
    assert history[0]["interrupt"]["payload"] == {"p": 1}
    done = await client.post(
        f"/v1/runs/{run_id}/finish",
        params={"worker_id": "w2"},
        json={"status": "SUCCESS", "output": 2},
    )
    got = done.json()
    assert (got["input"], got["output"]) == ({"q": 1}, 2), "old inline and new reference, one run"


async def test_a_webhook_body_is_sent_as_it_was_announced(
    app: Any, client: Any, ticker: Any, receiver: Any
) -> None:
    await client.post(
        "/v1/webhooks", json={"url": "http://hooks.test/runs", "events": ["run.paused"]}
    )
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(
        f"/v1/runs/{run['run_id']}/pause",
        json=pause(run["run_id"], payload={"z": [1, {"b": "ü", "a": 2}], "a": None}),
    )
    async with app.state.sessions() as db:
        delivery = (await db.scalars(select(WebhookDeliveryRow))).one()
        assert delivery.payload_ref is not None, "the body refers to the run's payload"
        assert "payload" not in delivery.payload["data"]["run"]["awaiting"]
        inline = json.loads(json.dumps(delivery.payload))
        inline["data"]["run"]["awaiting"]["payload"] = await payloads_of(db).get(
            delivery.payload_ref
        )
    await ticker.tick()
    sent = receiver.received[-1].content
    async with app.state.sessions() as db:  # the same delivery as the last release kept it
        db.add(
            WebhookDeliveryRow(
                delivery_id="dlv_inline",
                webhook_id=delivery.webhook_id,
                payload=inline,
                attempts=0,
                next_attempt_at=now(),
            )
        )
        await db.commit()
    await ticker.tick()
    assert len(receiver.received) == 2 and receiver.received[-1].content == sent


# ------------------------------------------------------------------ event compaction
async def test_an_ended_runs_events_become_one_object_and_replay_the_same(
    app: Any, client: Any, ticker: Any
) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    run_id = run["run_id"]
    events = [_event(run_id, n) for n in range(1, 8)]
    await client.post(f"/v1/runs/{run_id}/events", json={"events": events})
    live = (await client.get(f"/v1/runs/{run_id}/events?after=3")).content
    assert (await ticker.tick()).compacted == 0, "a live run's events stay rows"
    await client.post(f"/v1/runs/{run_id}/finish", json={"status": "SUCCESS"})
    before = [(await client.get(f"/v1/runs/{run_id}/events?after={n}")).content for n in (0, 3, 7)]
    stream = (
        await client.get(f"/v1/runs/{run_id}/events/stream", headers={"Last-Event-ID": "3"})
    ).text
    assert (await ticker.tick()).compacted == 1
    assert (await ticker.tick()).compacted == 0
    async with app.state.sessions() as db:
        left = await db.scalar(select(func.count()).where(RunEventRow.run_id == run_id))
    assert left == 0
    assert (await client.get(f"/v1/runs/{run_id}/events?after=3")).content == live
    after = [(await client.get(f"/v1/runs/{run_id}/events?after={n}")).content for n in (0, 3, 7)]
    assert after == before
    replay = await client.get(f"/v1/runs/{run_id}/events/stream", headers={"Last-Event-ID": "3"})
    assert replay.text == stream
    assert await _links(app, run_id) == {"events": 1}


async def test_compaction_another_ticker_won_changes_nothing(app: Any, client: Any) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    async with app.state.sessions() as db:
        assert await RunStore(db).compact_events(TENANT, run["run_id"])
        await db.commit()
    async with app.state.sessions() as db:
        assert not await RunStore(db).compact_events(TENANT, run["run_id"])


# ------------------------------------------------------------------ retention
def test_artifact_retention_is_a_setting_of_seven_days_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert BlobSettings().retention == timedelta(days=7)
    assert BlobSettings(retention_days=30).retention == timedelta(days=30)


# ------------------------------------------------------------------ reading and saving at scale
async def test_a_checkpoints_segments_read_back_in_one_request_in_order(
    app: Any, client: Any
) -> None:
    run_id, worker = await _claimed(client)
    refs = []
    for n in range(20):
        put = await client.post(
            f"/v1/runs/{run_id}/payloads", params={"worker_id": worker}, content=encode([n])
        )
        refs.append(put.json())
    read = await client.post("/v1/payloads/read", json={"sha256": [r["sha256"] for r in refs]})
    assert read.status_code == 200 and read.headers["content-type"] == FRAMED_MEDIA_TYPE
    assert [decode(f) for f in frames(read.content)] == [[n] for n in range(20)]
    async with client_of(app, "other-key") as other:
        refused = await other.post("/v1/payloads/read", json={"sha256": [refs[0]["sha256"]]})
        assert refused.status_code == 404, "another tenant's objects are not read"
    too_many = {"sha256": [refs[0]["sha256"]] * (MAX_READ_OBJECTS + 1)}
    assert (await client.post("/v1/payloads/read", json=too_many)).status_code == 422


async def test_a_save_that_adds_a_segment_costs_the_same_however_many_there_are(
    app: Any, client: Any
) -> None:
    run_id, worker = await _claimed(client)
    statements = 0

    def count(*_: Any) -> None:
        nonlocal statements
        statements += 1

    refs: list[dict[str, Any]] = []
    costs = []
    event.listen(app.state.engine.sync_engine, "before_cursor_execute", count)
    try:
        for n in range(30):
            put = await client.post(
                f"/v1/runs/{run_id}/payloads", params={"worker_id": worker}, content=encode([n])
            )
            refs.append(put.json())
            before = statements
            beat = await client.post(
                f"/v1/runs/{run_id}/heartbeat",
                json={"worker_id": worker, "checkpoint": {SEGMENTS: refs}},
            )
            assert beat.status_code == 200, beat.text
            costs.append(statements - before)
    finally:
        event.remove(app.state.engine.sync_engine, "before_cursor_execute", count)
    assert len(set(costs[1:])) == 1, costs
    # the rate limit, the run's row locked, the new segment linked (checked stored), the row
    # written once: as many as a whole checkpoint's save took before references, plus the link
    assert costs[-1] == 4, costs
    assert await _links(app, run_id) == {"checkpoint": 30}
