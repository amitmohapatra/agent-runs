"""Run payloads are written by reference: a run's rows hold no payload, reads answer the JSON
that was sent, a checkpoint may be a list of segments the worker uploaded, an unchanged save
stores nothing new, a write that never commits leaves only an orphan the sweep deletes, and an
ended run's events become objects, a page's worth each."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import event, func, select
from trellis.contracts.ids import now
from trellis.runs.payloads import FRAMED_MEDIA_TYPE, SEGMENTS, decode, encode, frames, ref_of

from agent_runs.api.routers import artifacts
from agent_runs.api.routers.artifacts import MAX_READ_OBJECTS
from agent_runs.blob.payloads import ORPHAN_AGE
from agent_runs.config.settings import BlobSettings
from agent_runs.store import runs
from agent_runs.store.runs import RunStore
from agent_runs.store.tables import (
    PayloadBlobRow,
    RunEventRow,
    RunPayloadRow,
    RunRow,
    WebhookDeliveryRow,
)
from tests.conftest import SETTINGS, client_of, pause, resolution, started
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
    assert row.input_ref and row.output_ref and row.last_resolution_ref
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


async def test_a_checkpoint_kept_past_the_end_outlives_the_sweep(
    app: Any, client: Any, ticker: Any
) -> None:
    """A run finished with ``keep_checkpoint`` still refers to its checkpoint's object: the
    sweep, which deletes what nothing refers to, leaves it, and it reads back a day later."""
    run_id, worker = await _claimed(client)
    kept = {"asks": {"ask_1": "yes"}, "done": True}
    done = await client.post(
        f"/v1/runs/{run_id}/finish",
        params={"worker_id": worker},
        json={"status": "SUCCESS", "keep_checkpoint": True, "checkpoint": kept},
    )
    assert done.status_code == 200, done.text
    assert (await _links(app, run_id)).get("checkpoint") == 1
    await ticker.tick(now=now() + ORPHAN_AGE + timedelta(minutes=1))
    assert (await client.get(f"/v1/runs/{run_id}")).json()["checkpoint"] == kept


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


# ------------------------------------------------------------------ webhook bodies
async def test_a_webhook_body_is_sent_as_it_was_announced(
    app: Any, client: Any, ticker: Any, receiver: Any
) -> None:
    await client.post(
        "/v1/webhooks", json={"url": "http://hooks.test/runs", "events": ["run.paused"]}
    )
    run = (await client.post("/v1/runs", json=started())).json()
    payload = {"z": [1, {"b": "ü", "a": 2}], "a": None}
    await client.post(f"/v1/runs/{run['run_id']}/pause", json=pause(run["run_id"], payload=payload))
    async with app.state.sessions() as db:
        delivery = (await db.scalars(select(WebhookDeliveryRow))).one()
        assert delivery.payload_ref is not None, "the body refers to the run's payload"
        assert "payload" not in delivery.payload["data"]["run"]["awaiting"]
    await ticker.tick()
    sent = json.loads(receiver.received[-1].content)
    assert sent["data"]["run"]["awaiting"]["payload"] == payload


# ------------------------------------------------------------------ event compaction
async def test_an_ended_runs_events_become_objects_and_replay_the_same(
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


async def test_a_page_of_a_compacted_log_reads_only_the_objects_it_spans(
    app: Any, client: Any, ticker: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A compacted log is an object per ``EVENT_CHUNK`` events, so a page reads the chunk it
    starts in (and the next, when it runs past its end), never the whole log; a run that
    logged nothing has no object at all."""
    monkeypatch.setattr(runs, "EVENT_CHUNK", 3)
    run_id = (await client.post("/v1/runs", json=started())).json()["run_id"]
    events = [_event(run_id, n) for n in range(1, 9)]
    await client.post(f"/v1/runs/{run_id}/events", json={"events": events})
    await client.post(f"/v1/runs/{run_id}/finish", json={"status": "SUCCESS"})
    silent = (await client.post("/v1/runs", json=started())).json()["run_id"]
    await client.post(f"/v1/runs/{silent}/finish", json={"status": "SUCCESS"})
    pages = [(0, 3), (3, 3), (1, 3), (6, 500), (8, 500), (0, 500)]
    before = [await _page(client, run_id, after, limit) for after, limit in pages]
    stream = (await client.get(f"/v1/runs/{run_id}/events/stream")).text

    assert (await ticker.tick()).compacted == 2
    assert await _links(app, run_id) == {"events": 3} and await _links(app, silent) == {}
    reads = _counting_reads(app, monkeypatch)
    spanned = []
    for after, limit in pages:
        reads.clear()
        assert await _page(client, run_id, after, limit) == before[pages.index((after, limit))]
        spanned.append(len(reads))
    assert spanned == [1, 1, 2, 1, 0, 3]
    assert (await client.get(f"/v1/runs/{run_id}/events/stream")).text == stream
    assert await _page(client, silent, 0, 500) == b"[]"
    assert (await ticker.tick()).compacted == 0


async def _page(client: Any, run_id: str, after: int, limit: int) -> bytes:
    page = await client.get(f"/v1/runs/{run_id}/events", params={"after": after, "limit": limit})
    assert page.status_code == 200, page.text
    return page.content


async def test_compaction_another_ticker_won_changes_nothing(app: Any, client: Any) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    async with app.state.sessions() as db:
        assert await RunStore(db, SETTINGS.limits).compact_events(TENANT, run["run_id"])
        await db.commit()
    async with app.state.sessions() as db:
        assert not await RunStore(db, SETTINGS.limits).compact_events(TENANT, run["run_id"])


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


async def test_objects_asked_for_twice_are_read_once_and_answered_where_asked(
    app: Any, client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two segments with the same content are one object: it is read from the store once,
    and its bytes answered at each place it was asked for."""
    run_id, worker = await _claimed(client)
    params = {"worker_id": worker}
    one, two = [
        (
            await client.post(f"/v1/runs/{run_id}/payloads", params=params, content=encode([n]))
        ).json()["sha256"]
        for n in (1, 2)
    ]
    reads = _counting_reads(app, monkeypatch)
    read = await client.post(
        "/v1/payloads/read",
        json={"sha256": [one, two, one, one]},
        headers={"Accept-Encoding": "identity"},
    )
    assert read.status_code == 200, read.text
    assert int(read.headers["content-length"]) == len(read.content)
    assert [decode(f) for f in frames(read.content)] == [[1], [2], [1], [1]]
    assert len(reads) == 2


async def test_a_read_of_more_bytes_than_one_answer_holds_is_refused_before_any_is_read(
    app: Any, client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """What one answer holds is bounded by the bytes asked for, an object asked for twice
    counted twice: past the bound it is a 413 that read nothing from the store."""
    run_id, worker = await _claimed(client)
    stored = [encode([n] * 50) for n in range(3)]
    refs = [
        (
            await client.post(
                f"/v1/runs/{run_id}/payloads", params={"worker_id": worker}, content=data
            )
        ).json()
        for data in stored
    ]
    monkeypatch.setattr(artifacts, "MAX_READ_BYTES", refs[0]["size"] + refs[1]["size"])
    reads = _counting_reads(app, monkeypatch)
    fits = await client.post("/v1/payloads/read", json={"sha256": [refs[0]["sha256"]] * 2})
    assert fits.status_code == 200
    reads.clear()
    for asked in ([r["sha256"] for r in refs], [refs[0]["sha256"]] * 3):
        refused = await client.post("/v1/payloads/read", json={"sha256": asked})
        assert refused.status_code == 413, refused.text
        assert refused.json()["code"] == "PAYLOAD_TOO_LARGE"
    assert reads == []


async def test_an_object_that_decodes_past_the_bound_is_refused_undecoded(
    app: Any, client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A small upload may claim to decompress to far more: its declared size is checked
    before it is decoded, so validating it cannot be made to hold more than the bound."""
    run_id, worker = await _claimed(client)
    monkeypatch.setattr(artifacts, "MAX_PAYLOAD_DECODED_BYTES", 1024)
    decoded: list[bytes] = []

    async def decoding(data: bytes) -> Any:
        decoded.append(data)
        return decode(data)

    monkeypatch.setattr(artifacts, "adecode", decoding)
    path, params = f"/v1/runs/{run_id}/payloads", {"worker_id": worker}
    bomb = await client.post(path, params=params, content=encode("0" * 2048))
    assert bomb.status_code == 413 and "1024" in bomb.json()["detail"]
    assert decoded == [] and await _objects(app) == 0
    assert (await client.post(path, params=params, content=encode("0" * 512))).status_code == 201
    assert (await client.post(path, params=params, content=b"not zstd")).status_code == 422


def _counting_reads(app: Any, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The keys read from the payload store from now on, in order."""
    read: list[str] = []
    get_encoded = app.state.payloads.get_encoded

    async def counted(ref: Any) -> bytes:
        read.append(ref.ref)
        return await get_encoded(ref)

    monkeypatch.setattr(app.state.payloads, "get_encoded", counted)
    return read


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
