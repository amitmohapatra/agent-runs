"""Run payloads by reference: the format, the store over each blob adapter, the orphan sweep,
and the readers of a segmented checkpoint and of a compacted event log."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import delete, select, text, update
from trellis.contracts.ids import now
from trellis.runs.blob import FilesystemBlobStore
from trellis.runs.payloads import (
    SEGMENTS,
    PayloadRef,
    encode,
    jsonb_order,
    ref_of,
)

from agent_runs.blob import BlobStore
from agent_runs.blob.payloads import (
    ORPHAN_AGE,
    PARALLEL_READS,
    PayloadLost,
    PayloadStore,
    payloads_of,
)
from agent_runs.domain.errors import Unavailable
from agent_runs.store.tables import (
    PayloadBlobRow,
    RunEventRow,
    RunPayloadRow,
    RunRow,
)
from tests.conftest import started
from tests.test_blob import fake_gcs, gcs, store  # noqa: F401  (fixtures)
from tests.test_events import _event

TENANT = "acme"
LARGE = {"rows": [{"id": i, "name": f"row {i}", "amount": i * 1.5} for i in range(20_000)]}


# ------------------------------------------------------------------ the format
async def test_keys_are_in_the_order_jsonb_keeps_them(app: Any) -> None:
    value = {"zz": 1, "a": 2, "é": 3, "ab": {"y": 1, "b": 2}, "b": [{"cc": 1, "c": 2}]}
    async with app.state.engine.connect() as conn:
        kept = (
            await conn.execute(text("SELECT CAST(:v AS jsonb)::text"), {"v": json.dumps(value)})
        ).scalar()
    assert json.dumps(json.loads(kept)) == json.dumps(jsonb_order(value))


# ------------------------------------------------------------------ the store
@pytest.fixture
def payloads(app: Any, store: BlobStore) -> PayloadStore:  # noqa: F811
    """The payload store over each blob adapter, registering in the app's database."""
    return PayloadStore(store, app.state.sessions)


async def _registered(app: Any) -> list[str]:
    async with app.state.sessions() as db:
        return list((await db.scalars(select(PayloadBlobRow.key))).all())


@pytest.mark.parametrize("value", [None, {}, "x", LARGE])
async def test_a_payload_is_stored_once_and_read_back_verified(
    app: Any, payloads: PayloadStore, value: Any
) -> None:
    refs = await asyncio.gather(*(payloads.put(TENANT, value) for _ in range(4)))
    assert len(set(refs)) == 1, "four concurrent writes of one content: one object"
    assert await _registered(app) == [refs[0].ref]
    assert await payloads.get(refs[0]) == value
    assert await payloads.get(refs[0].model_dump()) == value


async def test_a_missing_or_corrupt_payload_is_an_error_never_wrong_data(
    payloads: PayloadStore,
) -> None:
    ref = await payloads.put(TENANT, {"a": 1})
    wrong = ref.model_copy(update={"sha256": "0" * 64})
    with pytest.raises(PayloadLost):
        await payloads.get(wrong)
    with pytest.raises(PayloadLost):
        await payloads.get(ref.model_copy(update={"ref": ref.ref.replace(ref.sha256, "1" * 64)}))


class Unreachable(FilesystemBlobStore):
    """A store whose disk (or network) is gone."""

    async def put(self, *args: Any, **kwargs: Any) -> Any:
        raise OSError("no route to the store")

    async def get(self, key: str) -> bytes:
        raise OSError("no route to the store")

    async def delete(self, key: str) -> None:
        raise OSError("no route to the store")


async def test_a_store_that_does_not_answer_is_retryable(app: Any, tmp_path: Any) -> None:
    payloads = PayloadStore(Unreachable(tmp_path), app.state.sessions)
    with pytest.raises(Unavailable):
        await payloads.put(TENANT, {"a": 1})
    with pytest.raises(Unavailable):
        await payloads.get(ref_of(TENANT, encode({"a": 1})))


async def test_an_orphan_the_store_cannot_delete_is_kept_for_the_next_sweep(
    app: Any, blobs: FilesystemBlobStore, tmp_path: Any
) -> None:
    ref = await PayloadStore(blobs, app.state.sessions).put(TENANT, {"a": 1})
    await _age(app, ref.ref, ORPHAN_AGE * 2)
    assert (
        await PayloadStore(Unreachable(tmp_path), app.state.sessions).sweep(now=now(), limit=10)
        == 0
    )
    assert await _registered(app) == [ref.ref]


class Slow(FilesystemBlobStore):
    """Reads that finish in the reverse of the order they started, counting how many run
    at once."""

    def __init__(self, root: Any) -> None:
        super().__init__(root)
        self.running = self.most = 0

    async def get(self, key: str) -> bytes:
        self.running += 1
        self.most = max(self.most, self.running)
        await asyncio.sleep(0.001 * (64 - int(key.removesuffix(".json.zst")[-2:], 16) % 64))
        self.running -= 1
        return await super().get(key)


async def test_many_payloads_read_in_parallel_come_back_in_order(app: Any, tmp_path: Any) -> None:
    slow = Slow(tmp_path)
    payloads = PayloadStore(slow, app.state.sessions)
    refs = [await payloads.put(TENANT, {"segment": i}) for i in range(40)]
    assert await payloads.get_many(refs) == [{"segment": i} for i in range(40)]
    assert 1 < slow.most <= PARALLEL_READS


# ------------------------------------------------------------------ the orphan sweep
async def _age(app: Any, key: str, by: timedelta) -> None:
    async with app.state.sessions() as db:
        await db.execute(
            update(PayloadBlobRow).where(PayloadBlobRow.key == key).values(touched_at=now() - by)
        )
        await db.commit()


async def test_the_sweep_deletes_only_old_objects_nothing_refers_to(
    app: Any, client: Any, blobs: FilesystemBlobStore
) -> None:
    payloads = PayloadStore(blobs, app.state.sessions)
    young = await payloads.put(TENANT, {"n": 0})
    old = await payloads.put(TENANT, {"n": 1})
    linked = await payloads.put(TENANT, {"n": 2})
    run = (await client.post("/v1/runs", json=started())).json()
    async with app.state.sessions() as db:
        db.add(RunPayloadRow(run_id=run["run_id"], key=linked.ref, kind="input"))
        await db.commit()
    for ref in (old, linked):
        await _age(app, ref.ref, ORPHAN_AGE + timedelta(minutes=1))
    await _age(app, young.ref, ORPHAN_AGE - timedelta(minutes=1))
    assert await payloads.sweep(now=now(), limit=10) == 1
    assert sorted(await _registered(app)) == sorted([young.ref, linked.ref])
    with pytest.raises(PayloadLost):
        await payloads.get(old)
    assert await payloads.get(young) == {"n": 0}, "a young orphan is never deleted"
    assert await payloads.get(linked) == {"n": 2}


async def test_a_write_after_a_sweep_deleted_its_object_stores_it_again(
    app: Any, blobs: FilesystemBlobStore
) -> None:
    payloads = PayloadStore(blobs, app.state.sessions)
    ref = await payloads.put(TENANT, {"n": 1})
    await _age(app, ref.ref, ORPHAN_AGE * 2)
    assert await payloads.sweep(now=now(), limit=10) == 1
    assert await payloads.put(TENANT, {"n": 1}) == ref
    assert await payloads.get(ref) == {"n": 1}


# ------------------------------------------------------------------ the readers
async def test_a_segmented_checkpoint_reads_as_its_segment_list(app: Any, client: Any) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    segment = ref_of(TENANT, encode({"calls": {}})).model_dump()
    async with app.state.sessions() as db:
        await db.execute(
            update(RunRow)
            .where(RunRow.run_id == run["run_id"])
            .values(checkpoint_segments=[segment])
        )
        await db.commit()
    got = (await client.get(f"/v1/runs/{run['run_id']}")).json()
    assert got["checkpoint"] == {SEGMENTS: [segment]}
    assert PayloadRef.model_validate(got["checkpoint"][SEGMENTS][0]).ref == segment["ref"]


async def test_an_ended_runs_compacted_log_streams_and_replays(app: Any, client: Any) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    run_id = run["run_id"]
    sent = [_event(run_id, n) for n in range(1, 6)]
    appended = await client.post(f"/v1/runs/{run_id}/events", json={"events": sent})
    assert appended.status_code == 200, appended.text
    await client.post(
        f"/v1/runs/{run_id}/finish", json={"status": "SUCCESS", "output": {"ok": True}}
    )
    before = (await client.get(f"/v1/runs/{run_id}/events?after=2")).content
    stream = (
        await client.get(f"/v1/runs/{run_id}/events/stream", headers={"Last-Event-ID": "2"})
    ).text
    async with app.state.sessions() as db:
        logged = (
            await db.scalars(
                select(RunEventRow)
                .where(RunEventRow.run_id == run_id)
                .order_by(RunEventRow.position)
            )
        ).all()
        ref = await payloads_of(db).put(TENANT, [[r.position, r.event] for r in logged])
        await db.execute(
            update(RunRow).where(RunRow.run_id == run_id).values(events_ref=ref.model_dump())
        )
        await db.execute(delete(RunEventRow).where(RunEventRow.run_id == run_id))
        await db.commit()
    assert (await client.get(f"/v1/runs/{run_id}/events?after=2")).content == before
    assert (
        await client.get(f"/v1/runs/{run_id}/events/stream", headers={"Last-Event-ID": "2"})
    ).text == stream
    assert (await client.get(f"/v1/runs/{run_id}/events?after=4&limit=1")).json()[0][
        "position"
    ] == 5
