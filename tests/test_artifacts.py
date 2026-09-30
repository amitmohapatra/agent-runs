"""Run artifacts over HTTP: uploaded while a run works (fenced by its lease) or waits (by the
service principal), read back by the tenant only, bounded in size, and deleted by the ticker
once the run has been over for ``ARTIFACT_RETENTION``."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from datetime import timedelta

from sqlalchemy import text
from trellis.contracts.ids import now

from agent_runs.config.constants import ARTIFACT_RETENTION
from tests.conftest import pause, paused, resolution, started

TABLE = {"columns": ["sku", "qty"], "rows": [["A-1", 3], ["B-2", 5]]}
TABLE_BYTES = json.dumps(TABLE).encode()
JSON = {"Content-Type": "application/json"}


def sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


async def running(client, **over) -> dict:
    return (await client.post("/v1/runs", json=started(**over))).json()


async def upload(client, run_id: str, data: bytes = TABLE_BYTES, headers=JSON, **params):
    return await client.post(
        f"/v1/runs/{run_id}/artifacts", content=data, headers=headers, params=params
    )


def blob_files(blobs) -> list:
    return [p for p in blobs._root.rglob("*") if p.is_file()]


async def expiry(app, artifact_id: str):
    async with app.state.engine.connect() as conn:
        return await conn.scalar(
            text("SELECT expires_at FROM run_artifacts WHERE artifact_id = :a"),
            {"a": artifact_id},
        )


# ------------------------------------------------------------------ upload and read back


async def test_an_ask_table_is_stored_and_read_back_by_its_reference(client, blobs) -> None:
    run = await running(client)
    response = await upload(client, run["run_id"])
    assert response.status_code == 201, response.text
    ref = response.json()
    assert ref["artifact_id"].startswith("art_")
    assert ref["uri"] == f"/v1/artifacts/{ref['artifact_id']}"
    assert ref["mime_type"] == "application/json"
    assert ref["checksum"] == sha(TABLE_BYTES) and ref["size_bytes"] == len(TABLE_BYTES)
    assert ref["metadata"] == {"run_id": run["run_id"]}

    got = await client.get(ref["uri"])
    assert got.status_code == 200
    assert got.json() == TABLE
    assert got.headers["content-type"] == "application/json"
    assert got.headers["etag"] == f'"{sha(TABLE_BYTES)}"'
    assert int(got.headers["content-length"]) == len(TABLE_BYTES)
    assert len(blob_files(blobs)) == 1


async def test_the_reference_travels_in_the_interrupt_not_the_checkpoint(client) -> None:
    run = await running(client)
    ref = (await upload(client, run["run_id"])).json()
    body = pause(run["run_id"], checkpoint={"asked": 1}, payload_ref=ref, ui="table")
    paused_run = (await client.post(f"/v1/runs/{run['run_id']}/pause", json=body)).json()
    assert paused_run["awaiting"]["payload_ref"]["artifact_id"] == ref["artifact_id"]
    assert paused_run["checkpoint"] == {"asked": 1}


async def test_bytes_of_any_type_keep_their_type(client) -> None:
    run = await running(client)
    diff = b"--- a\n+++ b\n@@ -1 +1 @@\n-old\n+new\n"
    ref = (await upload(client, run["run_id"], diff, {"Content-Type": "text/x-diff"})).json()
    got = await client.get(ref["uri"])
    assert got.content == diff and got.headers["content-type"].startswith("text/x-diff")
    bare = await client.post(f"/v1/runs/{run['run_id']}/artifacts", content=b"\x00\x01")
    assert bare.status_code == 201
    assert bare.json()["mime_type"] == "application/octet-stream"


async def test_a_retried_upload_is_the_same_artifact(client, blobs) -> None:
    run = await running(client)
    first = await upload(client, run["run_id"])
    again = await upload(client, run["run_id"])
    assert (first.status_code, again.status_code) == (201, 200)
    assert first.json()["artifact_id"] == again.json()["artifact_id"]
    assert len(blob_files(blobs)) == 1, "the duplicate's blob is not left behind"


async def test_a_checksum_the_bytes_do_not_match_is_refused(client, blobs) -> None:
    run = await running(client)
    wrong = await upload(client, run["run_id"], checksum=sha(b"something else"))
    assert wrong.status_code == 422
    assert blob_files(blobs) == []
    right = await upload(client, run["run_id"], checksum=sha(TABLE_BYTES))
    assert right.status_code == 201


async def test_an_empty_body_is_not_an_artifact(client) -> None:
    run = await running(client)
    assert (await upload(client, run["run_id"], b"")).status_code == 422


# ------------------------------------------------------------------ the size cap


async def test_an_artifact_past_the_cap_is_refused(client, blobs, monkeypatch) -> None:
    monkeypatch.setattr("agent_runs.api.routers.artifacts.MAX_ARTIFACT_BYTES", 64)
    run = await running(client)
    assert (await upload(client, run["run_id"], b"x" * 64)).status_code == 201
    declared = await upload(client, run["run_id"], b"y" * 65)
    assert declared.status_code == 413

    async def chunked() -> AsyncIterator[bytes]:  # no Content-Length: counted as it arrives
        for _ in range(10):
            yield b"z" * 10

    streamed = await client.post(f"/v1/runs/{run['run_id']}/artifacts", content=chunked())
    assert streamed.status_code == 413
    assert len(blob_files(blobs)) == 1


# ------------------------------------------------------------------ fencing


async def test_a_leased_run_takes_artifacts_only_from_its_lease_holder(client) -> None:
    await client.post("/v1/runs", json=started(queue=True))
    claim = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 30}
    run_id = (await client.post("/v1/runs/claim", json=claim)).json()["run"]["run_id"]
    assert (await upload(client, run_id)).status_code == 409, "a leased run needs worker_id"
    assert (await upload(client, run_id, worker_id="w2")).status_code == 409
    assert (await upload(client, run_id, worker_id="w1")).status_code == 201


async def test_a_run_in_the_callers_process_takes_no_worker_id(client) -> None:
    run = await running(client)
    assert (await upload(client, run["run_id"], worker_id="w1")).status_code == 409


async def test_a_worker_whose_lease_lapsed_cannot_add_artifacts(client, ticker) -> None:
    await client.post("/v1/runs", json=started(queue=True))
    claim = {"worker_id": "w1", "agent_ids": ["triage"], "lease_seconds": 5}
    run_id = (await client.post("/v1/runs/claim", json=claim)).json()["run"]["run_id"]
    await ticker.tick(now=now() + timedelta(seconds=30))
    assert (await client.get(f"/v1/runs/{run_id}")).json()["status"] == "QUEUED"
    assert (await upload(client, run_id, worker_id="w1")).status_code == 409


async def test_a_paused_run_takes_artifacts_only_from_a_service_key(client, platform) -> None:
    run = await paused(client)
    assert (await upload(client, run["run_id"])).status_code == 201
    other = await upload(platform, run["run_id"], b'{"corrected": true}')
    assert other.status_code == 403


async def test_an_ended_run_takes_no_artifacts(client) -> None:
    run = await running(client)
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    assert (await upload(client, run["run_id"])).status_code == 409
    assert (await upload(client, "run_nope")).status_code == 404


# ------------------------------------------------------------------ isolation


async def test_artifacts_are_the_tenants_own(client, other_tenant) -> None:
    run = await running(client)
    ref = (await upload(client, run["run_id"])).json()
    assert (await other_tenant.get(ref["uri"])).status_code == 404
    assert (await upload(other_tenant, run["run_id"])).status_code == 404
    assert (await client.get("/v1/artifacts/art_nope")).status_code == 404


async def test_bytes_that_no_longer_match_are_not_served(client, blobs) -> None:
    run = await running(client)
    ref = (await upload(client, run["run_id"])).json()
    [path] = blob_files(blobs)
    path.write_bytes(TABLE_BYTES.replace(b"A-1", b"A-9"))
    assert (await client.get(ref["uri"])).status_code == 500
    path.unlink()
    assert (await client.get(ref["uri"])).status_code == 404


# ------------------------------------------------------------------ retention


async def test_artifacts_go_a_retention_after_the_run_ends(app, client, ticker, blobs) -> None:
    run = await running(client)
    ref = (await upload(client, run["run_id"])).json()
    assert await expiry(app, ref["artifact_id"]) is None, "a live run's artifacts stay"
    assert (await ticker.tick(now=now() + 2 * ARTIFACT_RETENTION)).purged == 0

    ended = now()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    expires = await expiry(app, ref["artifact_id"])
    assert ended + ARTIFACT_RETENTION <= expires <= now() + ARTIFACT_RETENTION
    assert (await client.get(ref["uri"])).status_code == 200, "readable after the run ends"
    assert (await ticker.tick(now=expires - timedelta(seconds=1))).purged == 0

    assert (await ticker.tick(now=expires + timedelta(seconds=1))).purged == 1
    assert blob_files(blobs) == []
    assert (await client.get(ref["uri"])).status_code == 404


async def test_a_cancelled_pause_starts_the_retention_too(app, client) -> None:
    run = await running(client)
    ref = (await upload(client, run["run_id"])).json()
    await client.post(f"/v1/runs/{run['run_id']}/pause", json=pause(run["run_id"]))
    record = (await client.get(f"/v1/runs/{run['run_id']}")).json()
    assert await expiry(app, ref["artifact_id"]) is None
    await client.post(f"/v1/runs/{run['run_id']}/resume", json=resolution(record, "CANCEL"))
    assert await expiry(app, ref["artifact_id"]) is not None


async def test_a_blob_that_will_not_delete_keeps_its_row(client, ticker, blobs, monkeypatch):
    run = await running(client)
    ref = (await upload(client, run["run_id"])).json()
    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})

    async def refuse(key: str) -> None:
        raise OSError("the store is down")

    monkeypatch.setattr(blobs, "delete", refuse)
    later = now() + ARTIFACT_RETENTION + timedelta(minutes=1)
    assert (await ticker.tick(now=later)).purged == 0
    assert (await client.get(ref["uri"])).status_code == 200
    monkeypatch.undo()
    assert (await ticker.tick(now=later)).purged == 1
