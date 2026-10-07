"""The blob port, against both adapters: create-only puts, reads verified against the
recorded checksum as they stream, idempotent deletes. The GCS adapter runs over an in-memory
stand-in for the Google client, and against a local fake GCS server
(``fsouza/fake-gcs-server`` in Docker, on a free port, removed afterwards); the end-to-end test
drives the API and the ticker on it."""

from __future__ import annotations

import hashlib
import socket
import subprocess
import time
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from typing import Any

import httpx
import pytest
from google.api_core import exceptions as gexc
from trellis.contracts.ids import now

from agent_runs.blob import (
    BlobCorrupt,
    BlobExists,
    BlobNotFound,
    BlobStore,
    open_blob_store,
    read,
)
from agent_runs.blob.filesystem import FilesystemBlobStore
from agent_runs.blob.gcs import GCSBlobStore
from agent_runs.config.constants import ARTIFACT_RETENTION
from agent_runs.config.settings import BlobSettings
from tests.conftest import started

FAKE_GCS_IMAGE = "fsouza/fake-gcs-server:latest"
BUCKET = "runs-artifacts-test"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def fake_gcs() -> Iterator[str]:
    """A fake GCS server for this session, and its URL; stopped and removed afterwards."""
    port = _free_port()
    name = f"agent-runs-fake-gcs-{port}"
    url = f"http://127.0.0.1:{port}"
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, "-p", f"127.0.0.1:{port}:4443",
         FAKE_GCS_IMAGE, "-scheme", "http", "-port", "4443", "-external-url", url],
        check=True,
        capture_output=True,
    )  # fmt: skip
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                if httpx.get(f"{url}/storage/v1/b", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                raise RuntimeError("the fake GCS server did not start")
            time.sleep(0.2)
        httpx.post(f"{url}/storage/v1/b", json={"name": BUCKET}).raise_for_status()
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True)


@pytest.fixture
def gcs(fake_gcs: str, monkeypatch: pytest.MonkeyPatch) -> GCSBlobStore:
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", fake_gcs)
    return GCSBlobStore(BUCKET)


class FakeGCSBlob:
    """The slice of ``google.cloud.storage.Blob`` the adapter uses, over a dict, with the
    server's semantics: a create-only upload refused with 412, ranged reads with an inclusive
    end, and 404 for an object that is gone."""

    def __init__(self, bucket: FakeGCSBucket, key: str) -> None:
        self._bucket, self._key = bucket, key
        self.metadata: dict[str, str] | None = None

    @property
    def size(self) -> int:
        return len(self._bucket.objects[self._key][0])

    def upload_from_file(
        self,
        file_obj: Any,
        *,
        size: int,
        content_type: str,
        if_generation_match: int,
        checksum: str,
    ) -> None:
        assert (if_generation_match, checksum) == (0, "crc32c"), "create-only, checksummed"
        data = file_obj.read()
        assert len(data) == size, "the spool is rewound and its size declared"
        if self._key in self._bucket.objects:
            raise gexc.PreconditionFailed("the object exists")
        self._bucket.objects[self._key] = (data, content_type, self.metadata)

    def download_as_bytes(self, *, start: int, end: int) -> bytes:
        if self._key not in self._bucket.objects:
            raise gexc.NotFound("the object is gone")
        return self._bucket.objects[self._key][0][start : end + 1]

    def delete(self) -> None:
        if self._bucket.objects.pop(self._key, None) is None:
            raise gexc.NotFound("no such object")


class FakeGCSBucket:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str, dict[str, str] | None]] = {}

    def blob(self, key: str) -> FakeGCSBlob:
        return FakeGCSBlob(self, key)

    def get_blob(self, key: str) -> FakeGCSBlob | None:
        return FakeGCSBlob(self, key) if key in self.objects else None


class FakeGCSClient:
    def __init__(self) -> None:
        self.buckets: dict[str, FakeGCSBucket] = {}
        self.closed = False

    def bucket(self, name: str) -> FakeGCSBucket:
        return self.buckets.setdefault(name, FakeGCSBucket())

    def close(self) -> None:
        self.closed = True


def fake_gcs_store() -> tuple[GCSBlobStore, FakeGCSClient]:
    client = FakeGCSClient()
    return GCSBlobStore(BUCKET, client=client), client  # pyright: ignore[reportArgumentType]


@pytest.fixture(params=["filesystem", "gcs", "gcs-in-memory"])
def store(request: pytest.FixtureRequest, tmp_path: Any) -> BlobStore:
    """Each adapter. ``gcs`` is the real client against the fake GCS server (needs Docker);
    ``gcs-in-memory`` is the same adapter over an in-memory stand-in for the client,
    so its own logic runs in every suite."""
    if request.param == "gcs":
        return request.getfixturevalue("gcs")
    if request.param == "gcs-in-memory":
        return fake_gcs_store()[0]
    return FilesystemBlobStore(tmp_path)


async def _all(chunks: AsyncIterator[bytes]) -> bytes:
    return b"".join([chunk async for chunk in chunks])


def _key() -> str:
    return f"artifacts/art_{time.monotonic_ns()}"


# ------------------------------------------------------------------ the port, both adapters


async def test_a_put_is_read_back_verified_in_chunks(store, monkeypatch) -> None:
    monkeypatch.setattr("agent_runs.blob.filesystem.BLOB_CHUNK_BYTES", 7)
    monkeypatch.setattr("agent_runs.blob.gcs.BLOB_CHUNK_BYTES", 7)
    data = b"the quick brown fox jumps over the lazy dog" * 3
    key = _key()
    stored = await store.put(key, data, content_type="text/plain")
    assert (stored.key, stored.size) == (key, len(data))
    assert stored.sha256 == hashlib.sha256(data).hexdigest()
    assert await _all(read(store, key, sha256=stored.sha256, size=stored.size)) == data


async def test_an_object_is_never_overwritten(store) -> None:
    key = _key()
    await store.put(key, b"first", content_type="text/plain")
    with pytest.raises(BlobExists):
        await store.put(key, b"second", content_type="text/plain")
    assert await _all(store.chunks(key)) == b"first"


async def test_a_delete_is_idempotent_and_a_missing_key_is_not_found(store) -> None:
    key = _key()
    await store.put(key, b"bytes", content_type="text/plain")
    await store.delete(key)
    await store.delete(key)
    with pytest.raises(BlobNotFound):
        await _all(store.chunks(key))


async def test_bytes_that_do_not_match_never_arrive_whole(store, monkeypatch) -> None:
    """The last chunk is held back until the hash checks, so a reader that gets every byte
    of a corrupted object does not exist."""
    monkeypatch.setattr("agent_runs.blob.filesystem.BLOB_CHUNK_BYTES", 4)
    monkeypatch.setattr("agent_runs.blob.gcs.BLOB_CHUNK_BYTES", 4)
    data = b"0123456789abcdef"
    key = _key()
    await store.put(key, data, content_type="text/plain")
    got: list[bytes] = []
    with pytest.raises(BlobCorrupt):
        async for chunk in read(store, key, sha256=hashlib.sha256(b"other").hexdigest(), size=16):
            got.append(chunk)
    assert b"".join(got) == data[:-4]
    with pytest.raises(BlobCorrupt):
        await _all(read(store, key, sha256=hashlib.sha256(data).hexdigest(), size=17))


async def test_read_skips_empty_chunks_and_an_empty_object_reads_as_nothing() -> None:
    class Chunks:
        def __init__(self, *parts: bytes) -> None:
            self._parts = parts

        async def chunks(self, key: str) -> AsyncIterator[bytes]:
            for part in self._parts:
                yield part

    data = b"abcdef"
    sha = hashlib.sha256(data).hexdigest()
    store = Chunks(b"", b"abc", b"", b"def", b"")
    assert await _all(read(store, "k", sha256=sha, size=6)) == data  # pyright: ignore[reportArgumentType]
    empty = hashlib.sha256(b"").hexdigest()
    assert await _all(read(Chunks(), "k", sha256=empty, size=0)) == b""  # pyright: ignore[reportArgumentType]


async def test_the_gcs_adapter_writes_create_only_with_the_sha256_in_metadata() -> None:
    store, client = fake_gcs_store()
    stored = await store.put("artifacts/a", b"bytes", content_type="application/json")
    data, content_type, metadata = client.buckets[BUCKET].objects["artifacts/a"]
    assert (data, content_type) == (b"bytes", "application/json")
    assert metadata == {"sha256": stored.sha256}
    await store.aclose()
    assert client.closed


async def test_a_gcs_object_deleted_mid_read_is_not_found(monkeypatch) -> None:
    monkeypatch.setattr("agent_runs.blob.gcs.BLOB_CHUNK_BYTES", 2)
    store, client = fake_gcs_store()
    await store.put("artifacts/a", b"abcdef", content_type="text/plain")
    chunks = store.chunks("artifacts/a")
    assert await anext(chunks) == b"ab"
    del client.buckets[BUCKET].objects["artifacts/a"]
    with pytest.raises(BlobNotFound):
        await anext(chunks)


def test_the_filesystem_store_refuses_keys_that_leave_its_root(tmp_path) -> None:
    store = FilesystemBlobStore(tmp_path)
    for key in ("../escape", "a//b", "", "a/./b"):
        with pytest.raises(ValueError, match="invalid blob key"):
            store._path(key)


# ------------------------------------------------------------------ settings


def test_without_a_bucket_artifacts_go_to_the_filesystem(tmp_path) -> None:
    for bucket in (None, ""):
        store = open_blob_store(BlobSettings(root=tmp_path, bucket=bucket))
        assert isinstance(store, FilesystemBlobStore)


def test_gcs_settings_open_the_gcs_adapter_on_their_bucket(monkeypatch) -> None:
    opened: list[str] = []

    class Recorded:
        def __init__(self, bucket: str) -> None:
            opened.append(bucket)

    monkeypatch.setattr("agent_runs.blob.GCSBlobStore", Recorded)
    store = open_blob_store(BlobSettings(bucket="b-1"))
    assert isinstance(store, Recorded) and opened == ["b-1"]


def test_gcs_is_chosen_with_a_bucket(fake_gcs, monkeypatch) -> None:
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", fake_gcs)
    settings = BlobSettings(bucket=BUCKET)
    assert isinstance(open_blob_store(settings), GCSBlobStore)


# ------------------------------------------------------------------ end to end on GCS


async def test_artifacts_live_in_gcs_end_to_end(app, client, ticker, gcs) -> None:
    """Upload through the API, read back, then the ticker deletes the object from the bucket
    once the run has been over for the retention."""
    app.state.blobs = gcs
    ticker._blobs = gcs
    run = (await client.post("/v1/runs", json=started())).json()
    table = b'{"columns": ["sku"], "rows": [["A-1"]]}'
    response = await client.post(
        f"/v1/runs/{run['run_id']}/artifacts",
        content=table,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 201, response.text
    ref = response.json()
    got = await client.get(ref["uri"])
    assert got.content == table and got.headers["content-type"] == "application/json"
    key = f"artifacts/{ref['artifact_id']}"
    blob = gcs._bucket.get_blob(key)
    assert blob is not None and blob.metadata == {"sha256": ref["checksum"][len("sha256:") :]}

    await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "SUCCESS"})
    report = await ticker.tick(now=now() + ARTIFACT_RETENTION + timedelta(minutes=1))
    assert report.purged == 1
    assert gcs._bucket.get_blob(key) is None
    assert (await client.get(ref["uri"])).status_code == 404
