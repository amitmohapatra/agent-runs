"""The blob port's reader and its filesystem adapter (agent-runs' artifacts and run payloads,
and a local runner's payload objects): create-only, atomic writes; reads verified as they
stream. agent-runs' suite runs the same port contract against its GCS adapter too."""

from __future__ import annotations

import hashlib
import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from trellis.runs.blob import (
    BlobCorrupt,
    BlobExists,
    BlobNotFound,
    FilesystemBlobStore,
    read,
    sha256_hex,
)


async def _all(chunks: AsyncIterator[bytes]) -> bytes:
    return b"".join([chunk async for chunk in chunks])


async def test_a_put_reads_back_once_and_is_never_overwritten(tmp_path: Path) -> None:
    store = FilesystemBlobStore(tmp_path)
    stored = await store.put("t/blobs/a", b"bytes", content_type="text/plain")
    assert (stored.size, stored.sha256) == (5, sha256_hex(b"bytes"))
    assert await _all(read(store, "t/blobs/a", sha256=stored.sha256, size=5)) == b"bytes"
    with pytest.raises(BlobExists):
        await store.put("t/blobs/a", b"other", content_type="text/plain")
    with pytest.raises(BlobCorrupt):
        await _all(read(store, "t/blobs/a", sha256=stored.sha256, size=6))
    await store.delete("t/blobs/a")
    await store.delete("t/blobs/a")
    with pytest.raises(BlobNotFound):
        await _all(store.chunks("t/blobs/a"))
    await store.aclose()


async def test_a_stream_is_written_as_it_arrives(tmp_path: Path) -> None:
    async def parts() -> AsyncIterator[bytes]:
        yield b"one "
        yield b"two"

    store = FilesystemBlobStore(tmp_path)
    stored = await store.put_stream("t/s", parts(), content_type="text/plain")
    assert (stored.size, stored.sha256) == (7, sha256_hex(b"one two"))
    assert await _all(store.chunks("t/s")) == b"one two"


async def test_a_stream_that_fails_leaves_nothing_behind(tmp_path: Path) -> None:
    async def broken() -> AsyncIterator[bytes]:
        yield b"half"
        raise OSError("the client went away")

    store = FilesystemBlobStore(tmp_path)
    with pytest.raises(OSError, match="went away"):
        await store.put_stream("t/x", broken(), content_type="text/plain")
    assert not [f for _, _, files in os.walk(tmp_path) for f in files], "nothing is left"


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


def test_the_filesystem_store_refuses_keys_that_leave_its_root(tmp_path: Path) -> None:
    store = FilesystemBlobStore(tmp_path)
    for key in ("../escape", "a//b", "", "a/./b"):
        with pytest.raises(ValueError, match="invalid blob key"):
            store._path(key)
