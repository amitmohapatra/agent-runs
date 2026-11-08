"""The blob store port: where run artifacts' and run payloads' bytes live, and its filesystem
adapter (agent-runs adds GCS). The database keeps what an object is; the store keeps only
bytes, under a key. A local runner without agent-runs (the harness's ``LocalRuns``) keeps its
payload objects in the same filesystem store.

The Memory Service's semantics, reduced to what artifacts need: objects are immutable (a put
never overwrites), the SHA-256 of the bytes is computed on write, and a read is verified
against the checksum the database recorded, so corrupted or truncated bytes are never served
as the artifact.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import BinaryIO, Final, Protocol

from pydantic import BaseModel, ConfigDict

#: The unit a blob is read and streamed in.
BLOB_CHUNK_BYTES: Final = 1024 * 1024


class StoredBlob(BaseModel):
    """What a put wrote: the key, its size, and the SHA-256 (hex) of its bytes."""

    model_config = ConfigDict(frozen=True)

    key: str
    size: int
    sha256: str


class BlobNotFound(Exception):
    pass


class BlobExists(Exception):
    """A put to a key that already holds an object: objects are never overwritten."""


class BlobCorrupt(Exception):
    """The bytes read do not match the checksum (or size) recorded when they were written."""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class BlobStore(Protocol):
    async def put(self, key: str, data: bytes, *, content_type: str) -> StoredBlob:
        """Create ``key`` holding ``data``; ``BlobExists`` when it already exists."""
        ...

    async def put_stream(
        self, key: str, chunks: AsyncIterator[bytes], *, content_type: str
    ) -> StoredBlob:
        """Create ``key`` from ``chunks`` as they arrive, hashing as it goes, holding at most
        a chunk (or a bounded spool) in memory. An exception from ``chunks`` (a body past
        its cap) leaves nothing behind and propagates."""
        ...

    def chunks(self, key: str) -> AsyncIterator[bytes]:
        """The object's bytes in order, unverified (``read`` verifies);
        ``BlobNotFound`` when there is none."""
        ...

    async def delete(self, key: str) -> None:
        """Remove ``key``; a key that is already gone is not an error."""
        ...

    async def aclose(self) -> None: ...


async def read(store: BlobStore, key: str, *, sha256: str, size: int) -> AsyncIterator[bytes]:
    """The object's bytes, verified against ``sha256`` and ``size`` as they stream. The last
    chunk is held back until the whole object has been hashed, so a corrupted object never
    reaches a caller complete: ``BlobCorrupt`` is raised instead of the final chunk."""
    digest = hashlib.sha256()
    seen = 0
    held: bytes | None = None
    async for chunk in store.chunks(key):
        if not chunk:
            continue
        if held is not None:
            yield held
        digest.update(chunk)
        seen += len(chunk)
        held = chunk
    if seen != size or digest.hexdigest() != sha256:
        raise BlobCorrupt(f"{key}: {seen} bytes, sha256 {digest.hexdigest()}; expected {size}")
    if held is not None:
        yield held


class FilesystemBlobStore:
    """The filesystem blob store: ``<root>/<key>``. For one machine, or several sharing a
    volume (agent-runs' API and ticker both need it). Writes are atomic (a temporary file,
    fsync, a create-only link) and create-only."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    def _path(self, key: str) -> Path:
        parts = key.split("/")
        if not key or any(part in {"", ".", ".."} for part in parts):
            raise ValueError(f"invalid blob key {key!r}")
        return self._root.joinpath(*parts)

    async def put(self, key: str, data: bytes, *, content_type: str) -> StoredBlob:
        """Bytes already in hand: written in one thread (one hop off the event loop, not one
        per step), as ``put_stream`` writes them."""
        path = self._path(key)
        await asyncio.to_thread(self._write, path, data)
        return StoredBlob(key=key, size=len(data), sha256=sha256_hex(data))

    def _write(self, path: Path, data: bytes) -> None:
        fh, tmp = self._temporary(path)
        try:
            try:
                fh.write(data)
            finally:
                self._close(fh)
            self._link(tmp, path)
        finally:
            self._discard(tmp)

    async def put_stream(
        self, key: str, chunks: AsyncIterator[bytes], *, content_type: str
    ) -> StoredBlob:
        """Each chunk to a temporary file next to the key as it arrives, then fsync and a
        create-only link into place; the temporary file goes whatever happens."""
        path = self._path(key)
        fh, tmp = await asyncio.to_thread(self._temporary, path)
        digest = hashlib.sha256()
        size = 0
        try:
            try:
                async for chunk in chunks:
                    digest.update(chunk)
                    size += len(chunk)
                    await asyncio.to_thread(fh.write, chunk)
            finally:
                await asyncio.to_thread(self._close, fh)
            await asyncio.to_thread(self._link, tmp, path)
        finally:
            await asyncio.to_thread(self._discard, tmp)
        return StoredBlob(key=key, size=size, sha256=digest.hexdigest())

    @staticmethod
    def _temporary(path: Path) -> tuple[BinaryIO, str]:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".part")
        return os.fdopen(fd, "wb"), tmp

    @staticmethod
    def _close(fh: BinaryIO) -> None:
        with fh:
            fh.flush()
            os.fsync(fh.fileno())

    @staticmethod
    def _link(tmp: str, path: Path) -> None:
        # create-only: link fails when the key exists, where a rename would replace it
        try:
            os.link(tmp, path)
        except FileExistsError as exc:
            raise BlobExists(str(path)) from exc

    @staticmethod
    def _discard(tmp: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)

    async def chunks(self, key: str) -> AsyncIterator[bytes]:
        path = self._path(key)
        try:
            fh = await asyncio.to_thread(path.open, "rb")
        except FileNotFoundError as exc:
            raise BlobNotFound(key) from exc
        try:
            while chunk := await asyncio.to_thread(fh.read, BLOB_CHUNK_BYTES):
                yield chunk
        finally:
            await asyncio.to_thread(fh.close)

    async def delete(self, key: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            await asyncio.to_thread(self._path(key).unlink)

    async def aclose(self) -> None:
        return None
