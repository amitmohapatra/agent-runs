"""The blob store port: where run artifacts' bytes live. The database keeps what an artifact
is (tenant, run, mime, size, checksum); the store keeps only bytes, under a key.

The Memory Service's semantics, reduced to what artifacts need: objects are immutable (a put
never overwrites), the SHA-256 of the bytes is computed on write, and a read is verified
against the checksum the database recorded, so corrupted or truncated bytes are never served
as the artifact.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from typing import Protocol

from pydantic import BaseModel, ConfigDict


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


async def one_chunk(data: bytes) -> AsyncIterator[bytes]:
    """``data`` as a stream, so ``put`` is ``put_stream`` of one chunk."""
    yield data


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
