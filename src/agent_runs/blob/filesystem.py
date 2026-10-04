"""The filesystem blob store: ``<root>/<key>``. For one machine, or several sharing a volume
(the API and the ticker both need it). Writes are atomic (a temporary file, fsync, rename)
and create-only."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import BinaryIO

from agent_runs.blob.port import BlobExists, BlobNotFound, StoredBlob, one_chunk
from agent_runs.config.constants import BLOB_CHUNK_BYTES


class FilesystemBlobStore:
    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    def _path(self, key: str) -> Path:
        parts = key.split("/")
        if not key or any(part in {"", ".", ".."} for part in parts):
            raise ValueError(f"invalid blob key {key!r}")
        return self._root.joinpath(*parts)

    async def put(self, key: str, data: bytes, *, content_type: str) -> StoredBlob:
        return await self.put_stream(key, one_chunk(data), content_type=content_type)

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
