"""The filesystem blob store: ``<root>/<key>``. For one machine, or several sharing a volume
(the API and the ticker both need it). Writes are atomic (a temporary file, fsync, rename)
and create-only."""

from __future__ import annotations

import asyncio
import contextlib
import os
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path

from agent_runs.blob.port import BlobExists, BlobNotFound, StoredBlob, sha256_hex
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
        path = self._path(key)
        await asyncio.to_thread(self._write, path, data)
        return StoredBlob(key=key, size=len(data), sha256=sha256_hex(data))

    @staticmethod
    def _write(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".part")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            # create-only: link fails when the key exists, where a rename would replace it
            try:
                os.link(tmp, path)
            except FileExistsError as exc:
                raise BlobExists(str(path)) from exc
        finally:
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
