"""The Google Cloud Storage blob store: ``gs://<bucket>/<key>``. Credentials are the
environment's (Application Default Credentials); ``STORAGE_EMULATOR_HOST`` points the
client at an emulator. The client is synchronous, so every call runs in a thread.

Uploads are create-only (``if_generation_match=0``) and carry a CRC32C the server checks;
the SHA-256 is kept in the object's metadata as well as in the database. Reads pin the
generation they started on."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

from google.api_core import exceptions as gexc
from google.cloud import storage

from agent_runs.blob.port import BlobExists, BlobNotFound, StoredBlob, sha256_hex
from agent_runs.config.constants import BLOB_CHUNK_BYTES


class GCSBlobStore:
    def __init__(self, bucket: str, *, client: storage.Client | None = None) -> None:
        self._client = client or storage.Client()
        self._bucket = self._client.bucket(bucket)

    async def put(self, key: str, data: bytes, *, content_type: str) -> StoredBlob:
        digest = sha256_hex(data)

        def upload() -> None:
            blob = self._bucket.blob(key)
            blob.metadata = {"sha256": digest}
            try:
                blob.upload_from_string(
                    data, content_type=content_type, if_generation_match=0, checksum="crc32c"
                )
            except gexc.PreconditionFailed as exc:
                raise BlobExists(key) from exc

        await asyncio.to_thread(upload)
        return StoredBlob(key=key, size=len(data), sha256=digest)

    async def chunks(self, key: str) -> AsyncIterator[bytes]:
        blob = await asyncio.to_thread(self._bucket.get_blob, key)
        if blob is None:
            raise BlobNotFound(key)
        size = int(blob.size or 0)
        for start in range(0, size, BLOB_CHUNK_BYTES):
            end = min(start + BLOB_CHUNK_BYTES, size) - 1
            try:
                yield await asyncio.to_thread(blob.download_as_bytes, start=start, end=end)
            except gexc.NotFound as exc:
                raise BlobNotFound(key) from exc

    async def delete(self, key: str) -> None:
        def remove() -> None:
            with contextlib.suppress(gexc.NotFound):
                self._bucket.blob(key).delete()

        await asyncio.to_thread(remove)

    async def aclose(self) -> None:
        await asyncio.to_thread(self._client.close)
