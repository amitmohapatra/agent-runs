"""Blob storage for run artifacts: one port, a filesystem and a GCS adapter; GCS exactly when
``RUNS__BLOB__BUCKET`` is set."""

from __future__ import annotations

from agent_runs.blob.filesystem import FilesystemBlobStore
from agent_runs.blob.gcs import GCSBlobStore
from agent_runs.blob.port import (
    BlobCorrupt,
    BlobExists,
    BlobNotFound,
    BlobStore,
    StoredBlob,
    read,
)
from agent_runs.config.settings import BlobSettings


def open_blob_store(settings: BlobSettings) -> BlobStore:
    if settings.bucket:
        return GCSBlobStore(settings.bucket)
    return FilesystemBlobStore(settings.root)


__all__ = [
    "BlobCorrupt",
    "BlobExists",
    "BlobNotFound",
    "BlobStore",
    "StoredBlob",
    "open_blob_store",
    "read",
]
