"""Run payloads by reference (the claim-check pattern): a run's input, output, the payload it
waits on, its answers, its checkpoint and its event log are stored as immutable,
content-addressed objects, and the run's row holds only a :class:`PayloadRef` to each.

One format for every store (the service's blob store, on GCS or disk, and a local runner's
folder): compact JSON, its object keys in PostgreSQL ``jsonb`` order (what an inline ``jsonb``
column hands back, so a value reads the same either way), compressed with zstd at level 3.
The key is the stored bytes' SHA-256 under the tenant, sharded by the tenant's hash so no one
prefix is hot, as the Memory Service lays out its archive
(``tenant-shard=<n>/tenant=<t>/blobs/sha256=<hex>.json.zst``): the same content is stored
once, and a put of a key that exists is skipped.

A checkpoint may be a list of segments instead of one object: ``{SEGMENTS: [ref, ...]}``,
oldest first, each segment holding what changed since the one before (a resume journal saved
as it grows). Whoever wrote the segments reads and applies them.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Final

import zstandard
from pydantic import BaseModel, ConfigDict

#: What a payload object is: JSON, zstd-compressed.
MEDIA_TYPE: Final = "application/vnd.trellis.payload+json+zstd"
#: Several payload objects in one answer: each after its length, 8 bytes big-endian.
FRAMED_MEDIA_TYPE: Final = "application/vnd.trellis.payloads"
#: zstd's default level: measured on resume journals of 0.1-12 MiB it stores 4-9 % of the
#: JSON at ~10x gzip's speed (benchmark/payloads.md).
ZSTD_LEVEL: Final = 3
#: Payloads past this many bytes are compressed and decompressed in a thread, off the event
#: loop (zstd at level 3 does ~1 MiB in ~1 ms; a thread hop costs ~50 us).
OFFLOAD_BYTES: Final = 256 * 1024
#: The checkpoint key that lists a checkpoint's segments.
SEGMENTS: Final = "trellis_segments"
#: The length of a SHA-256 in hex.
SHA256_HEX: Final = 64
#: Key prefixes a tenant's objects are spread over (the Memory Service's ``tenant_shards``).
TENANT_SHARDS: Final = 64
#: A payload object's file name ends in this.
SUFFIX: Final = ".json.zst"
#: The most a payload may decompress to: a frame claiming more is refused, not allocated.
MAX_DECODED_BYTES: Final = 512 * 1024 * 1024


class PayloadRef(BaseModel):
    """Where a payload is and what it must read back as: the store key, the stored size, the
    SHA-256 (hex) of the stored bytes, and the media type."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    ref: str
    size: int
    sha256: str
    media_type: str = MEDIA_TYPE


def jsonb_order(value: Any) -> Any:
    """``value`` with every object's keys in the order PostgreSQL ``jsonb`` keeps them:
    shorter keys first, then by their UTF-8 bytes."""
    if isinstance(value, dict):
        keys = sorted(value, key=lambda k: (len(k.encode()), k.encode()))
        return {k: jsonb_order(value[k]) for k in keys}
    if isinstance(value, list | tuple):
        return [jsonb_order(item) for item in value]
    return value


def _dump(value: Any) -> bytes:
    """Compact JSON (``ValueError`` for what JSON cannot hold: ``jsonb`` refuses NaN and
    infinities too)."""
    text = json.dumps(
        jsonb_order(value), separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    return text.encode()


def _compress(data: bytes) -> bytes:
    return zstandard.ZstdCompressor(level=ZSTD_LEVEL).compress(data)


def encode(value: Any) -> bytes:
    """The stored bytes of a JSON value."""
    return _compress(_dump(value))


def decode(data: bytes) -> Any:
    """The JSON value of stored bytes (``ValueError`` when they are not a payload)."""
    try:
        size = zstandard.frame_content_size(data)
        if not 0 <= size <= MAX_DECODED_BYTES:
            raise ValueError(f"a payload object decodes to at most {MAX_DECODED_BYTES} bytes")
        return json.loads(zstandard.ZstdDecompressor().decompress(data))
    except zstandard.ZstdError as exc:
        raise ValueError(f"not a payload object: {exc}") from exc


async def aencode(value: Any) -> bytes:
    """:func:`encode`, compressing a large payload in a thread."""
    raw = _dump(value)
    return await asyncio.to_thread(_compress, raw) if len(raw) > OFFLOAD_BYTES else _compress(raw)


async def adecode(data: bytes) -> Any:
    """:func:`decode`, a large payload in a thread (compressed ~10x: a tenth of the bound)."""
    return (
        await asyncio.to_thread(decode, data) if len(data) > OFFLOAD_BYTES // 10 else decode(data)
    )


def tenant_shard(tenant_id: str) -> int:
    """The shard a tenant's keys start with (the Memory Service's ``tenant_shard``)."""
    return int(hashlib.sha256(tenant_id.encode()).hexdigest()[:8], 16) % TENANT_SHARDS


def _prefix(tenant_id: str) -> str:
    return f"tenant-shard={tenant_shard(tenant_id):03d}/tenant={tenant_id}/blobs/sha256="


def key_of(tenant_id: str, sha256: str) -> str:
    """The key of ``tenant_id``'s object whose bytes have this SHA-256."""
    return f"{_prefix(tenant_id)}{sha256}{SUFFIX}"


def ref_of(tenant_id: str, data: bytes) -> PayloadRef:
    """The reference ``data`` is stored under for ``tenant_id``: its content's address."""
    digest = hashlib.sha256(data).hexdigest()
    return PayloadRef(ref=key_of(tenant_id, digest), size=len(data), sha256=digest)


def owned_by(ref: PayloadRef, tenant_id: str) -> bool:
    """Is ``ref`` one of ``tenant_id``'s payload objects (its well-formed key)?"""
    return ref.ref == key_of(tenant_id, ref.sha256) and len(ref.sha256) == SHA256_HEX


def frames(data: bytes) -> list[bytes]:
    """The objects of a :data:`FRAMED_MEDIA_TYPE` answer, in order (``ValueError`` when it
    is cut short)."""
    found, at = [], 0
    while at < len(data):
        size = int.from_bytes(data[at : at + 8], "big")
        at += 8
        if at + size > len(data):
            raise ValueError("payload objects cut short")
        found.append(data[at : at + size])
        at += size
    return found
