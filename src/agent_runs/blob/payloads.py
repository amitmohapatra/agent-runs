"""Run payloads in the blob store, by reference (``trellis.runs.payloads`` is the format).

A write registers the object's key (``payload_blobs``, in its own short transaction) before
it writes the bytes, and returns the :class:`PayloadRef` the caller's row then holds: the
bytes are durable before any row refers to them, and an object whose row never committed
(a crash, a lost lease) is an orphan the ticker deletes a day later (:meth:`sweep`). Writes
are create-only and keyed by content, so the same content is stored once and a second
write of it (two workers, a retry) is skipped.

A read verifies the bytes against the reference's size and SHA-256: a missing or corrupt
object is :class:`PayloadLost`, never wrong data; a store that does not answer is
``Unavailable`` (retryable). Several references read at once, a bounded number at a time.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import Any, Final

from google.api_core import exceptions as gexc
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from trellis.runs.blob import BlobCorrupt, BlobExists, BlobNotFound, BlobStore, read
from trellis.runs.payloads import MEDIA_TYPE, PayloadRef, adecode, aencode, ref_of

from agent_runs.config.constants import ARTIFACT_RETENTION
from agent_runs.domain.errors import ServiceError, Unavailable
from agent_runs.store.tables import PayloadBlobRow, RunPayloadRow, WebhookDeliveryRow

#: Objects read at once by one :meth:`PayloadStore.get_many` (a resume's segments, a page's
#: payloads): enough to hide a store's latency, few enough not to flood it.
PARALLEL_READS: Final = 16
#: How long an object no row refers to is kept before the sweep deletes it: far longer than
#: any write takes between registering an object and committing the row that refers to it.
ORPHAN_AGE: Final = timedelta(hours=24)

#: What a store raises when it cannot be reached (disk, network, the provider's 5xx).
_UNREACHABLE = (OSError, gexc.ServerError, gexc.TooManyRequests, gexc.RetryError)

Sessions = async_sessionmaker[AsyncSession]
#: Where a session carries the payload store its stores read and write through.
_PAYLOADS: Final = "payloads"


class PayloadLost(ServiceError):
    """A payload a row refers to is missing from the store or does not match its checksum."""


class PayloadStore:
    """The blob store's payload objects, and their registry."""

    def __init__(
        self, blobs: BlobStore, sessions: Sessions, *, retention: timedelta = ARTIFACT_RETENTION
    ) -> None:
        self._blobs = blobs
        self._sessions = sessions
        #: how long an ended run's artifacts are kept (``RUNS__BLOB__RETENTION_DAYS``)
        self.retention = retention

    # ------------------------------------------------------------------ writes
    async def put(self, tenant_id: str, value: Any) -> PayloadRef:
        """Store a JSON value for ``tenant_id``; its reference."""
        return await self.put_encoded(tenant_id, await aencode(value))

    async def put_encoded(self, tenant_id: str, data: bytes) -> PayloadRef:
        """Store bytes already in the payload format (an uploaded segment); their reference."""
        ref = ref_of(tenant_id, data)
        await self._register(tenant_id, [ref])
        try:
            await self._blobs.put(ref.ref, data, content_type=MEDIA_TYPE)
        except BlobExists:
            pass  # the same content, already stored: a key names its bytes
        except _UNREACHABLE as exc:
            raise Unavailable("the payload store is unavailable") from exc
        return ref

    async def _register(self, tenant_id: str, refs: Sequence[PayloadRef]) -> None:
        """Mark the objects as just written, committed before their bytes are: the sweep
        leaves them for :data:`ORPHAN_AGE`, and a sweep deleting one right now finishes
        before this returns (the row lock), so the write that follows stores it again."""
        async with self._sessions() as db:
            upsert = insert(PayloadBlobRow).values(
                [{"key": r.ref, "tenant_id": tenant_id, "size": r.size} for r in refs]
            )
            await db.execute(
                upsert.on_conflict_do_update(
                    index_elements=[PayloadBlobRow.key], set_={"touched_at": func.now()}
                )
            )
            await db.commit()

    # ------------------------------------------------------------------ reads
    async def get(self, ref: PayloadRef | dict[str, Any]) -> Any:
        """The JSON value ``ref`` names, verified."""
        return await adecode(await self.get_encoded(PayloadRef.model_validate(ref)))

    async def get_encoded(self, ref: PayloadRef) -> bytes:
        """The stored bytes ``ref`` names, verified."""
        chunks: list[bytes] = []
        try:
            async for chunk in read(self._blobs, ref.ref, sha256=ref.sha256, size=ref.size):
                chunks.append(chunk)
        except (BlobNotFound, BlobCorrupt) as exc:
            raise PayloadLost(f"payload {ref.ref} is missing or corrupt") from exc
        except _UNREACHABLE as exc:
            raise Unavailable("the payload store is unavailable") from exc
        return b"".join(chunks)

    async def get_many(self, refs: Iterable[PayloadRef | dict[str, Any]]) -> list[Any]:
        """The values of ``refs``, in their order, read :data:`PARALLEL_READS` at a time."""
        gate = asyncio.Semaphore(PARALLEL_READS)

        async def one(ref: PayloadRef | dict[str, Any]) -> Any:
            async with gate:
                return await self.get(ref)

        return list(await asyncio.gather(*(one(ref) for ref in refs)))

    # ------------------------------------------------------------------ the sweep
    async def sweep(self, *, now: datetime, limit: int) -> int:
        """Delete up to ``limit`` objects no run and no webhook delivery refers to, registered
        more than :data:`ORPHAN_AGE` ago: each object, then its registry row (a failed
        delete is tried again next time). Returns how many went."""
        linked = select(RunPayloadRow.key).where(RunPayloadRow.key == PayloadBlobRow.key)
        sent = select(WebhookDeliveryRow.delivery_id).where(
            WebhookDeliveryRow.payload_ref["ref"].astext == PayloadBlobRow.key
        )
        async with self._sessions() as db:
            keys = (
                await db.scalars(
                    select(PayloadBlobRow.key)
                    .where(
                        PayloadBlobRow.touched_at < now - ORPHAN_AGE,
                        ~linked.exists(),
                        ~sent.exists(),
                    )
                    .order_by(PayloadBlobRow.touched_at)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            gone: list[str] = []
            for key in keys:
                try:
                    await self._blobs.delete(key)
                except _UNREACHABLE:
                    continue
                gone.append(key)
            if gone:
                await db.execute(delete(PayloadBlobRow).where(PayloadBlobRow.key.in_(gone)))
            await db.commit()
        return len(gone)


def bind(
    sessions: Sessions, blobs: BlobStore, *, retention: timedelta = ARTIFACT_RETENTION
) -> PayloadStore:
    """The payload store over ``blobs``, carried by every session ``sessions`` opens."""
    store = PayloadStore(blobs, sessions, retention=retention)
    sessions.configure(info={_PAYLOADS: store})
    return store


def payloads_of(session: AsyncSession) -> PayloadStore:
    """The payload store ``session``'s maker was bound to (:func:`bind`)."""
    return session.info[_PAYLOADS]
