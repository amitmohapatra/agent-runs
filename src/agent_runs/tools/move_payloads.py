"""Move the run payloads an earlier release kept inline to the blob store (``agent-runs-payloads``).

The migrate step of the change-over, run once per deployment after the release that writes
references (rows written since hold references already). Three phases, each in batches, each
safe to stop and run again (a batch commits as a whole, and each phase selects only what is
left to do):

``copy``    stores each inline payload as an object and sets the row's reference beside it;
            the inline value stays, and readers already read the reference.
``verify``  reads every copied object back (its size and SHA-256 checked) and compares it with
            the inline value; it changes nothing. Prints the counts.
``clear``   verifies everything first and stops, changing nothing, unless every copy matches;
            then clears the inline values, checking each row again as it clears it.

What moves: a run's input, output, last answer, checkpoint and the payload of the question it
waits on; a resolution's question and answer; the payload of the question a webhook delivery
carries. An ended run's event log is compacted by the ticker. The inline columns themselves
are dropped by a later release, once ``clear`` has run.

``python -m agent_runs.tools.move_payloads copy|verify|clear [--batch N] [--batches N]``
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from sqlalchemy import ColumnElement, func, select, text, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_runs.blob import open_blob_store
from agent_runs.blob.payloads import PayloadLost, bind, payloads_of
from agent_runs.config.settings import get_settings
from agent_runs.store.database import connect
from agent_runs.store.tables import ResolutionRow, RunPayloadRow, RunRow, WebhookDeliveryRow

#: Rows a batch moves, in one transaction.
BATCH: Final = 200
PHASES: Final = ("copy", "verify", "clear")

Sessions = async_sessionmaker[AsyncSession]


def _present(column: Any) -> ColumnElement[bool]:
    """A JSON value that is not ``null`` (SQL ``NULL`` or JSON ``null``: nothing to move)."""
    return func.coalesce(func.jsonb_typeof(column), "null") != "null"


def _absent(column: Any) -> ColumnElement[bool]:
    """No reference (SQL ``NULL``, or the JSON ``null`` an earlier write may have stored)."""
    return ~_present(column)


def _asked(column: Any) -> ColumnElement[bool]:
    """An interrupt (``awaiting``) that carries its ``payload`` inline."""
    return _present(column) & column.has_key("payload")


def _body_asked() -> ColumnElement[bool]:
    return text("payload #> '{data,run,awaiting}' ? 'payload'")  # type: ignore[return-value]


def _without_payload(asked: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in asked.items() if k != "payload"}


def _body_without_payload(body: dict[str, Any]) -> dict[str, Any]:
    run = body["data"]["run"]
    return {
        **body,
        "data": {**body["data"], "run": {**run, "awaiting": _without_payload(run["awaiting"])}},
    }


@dataclass(frozen=True)
class Moved:
    """One payload column: where its inline value is, the reference it moves to, and how."""

    name: str
    model: Any
    ref: str
    #: rows whose inline value is there to move
    inline: ColumnElement[bool]
    value: Callable[[Any], Any]
    #: the row with its inline value cleared
    cleared: Callable[[Any], None]
    tenant: Callable[[Any], str]
    #: the run that refers to the object, and as what (``run_payloads``); none for a delivery
    run: Callable[[Any], str | None] = field(default=lambda row: None)
    kind: str | None = None


def _inline(name: str) -> Callable[[Any], None]:
    def clear(row: Any) -> None:
        setattr(row, name, None)

    return clear


def _strip_asked(row: RunRow) -> None:
    row.awaiting = _without_payload(row.awaiting or {})


def _strip_body(row: WebhookDeliveryRow) -> None:
    row.payload = _body_without_payload(row.payload)


def _run_column(name: str, kind: str) -> Moved:
    column = getattr(RunRow, name)
    return Moved(
        name=f"agent_runs.{name}",
        model=RunRow,
        ref=f"{name}_ref",
        inline=_present(column),
        value=lambda row: getattr(row, name),
        cleared=_inline(name),
        tenant=lambda row: row.tenant_id,
        run=lambda row: row.run_id,
        kind=kind,
    )


def _resolution_column(name: str) -> Moved:
    return Moved(
        name=f"run_resolutions.{name}",
        model=ResolutionRow,
        ref=f"{name}_ref",
        inline=_present(getattr(ResolutionRow, name)),
        value=lambda row: getattr(row, name),
        cleared=_inline(name),
        tenant=lambda row: row.tenant_id,
        run=lambda row: row.run_id,
        kind="answer",
    )


#: Every payload column an earlier release kept inline (``run_payloads.kind`` as the writers
#: link them).
MOVED: Final = (
    _run_column("input", "input"),
    _run_column("output", "output"),
    _run_column("last_resolution", "answer"),
    _run_column("checkpoint", "checkpoint"),
    Moved(
        name="agent_runs.awaiting.payload",
        model=RunRow,
        ref="awaiting_ref",
        inline=_asked(RunRow.awaiting),
        value=lambda row: row.awaiting["payload"],
        cleared=_strip_asked,
        tenant=lambda row: row.tenant_id,
        run=lambda row: row.run_id,
        kind="awaiting",
    ),
    _resolution_column("interrupt"),
    _resolution_column("resolution"),
    Moved(
        name="webhook_deliveries.payload",
        model=WebhookDeliveryRow,
        ref="payload_ref",
        inline=_body_asked(),
        value=lambda row: row.payload["data"]["run"]["awaiting"]["payload"],
        cleared=_strip_body,
        tenant=lambda row: row.payload["tenant_id"],
    ),
)


@dataclass
class Counts:
    """What a phase did to one column."""

    column: str
    copied: int = 0
    verified: int = 0
    mismatched: int = 0
    lost: int = 0
    cleared: int = 0


async def copy(sessions: Sessions, moved: Moved, *, batch: int, batches: int | None) -> Counts:
    """Store each inline value of ``moved`` not yet copied and set its reference: the object
    is durable before the reference is committed. Up to ``batches`` batches (all, unset)."""
    counts = Counts(moved.name)
    ref_column = getattr(moved.model, moved.ref)
    done = 0
    while batches is None or done < batches:
        async with sessions() as db:
            rows = (
                await db.scalars(
                    select(moved.model)
                    .where(moved.inline, _absent(ref_column))
                    .limit(batch)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            if not rows:
                break
            links = []
            for row in rows:
                ref = await payloads_of(db).put(moved.tenant(row), moved.value(row))
                setattr(row, moved.ref, ref.model_dump())
                if (run_id := moved.run(row)) is not None:
                    links.append({"run_id": run_id, "key": ref.ref, "kind": moved.kind})
            if links:
                await db.execute(insert(RunPayloadRow).values(links).on_conflict_do_nothing())
            await db.commit()
        counts.copied += len(rows)
        done += 1
    return counts


async def verify(sessions: Sessions, moved: Moved, *, batch: int) -> Counts:
    """Read back every copy of ``moved`` still held inline too, and compare: counts only."""
    counts = Counts(moved.name)
    ref_column = getattr(moved.model, moved.ref)
    keys = moved.model.__table__.primary_key.columns
    after: tuple[Any, ...] | None = None
    while True:
        async with sessions() as db:
            query = select(moved.model).where(moved.inline, _present(ref_column))
            if after is not None:
                query = query.where(tuple_(*keys) > tuple_(*after))
            rows = (await db.scalars(query.order_by(*keys).limit(batch))).all()
            for row in rows:
                await _check(db, moved, row, counts)
        if not rows:
            return counts
        after = tuple(getattr(rows[-1], column.key) for column in keys)


async def _check(db: AsyncSession, moved: Moved, row: Any, counts: Counts) -> bool:
    """Is the object ``row`` refers to its inline value? (Read verified: size, SHA-256.)"""
    try:
        stored = await payloads_of(db).get(getattr(row, moved.ref))
    except PayloadLost:
        counts.lost += 1
        return False
    if stored != moved.value(row):
        counts.mismatched += 1
        return False
    counts.verified += 1
    return True


async def clear(sessions: Sessions, moved: Moved, *, batch: int) -> Counts:
    """Clear the inline values whose copies match, checking each again as it is cleared."""
    counts = Counts(moved.name)
    ref_column = getattr(moved.model, moved.ref)
    while True:
        async with sessions() as db:
            rows = (
                await db.scalars(
                    select(moved.model)
                    .where(moved.inline, _present(ref_column))
                    .limit(batch)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            matched = [row for row in rows if await _check(db, moved, row, counts)]
            for row in matched:
                moved.cleared(row)
            await db.commit()
        counts.cleared += len(matched)
        if len(matched) < len(rows) or not rows:  # a row that does not match stays: stop
            return counts


async def run(
    sessions: Sessions, phase: str, *, batch: int = BATCH, batches: int | None = None
) -> list[Counts]:
    """One phase over every column. ``clear`` verifies everything first and clears nothing
    unless every copy matches."""
    if phase == "copy":
        return [await copy(sessions, m, batch=batch, batches=batches) for m in MOVED]
    checked = [await verify(sessions, m, batch=batch) for m in MOVED]
    if phase == "verify" or any(c.mismatched or c.lost for c in checked):
        return checked
    return [await clear(sessions, m, batch=batch) for m in MOVED]


def _failed(counts: Sequence[Counts]) -> bool:
    return any(c.mismatched or c.lost for c in counts)


async def _main(phase: str, *, batch: int, batches: int | None) -> int:
    settings = get_settings()
    engine = await connect(settings.database)
    blobs = open_blob_store(settings.blob)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    bind(sessions, blobs, retention=settings.blob.retention)
    try:
        counts = await run(sessions, phase, batch=batch, batches=batches)
    finally:
        await blobs.aclose()
        await engine.dispose()
    for c in counts:
        sys.stdout.write(json.dumps({"phase": phase, **c.__dict__}) + "\n")
    return 1 if _failed(counts) else 0


def main(argv: Sequence[str] | None = None) -> int:
    """``agent-runs-payloads copy|verify|clear``: exit 1 when a copy does not match."""
    parser = argparse.ArgumentParser(prog="agent-runs-payloads", description=__doc__)
    parser.add_argument("phase", choices=PHASES)
    parser.add_argument("--batch", type=int, default=BATCH, help="rows per transaction")
    parser.add_argument("--batches", type=int, default=None, help="copy at most this many")
    args = parser.parse_args(argv)
    return asyncio.run(_main(args.phase, batch=args.batch, batches=args.batches))


if __name__ == "__main__":  # pragma: no cover - the module's own entry
    raise SystemExit(main())
