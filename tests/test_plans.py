"""The plans of the queries on the claim's and the line's hot paths, kept: each is served by
an index (``EXPLAIN`` with sequential scans priced out, so a plan that still scans a table
whole has no index to use). Counts and plans, never time."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.dialects import postgresql
from trellis.contracts.ids import now
from trellis.contracts.runs import RunStatus

from agent_runs.domain.runs import ClaimRequest
from agent_runs.store.runs import RunStore, _after_the_line
from tests.conftest import SETTINGS, started


async def _plan(db: Any, statement: Any) -> str:
    """The plan PostgreSQL picks for ``statement`` when an index scan is always cheaper."""
    await db.execute(text("SET LOCAL enable_seqscan = off"))
    sql = statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    rows = await db.execute(text(f"EXPLAIN {sql}"))
    return "\n".join(row[0] for row in rows)


async def test_placing_a_run_reads_its_keys_line_from_the_index(app) -> None:
    """PERF-10: the latest of a key's running and queued runs is an index scan on
    ``ix_runs_concurrency``, however many runs the tenant keeps."""
    async with app.state.sessions() as db:
        plan = await _plan(db, select(_after_the_line("acme", "thread:1", now())))
    assert "ix_runs_concurrency" in plan and "Seq Scan" not in plan, plan


async def test_a_claim_scans_no_table_whole(app, client) -> None:
    """PERF-9: choosing the next queued run — its key's room, and the place of each run
    holding the key (its answers) — scans neither ``agent_runs`` nor ``run_resolutions``."""
    await client.post("/v1/runs", json=started(queue=True, concurrency_key="thread:1"))
    request = ClaimRequest(worker_id="w1", agent_ids=["triage"], lease_seconds=30)
    async with app.state.sessions() as db:
        plan = await _plan(
            db, RunStore(db, SETTINGS.limits)._next_queued("acme", request, now(), [])
        )
    assert "Seq Scan" not in plan and "ix_run_resolutions_run" in plan, plan


async def test_listing_a_keys_line_reads_the_index_and_each_place_from_its_answers(app) -> None:
    """SEC-R-11 and ARCH-15: a key's running (or queued) runs are listed from
    ``ix_runs_concurrency`` — not by walking the tenant's runs newest first, here 4,000 ended
    turns of the same conversation and others — and each one's ``placed_at`` is read from
    ``ix_run_resolutions_run``."""
    async with app.state.sessions() as db:
        await db.execute(
            text(
                "INSERT INTO agent_runs (run_id, tenant_id, agent_id, status, concurrency_key, "
                "attempt, created_at, updated_at) SELECT 'run_' || n, 'acme', 'triage', "
                "'SUCCESS', 'thread:' || (n % 2), 1, now(), now() "
                "FROM generate_series(1, 4000) AS n"
            )
        )
        await db.commit()
        await db.execute(text("ANALYZE agent_runs"))
        listing = RunStore(db, SETTINGS.limits)._listing(
            "acme", status=RunStatus.RUNNING, concurrency_key="thread:1"
        )
        plan = await _plan(db, listing)
    assert "ix_runs_concurrency" in plan and "ix_run_resolutions_run" in plan, plan
    assert "Seq Scan" not in plan, plan


async def test_a_tenants_claim_reads_its_queue_in_claim_order_from_the_index(app) -> None:
    """PERF-11: a tenant's claim takes the first run of ``ix_runs_queue`` (the tenant's and
    the agent's queued runs, highest priority then oldest) that has room, and stops: nothing
    sorts the queue, whose depth a claim's cost then does not grow with (measured on 100,000
    queued runs of one tenant: 400 ms a claim sorted, 0.1 ms read in order)."""
    async with app.state.sessions() as db:
        await db.execute(
            text(
                "INSERT INTO agent_runs (run_id, tenant_id, agent_id, status, priority, "
                "attempt, queued_at, created_at, updated_at) SELECT 'run_' || n, 'acme', "
                "'triage', 'QUEUED', n % 3, 1, now(), now(), now() "
                "FROM generate_series(1, 4000) AS n"
            )
        )
        await db.commit()
        await db.execute(text("ANALYZE agent_runs"))
        request = ClaimRequest(worker_id="w1", agent_ids=["triage"], lease_seconds=30)
        claim = RunStore(db, SETTINGS.limits)._next_queued("acme", request, now(), [])
        plan = await _plan(db, claim)
        first = (await db.scalars(claim)).one()
    assert "ix_runs_queue" in plan and "Sort" not in plan, plan
    assert first.priority == 2
