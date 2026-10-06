"""02: a run handed to workers: queue, claim under a lease, heartbeat, lose the lease, retry.

A queued run is claimed by a worker (highest ``priority`` first), kept alive by heartbeats
(which may save a ``checkpoint``), and fenced: a write by a worker that no longer holds the
lease is refused with ``LeaseLostError``. When a worker stops heartbeating, the ticker puts
the run back on the queue as the next attempt, with its checkpoint. In process.

    uv run python examples/02_queue_claim_heartbeat.py
"""

import asyncio
from datetime import UTC, datetime, timedelta

from _local import local_service
from trellis.contracts import RunStart, RunStatus
from trellis.runs import LeaseLostError


async def main() -> None:
    async with local_service() as local:
        runs = local.runs()
        low = await runs.start(RunStart(tenant_id="acme", agent_id="digest"), queue=True)
        urgent = await runs.start(
            RunStart(tenant_id="acme", agent_id="digest", priority=10), queue=True
        )
        print("queued:", low.status.value, urgent.status.value)

        claimed = await runs.claim("worker-a", ["digest"], lease_seconds=30)
        assert claimed is not None and claimed.run.run_id == urgent.run_id  # priority first
        print("claimed:", claimed.run.status.value, "lease until", claimed.lease.expires_at)

        lease = await runs.heartbeat(urgent.run_id, "worker-a", checkpoint={"step": 3})
        print("heartbeat: cancel requested?", lease.cancel_requested)

        try:
            await runs.finish(urgent.run_id, RunStatus.SUCCESS, worker_id="worker-b")
        except LeaseLostError:
            print("refused: worker-b does not hold the lease (409 LEASE_LOST)")

        # worker-a dies. One tick after the lease ran out (plus the backoff), the run is back.
        await local.tick(datetime.now(UTC) + timedelta(minutes=5))
        again = await runs.get(urgent.run_id)
        assert again is not None
        print("after the lapse:", again.status.value, "attempt", again.attempt, again.checkpoint)

        await local.let_backoff_pass()  # a lapsed run waits 5 s (doubling) before its retry
        retaken = await runs.claim("worker-c", ["digest"])
        assert retaken is not None and retaken.run.run_id == urgent.run_id
        print("claimed again with its checkpoint:", retaken.run.checkpoint)
        done = await runs.finish(urgent.run_id, RunStatus.SUCCESS, worker_id="worker-c")
        print("finished:", done.status.value, "attempt", done.attempt)


if __name__ == "__main__":
    asyncio.run(main())
