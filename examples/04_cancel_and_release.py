"""04: stopping runs: cancel a waiting run, cancel a running one, release one on shutdown.

A queued or paused run is ``CANCELLED`` at once. A run a worker holds stays ``RUNNING`` and
the worker learns of the cancel on its next heartbeat (``cancel_requested``), then ends it.
A worker that is stopping releases what it holds: back on the queue at once, as the next
attempt, with no lapse counted. In process.

    uv run python examples/04_cancel_and_release.py
"""

import asyncio

from _local import local_service
from trellis.contracts import RunStart, RunStatus


async def main() -> None:
    async with local_service() as local:
        runs = local.runs()

        waiting = await runs.start(RunStart(tenant_id="acme", agent_id="export"), queue=True)
        cancelled = await runs.cancel(waiting.run_id, reason="the customer withdrew")
        print("queued, cancelled:", cancelled.status.value)

        held = await runs.start(RunStart(tenant_id="acme", agent_id="export"), queue=True)
        claimed = await runs.claim("worker-a", ["export"])
        assert claimed is not None and claimed.run.run_id == held.run_id
        still = await runs.cancel(held.run_id, reason="wrong account")
        print("held by a worker, after cancel:", still.status.value)
        lease = await runs.heartbeat(held.run_id, "worker-a")
        print("the heartbeat says cancel_requested:", lease.cancel_requested)
        ended = await runs.finish(held.run_id, RunStatus.CANCELLED, worker_id="worker-a")
        print("the worker ended it:", ended.status.value)

        stopping = await runs.start(RunStart(tenant_id="acme", agent_id="export"), queue=True)
        assert (await runs.claim("worker-b", ["export"])) is not None
        back = await runs.release(stopping.run_id, "worker-b", checkpoint={"rows": 1200})
        print("released:", back.status.value, "attempt", back.attempt, back.checkpoint)
        again = await runs.claim("worker-c", ["export"])  # available at once
        assert again is not None and again.run.run_id == stopping.run_id
        await runs.finish(stopping.run_id, RunStatus.SUCCESS, worker_id="worker-c")
        print("another worker finished it from", again.run.checkpoint)


if __name__ == "__main__":
    asyncio.run(main())
