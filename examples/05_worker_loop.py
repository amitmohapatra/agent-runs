"""05: the SDK's ``Worker``: the claim loop around your own handler.

The worker claims a queued run, heartbeats while the handler runs, and ends the run with what
the handler does: ``job.finish``, ``job.pause``, or an exception, which ends it ``ERROR``. A
retryable error on a queued run is not the end: agent-runs puts it back on the queue for a
later attempt. ``run_once`` handles one run; ``serve`` loops until SIGTERM. In process.

    uv run python examples/05_worker_loop.py
"""

import asyncio
import logging

from _local import local_service
from trellis.contracts import (
    Interrupt,
    InterruptDecision,
    InterruptResolution,
    RunStart,
    RunStatus,
)
from trellis.runs import Job, Worker

attempts: dict[str, int] = {}


async def handle(job: Job) -> None:
    """Your agent, in any framework. The input says what this demo does."""
    run = job.record
    attempts[run.run_id] = attempts.get(run.run_id, 0) + 1
    if run.input == "flaky" and attempts[run.run_id] == 1:
        raise TimeoutError("the model gateway timed out")  # retryable: tried again later
    if run.input == "needs approval" and run.last_resolution is None:
        await job.checkpoint({"drafted": True})
        question = Interrupt(tenant_id=run.tenant_id, run_id=run.run_id, question="Send it?")
        await job.pause(question)  # the lease ends; a person answers from the inbox
        return
    await job.finish(RunStatus.SUCCESS, output=f"done: {run.input}")


async def main() -> None:
    async with local_service() as local:
        runs = local.runs()
        ids = {}
        for text in ("plain", "flaky", "needs approval"):
            run = await runs.start(
                RunStart(tenant_id="acme", agent_id="ops", input=text), queue=True
            )
            ids[text] = run.run_id

        # the worker logs a handler's exception with its traceback; this demo raises one on purpose
        logging.getLogger("trellis.runs.worker").setLevel(logging.CRITICAL)
        worker = Worker(runs, handle, ["ops"], worker_id="worker-1")
        while await worker.run_once():  # until nothing is queued
            pass
        for text, run_id in ids.items():
            record = await runs.get(run_id)
            assert record is not None
            print(f"{text:<15} {record.status.value:<8} attempt {record.attempt}")

        paused = await runs.get(ids["needs approval"])
        assert paused is not None and paused.awaiting is not None
        await runs.resume(
            InterruptResolution(
                interrupt_id=paused.awaiting.interrupt_id,
                run_id=paused.run_id,
                decision=InterruptDecision.APPROVE,
                reviewer="ada",
            )
        )  # a queued run goes back on the queue, with its checkpoint and the answer
        await local.let_backoff_pass()  # the flaky run's retry waits 10 s (doubling)
        while await worker.run_once():
            pass
        for text in ("flaky", "needs approval"):
            record = await runs.get(ids[text])
            assert record is not None and record.status is RunStatus.SUCCESS
            print(f"{text:<15} {record.status.value:<8} attempt {record.attempt}")


if __name__ == "__main__":
    asyncio.run(main())
