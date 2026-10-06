"""01: a run your own process executes, recorded durably: start, read, finish.

``start`` records the run ``RUNNING``; ``finish`` ends it. A retried start (same run id or
idempotency key) answers the run that exists instead of making a second one.
Runs in process against the local PostgreSQL (see ``_local.py``).

    uv run python examples/01_start_and_finish.py
"""

import asyncio

from _local import local_service
from trellis.contracts import AgentExecutionContext, AgentRequest, RunStart, RunStatus


async def main() -> None:
    async with local_service() as local:
        runs = local.runs()
        ctx = AgentExecutionContext.create(tenant_id="acme", user_id="u1", agent_id="triage")
        start = RunStart.from_request(AgentRequest.create(ctx, {"ticket": 7}))

        run = await runs.start(start)
        print("started:", run.run_id, run.status.value, "attempt", run.attempt)

        again = await runs.start(start)  # a retry of the same start: the same run
        assert again.run_id == run.run_id

        done = await runs.finish(run.run_id, RunStatus.SUCCESS, output={"category": "billing"})
        print("finished:", done.status.value, done.output, f"worked {done.worked_seconds:.2f}s")

        read = await runs.get(run.run_id)
        assert read is not None and read.final
        page = await runs.list(agent_id="triage")
        print("listed:", [(s.run_id == run.run_id, s.status.value) for s in page.items])
        assert await runs.get("run_missing") is None  # a read of nothing is None


if __name__ == "__main__":
    asyncio.run(main())
