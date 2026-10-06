"""08: a run's events, kept by agent-runs and read from any replica, or followed as SSE.

The executor appends the run's ``RunEvent``s while it runs (a repeat is stored once); anyone
reads them past a position, or follows them as server-sent events that end once the run has.
In process.

    uv run python examples/08_events_and_sse.py
"""

import asyncio

from _local import local_service
from trellis.contracts import (
    AgentExecutionContext,
    AgentRequest,
    RunEvent,
    RunEventType,
    RunOutcome,
    RunStart,
    RunStatus,
)


async def main() -> None:
    async with local_service() as local:
        runs = local.runs()
        ctx = AgentExecutionContext.create(tenant_id="acme", agent_id="writer", thread_id="t1")
        run = await runs.start(RunStart.from_request(AgentRequest.create(ctx, "a short poem")))

        events = [
            RunEvent.started(ctx, 0),
            RunEvent.text(ctx, RunEventType.TEXT_MESSAGE_START, "m1", 1),
            RunEvent.text(ctx, RunEventType.TEXT_MESSAGE_CONTENT, "m1", 2, delta="Autumn wind, "),
            RunEvent.text(ctx, RunEventType.TEXT_MESSAGE_CONTENT, "m1", 3, delta="a lone crow."),
            RunEvent.text(ctx, RunEventType.TEXT_MESSAGE_END, "m1", 4),
        ]
        appended = await runs.append_events(run.run_id, events)
        again = await runs.append_events(run.run_id, events[-1:])  # a retried append
        print("appended:", appended.appended, "then", again.appended, "(a repeat is stored once)")

        first = await runs.events(run.run_id, after=0, limit=2)
        rest = await runs.events(run.run_id, after=first[-1].position)
        print("read in pages:", [e.event.type.value for e in first + rest])

        await runs.append_events(run.run_id, [RunEvent.finished(ctx, RunOutcome.SUCCESS, 5)])
        await runs.finish(run.run_id, RunStatus.SUCCESS, output="Autumn wind, a lone crow.")
        text = ""
        async for entry in runs.stream_events(run.run_id):  # SSE; ends once the run has
            text += str(entry.event.data.get("delta", ""))
        print("followed as SSE:", text)


if __name__ == "__main__":
    asyncio.run(main())
