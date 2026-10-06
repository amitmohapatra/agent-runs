"""03: a run that waits for a person: pause, the inbox, and every check an answer meets.

A run pauses on an ``Interrupt`` (labelled options, an assignee) and is listed in that
person's inbox. Before anything is written, agent-runs checks the answer: it must fit the
question (422 ``ValidationError``), come from a key that may answer for the assignee (403
``AuthorizationError``), and be the first answer (409 ``ConflictError``; the very same
resolution resent is answered with the run as it is). In process.

    uv run python examples/03_pause_resume_answer_check.py
"""

import asyncio

from _local import PRIYA_KEY, local_service
from trellis.contracts import (
    AgentExecutionContext,
    AgentPaused,
    AgentRequest,
    Interrupt,
    InterruptDecision,
    InterruptReason,
    InterruptResolution,
    Option,
    RunStart,
    RunStatus,
)
from trellis.runs import AuthorizationError, ConflictError, ValidationError


def asked(ctx: AgentExecutionContext, assignee: str) -> Interrupt:
    return Interrupt.from_paused(
        AgentPaused("Which supplier?"),
        context=ctx,
        reason=InterruptReason.CHOICE,
        ui="choice",
        options=["acme", Option(value="globex", label="Globex GmbH", description="EU stock")],
        assignee=assignee,
    )


async def main() -> None:
    async with local_service() as local:
        app, priya = local.runs(), local.runs(PRIYA_KEY)  # the application's key, priya's UI's
        ctx = AgentExecutionContext.create(tenant_id="acme", user_id="u1", agent_id="buyer")
        run = await app.start(RunStart.from_request(AgentRequest.create(ctx, {"sku": "A-1"})))
        question = asked(ctx, "user:priya")
        paused = await app.pause(question, checkpoint={"journal": ["looked up stock"]})
        assert paused.awaiting is not None
        print("paused:", paused.status.value, "assigned to", paused.awaiting.assignee)

        inbox = await priya.list(status=RunStatus.PAUSED, assignee="user:priya")
        print("priya's inbox:", [s.awaiting and s.awaiting.question for s in inbox.items])

        def answer(value: object, reviewer: str = "priya") -> InterruptResolution:
            return InterruptResolution(
                interrupt_id=question.interrupt_id,
                run_id=run.run_id,
                decision=InterruptDecision.ANSWER,
                answer=value,
                reviewer=reviewer,
            )

        try:
            await priya.resume(answer("Globex GmbH"))  # a label, not a value
        except ValidationError as exc:
            print("422:", exc)
        try:
            await priya.resume(answer("globex", reviewer="raj"))  # she may act only as herself
        except AuthorizationError as exc:
            print("403:", exc)

        good = answer("globex")
        resumed = await priya.resume(good)
        print("resumed:", resumed.status.value, "attempt", resumed.attempt, resumed.checkpoint)
        assert (await priya.resume(good)).attempt == resumed.attempt  # resent: no second answer
        try:
            await app.resume(answer("acme", reviewer="ada"))  # a second click
        except ConflictError:
            print("409: the question was already answered")

        audit = await app.resolutions(run.run_id)
        print("audit:", [(e.resolution.decision.value, e.resolution.reviewer) for e in audit.items])
        await app.finish(run.run_id, RunStatus.SUCCESS, output={"supplier": "globex"})


if __name__ == "__main__":
    asyncio.run(main())
