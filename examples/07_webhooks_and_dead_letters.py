"""07: webhooks: subscribe, receive a signed delivery, lose one, redeliver it, rotate the secret.

A run change writes an event to an outbox in the same transaction; the ticker delivers it,
signed (``X-Trellis-Signature``). A receiver checks it with ``verify_signature``. A delivery
refused for good (here a 410) is kept dead, listed, and owed again by ``redeliver``. During a
secret rotation every delivery is signed with both secrets. In process; the receiver is an
``httpx.MockTransport``. The secrets are never printed.

    uv run python examples/07_webhooks_and_dead_letters.py
"""

import asyncio

from _local import local_service
from trellis.contracts import Interrupt, RunStart, RunStatus
from trellis.runs import DeliveryState, WebhookEvent, parse_delivery, verify_signature


async def main() -> None:
    async with local_service() as local:
        runs, receiver = local.runs(), local.receiver
        hook = await runs.webhooks.create(
            "https://hooks.example/runs", [WebhookEvent.PAUSED, WebhookEvent.FINISHED]
        )
        secret = hook.secret  # shown once: keep it in your receiver's settings

        run = await runs.start(RunStart(tenant_id="acme", agent_id="refunds"))
        await runs.pause(Interrupt(tenant_id="acme", run_id=run.run_id, question="Refund 240?"))
        await local.tick()  # the ticker sends what the outbox owes
        request = receiver.received[-1]
        header = request.headers["x-trellis-signature"]
        assert verify_signature(secret, header, request.content)  # the raw bytes, unparsed
        delivery = parse_delivery(request.content)
        print("delivered:", delivery.type.value, delivery.data.run.status.value, "signature ok")

        receiver.answer = 410  # the receiver refuses for good: the delivery is kept, dead
        await runs.cancel(run.run_id, reason="duplicate")
        await local.tick()
        dead = await runs.webhooks.deliveries(state=DeliveryState.DEAD)
        print("dead:", [(d.type.value, d.last_error) for d in dead.items])

        receiver.answer = 200
        await runs.webhooks.redeliver(dead.items[0].delivery_id)
        await local.tick()
        print("redelivered:", parse_delivery(receiver.received[-1].content).type.value)

        rotated = await runs.webhooks.rotate_secret(hook.webhook_id)
        other = await runs.start(RunStart(tenant_id="acme", agent_id="refunds"))
        await runs.finish(other.run_id, RunStatus.SUCCESS)
        await local.tick()
        last = receiver.received[-1]
        header = last.headers["x-trellis-signature"]
        both = [verify_signature(s, header, last.content) for s in (secret, rotated.secret)]
        print("during the overlap, old and new secret both verify:", both)
        assert all(both)


if __name__ == "__main__":
    asyncio.run(main())
