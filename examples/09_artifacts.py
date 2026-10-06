"""09: a payload too large for a question: an artifact, shown to the reviewer.

The executor uploads the table or diff (checksummed, at most 50 MiB) and pauses with the
returned ``ArtifactRef`` as the interrupt's ``payload_ref``; the inbox UI downloads it, and
the bytes are verified against their SHA-256 on every read. In process; the blob store is a
temporary directory.

    uv run python examples/09_artifacts.py
"""

import asyncio
import json

from _local import local_service
from trellis.contracts import Interrupt, InterruptReason, RunStart


async def main() -> None:
    async with local_service() as local:
        runs = local.runs()
        run = await runs.start(RunStart(tenant_id="acme", agent_id="payables"))
        table = json.dumps([{"invoice": f"INV-{n}", "eur": 100 * n} for n in range(1, 501)])

        ref = await runs.artifacts.upload(run.run_id, table.encode())
        print("uploaded:", ref.artifact_id, ref.size_bytes, "bytes", ref.mime_type)

        await runs.pause(
            Interrupt(
                tenant_id="acme",
                run_id=run.run_id,
                reason=InterruptReason.REVIEW,
                question="Pay these 500 invoices?",
                ui="table",
                expects={"type": "array", "items": {"type": "string"}},
                payload_ref=ref,
            )
        )
        record = await runs.get(run.run_id)
        assert record is not None and record.awaiting is not None
        shown = record.awaiting.payload_ref
        assert shown is not None
        data = await runs.artifacts.download(shown.artifact_id)  # what the inbox UI shows
        assert data is not None and json.loads(data)[0] == {"invoice": "INV-1", "eur": 100}
        print("downloaded and verified:", len(data), "bytes")


if __name__ == "__main__":
    asyncio.run(main())
