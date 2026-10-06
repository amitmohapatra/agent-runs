"""06: a schedule: created once, fired by the ticker, its run carrying everything a start can.

``schedules.create`` is an upsert on the schedule's identity. Each fire queues a run as the
schedule's ``on_behalf_of``, copying ``timeout_seconds``, ``agent_version``, ``priority`` and
``concurrency_key``, and putting the schedule's ``metadata`` into the run's under the fire's
own keys (``schedule_id``, ``schedule_name``, ``fire_time``, ``created_by``), which win.
In process; the ticker is told what time it is.

    uv run python examples/06_schedule_fire.py
"""

import asyncio
from datetime import timedelta

from _local import local_service
from trellis.contracts import ScheduleSpec


async def main() -> None:
    async with local_service() as local:
        runs = local.runs()
        spec = ScheduleSpec(
            tenant_id="acme",
            agent_id="digest",
            name="Weekday digest",
            cadence="0 8 * * 1-5",
            timezone="Europe/Berlin",
            on_behalf_of="user:ada",
            input={"team": "sales"},
            timeout_seconds=600,
            agent_version="2026.10.06",
            priority=-10,
            concurrency_key="digest",
            metadata={"without": ["memory_push"], "schedule_id": "ignored: the fire's key wins"},
        )
        schedule = await runs.schedules.create(spec)
        assert (await runs.schedules.create(spec)).schedule_id == schedule.schedule_id  # upsert
        print("schedule:", schedule.schedule_id, "next fire", schedule.next_fire_at)

        now = await runs.schedules.fire(schedule.schedule_id)  # on demand
        print("fired on demand:", now.run_id)

        assert schedule.next_fire_at is not None
        report = await local.tick(schedule.next_fire_at + timedelta(seconds=1))  # the ticker
        print("the ticker fired:", report.fired)

        claimed = await runs.claim("worker-1", ["digest"])
        assert claimed is not None
        run = claimed.run
        print(
            "the fired run:",
            run.on_behalf_of,
            run.priority,
            run.concurrency_key,
            run.timeout_seconds,
        )
        print(
            "its metadata:", {k: run.metadata[k] for k in sorted(run.metadata) if k != "fire_time"}
        )
        assert run.metadata["schedule_id"] == schedule.schedule_id
        assert run.metadata["without"] == ["memory_push"]


if __name__ == "__main__":
    asyncio.run(main())
