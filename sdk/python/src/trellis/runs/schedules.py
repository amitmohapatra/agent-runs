"""Schedules: runs started on a timetable, as the person who set them, while nobody is
present. A create is an upsert on the schedule's identity (tenant, agent, ``on_behalf_of``,
cadence and input), so a client repeats it freely; pausing and resuming are updates
(``ScheduleUpdate(enabled=False)`` / ``True``)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from trellis.contracts.runs import Schedule, ScheduleSpec
from trellis.runs._transport import PAGE_LIMIT, Transport
from trellis.runs.models import FireResult, Page, ScheduleUpdate


class SchedulesAPI:
    """``runs.schedules``: ``create``, ``list``, ``get``, ``update``, ``delete``, ``fire``."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    async def create(self, spec: ScheduleSpec) -> Schedule:
        """The new schedule, armed for its next occurrence; or, when one with the same
        identity exists, that one, unchanged."""
        data = await self._transport.json(
            "POST", "/v1/schedules", tenant=spec.tenant_id, json=spec.model_dump(mode="json")
        )
        return Schedule.model_validate(data)

    async def list(
        self,
        *,
        enabled: bool | None = None,
        agent_id: str | None = None,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT,
        tenant: str | None = None,
    ) -> Page[Schedule]:
        """One page of the tenant's schedules, newest first."""
        params: dict[str, Any] = {"limit": limit}
        if enabled is not None:
            params["enabled"] = "true" if enabled else "false"
        if agent_id is not None:
            params["agent_id"] = agent_id
        if cursor is not None:
            params["cursor"] = cursor
        rows, after = await self._transport.page("/v1/schedules", tenant=tenant, params=params)
        return Page[Schedule](
            items=[Schedule.model_validate(row) for row in rows], next_cursor=after
        )

    async def get(self, schedule_id: str, *, tenant: str | None = None) -> Schedule | None:
        """The schedule, or None when there is none."""
        data = await self._transport.found("GET", f"/v1/schedules/{schedule_id}", tenant=tenant)
        return None if data is None else Schedule.model_validate(data)

    async def update(
        self, schedule_id: str, changes: ScheduleUpdate, *, tenant: str | None = None
    ) -> Schedule:
        """Change the fields ``changes`` sets (and only those); a new cadence or zone re-arms
        the next fire."""
        data = await self._transport.json(
            "PATCH",
            f"/v1/schedules/{schedule_id}",
            tenant=tenant,
            json=changes.model_dump(mode="json", exclude_unset=True),
        )
        return Schedule.model_validate(data)

    async def delete(self, schedule_id: str, *, tenant: str | None = None) -> None:
        await self._transport.send("DELETE", f"/v1/schedules/{schedule_id}", tenant=tenant)

    async def fire(
        self, schedule_id: str, *, at: datetime | None = None, tenant: str | None = None
    ) -> FireResult:
        """Queue the schedule's run now: for the tick ``at`` (an instant that has arrived,
        with an offset; a repeat for it answers the same run), else for the tick it is due
        for, else for now."""
        body = {"at": at.isoformat()} if at is not None else None
        data = await self._transport.json(
            "POST", f"/v1/schedules/{schedule_id}/fire", tenant=tenant, json=body
        )
        return FireResult.model_validate(data)
