"""How often a schedule fires, and when it next does.

Granularity is capped at one run per hour: a schedule is an unattended loop, and a
per-minute unattended loop is a slow denial of service against whatever it calls, paid for
by someone who is not watching.

Nothing here reads the clock; every entry point takes its reference time, so a DST
transition or a schedule created at 23:59 is a test rather than a wait.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from itertools import pairwise
from zoneinfo import ZoneInfo

from croniter import croniter

from agent_runs.domain.errors import Unprocessable

#: The named buckets and the cron expression each means; ``manual`` never fires on its own.
#: They fire at local midnight (weekly on Monday): a schedule that wants a time of day says
#: so with a cron string.
BUCKETS: dict[str, str | None] = {
    "hourly": "0 * * * *",
    "daily": "0 0 * * *",
    "weekly": "0 0 * * 1",
    "weekdays": "0 0 * * 1-5",
    "manual": None,
}

#: The floor: one run per hour.
MIN_INTERVAL = timedelta(hours=1)

#: Validation measures real occurrences from a fixed instant, so whether a cadence is
#: accepted never depends on when the check runs.
_PROBE_FROM = datetime(2024, 1, 1, tzinfo=UTC)
_PROBE_COUNT = 8


class InvalidCadence(Unprocessable):
    """A cadence this service refuses to run."""


def validate_cadence(cadence: str) -> str:
    """The normalised cadence: a bucket, or a cron expression firing at most hourly.

    ``croniter`` accepts ``* * * * *`` and six-field expressions with a seconds column, so
    "too frequent" is measured on actual consecutive occurrences, not read off a field.
    """
    normalized = cadence.strip()
    if normalized in BUCKETS:
        return normalized
    if not croniter.is_valid(normalized):
        raise InvalidCadence(
            f"cadence {cadence!r} is neither one of {sorted(BUCKETS)} nor a cron expression"
        )
    cursor = croniter(normalized, _PROBE_FROM)
    times = [cursor.get_next(datetime) for _ in range(_PROBE_COUNT)]
    interval = min(later - earlier for earlier, later in pairwise(times))
    if interval < MIN_INTERVAL:
        raise InvalidCadence(
            f"cadence {cadence!r} fires every {interval}, under the one-run-per-hour floor: "
            "per-minute schedules are refused"
        )
    return normalized


def next_fire_at(cadence: str, *, after: datetime, timezone: str) -> datetime | None:
    """The first instant ``cadence`` fires strictly after ``after``, in UTC; ``None`` for
    ``manual``. Computed in the schedule's own zone (a Berlin "daily" is local midnight on
    both sides of a DST change) and returned as an absolute instant."""
    if after.tzinfo is None:
        raise ValueError("next_fire_at needs a timezone-aware reference time")
    expression = BUCKETS.get(cadence, cadence)
    if expression is None:
        return None
    local = after.astimezone(ZoneInfo(timezone))
    return croniter(expression, local).get_next(datetime).astimezone(UTC)
