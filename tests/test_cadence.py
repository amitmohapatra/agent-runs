"""Cadence arithmetic, with the reference time supplied by the test.

These are the cases a running service cannot be asked about: what a daily schedule does on
the night a country changes its clocks, and what happens when someone asks for a schedule
that fires every minute. None of them are reachable by waiting.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from agent_runs.domain.cadence import (
    JITTER_MAX,
    MIN_INTERVAL,
    CadenceTooFrequent,
    InvalidCadence,
    jitter,
    next_fire_at,
    validate_cadence,
)

NY = "America/New_York"
_REFERENCE = datetime(2026, 9, 21, 14, 37, tzinfo=UTC)  # a Monday afternoon


def test_hourly_fires_on_the_next_top_of_the_hour() -> None:
    """The bucket is the top of the hour, not "an hour from whenever you asked" — otherwise
    every restart quietly walks the schedule forward by a few minutes."""
    assert next_fire_at("hourly", after=_REFERENCE, timezone="UTC") == datetime(
        2026, 9, 21, 15, tzinfo=UTC
    )


def test_daily_fires_at_local_midnight_not_utc_midnight() -> None:
    """The failure this prevents: a Berlin user's "daily" arriving at 02:00 local because
    the computation was done in UTC and the zone was decoration."""
    assert next_fire_at("daily", after=_REFERENCE, timezone="Europe/Berlin") == datetime(
        2026, 9, 21, 22, tzinfo=UTC
    )


def test_weekly_fires_on_the_next_monday() -> None:
    assert next_fire_at("weekly", after=_REFERENCE, timezone="UTC") == datetime(
        2026, 9, 28, tzinfo=UTC
    )


def test_weekdays_skips_the_weekend() -> None:
    """From Friday the next weekday fire is Monday. A cadence that counted days would land
    on Saturday and deliver a Monday-morning briefing to nobody."""
    friday = datetime(2026, 9, 25, 12, tzinfo=UTC)
    assert next_fire_at("weekdays", after=friday, timezone="UTC") == datetime(
        2026, 9, 28, tzinfo=UTC
    )


def test_a_manual_schedule_never_fires_on_its_own() -> None:
    """``manual`` has no next instant at all, so it can never appear in the due list."""
    assert next_fire_at("manual", after=_REFERENCE, timezone="UTC") is None


def test_a_cron_cadence_is_honoured_in_the_schedules_own_zone() -> None:
    """14:37 UTC is already 10:37 in New York, so today's 09:00 is gone and the answer is
    tomorrow. Computing in UTC would have said "today at 09:00" — four hours in the past."""
    assert next_fire_at("0 9 * * 1-5", after=_REFERENCE, timezone=NY) == datetime(
        2026, 9, 22, 13, tzinfo=UTC
    )


def test_daily_keeps_local_midnight_across_the_spring_dst_transition() -> None:
    """The night the US springs forward, consecutive daily fires are 23 hours apart in UTC.

    That gap is the whole point: the wall clock the user asked for stays 00:00, so the
    absolute instant has to move. A service that stored the interval instead of the rule
    would fire this one at 01:00 local and drift every March."""
    first = next_fire_at("daily", after=datetime(2026, 3, 7, 12, tzinfo=UTC), timezone=NY)
    assert first is not None
    second = next_fire_at("daily", after=first, timezone=NY)
    assert second is not None

    local = ZoneInfo(NY)
    assert first.astimezone(local).hour == 0
    assert second.astimezone(local).hour == 0
    assert second - first == timedelta(hours=23)


def test_daily_keeps_local_midnight_across_the_autumn_dst_transition() -> None:
    """The mirror case: falling back makes the same two fires 25 hours apart."""
    first = next_fire_at("daily", after=datetime(2026, 10, 31, 12, tzinfo=UTC), timezone=NY)
    assert first is not None
    second = next_fire_at("daily", after=first, timezone=NY)
    assert second is not None

    local = ZoneInfo(NY)
    assert (first.astimezone(local).hour, second.astimezone(local).hour) == (0, 0)
    assert second - first == timedelta(hours=25)


def test_hourly_skips_the_local_hour_that_does_not_exist() -> None:
    """02:00 never happens on the spring-forward morning. The fire after 01:00 EST is 03:00
    EDT — one real hour later — and not a crash on a wall-clock time with no instant."""
    one_am = next_fire_at("hourly", after=datetime(2026, 3, 8, 5, 30, tzinfo=UTC), timezone=NY)
    assert one_am == datetime(2026, 3, 8, 6, tzinfo=UTC)
    assert one_am is not None
    following = next_fire_at("hourly", after=one_am, timezone=NY)
    assert following == datetime(2026, 3, 8, 7, tzinfo=UTC)
    assert following is not None
    assert following.astimezone(ZoneInfo(NY)).hour == 3


@pytest.mark.parametrize("cadence", ["* * * * *", "*/5 * * * *", "0,30 * * * *", "0 * * * * *"])
def test_a_cadence_faster_than_once_an_hour_is_refused(cadence: str) -> None:
    """Including the six-field spellings with a seconds column, which are valid cron and
    would sail past any check that only read the minute field."""
    with pytest.raises(InvalidCadence, match="per-minute"):
        validate_cadence(cadence)


def test_an_hourly_cron_is_accepted() -> None:
    """The floor is one run per hour, not "no cron" — exactly at the floor still passes."""
    assert validate_cadence("0 * * * *") == "0 * * * *"


def test_a_cadence_that_is_neither_bucket_nor_cron_is_refused() -> None:
    with pytest.raises(InvalidCadence, match="neither"):
        validate_cadence("every so often")


def test_a_naive_reference_time_is_refused() -> None:
    """A naive instant would be read as UTC and shift every fire by the caller's offset."""
    with pytest.raises(ValueError, match="timezone-aware"):
        next_fire_at("daily", after=datetime(2026, 9, 21, 14, 37), timezone="UTC")


@pytest.mark.parametrize(
    "after", [datetime(2026, 3, 7, 12, tzinfo=UTC), datetime(2026, 10, 31, 12, tzinfo=UTC)]
)
def test_daily_at_nine_stays_nine_across_both_dst_transitions(after: datetime) -> None:
    """``0 9 * * *`` in New York, jitter included: 9:00 local before and after the change."""
    shift = jitter("sch_dst")
    fires = [next_fire_at("0 9 * * *", after=after, timezone=NY)]
    for _ in range(2):
        previous = fires[-1]
        assert previous is not None
        fires.append(next_fire_at("0 9 * * *", after=previous + shift, timezone=NY))
    local = [(f + shift).astimezone(ZoneInfo(NY)) for f in fires if f is not None]
    assert [(t.hour, t.minute) for t in local] == [(9, 0)] * 3
    assert len({t.date() for t in local}) == 3


def test_jitter_is_stable_bounded_and_spread() -> None:
    """Under a minute, a tenth of the one-hour floor at most, and the same for a schedule on
    every replica and restart; different schedules land on different seconds."""
    shifts = [jitter(f"sch_{n}") for n in range(200)]
    assert all(timedelta(0) <= s < timedelta(seconds=60) <= MIN_INTERVAL / 10 for s in shifts)
    assert jitter("sch_1") == jitter("sch_1")
    assert len(set(shifts)) > 40


def test_a_deployment_may_lower_the_floor_and_the_cadence_is_measured_against_it() -> None:
    """``RUNS__SCHEDULES__MIN_INTERVAL_SECONDS`` passes its floor here: a quarter-hour cron is
    accepted under a fifteen-minute floor and still refused under the default one; anything
    faster than the floor given is refused the same way."""
    quarter = timedelta(minutes=15)
    assert validate_cadence("*/15 * * * *", floor=quarter) == "*/15 * * * *"
    with pytest.raises(InvalidCadence, match="per-minute"):
        validate_cadence("*/15 * * * *")
    with pytest.raises(InvalidCadence, match="0:15:00"):
        validate_cadence("*/5 * * * *", floor=quarter)


def test_the_jitter_spread_is_the_deployments_and_zero_turns_it_off() -> None:
    """``RUNS__SCHEDULES__JITTER_SECONDS`` passes its spread here; the default is the minute
    of ``JITTER_MAX``, and the same id lands on the same second for a given spread."""
    narrow = [jitter(f"sch_{n}", spread=timedelta(seconds=10)) for n in range(200)]
    assert all(timedelta(0) <= s < timedelta(seconds=10) for s in narrow)
    assert len(set(narrow)) == 10
    assert jitter("sch_1", spread=JITTER_MAX) == jitter("sch_1")
    assert {jitter(f"sch_{n}", spread=timedelta(0)) for n in range(20)} == {timedelta(0)}


def test_a_cadence_under_the_floor_says_the_floor_and_its_real_gap_in_seconds() -> None:
    """``CadenceTooFrequent``: its own code, and the numbers a caller tells a person,
    measured on real occurrences (``0,30`` is every 30 minutes, not every hour)."""
    with pytest.raises(CadenceTooFrequent) as refused:
        validate_cadence("0,30 * * * *")
    assert refused.value.code == "CADENCE_TOO_FREQUENT" and refused.value.status_code == 422
    assert refused.value.details == {"min_interval_seconds": 3600, "interval_seconds": 1800}
    assert isinstance(refused.value, InvalidCadence)
    with pytest.raises(CadenceTooFrequent) as lower:
        validate_cadence("*/5 * * * *", floor=timedelta(minutes=15))
    assert lower.value.details == {"min_interval_seconds": 900, "interval_seconds": 300}


def test_a_cadence_that_is_not_one_is_not_too_frequent() -> None:
    with pytest.raises(InvalidCadence) as refused:
        validate_cadence("every so often")
    assert not isinstance(refused.value, CadenceTooFrequent)
    assert refused.value.code == "VALIDATION" and refused.value.details == {}


@pytest.mark.parametrize(
    ("cadence", "times"), [("30 1 * * *", ["01:30"]), ("0,30 1 * * *", ["01:00", "01:30"])]
)
def test_a_time_the_autumn_change_repeats_fires_once_on_its_first_occurrence(
    cadence: str, times: list[str]
) -> None:
    """01:00-02:00 comes twice in New York on 1 November 2026: a schedule at a fixed hour in
    it fires on the first pass only, as cron does, never twice in one night."""
    fires: list[datetime] = []
    after = datetime(2026, 11, 1, 4, tzinfo=UTC)  # 00:00 EDT
    while (due := next_fire_at(cadence, after=after, timezone=NY)) is not None and due < datetime(
        2026, 11, 2, tzinfo=UTC
    ):
        fires.append(due)
        after = due
    local = [f.astimezone(ZoneInfo(NY)) for f in fires]
    assert [(f"{t:%H:%M}", t.tzname()) for t in local] == [(t, "EDT") for t in times]


def test_hourly_still_fires_every_real_hour_through_the_repeated_one() -> None:
    """A wildcard hour is a rate, not a time of day: both 01:00s fire, an hour apart."""
    first = next_fire_at("0 * * * *", after=datetime(2026, 11, 1, 4, 30, tzinfo=UTC), timezone=NY)
    assert first == datetime(2026, 11, 1, 5, tzinfo=UTC)  # 01:00 EDT
    assert first is not None
    assert next_fire_at("0 * * * *", after=first, timezone=NY) == datetime(
        2026, 11, 1, 6, tzinfo=UTC
    )  # 01:00 EST
