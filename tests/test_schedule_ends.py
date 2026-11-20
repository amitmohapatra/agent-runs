"""A schedule that ends (``end_at``): nothing fires at or after it, whoever asks and however
often the ticker comes round, and a schedule without one behaves as it always did.

The last fire before the end happens once: an end is a reason to stop firing, never a reason
to fire again, so a re-tick, a ticker restarted over the same rows and a resume all find
nothing to do."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

from sqlalchemy import text

from agent_runs.config.settings import SchedulesSettings
from agent_runs.domain.cadence import jitter
from agent_runs.ticker import Ticker
from tests.conftest import SETTINGS, arm, client_of, queued_runs, scheduled, sender, serving


def _last_tick() -> datetime:
    """The most recent hourly boundary: an instant that has arrived."""
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)


def _iso(instant: datetime) -> str:
    return instant.isoformat().replace("+00:00", "Z")


async def _created(client, **over) -> dict:
    response = await client.post("/v1/schedules", json=scheduled(**over))
    assert response.status_code == 201, response.text
    return response.json()


async def _end(app, schedule_id: str, end_at: datetime) -> None:
    """Put the end where a test needs it, straight on the row (as ``arm`` does the clock)."""
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE agent_schedules SET end_at = :end WHERE schedule_id = :sid"),
            {"end": end_at, "sid": schedule_id},
        )


async def test_an_end_is_kept_returned_and_in_utc(client) -> None:
    """``end_at`` is stored and read back as the instant it names, in UTC, whatever offset
    it was sent in; the first fire is armed before it."""
    ending = datetime.now(UTC).replace(microsecond=0) + timedelta(days=30)
    berlin = ending.astimezone(timezone(timedelta(hours=2))).isoformat()
    created = await _created(client, cadence="daily", end_at=berlin)
    assert datetime.fromisoformat(created["end_at"]) == ending
    assert created["end_at"].endswith(("Z", "+00:00"))
    assert datetime.fromisoformat(created["next_fire_at"]) < ending
    fetched = (await client.get(f"/v1/schedules/{created['schedule_id']}")).json()
    assert fetched == created
    naive = await client.post("/v1/schedules", json=scheduled(end_at="2030-01-01T00:00:00"))
    assert naive.status_code == 422 and "end_at" in naive.text


async def test_a_schedule_whose_first_occurrence_is_past_its_end_is_never_due(
    client, ticker
) -> None:
    """The first occurrence at or after the end is no occurrence: ``next_fire_at`` is null,
    and no tick, however late, fires it."""
    soon = datetime.now(UTC) + timedelta(minutes=1)
    created = await _created(client, cadence="weekly", end_at=_iso(soon))
    assert created["next_fire_at"] is None
    assert (await ticker.tick(now=datetime.now(UTC) + timedelta(days=30))).fired == 0


async def test_an_end_that_falls_between_the_tick_and_its_jitter_ends_it(app, client) -> None:
    """The instant that fires is the jittered one, so an end in the seconds between an
    occurrence and its jitter ends the schedule before that occurrence."""
    created = await _created(client, cadence="hourly")
    sid = created["schedule_id"]
    shift = jitter(sid)
    tick = datetime.fromisoformat(created["next_fire_at"]) - shift
    # an id that fires on the tick itself (no shift) ends on the tick
    ending = tick if shift == timedelta(0) else tick + shift / 2
    updated = await client.patch(f"/v1/schedules/{sid}", json={"end_at": _iso(ending)})
    assert updated.status_code == 200, updated.text
    assert updated.json()["next_fire_at"] is None


async def test_the_last_fire_before_the_end_happens_once_and_never_again(
    app, client, ticker, receiver, tmp_path
) -> None:
    """An hourly schedule ending half an hour after its due tick: the ticker fires that tick
    once, the next occurrence is past the end so nothing is armed, and neither a re-tick, a
    tick days later, a new ticker over the same rows nor a fire by hand queues another run."""
    created = await _created(client, cadence="hourly")
    sid = created["schedule_id"]
    # an hour back, so the end (half an hour after the tick) has passed by the fire by hand
    due = await arm(app, sid, _last_tick() - timedelta(hours=1) + jitter(sid))
    await _end(app, sid, due + timedelta(minutes=30))
    now = due + timedelta(seconds=1)

    assert (await ticker.tick(now=now)).fired == 1
    [run] = await queued_runs(app)
    assert run["idempotency_key"] == f"{sid}@{due.isoformat()}"
    after = (await client.get(f"/v1/schedules/{sid}")).json()
    assert after["next_fire_at"] is None
    assert after["last_run_id"] == run["run_id"] and after["enabled"] is True

    async with app.state.engine.begin() as conn:  # the run ends: no overlap skip hides a fire
        await conn.execute(
            text("UPDATE agent_runs SET status = 'SUCCESS' WHERE run_id = :r"), {"r": run["run_id"]}
        )
    assert (await ticker.tick(now=now)).fired == 0
    assert (await ticker.tick(now=now + timedelta(days=3))).fired == 0
    hooks = sender(receiver)
    restarted = Ticker(
        app.state.sessions,
        hooks,
        app.state.blobs,
        heartbeat_path=tmp_path / "beat-2",
        limits=SETTINGS.limits,
    )
    assert (await restarted.tick(now=now + timedelta(days=3))).fired == 0
    await hooks.aclose()
    refused = await client.post(f"/v1/schedules/{sid}/fire")
    assert refused.status_code == 409 and "ended" in refused.json()["detail"]
    async with app.state.engine.connect() as conn:
        fired = await conn.scalar(text("SELECT count(*) FROM agent_runs"))
    assert fired == 1


async def test_a_tick_owed_before_the_end_is_not_fired_once_the_end_has_passed(
    app, client, ticker
) -> None:
    """A ticker that was down across the end finds a tick it owed from before it: nothing is
    queued at or after the end, so it is dropped and the schedule shows nothing due."""
    sid = (await _created(client, cadence="hourly"))["schedule_id"]
    owed = await arm(app, sid, _last_tick() - timedelta(hours=3))
    await _end(app, sid, owed + timedelta(minutes=30))
    assert (await ticker.tick()).fired == 0
    assert await queued_runs(app) == []
    ended = (await client.get(f"/v1/schedules/{sid}")).json()
    assert ended["next_fire_at"] is None and ended["enabled"] is True
    assert ended["consecutive_failures"] == 0 and ended["last_error"] is None
    assert (await ticker.tick()).fired == 0


async def test_a_fire_by_hand_at_or_after_the_end_is_refused(app, client) -> None:
    """``POST /fire`` cannot reach past the end either: not for now once the end has passed,
    and not for an ``at`` at or after it while it has not; before it, it fires."""
    sid = (await _created(client, cadence="hourly"))["schedule_id"]
    tick = _last_tick()
    await _end(app, sid, tick)
    at_end = await client.post(f"/v1/schedules/{sid}/fire", json={"at": tick.isoformat()})
    assert at_end.status_code == 409, at_end.text
    assert at_end.json()["code"] == "CONFLICT"
    assert (await client.post(f"/v1/schedules/{sid}/fire")).status_code == 409
    earlier = (tick - timedelta(hours=1)).isoformat()
    assert (await client.post(f"/v1/schedules/{sid}/fire", json={"at": earlier})).status_code == 409
    assert await queued_runs(app) == []

    later = datetime.now(UTC) + timedelta(days=1)
    await _end(app, sid, later)
    fired = await client.post(f"/v1/schedules/{sid}/fire", json={"at": earlier})
    assert fired.status_code == 200, fired.text
    assert len(await queued_runs(app)) == 1


async def test_a_paused_schedule_past_its_end_stays_unfired_after_resume(
    app, client, ticker
) -> None:
    """Paused with a tick owed, resumed after the end: the owed tick it would otherwise keep
    (it is merely late) is not fired, nothing is armed, no tick fires it."""
    sid = (await _created(client, cadence="daily"))["schedule_id"]
    owed = await arm(app, sid, _last_tick() - timedelta(hours=2))
    await client.patch(f"/v1/schedules/{sid}", json={"enabled": False})
    await _end(app, sid, owed + timedelta(minutes=30))
    resumed = (await client.patch(f"/v1/schedules/{sid}", json={"enabled": True})).json()
    assert resumed["enabled"] is True and resumed["next_fire_at"] is None
    assert (await ticker.tick()).fired == 0
    assert (await ticker.tick(now=datetime.now(UTC) + timedelta(days=7))).fired == 0
    assert await queued_runs(app) == []


async def test_an_end_is_set_moved_and_cleared_by_an_update(app, client, ticker) -> None:
    """``end_at`` in a PATCH sets it (the armed fire is dropped when it is at or past the new
    end), and ``null`` clears it (the schedule is armed again for its next occurrence); an
    update that does not send it leaves it alone."""
    created = await _created(client, cadence="daily")
    sid = created["schedule_id"]
    armed = datetime.fromisoformat(created["next_fire_at"])

    later = _iso(armed + timedelta(days=2))
    kept = (await client.patch(f"/v1/schedules/{sid}", json={"end_at": later})).json()
    assert datetime.fromisoformat(kept["next_fire_at"]) == armed
    renamed = (await client.patch(f"/v1/schedules/{sid}", json={"name": "renamed"})).json()
    assert renamed["end_at"] == kept["end_at"]

    cut = (await client.patch(f"/v1/schedules/{sid}", json={"end_at": _iso(armed)})).json()
    assert cut["next_fire_at"] is None
    assert (await ticker.tick(now=armed + timedelta(minutes=5))).fired == 0

    cleared = (await client.patch(f"/v1/schedules/{sid}", json={"end_at": None})).json()
    assert cleared["end_at"] is None
    assert datetime.fromisoformat(cleared["next_fire_at"]) == armed
    shortened = await client.patch(
        f"/v1/schedules/{sid}", json={"end_at": _iso(armed - timedelta(days=1))}
    )
    assert shortened.json()["next_fire_at"] is None
    again = (
        await client.patch(f"/v1/schedules/{sid}", json={"end_at": _iso(armed + timedelta(days=9))})
    ).json()
    assert datetime.fromisoformat(again["next_fire_at"]) == armed


async def test_without_an_end_a_schedule_fires_as_it_always_did(app, client, ticker) -> None:
    """``end_at`` null (the default): armed, fired and re-armed exactly as before."""
    created = await _created(client, cadence="hourly")
    sid = created["schedule_id"]
    assert created["end_at"] is None
    due = await arm(app, sid, _last_tick() - timedelta(hours=1) + jitter(sid))
    assert (await ticker.tick()).fired == 1
    after = (await client.get(f"/v1/schedules/{sid}")).json()
    assert datetime.fromisoformat(after["next_fire_at"]) > datetime.now(UTC)
    assert datetime.fromisoformat(after["last_fired_at"]) == due


async def test_an_end_is_measured_with_the_deployments_jitter(migrated, memory, blobs) -> None:
    """With ``RUNS__SCHEDULES__JITTER_SECONDS=0`` every schedule fires on its tick, so an
    end one second after a tick keeps that tick."""
    settings = SETTINGS.model_copy(update={"schedules": SchedulesSettings(jitter_seconds=0)})
    async with serving(settings, memory, blobs) as served, client_of(served) as client:
        created = await _created(client, cadence="hourly")
        tick = datetime.fromisoformat(created["next_fire_at"])
        assert tick == tick.replace(minute=0, second=0, microsecond=0)
        sid = created["schedule_id"]
        ending = tick + timedelta(seconds=1)
        kept = (await client.patch(f"/v1/schedules/{sid}", json={"end_at": _iso(ending)})).json()
        assert datetime.fromisoformat(kept["next_fire_at"]) == tick
