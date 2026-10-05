"""The worker queue: queued runs, ``SKIP LOCKED`` claims, leases a worker must keep alive,
and what happens to a run whose worker went quiet."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from sqlalchemy import text
from trellis.contracts.ids import now

from agent_runs.config.constants import MAX_CHECKPOINT_BYTES, MAX_LEASE_LAPSES
from agent_runs.domain.runs import ClaimRequest
from agent_runs.store.runs import RunStore
from tests.conftest import pause, resolution, started


def claim(worker: str = "w1", agents: tuple[str, ...] = ("triage",), lease: int = 30) -> dict:
    return {"worker_id": worker, "agent_ids": list(agents), "lease_seconds": lease}


async def queued(client, **over) -> dict:
    response = await client.post("/v1/runs", json=started(queue=True, **over))
    assert response.status_code == 201, response.text
    return response.json()


async def test_a_queued_run_is_claimed_once_with_a_lease(client) -> None:
    run = await queued(client, input={"q": 1})
    assert run["status"] == "QUEUED"

    response = await client.post("/v1/runs/claim", json=claim())
    assert response.status_code == 200, response.text
    claimed = response.json()
    assert claimed["run"]["run_id"] == run["run_id"]
    assert (claimed["run"]["status"], claimed["run"]["attempt"]) == ("RUNNING", 1)
    assert claimed["lease"]["worker_id"] == "w1"

    assert (await client.post("/v1/runs/claim", json=claim("w2"))).status_code == 204


async def test_a_claim_takes_the_oldest_run_of_its_own_agents(client) -> None:
    other = await queued(client, agent_id="billing")
    first = await queued(client)
    second = await queued(client)
    got = [
        (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
        for _ in range(2)
    ]
    assert got == [first["run_id"], second["run_id"]]
    assert (await client.post("/v1/runs/claim", json=claim())).status_code == 204
    billing = (await client.post("/v1/runs/claim", json=claim(agents=("billing",)))).json()
    assert billing["run"]["run_id"] == other["run_id"]


async def test_a_claim_is_scoped_to_the_tenant(client, other_tenant) -> None:
    await queued(client)
    assert (await other_tenant.post("/v1/runs/claim", json=claim())).status_code == 204


async def test_a_claim_skips_a_run_another_claim_holds(app, client) -> None:
    """The mechanism, shown directly: claim A holds its row (uncommitted) and claim B, in
    its own transaction, is handed the next run instead of waiting for A or taking A's."""
    one, two = await queued(client), await queued(client)
    request = ClaimRequest(**claim())
    async with app.state.sessions() as a, app.state.sessions() as b:
        first = await RunStore(a).claim("acme", request, now=now())
        second = await RunStore(b).claim("acme", request, now=now())
        third = await RunStore(b).claim("acme", request, now=now())
        await a.commit()
        await b.commit()
    assert first is not None and second is not None and third is None
    assert {first.run.run_id, second.run.run_id} == {one["run_id"], two["run_id"]}


async def test_concurrent_claimers_never_get_the_same_run(client) -> None:
    ids = {(await queued(client))["run_id"] for _ in range(20)}

    async def worker(name: str) -> list[str]:
        got = []
        while (
            response := await client.post("/v1/runs/claim", json=claim(name))
        ).status_code == 200:
            got.append(response.json()["run"]["run_id"])
        return got

    claimed = await asyncio.gather(*(worker(f"w{i}") for i in range(8)))
    flat = [run_id for batch in claimed for run_id in batch]
    assert len(flat) == len(set(flat)) == 20
    assert set(flat) == ids


async def test_a_heartbeat_extends_only_the_holders_lease(client) -> None:
    await queued(client)
    claimed = (await client.post("/v1/runs/claim", json=claim(lease=10))).json()
    rid = claimed["run"]["run_id"]

    beat = await client.post(
        f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1", "lease_seconds": 60}
    )
    assert beat.status_code == 200
    assert beat.json()["expires_at"] > claimed["lease"]["expires_at"]
    assert (
        await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "intruder"})
    ).status_code == 409


async def test_a_cancelled_run_answers_its_workers_heartbeat_with_409(client) -> None:
    """The cancel signal a worker gets: its next heartbeat is refused."""
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    await client.post(f"/v1/runs/{rid}/finish", json={"status": "CANCELLED"})
    assert (
        await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1"})
    ).status_code == 409


async def test_a_worker_fences_its_writes_with_its_id(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    stale = await client.post(
        f"/v1/runs/{rid}/finish", params={"worker_id": "w_old"}, json={"status": "SUCCESS"}
    )
    assert stale.status_code == 409
    mine = await client.post(
        f"/v1/runs/{rid}/finish", params={"worker_id": "w1"}, json={"status": "SUCCESS"}
    )
    assert mine.status_code == 200


async def test_lease_bounds_are_enforced(client) -> None:
    assert (await client.post("/v1/runs/claim", json=claim(lease=1))).status_code == 422
    assert (
        await client.post("/v1/runs/claim", json={**claim(), "agent_ids": []})
    ).status_code == 422


async def test_a_lapsed_lease_puts_the_run_back_on_the_queue_as_the_next_attempt(
    app, client
) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim(lease=10))).json()["run"]["run_id"]

    async with app.state.sessions() as db:
        assert await RunStore(db).requeue_lapsed(now=now(), limit=10) == [], "not lapsed yet"
        moved = await RunStore(db).requeue_lapsed(now=now() + timedelta(seconds=11), limit=10)
        await db.commit()
    assert [(r.run_id, r.status.value, r.attempt) for r in moved] == [(rid, "QUEUED", 2)]

    assert (
        await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1"})
    ).status_code == 409
    again = (await client.post("/v1/runs/claim", json=claim("w2"))).json()
    assert (again["run"]["run_id"], again["run"]["attempt"]) == (rid, 2)


async def _lapse(app) -> list:
    """The ticker's lease sweep, past every lease a test hands out."""
    async with app.state.sessions() as db:
        moved = await RunStore(db).requeue_lapsed(now=now() + timedelta(seconds=6), limit=10)
        await db.commit()
    return moved


async def test_a_run_whose_lease_keeps_lapsing_ends_in_error(app, client) -> None:
    await queued(client)
    moved = []
    for attempt in range(1, MAX_LEASE_LAPSES + 1):
        claimed = (await client.post("/v1/runs/claim", json=claim(lease=5))).json()
        assert claimed["run"]["attempt"] == attempt
        moved = await _lapse(app)
    assert moved[0].status.value == "ERROR"
    assert moved[0].error is not None and moved[0].error.code == "lease_expired"
    assert f"lapsed {MAX_LEASE_LAPSES} times" in moved[0].error.message


async def test_review_rounds_do_not_use_up_the_lapses_a_crash_may_take(app, client) -> None:
    """Each answer is an attempt, but only a lapsed lease counts toward failing the run: a
    run reviewed more times than MAX_LEASE_LAPSES is still re-queued when its worker dies,
    and the lapses before and after the reviews add up."""
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim(lease=5))).json()["run"]["run_id"]
    for round_ in range(MAX_LEASE_LAPSES + 1):
        waiting = (
            await client.post(f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=pause(rid))
        ).json()
        await client.post(f"/v1/runs/{rid}/resume", json=resolution(waiting))
        claimed = (await client.post("/v1/runs/claim", json=claim(lease=5))).json()
        assert claimed["run"]["attempt"] == round_ + 2
    for _ in range(MAX_LEASE_LAPSES - 1):
        [requeued] = await _lapse(app)
        assert requeued.status.value == "QUEUED"
        await client.post("/v1/runs/claim", json=claim(lease=5))
    [failed] = await _lapse(app)
    assert (failed.status.value, failed.attempt) == ("ERROR", MAX_LEASE_LAPSES + 6)


async def test_a_durable_run_resumes_onto_the_queue(client) -> None:
    """A paused queued run is continued by a worker, not by whoever answered."""
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    paused = (
        await client.post(f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=pause(rid))
    ).json()
    assert paused["status"] == "PAUSED"

    resumed = (await client.post(f"/v1/runs/{rid}/resume", json=resolution(paused))).json()
    assert (resumed["status"], resumed["attempt"]) == ("QUEUED", 2)
    again = (await client.post("/v1/runs/claim", json=claim("w2"))).json()
    assert again["run"]["last_resolution"]["decision"] == "APPROVE"


async def test_a_queued_run_can_be_cancelled_before_anyone_claims_it(client) -> None:
    run = await queued(client)
    cancelled = await client.post(f"/v1/runs/{run['run_id']}/finish", json={"status": "CANCELLED"})
    assert cancelled.status_code == 200
    assert (await client.post("/v1/runs/claim", json=claim())).status_code == 204


# ------------------------------------------------------------------ the executor's checkpoint

_JOURNAL = {
    "asks": {"ask_1": "yes"},
    "tools": {"sha256:9f2c": {"output": {"po": "PO-7"}}},
    "framework": {"langgraph": {"interrupt_id": "lg_1"}},
}


async def _claimed_and_paused(client, checkpoint=_JOURNAL) -> dict:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    response = await client.post(
        f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=pause(rid, checkpoint)
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_a_pause_keeps_the_checkpoint_for_the_worker_that_resumes(client) -> None:
    """Paused by one worker, resumed by another: the second gets the first's journal, so it
    repeats no side effect."""
    paused = await _claimed_and_paused(client)
    rid = paused["run_id"]
    assert paused["checkpoint"] == _JOURNAL
    assert (await client.get(f"/v1/runs/{rid}")).json()["checkpoint"] == _JOURNAL
    [listed] = (await client.get("/v1/runs", params={"status": "PAUSED"})).json()
    assert "checkpoint" not in listed  # a listing is summaries; the record carries it

    resumed = (await client.post(f"/v1/runs/{rid}/resume", json=resolution(paused))).json()
    assert (resumed["status"], resumed["checkpoint"]) == ("QUEUED", _JOURNAL)
    again = (await client.post("/v1/runs/claim", json=claim("w2"))).json()
    assert again["run"]["checkpoint"] == _JOURNAL
    assert again["run"]["last_resolution"]["decision"] == "APPROVE"


async def test_a_later_pause_replaces_the_checkpoint(client) -> None:
    paused = await _claimed_and_paused(client)
    rid = paused["run_id"]
    await client.post(f"/v1/runs/{rid}/resume", json=resolution(paused))
    await client.post("/v1/runs/claim", json=claim("w2"))
    newer = {"asks": {"ask_1": "yes", "ask_2": "no"}}
    repaused = await client.post(
        f"/v1/runs/{rid}/pause", params={"worker_id": "w2"}, json=pause(rid, newer)
    )
    assert repaused.json()["checkpoint"] == newer


async def test_finishing_clears_the_checkpoint(client) -> None:
    paused = await _claimed_and_paused(client)
    rid = paused["run_id"]
    await client.post(f"/v1/runs/{rid}/resume", json=resolution(paused))
    await client.post("/v1/runs/claim", json=claim("w2"))
    done = await client.post(
        f"/v1/runs/{rid}/finish", params={"worker_id": "w2"}, json={"status": "SUCCESS"}
    )
    assert done.status_code == 200, done.text
    assert done.json()["checkpoint"] is None
    assert (await client.get(f"/v1/runs/{rid}")).json()["checkpoint"] is None


async def test_cancelling_a_paused_run_clears_the_checkpoint(client) -> None:
    paused = await _claimed_and_paused(client)
    cancelled = await client.post(
        f"/v1/runs/{paused['run_id']}/resume", json=resolution(paused, "CANCEL")
    )
    assert (cancelled.json()["status"], cancelled.json()["checkpoint"]) == ("CANCELLED", None)


async def test_a_checkpoint_past_the_bound_is_refused_and_nothing_moves(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    huge = {"blob": "x" * MAX_CHECKPOINT_BYTES}
    refused = await client.post(
        f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=pause(rid, huge)
    )
    assert refused.status_code == 413
    run = (await client.get(f"/v1/runs/{rid}")).json()
    assert (run["status"], run["checkpoint"]) == ("RUNNING", None)


async def test_a_worker_without_the_lease_cannot_write_a_checkpoint(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    fenced = await client.post(
        f"/v1/runs/{rid}/pause", params={"worker_id": "w2"}, json=pause(rid, _JOURNAL)
    )
    assert fenced.status_code == 409
    run = (await client.get(f"/v1/runs/{rid}")).json()
    assert (run["status"], run["checkpoint"]) == ("RUNNING", None)


async def test_a_bare_interrupt_is_no_longer_a_pause_body(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    bare = pause(rid)["interrupt"]
    response = await client.post(f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=bare)
    assert response.status_code == 422


async def test_a_heartbeat_is_refused_once_the_run_stops_running_whatever_the_row_says(
    app, client
) -> None:
    """Defence in depth: every way out of RUNNING clears the lease, but the heartbeat checks
    the status itself too, so a row that still names the worker cannot keep a lease alive on
    a run that is not running."""
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE agent_runs SET status = 'PAUSED' WHERE run_id = :r"), {"r": rid}
        )
    beat = await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1"})
    assert beat.status_code == 409 and "PAUSED" in beat.json()["detail"]


async def test_heartbeat_lease_bounds_are_enforced(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    for lease in (4, 3601):
        body = {"worker_id": "w1", "lease_seconds": lease}
        assert (await client.post(f"/v1/runs/{rid}/heartbeat", json=body)).status_code == 422
    blank = {"worker_id": "   "}
    assert (await client.post(f"/v1/runs/{rid}/heartbeat", json=blank)).status_code == 422


async def test_a_claim_without_a_worker_or_with_too_many_agents_is_refused(client) -> None:
    assert (await client.post("/v1/runs/claim", json={"agent_ids": ["triage"]})).status_code == 422
    many = claim(agents=tuple(f"a{i}" for i in range(101)))
    assert (await client.post("/v1/runs/claim", json=many)).status_code == 422


# ------------------------------------------------------------------ repeated endings


async def test_a_workers_repeated_finish_answers_the_stored_run_and_announces_once(
    app, client
) -> None:
    """A worker whose finish succeeded but whose answer was lost retries it: the same run,
    as stored, and no second run.finished in the outbox."""
    await client.post(
        "/v1/webhooks", json={"url": "https://hooks.example/x", "events": ["run.finished"]}
    )
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    body = {"status": "SUCCESS", "output": {"n": 1}}
    first = await client.post(f"/v1/runs/{rid}/finish", params={"worker_id": "w1"}, json=body)
    again = await client.post(
        f"/v1/runs/{rid}/finish", params={"worker_id": "w1"}, json={**body, "output": 2}
    )
    assert (first.status_code, again.status_code) == (200, 200)
    assert again.json() == first.json()
    async with app.state.engine.connect() as conn:
        owed = await conn.scalar(text("SELECT count(*) FROM webhook_deliveries"))
    assert owed == 1


async def test_a_finish_repeated_by_another_worker_or_another_way_is_refused(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    done = {"status": "SUCCESS"}
    await client.post(f"/v1/runs/{rid}/finish", params={"worker_id": "w1"}, json=done)
    other = await client.post(f"/v1/runs/{rid}/finish", params={"worker_id": "w2"}, json=done)
    assert (other.status_code, other.json()["code"]) == (409, "LEASE_LOST")
    changed = await client.post(
        f"/v1/runs/{rid}/finish", params={"worker_id": "w1"}, json={"status": "ERROR"}
    )
    assert (changed.status_code, changed.json()["code"]) == (409, "LEASE_LOST")
    unfenced = await client.post(f"/v1/runs/{rid}/finish", json=done)
    assert (unfenced.status_code, unfenced.json()["code"]) == (409, "CONFLICT")


async def test_a_workers_repeated_pause_answers_the_stored_run(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    body = pause(rid, checkpoint={"step": 1})
    first = await client.post(f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=body)
    again = await client.post(f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=body)
    assert (first.status_code, again.status_code) == (200, 200)
    assert again.json() == first.json()

    another_question = pause(rid)
    refused = await client.post(
        f"/v1/runs/{rid}/pause", params={"worker_id": "w1"}, json=another_question
    )
    assert (refused.status_code, refused.json()["code"]) == (409, "LEASE_LOST")
    stranger = await client.post(f"/v1/runs/{rid}/pause", params={"worker_id": "w2"}, json=body)
    assert (stranger.status_code, stranger.json()["code"]) == (409, "LEASE_LOST")


async def test_a_lapsed_workers_heartbeat_is_lease_lost(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    await client.post(f"/v1/runs/{rid}/finish", json={"status": "CANCELLED"})
    beat = await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1"})
    assert (beat.status_code, beat.json()["code"]) == (409, "LEASE_LOST")


# ------------------------------------------------------------------ progress checkpoints


async def test_a_heartbeat_saves_progress_the_next_attempt_resumes_from(
    app, client, ticker
) -> None:
    """A worker that crashes after a side effect it checkpointed: the next claim gets the
    journal and repeats nothing."""
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim(lease=5))).json()["run"]["run_id"]
    journal = {"tools": {"call_1": {"output": "PO-17 created"}}}
    beat = await client.post(
        f"/v1/runs/{rid}/heartbeat",
        json={"worker_id": "w1", "lease_seconds": 5, "checkpoint": journal},
    )
    assert beat.status_code == 200
    again = await client.post(
        f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1", "checkpoint": journal}
    )
    assert again.status_code == 200, "saving the same progress twice is harmless"
    plain = await client.post(f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1"})
    assert plain.status_code == 200
    assert (await client.get(f"/v1/runs/{rid}")).json()["checkpoint"] == journal, "kept"

    await ticker.tick(now=now() + timedelta(hours=2))  # the worker died: the lease lapses
    reclaimed = (await client.post("/v1/runs/claim", json=claim("w2"))).json()["run"]
    assert (reclaimed["run_id"], reclaimed["attempt"]) == (rid, 2)
    assert reclaimed["checkpoint"] == journal


async def test_only_the_lease_holder_saves_progress_and_within_the_bound(client) -> None:
    await queued(client)
    rid = (await client.post("/v1/runs/claim", json=claim())).json()["run"]["run_id"]
    stranger = await client.post(
        f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w2", "checkpoint": {"x": 1}}
    )
    assert (stranger.status_code, stranger.json()["code"]) == (409, "LEASE_LOST")
    huge = {"blob": "x" * (MAX_CHECKPOINT_BYTES + 1)}
    too_big = await client.post(
        f"/v1/runs/{rid}/heartbeat", json={"worker_id": "w1", "checkpoint": huge}
    )
    assert (too_big.status_code, too_big.json()["code"]) == (413, "PAYLOAD_TOO_LARGE")
    assert (await client.get(f"/v1/runs/{rid}")).json()["checkpoint"] is None
