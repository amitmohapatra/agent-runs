"""A concurrency key's line as clients read it: each run's place (``placed_at``, decided here,
under the key's lock, and read by everyone else) on its record and its summary, and the
listing of exactly one key's runs (``concurrency_key``), tenant by tenant, paged without
missing a run that stays in it."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from tests.conftest import pause, resolution, started


def _at(value: str) -> datetime:
    return datetime.fromisoformat(value)


async def _start(client, **over: Any) -> dict[str, Any]:
    response = await client.post("/v1/runs", json=started(**over))
    assert response.status_code == 201, response.text
    return response.json()


async def _listed(client, **params: Any) -> list[dict[str, Any]]:
    response = await client.get("/v1/runs", params=params)
    assert response.status_code == 200, response.text
    return response.json()


async def _answered(client, run: dict[str, Any]) -> dict[str, Any]:
    """The run paused on a question and continued after its answer."""
    asked = await client.post(f"/v1/runs/{run['run_id']}/pause", json=pause(run["run_id"]))
    assert asked.status_code == 200, asked.text
    answered = await client.post(f"/v1/runs/{run['run_id']}/resume", json=resolution(asked.json()))
    assert answered.status_code == 200, answered.text
    return answered.json()


async def test_a_keyed_runs_place_is_on_its_record_and_its_summary(client) -> None:
    """ARCH-15: a run of a key is placed when it is started; continued after its answer it
    takes its place again, after the run that came in while it waited (its answer's
    ``recorded_at``), and its ``created_at`` stays. A run with no key has no place."""
    first = await _start(client, concurrency_key="thread:place")
    second = await _start(client, concurrency_key="thread:place")
    keyless = await _start(client)
    assert first["placed_at"] == first["created_at"]
    assert _at(second["placed_at"]) > _at(first["placed_at"])
    assert keyless["placed_at"] is None
    continued = await _answered(client, first)
    [entry] = (await client.get(f"/v1/runs/{first['run_id']}/resolutions")).json()
    assert continued["placed_at"] == entry["recorded_at"]
    assert _at(continued["placed_at"]) > _at(second["placed_at"])
    read = (await client.get(f"/v1/runs/{first['run_id']}")).json()
    assert (read["placed_at"], read["created_at"]) == (entry["recorded_at"], first["created_at"])
    listed = {s["run_id"]: s["placed_at"] for s in await _listed(client, agent_id="triage")}
    assert listed[first["run_id"]] == entry["recorded_at"]
    assert listed[second["run_id"]] == second["placed_at"]
    assert listed[keyless["run_id"]] is None


async def test_a_listing_by_key_is_that_keys_runs_of_the_callers_tenant(
    client, other_tenant
) -> None:
    """SEC-R-11: ``concurrency_key`` lists exactly that key's runs, in the caller's tenant: a
    run of another key on the same thread name, or another tenant's run of the same key, is
    never listed."""
    mine = await _start(client, concurrency_key="thread:c1", thread_id="t1")
    await _start(client, concurrency_key="thread:c2", thread_id="t1")
    theirs = await other_tenant.post(
        "/v1/runs", json=started(tenant_id="globex", concurrency_key="thread:c1")
    )
    assert theirs.status_code == 201, theirs.text
    for status in (None, "RUNNING"):
        params = {"concurrency_key": "thread:c1"} | ({"status": status} if status else {})
        assert [s["run_id"] for s in await _listed(client, **params)] == [mine["run_id"]]
    listed = await other_tenant.get("/v1/runs", params={"concurrency_key": "thread:c1"})
    assert [s["run_id"] for s in listed.json()] == [theirs.json()["run_id"]]
    assert await _listed(client, concurrency_key="thread:none") == []


async def test_paging_a_keys_line_never_misses_a_run_that_stays_in_it(client) -> None:
    """The listing's cursor is ``(created_at, run_id)``, newest first, and ``created_at`` is
    written once: runs that take their place again between two pages (answered, so their
    ``placed_at`` and ``updated_at`` move past every other run's) do not move in it, so a
    run running throughout is on exactly one page."""
    runs = [await _start(client, concurrency_key="thread:paged") for _ in range(5)]
    mine = runs[2]["run_id"]
    url: str | None = "/v1/runs?concurrency_key=thread:paged&status=RUNNING&limit=2"
    seen: list[dict[str, Any]] = []
    while url is not None:
        response = await client.get(url)
        seen += response.json()
        unseen = [r for r in runs if r["run_id"] not in {s["run_id"] for s in seen}]
        for moved in [r for r in unseen if r["run_id"] != mine][:1]:
            await _answered(client, moved)  # re-placed after the line, RUNNING again
        url = response.links.get("next", {}).get("url")
    ids = [s["run_id"] for s in seen]
    assert ids.count(mine) == 1
    created = {r["run_id"]: _at(r["created_at"]) for r in runs}
    assert [created[i] for i in ids] == sorted((created[i] for i in ids), reverse=True)
