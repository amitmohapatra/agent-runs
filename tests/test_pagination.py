"""Every listing pages the same way: ``cursor`` and ``limit`` in, ``Link: <…>; rel="next"``
out exactly when there is more, bare arrays as before. And every create that answers 201
says where the new record lives (``Location``)."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest
from httpx import AsyncClient
from trellis.contracts.ids import now
from trellis.contracts.runs import RunStart

from agent_runs.api.pagination import decode_cursor, encode_cursor
from agent_runs.domain.errors import Unprocessable
from agent_runs.store.runs import RunStore
from tests.conftest import pause, resolution, scheduled, started


def next_url(response: Any) -> str | None:
    link = response.headers.get("link")
    if link is None:
        return None
    url, rel = link.split(";")
    assert rel.strip() == 'rel="next"'
    return url.strip().removeprefix("<").removesuffix(">")


async def every_page(client: AsyncClient, url: str) -> list[list[dict[str, Any]]]:
    pages: list[list[dict[str, Any]]] = []
    next_page: str | None = url
    while next_page is not None:
        response = await client.get(next_page)
        assert response.status_code == 200, response.text
        pages.append(response.json())
        next_page = next_url(response)
    return pages


async def test_runs_page_newest_first_without_gaps_or_repeats(client) -> None:
    made = [(await client.post("/v1/runs", json=started())).json()["run_id"] for _ in range(5)]
    pages = await every_page(client, "/v1/runs?limit=2")
    assert [len(p) for p in pages] == [2, 2, 1]
    assert [r["run_id"] for p in pages for r in p] == made[::-1]


async def test_the_link_keeps_the_filters_and_the_limit(client) -> None:
    for _ in range(3):
        await client.post("/v1/runs", json=started(agent_id="billing"))
    await client.post("/v1/runs", json=started(agent_id="triage"))
    first = await client.get("/v1/runs", params={"agent_id": "billing", "limit": 2})
    url = next_url(first)
    assert url is not None and "agent_id=billing" in url and "limit=2" in url
    rest = (await client.get(url)).json()
    assert [r["agent_id"] for r in rest] == ["billing"]


async def test_runs_started_in_the_same_instant_still_page_exactly(app, client) -> None:
    """The keyset is (created_at, run_id): a tie on the instant is broken by the id."""
    instant = now()
    async with app.state.sessions() as db:
        for n in range(4):
            await RunStore(db).start(
                RunStart(**started(run_id=f"run_tie{n}")), queue=False, now=instant
            )
        await db.commit()
    pages = await every_page(client, "/v1/runs?limit=3")
    assert [r["run_id"] for p in pages for r in p] == [f"run_tie{n}" for n in (3, 2, 1, 0)]


async def test_a_page_that_is_the_last_has_no_link(client) -> None:
    await client.post("/v1/runs", json=started())
    exactly = await client.get("/v1/runs", params={"limit": 1})
    assert len(exactly.json()) == 1 and "link" not in exactly.headers
    assert "link" not in (await client.get("/v1/runs")).headers


async def test_schedules_page_newest_first(client) -> None:
    made = [
        (await client.post("/v1/schedules", json=scheduled())).json()["schedule_id"]
        for _ in range(3)
    ]
    pages = await every_page(client, "/v1/schedules?limit=2")
    assert [s["schedule_id"] for p in pages for s in p] == made[::-1]


async def test_webhooks_page_oldest_first(client) -> None:
    made = [
        (
            await client.post(
                "/v1/webhooks",
                json={"url": f"https://hooks.example/{n}", "events": ["run.paused"]},
            )
        ).json()["webhook_id"]
        for n in range(3)
    ]
    pages = await every_page(client, "/v1/webhooks?limit=2")
    assert [w["webhook_id"] for p in pages for w in p] == made


async def test_a_runs_resolutions_page_oldest_first(client) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    rid = run["run_id"]
    asked = []
    for assignee in ("user_a", "user_b", "user_c"):
        waiting = (
            await client.post(f"/v1/runs/{rid}/pause", json=pause(rid, assignee=assignee))
        ).json()
        asked.append(waiting["awaiting"]["interrupt_id"])
        assert (await client.post(f"/v1/runs/{rid}/resume", json=resolution(waiting))).is_success
    pages = await every_page(client, f"/v1/runs/{rid}/resolutions?limit=2")
    assert [len(p) for p in pages] == [2, 1]
    assert [e["interrupt"]["interrupt_id"] for p in pages for e in p] == asked


@pytest.mark.parametrize(
    "cursor",
    [
        "not-base64-json!",
        base64.urlsafe_b64encode(b"[1, 2]").decode(),
        encode_cursor({"created_at": "2026-10-01T00:00:00+00:00", "schedule_id": "sch_x"}),
        encode_cursor({"created_at": "2026-10-01T00:00:00", "run_id": "run_x"}),
        encode_cursor({"created_at": 5, "run_id": "run_x"}),
        encode_cursor({"created_at": "yesterday", "run_id": "run_x"}),
    ],
    ids=["garbage", "not-an-object", "another-listing", "naive", "not-a-string", "not-a-date"],
)
async def test_a_cursor_this_listing_did_not_issue_is_a_422(client, cursor: str) -> None:
    response = await client.get("/v1/runs", params={"cursor": cursor})
    assert (response.status_code, response.json()["code"]) == (422, "VALIDATION")
    assert response.json()["details"]["errors"][0]["loc"] == ["query", "cursor"]


def test_a_cursor_round_trips_its_position() -> None:
    position = {"created_at": now(), "run_id": "run_x"}
    fields = {"created_at": type(position["created_at"]), "run_id": str}
    assert decode_cursor(encode_cursor(position), fields=fields) == position
    assert decode_cursor(None, fields=fields) is None
    raw = json.loads(base64.urlsafe_b64decode(encode_cursor(position) + "=="))
    assert set(raw) == {"created_at", "run_id"}
    with pytest.raises(Unprocessable):
        decode_cursor("@@@", fields=fields)


# ------------------------------------------------------------------ Location


async def test_creates_say_where_the_record_lives_and_repeats_do_not(client) -> None:
    body = started(run_id="run_loc")
    created = await client.post("/v1/runs", json=body)
    assert created.headers["location"] == "/v1/runs/run_loc"
    assert (await client.get(created.headers["location"])).status_code == 200
    assert "location" not in (await client.post("/v1/runs", json=body)).headers

    spec = scheduled()
    schedule = await client.post("/v1/schedules", json=spec)
    assert schedule.headers["location"] == f"/v1/schedules/{schedule.json()['schedule_id']}"
    assert (await client.get(schedule.headers["location"])).status_code == 200
    assert "location" not in (await client.post("/v1/schedules", json=spec)).headers

    hook = await client.post(
        "/v1/webhooks", json={"url": "https://hooks.example/x", "events": ["run.finished"]}
    )
    read = await client.get(hook.headers["location"])
    assert read.status_code == 200 and "secret" not in read.json()

    blob = await client.post("/v1/runs/run_loc/artifacts", content=b"x")
    assert blob.headers["location"] == blob.json()["uri"]
    assert (await client.get(blob.headers["location"])).content == b"x"
    assert "location" not in (await client.post("/v1/runs/run_loc/artifacts", content=b"x")).headers


async def test_a_webhook_is_read_only_in_its_own_tenant(client, other_tenant) -> None:
    hook = await client.post(
        "/v1/webhooks", json={"url": "https://hooks.example/x", "events": ["run.finished"]}
    )
    theirs = await other_tenant.get(hook.headers["location"])
    assert (theirs.status_code, theirs.json()["code"]) == (404, "NOT_FOUND")
