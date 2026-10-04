"""Artifacts, schedules and webhooks: what each call sends and what it answers."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import httpx
import pytest
import respx
from conftest import URL, artifact, record, schedule, webhook
from trellis.contracts.runs import ScheduleSpec
from trellis.runs import NotFoundError, RunsClient, ScheduleUpdate, WebhookEvent


@pytest.fixture
def runs() -> RunsClient:
    return RunsClient(URL, api_key="k", max_retries=0)


def sent(route: respx.Route) -> httpx.Request:
    return route.calls.last.request


# --------------------------------------------------------------------------- artifacts
@respx.mock
async def test_an_upload_sends_the_bytes_their_type_and_their_checksum(runs: RunsClient) -> None:
    route = respx.post(f"{URL}/v1/runs/run_1/artifacts").respond(201, json=artifact())
    data = b'{"rows": []}'
    ref = await runs.artifacts.upload("run_1", data, worker_id="w-1", tenant="acme")
    assert ref.artifact_id == "art_1"
    request = sent(route)
    assert request.content == data
    assert request.headers["Content-Type"] == "application/json"
    assert request.url.params["checksum"] == f"sha256:{hashlib.sha256(data).hexdigest()}"
    assert request.url.params["worker_id"] == "w-1"
    await runs.artifacts.upload("run_1", b"diff", mime_type="text/x-diff")
    assert sent(route).headers["Content-Type"] == "text/x-diff"
    assert "worker_id" not in sent(route).url.params


@respx.mock
async def test_a_download_answers_the_bytes_or_none(runs: RunsClient) -> None:
    route = respx.get(f"{URL}/v1/artifacts/art_1")
    route.side_effect = [
        httpx.Response(200, content=b"\x00\x01", headers={"Content-Type": "image/png"}),
        httpx.Response(404, json={"code": "NOT_FOUND", "detail": "no artifact"}),
    ]
    assert await runs.artifacts.download("art_1", tenant="acme") == b"\x00\x01"
    assert sent(route).headers["X-Trellis-Tenant"] == "acme"
    assert await runs.artifacts.download("art_1") is None


# --------------------------------------------------------------------------- schedules
SPEC = ScheduleSpec(
    tenant_id="acme", agent_id="briefing", name="morning", cadence="daily", on_behalf_of="u"
)


@respx.mock
async def test_create_sends_the_spec_with_its_tenant(runs: RunsClient) -> None:
    route = respx.post(f"{URL}/v1/schedules").respond(201, json=schedule())
    made = await runs.schedules.create(SPEC)
    assert made.schedule_id == "sch_1"
    assert json.loads(sent(route).content)["cadence"] == "daily"
    assert sent(route).headers["X-Trellis-Tenant"] == "acme"


@respx.mock
async def test_list_sends_the_filters(runs: RunsClient) -> None:
    link = '<http://runs.test/v1/schedules?cursor=c2&limit=1>; rel="next"'
    route = respx.get(f"{URL}/v1/schedules").respond(200, json=[schedule()], headers={"Link": link})
    page = await runs.schedules.list(enabled=False, agent_id="briefing", cursor="c1", limit=1)
    assert page.items[0].schedule_id == "sch_1" and page.next_cursor == "c2"
    assert dict(sent(route).url.params) == {
        "enabled": "false",
        "agent_id": "briefing",
        "cursor": "c1",
        "limit": "1",
    }
    await runs.schedules.list(enabled=True, tenant="acme")
    assert dict(sent(route).url.params) == {"enabled": "true", "limit": "50"}
    await runs.schedules.list()
    assert dict(sent(route).url.params) == {"limit": "50"}


@respx.mock
async def test_get_answers_the_schedule_or_none(runs: RunsClient) -> None:
    route = respx.get(f"{URL}/v1/schedules/sch_1")
    route.side_effect = [httpx.Response(200, json=schedule()), httpx.Response(404)]
    found = await runs.schedules.get("sch_1")
    assert found is not None and found.name == "morning briefing"
    assert await runs.schedules.get("sch_1", tenant="acme") is None


@respx.mock
async def test_update_sends_only_the_fields_set(runs: RunsClient) -> None:
    route = respx.patch(f"{URL}/v1/schedules/sch_1").respond(200, json=schedule(enabled=False))
    paused = await runs.schedules.update("sch_1", ScheduleUpdate(enabled=False), tenant="acme")
    assert paused.enabled is False
    assert json.loads(sent(route).content) == {"enabled": False}
    await runs.schedules.update("sch_1", ScheduleUpdate(input=None, metadata={"a": 1}))
    assert json.loads(sent(route).content) == {"input": None, "metadata": {"a": 1}}


@respx.mock
async def test_delete_answers_nothing_and_a_missing_one_raises(runs: RunsClient) -> None:
    route = respx.delete(f"{URL}/v1/schedules/sch_1")
    route.side_effect = [httpx.Response(204), httpx.Response(404, json={"code": "NOT_FOUND"})]
    assert await runs.schedules.delete("sch_1", tenant="acme") is None
    with pytest.raises(NotFoundError):
        await runs.schedules.delete("sch_1")


@respx.mock
async def test_fire_sends_the_tick_when_given(runs: RunsClient) -> None:
    fired = {
        "schedule_id": "sch_1",
        "run_id": "run_9",
        "fire_time": "2026-10-01T06:00:00Z",
        "idempotency_key": "sch_1@2026-10-01T06:00:00+00:00",
        "schedule": schedule(),
    }
    route = respx.post(f"{URL}/v1/schedules/sch_1/fire").respond(200, json=fired)
    at = datetime(2026, 10, 1, 6, tzinfo=UTC)
    result = await runs.schedules.fire("sch_1", at=at, tenant="acme")
    assert result.run_id == "run_9" and result.schedule.schedule_id == "sch_1"
    assert json.loads(sent(route).content) == {"at": "2026-10-01T06:00:00+00:00"}
    await runs.schedules.fire("sch_1")
    assert sent(route).content == b""


# --------------------------------------------------------------------------- webhooks
@respx.mock
async def test_create_answers_the_secret_once(runs: RunsClient) -> None:
    route = respx.post(f"{URL}/v1/webhooks").respond(201, json={**webhook(), "secret": "whsec_x"})
    made = await runs.webhooks.create(
        "https://ui.example/h", [WebhookEvent.PAUSED, WebhookEvent("run.finished")], tenant="acme"
    )
    assert made.secret == "whsec_x" and made.events == [WebhookEvent.FINISHED, WebhookEvent.PAUSED]
    assert json.loads(sent(route).content) == {
        "url": "https://ui.example/h",
        "events": ["run.paused", "run.finished"],
    }


@respx.mock
async def test_subscriptions_are_listed_read_and_deleted(runs: RunsClient) -> None:
    listing = respx.get(f"{URL}/v1/webhooks").respond(200, json=[webhook()])
    page = await runs.webhooks.list(cursor="c1", limit=5, tenant="acme")
    assert page.items[0].webhook_id == "wh_1" and not hasattr(page.items[0], "secret")
    assert dict(sent(listing).url.params) == {"cursor": "c1", "limit": "5"}
    await runs.webhooks.list()
    assert dict(sent(listing).url.params) == {"limit": "50"}
    read = respx.get(f"{URL}/v1/webhooks/wh_1")
    read.side_effect = [httpx.Response(200, json=webhook()), httpx.Response(404)]
    found = await runs.webhooks.get("wh_1")
    assert found is not None and found.url == "https://ui.example/h"
    assert await runs.webhooks.get("wh_1", tenant="acme") is None
    gone = respx.delete(f"{URL}/v1/webhooks/wh_1").respond(204)
    assert await runs.webhooks.delete("wh_1", tenant="acme") is None
    assert gone.call_count == 1


async def test_a_record_with_a_field_the_sdk_does_not_know_still_parses() -> None:
    with respx.mock:
        respx.get(f"{URL}/v1/runs/run_1").respond(200, json={**record(), "future_field": 1})
        found = await RunsClient(URL).get("run_1")
    assert found is not None and found.run_id == "run_1"
