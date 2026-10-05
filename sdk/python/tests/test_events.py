"""A run's event log: appending (fenced like a heartbeat), reading by position, and
following it as server-sent events, reconnecting from the last position seen."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import httpx
import pytest
import respx
from conftest import URL, problem
from trellis.contracts.runs import RunEvent, RunEventType
from trellis.runs import (
    DependencyUnavailableError,
    EventsAppended,
    NotFoundError,
    RunEventEntry,
    RunsClient,
)

STREAM = f"{URL}/v1/runs/run_1/events/stream"


def _event(sequence: int) -> RunEvent:
    return RunEvent(
        type=RunEventType.STEP_STARTED, tenant_id="acme", run_id="run_1", sequence=sequence
    )


def _entry(position: int) -> str:
    entry = RunEventEntry(position=position, event=_event(position - 1))
    return f"id: {position}\nevent: STEP_STARTED\ndata: {entry.model_dump_json()}\n\n"


END = 'event: end\ndata: {"status": "SUCCESS"}\n\n'


class Chunks(httpx.AsyncByteStream):
    """A response body sent in pieces, then cut off (``cut``) or ended."""

    def __init__(self, *chunks: str, cut: Exception | None = None) -> None:
        self.chunks = chunks
        self.cut = cut

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk.encode()
        if self.cut is not None:
            raise self.cut


def _sse(*chunks: str, cut: Exception | None = None) -> httpx.Response:
    return httpx.Response(
        200, headers={"Content-Type": "text/event-stream"}, stream=Chunks(*chunks, cut=cut)
    )


async def _follow(runs: RunsClient, after: int = 0) -> list[int]:
    return [entry.position async for entry in runs.stream_events("run_1", after=after)]


async def test_append_sends_the_events_fenced_by_the_worker() -> None:
    with respx.mock:
        route = respx.post(f"{URL}/v1/runs/run_1/events").respond(
            200, json={"appended": 2, "position": 7}
        )
        async with RunsClient(URL, api_key="k") as runs:
            done = await runs.append_events(
                "run_1", [_event(0), _event(1)], worker_id="w-1", tenant="acme"
            )
    assert done == EventsAppended(appended=2, position=7)
    request = route.calls.last.request
    assert request.url.params["worker_id"] == "w-1"
    assert request.headers["X-Trellis-Tenant"] == "acme"
    assert [e["sequence"] for e in json.loads(request.content)["events"]] == [0, 1]


async def test_events_are_read_by_position() -> None:
    with respx.mock:
        entry = json.loads(RunEventEntry(position=3, event=_event(2)).model_dump_json())
        route = respx.get(f"{URL}/v1/runs/run_1/events").respond(200, json=[entry])
        async with RunsClient(URL, api_key="k") as runs:
            got = await runs.events("run_1", after=2, limit=10)
    assert [e.position for e in got] == [3] and got[0].event.sequence == 2
    assert dict(route.calls.last.request.url.params) == {"after": "2", "limit": "10"}


async def test_a_stream_yields_each_event_until_the_run_ended() -> None:
    with respx.mock:
        route = respx.get(STREAM).mock(
            return_value=_sse(_entry(1), ": keepalive\n\n", _entry(2), END, _entry(9))
        )
        async with RunsClient(URL, api_key="k", tenant="acme") as runs:
            assert await _follow(runs, after=0) == [1, 2]
    request = route.calls.last.request
    assert request.headers["Accept"] == "text/event-stream"
    assert request.headers["X-Trellis-Tenant"] == "acme"


async def test_a_stream_reconnects_from_the_last_position(slept: list[float]) -> None:
    answers = [
        _sse(_entry(4), cut=httpx.ReadError("cut")),  # the connection drops
        _sse(_entry(5)),  # the service ended a long stream: no `end`
        _sse(_entry(6), END),
    ]
    with respx.mock:
        route = respx.get(STREAM).mock(side_effect=answers)
        async with RunsClient(URL, api_key="k") as runs:
            assert await _follow(runs, after=3) == [4, 5, 6]
    assert [c.request.url.params["after"] for c in route.calls] == ["3", "4", "5"]
    assert len(slept) == 1, "a drop waits a little; an ended stream reconnects at once"


async def test_a_stream_that_cannot_be_reopened_raises(slept: list[float]) -> None:
    with respx.mock:
        respx.get(STREAM).mock(side_effect=httpx.ConnectError("down"))
        async with RunsClient(URL, api_key="k", max_retries=2) as runs:
            with pytest.raises(DependencyUnavailableError, match="events of run_1 unreachable"):
                await _follow(runs)
    assert len(slept) == 2


async def test_a_stream_refused_by_the_service_raises_its_error() -> None:
    with respx.mock:
        respx.get(STREAM).respond(404, json=problem(404, "NOT_FOUND", "no run run_1"))
        async with RunsClient(URL, api_key="k") as runs:
            with pytest.raises(NotFoundError, match="no run run_1"):
                await _follow(runs)
