"""The worker loop over a store in memory: claiming, the job's fenced writes, heartbeats and a
lost lease, failures, idling, stopping (grace, release, a second stop) and the signals."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from collections.abc import Callable, Iterator, Sequence
from typing import Any

import pytest
from conftest import NOW, interrupt
from trellis.contracts.errors import AgentError
from trellis.contracts.runs import Interrupt, RunRecord, RunStatus
from trellis.runs import (
    RELEASED,
    Claimed,
    Job,
    Lease,
    LeaseLostError,
    RunsClient,
    Worker,
    WorkerStore,
)
from trellis.runs import worker as worker_module


def run(run_id: str = "run_1", tenant: str = "acme") -> RunRecord:
    return RunRecord(run_id=run_id, tenant_id=tenant, agent_id="triage", status=RunStatus.RUNNING)


class Store:
    """A queue in memory that records every call, as agent-runs would see them."""

    def __init__(self, *queued: RunRecord) -> None:
        self.queue = list(queued)
        self.claims: list[tuple[str, list[str], int, str | None]] = []
        self.beats: list[dict[str, Any]] = []
        self.writes: list[tuple[str, dict[str, Any]]] = []
        self.claim_error: Exception | None = None
        self.beat_error: Exception | None = None
        #: set on every claim and heartbeat, for a test waiting until enough of them came
        self.changed = asyncio.Event()

    async def claim(
        self,
        worker_id: str,
        agent_ids: Sequence[str],
        *,
        lease_seconds: int = 60,
        tenant: str | None = None,
    ) -> Claimed | None:
        self.claims.append((worker_id, [*agent_ids], lease_seconds, tenant))
        self.changed.set()
        if self.claim_error is not None:
            raise self.claim_error
        if not self.queue:
            return None
        record = self.queue.pop(0)
        lease = Lease(run_id=record.run_id, worker_id=worker_id, expires_at=NOW)
        return Claimed(run=record, lease=lease)

    async def heartbeat(
        self,
        run_id: str,
        worker_id: str,
        *,
        lease_seconds: int = 60,
        checkpoint: dict[str, Any] | None = None,
        tenant: str | None = None,
    ) -> Lease:
        self.beats.append(
            {
                "run_id": run_id,
                "worker_id": worker_id,
                "lease_seconds": lease_seconds,
                "checkpoint": checkpoint,
                "tenant": tenant,
            }
        )
        self.changed.set()
        if self.beat_error is not None:
            raise self.beat_error
        return Lease(run_id=run_id, worker_id=worker_id, expires_at=NOW)

    async def pause(
        self,
        interrupt: Interrupt,
        *,
        checkpoint: dict[str, Any] | None = None,
        worker_id: str | None = None,
    ) -> RunRecord:
        self.writes.append(
            ("pause", {"interrupt": interrupt, "checkpoint": checkpoint, "worker_id": worker_id})
        )
        return run(interrupt.run_id).model_copy(
            update={"status": RunStatus.PAUSED, "awaiting": interrupt}
        )

    async def finish(
        self,
        run_id: str,
        status: RunStatus,
        *,
        output: Any = None,
        error: AgentError | None = None,
        worker_id: str | None = None,
        tenant: str | None = None,
    ) -> RunRecord:
        self.writes.append(
            (
                "finish",
                {
                    "run_id": run_id,
                    "status": status,
                    "output": output,
                    "error": error,
                    "worker_id": worker_id,
                    "tenant": tenant,
                },
            )
        )
        return run(run_id).model_copy(update={"status": status, "output": output})

    async def until(self, done: Callable[[], bool]) -> None:
        while not done():
            self.changed.clear()
            await self.changed.wait()


@pytest.fixture(autouse=True)
def quick_heartbeats(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """A heartbeat every few milliseconds instead of every 20 s."""

    async def tick(seconds: float) -> None:
        assert seconds == pytest.approx(60 / 3)
        await asyncio.sleep(0.005)

    monkeypatch.setattr(worker_module, "_sleep", tick)
    yield


def forever(started: asyncio.Event | None = None) -> Callable[[Job], Any]:
    async def handler(job: Job) -> None:
        if started is not None:
            started.set()
        await asyncio.Event().wait()

    return handler


async def nothing(job: Job) -> None:
    return None


# --------------------------------------------------------------------------- set-up
def test_a_worker_needs_an_agent_and_a_slot() -> None:
    with pytest.raises(ValueError, match="at least one agent"):
        Worker(Store(), nothing, [])
    with pytest.raises(ValueError, match="at least one run"):
        Worker(Store(), nothing, ["triage"], concurrency=0)


def test_the_default_concurrency_is_the_cpu_count_within_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for cpus, expected in ((None, 1), (2, 2), (64, 8)):
        monkeypatch.setattr("trellis.runs.worker.os.cpu_count", lambda cpus=cpus: cpus)
        assert Worker(Store(), nothing, ["triage"]).concurrency == expected
    assert Worker(Store(), nothing, ["triage"], concurrency=3).concurrency == 3


def test_the_worker_id_names_the_host_and_process_unless_given() -> None:
    assert f":{os.getpid()}:" in Worker(Store(), nothing, ["triage"]).worker_id
    assert Worker(Store(), nothing, ["triage"], worker_id="w-7").worker_id == "w-7"


def test_a_runs_client_is_a_worker_store() -> None:
    store: WorkerStore = RunsClient("http://runs.test")
    assert Worker(store, nothing, ["triage"]).store is store


# --------------------------------------------------------------------------- one run
async def test_run_once_hands_the_claimed_run_to_the_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = Store(run("run_1", tenant="globex"))
    jobs: list[Job] = []

    async def handler(job: Job) -> str:  # any result: the harness's composition returns one
        jobs.append(job)
        await job.checkpoint({"step": 1})
        await job.pause(interrupt("run_1"), checkpoint={"step": 2})
        await job.finish(RunStatus.SUCCESS, output={"ok": True})
        return "the agent's answer"

    worker = Worker(
        store, handler, ("triage", "billing"), worker_id="w-1", lease_seconds=90, tenant="globex"
    )
    # no heartbeat of the worker's own here: only the job's checkpoint is a heartbeat
    monkeypatch.setattr(worker_module, "_sleep", lambda seconds: asyncio.Event().wait())
    assert await worker.run_once() is True
    assert store.claims == [("w-1", ["triage", "billing"], 90, "globex")]
    [job] = jobs
    assert (job.record.run_id, job.worker_id, job.lease_seconds) == ("run_1", "w-1", 90)
    assert "store" not in repr(job)
    assert store.beats == [
        {
            "run_id": "run_1",
            "worker_id": "w-1",
            "lease_seconds": 90,
            "checkpoint": {"step": 1},
            "tenant": "globex",
        }
    ]
    (pause, paused), (finish, finished) = store.writes
    assert pause == "pause" and paused["worker_id"] == "w-1" and paused["checkpoint"] == {"step": 2}
    assert finish == "finish" and finished == {
        "run_id": "run_1",
        "status": RunStatus.SUCCESS,
        "output": {"ok": True},
        "error": None,
        "worker_id": "w-1",
        "tenant": "globex",
    }
    assert await worker.run_once() is False  # nothing queued any more


async def test_the_lease_is_renewed_while_the_handler_runs() -> None:
    store = Store(run())
    done = asyncio.Event()

    async def handler(job: Job) -> None:
        await store.until(lambda: bool(store.beats))
        done.set()

    assert await Worker(store, handler, ["triage"], worker_id="w-1").run_once()
    assert done.is_set()
    assert store.beats[0] == {
        "run_id": "run_1",
        "worker_id": "w-1",
        "lease_seconds": 60,
        "checkpoint": None,
        "tenant": "acme",
    }


async def test_a_failed_heartbeat_is_logged_and_the_run_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = Store(run())
    store.beat_error = ConnectionError("blip")
    beats_seen = asyncio.Event()

    async def handler(job: Job) -> None:
        await store.until(lambda: len(store.beats) >= 2)
        beats_seen.set()

    with caplog.at_level(logging.WARNING, logger="trellis.runs.worker"):
        assert await Worker(store, handler, ["triage"]).run_once()
    assert beats_seen.is_set()
    assert "heartbeat for run_1 failed: blip" in caplog.text


async def test_a_lost_lease_cancels_the_handler(caplog: pytest.LogCaptureFixture) -> None:
    store = Store(run())
    store.beat_error = LeaseLostError("another worker holds run_1", status=409)
    cancelled = asyncio.Event()

    async def handler(job: Job) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as exc:
            assert RELEASED not in exc.args  # lost, not released
            cancelled.set()
            raise

    with caplog.at_level(logging.WARNING, logger="trellis.runs.worker"):
        assert await Worker(store, handler, ["triage"]).run_once()
    assert cancelled.is_set() and len(store.beats) == 1
    assert "lease on run_1 lost: stopping it" in caplog.text


async def test_a_handler_whose_write_lost_the_lease_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def handler(job: Job) -> None:
        raise LeaseLostError("not yours", status=409)

    with caplog.at_level(logging.WARNING, logger="trellis.runs.worker"):
        assert await Worker(Store(run()), handler, ["triage"]).run_once()
    assert "lease on run_1 lost while the handler wrote" in caplog.text


async def test_a_handler_that_breaks_is_logged_and_the_worker_goes_on(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def broken(job: Job) -> None:
        raise RuntimeError("boom")

    worker = Worker(Store(run("run_1"), run("run_2")), broken, ["triage"])
    with caplog.at_level(logging.ERROR, logger="trellis.runs.worker"):
        assert await worker.run_once() and await worker.run_once()
    assert caplog.text.count("failed in the worker") == 2


async def test_a_failed_claim_is_logged_and_means_no_work(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = Store(run())
    store.claim_error = ConnectionError("agent-runs is down")
    with caplog.at_level(logging.WARNING, logger="trellis.runs.worker"):
        assert await Worker(store, nothing, ["triage"]).run_once() is False
    assert "claim failed: agent-runs is down" in caplog.text


async def test_a_worker_cancelled_as_its_run_finishes_still_stops() -> None:
    """The handler ends normally, but the worker was cancelled in the same moment: the
    cancellation is the worker's own and is not swallowed."""
    outer: list[asyncio.Task[Any]] = []

    async def finishing(job: Job) -> str:
        asyncio.get_running_loop().call_soon(outer[0].cancel)
        return "finished"

    worker = Worker(Store(), finishing, ["triage"])
    execute = asyncio.create_task(worker._execute(run()))
    outer.append(execute)
    with pytest.raises(asyncio.CancelledError):
        await execute


async def test_the_harness_composition_fits() -> None:
    """``handler=lambda job: agent._claimed(job.record, job.worker_id, lease_seconds=...)``:
    a lambda answering a coroutine with a result."""
    seen: list[tuple[str, str, int]] = []

    async def claimed(record: RunRecord, worker_id: str, *, lease_seconds: float) -> RunRecord:
        seen.append((record.run_id, worker_id, int(lease_seconds)))
        return record

    worker = Worker(
        Store(run()),
        lambda job: claimed(job.record, job.worker_id, lease_seconds=job.lease_seconds),
        ["triage"],
        worker_id="w-1",
    )
    assert await worker.run_once()
    assert seen == [("run_1", "w-1", 60)]


# --------------------------------------------------------------------------- the loop
async def test_an_idle_worker_keeps_asking_and_takes_work_that_arrives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module, "IDLE_SECONDS", 0.0)
    store = Store()
    handled = asyncio.Event()

    async def handler(job: Job) -> None:
        handled.set()

    task = asyncio.create_task(Worker(store, handler, ["triage"]).run())
    await store.until(lambda: len(store.claims) >= 3)
    store.queue.append(run())  # queued while the worker idles
    await asyncio.wait_for(handled.wait(), 5)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_cancelling_the_loop_cancels_the_handlers_it_holds() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(job: Job) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    task = asyncio.create_task(Worker(Store(run()), handler, ["triage"]).run())
    await started.wait()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert cancelled.is_set()


async def test_a_stopped_worker_lets_its_runs_finish_and_claims_nothing_more() -> None:
    release = asyncio.Event()
    started = asyncio.Event()
    finished: list[str] = []

    async def slow(job: Job) -> None:
        started.set()
        await release.wait()
        finished.append(job.record.run_id)

    store = Store(run("run_1"))
    worker = Worker(store, slow, ["triage"], concurrency=1)
    task = asyncio.create_task(worker.run())
    await started.wait()
    store.queue.append(run("run_2"))  # queued after the stop: left for later
    worker.stop()
    await asyncio.sleep(0.01)
    assert not task.done()  # waiting for the run it holds
    release.set()
    await asyncio.wait_for(task, 5)
    assert finished == ["run_1"] and [r.run_id for r in store.queue] == ["run_2"]


async def test_a_run_past_the_grace_period_is_released(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(worker_module, "GRACE_SECONDS", 0.05)
    started = asyncio.Event()
    reasons: list[tuple[Any, ...]] = []

    async def handler(job: Job) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as exc:
            reasons.append(exc.args)
            raise

    worker = Worker(Store(run()), handler, ["triage"])
    task = asyncio.create_task(worker.run())
    await started.wait()
    with caplog.at_level(logging.INFO, logger="trellis.runs.worker"):
        worker.stop()
        await asyncio.wait_for(task, 5)
    assert reasons == [(RELEASED,)]  # released, so the handler writes nothing
    assert "1 run(s) in flight" in caplog.text
    assert "run run_1 released" in caplog.text


async def test_a_second_stop_releases_the_runs_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_module, "GRACE_SECONDS", 60.0)
    started = asyncio.Event()
    worker = Worker(Store(run()), forever(started), ["triage"])
    task = asyncio.create_task(worker.run())
    await started.wait()
    worker.stop()
    await asyncio.sleep(0.01)
    worker.stop()
    await asyncio.wait_for(task, 5)


async def test_a_stop_while_every_slot_is_busy_is_heard() -> None:
    worker = Worker(Store(), nothing, ["triage"], concurrency=1)
    slots = asyncio.Semaphore(1)
    await slots.acquire()
    waiting = asyncio.create_task(worker._slot(slots))
    await asyncio.sleep(0)
    worker.stop()
    assert await waiting is False
    assert await worker._slot(asyncio.Semaphore(1)) is False  # stopped: no slot at all


async def test_a_slot_freed_while_busy_is_taken() -> None:
    worker = Worker(Store(), nothing, ["triage"], concurrency=1)
    slots = asyncio.Semaphore(1)
    await slots.acquire()
    waiting = asyncio.create_task(worker._slot(slots))
    await asyncio.sleep(0)
    slots.release()
    assert await waiting is True and slots.locked()


async def test_a_slot_freed_as_the_stop_comes_is_given_back() -> None:
    worker = Worker(Store(), nothing, ["triage"], concurrency=1)
    slots = asyncio.Semaphore(1)
    await slots.acquire()
    waiting = asyncio.create_task(worker._slot(slots))
    await asyncio.sleep(0)
    slots.release()
    worker.stop()
    assert await waiting is False
    assert not slots.locked()


async def test_an_idle_worker_backs_off_up_to_a_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[float] = []
    real_timeout = asyncio.timeout

    def recording(delay: float | None) -> Any:
        waits.append(delay or 0.0)
        return real_timeout(0)

    worker = Worker(Store(), nothing, ["triage"])
    monkeypatch.setattr("trellis.runs.worker.asyncio.timeout", recording)
    for rounds in (1, 2, 3, 30):
        await worker._idle(rounds)
    first, second, third, capped = waits
    assert 0.25 <= first <= 0.5 and 0.5 <= second <= 1.0 and 1.0 <= third <= 2.0
    assert worker_module.IDLE_MAX_SECONDS / 2 <= capped <= worker_module.IDLE_MAX_SECONDS


async def test_a_stop_cuts_an_idle_wait_short() -> None:
    worker = Worker(Store(), nothing, ["triage"])
    idling = asyncio.create_task(worker._idle(30))  # up to 10 s
    await asyncio.sleep(0)
    worker.stop()
    await asyncio.wait_for(idling, 1)


# --------------------------------------------------------------------------- serve
async def test_sigterm_stops_the_served_worker_gracefully() -> None:
    worker = Worker(Store(), nothing, ["triage"], concurrency=1)
    task = asyncio.create_task(worker.serve())
    await asyncio.sleep(0.05)
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(task, 5)  # returned on its own: no cancellation needed
    loop = asyncio.get_running_loop()
    assert loop.remove_signal_handler(signal.SIGTERM) is False  # serve removed its handlers
    assert loop.remove_signal_handler(signal.SIGINT) is False


async def test_serve_without_signal_handling_still_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    loop = asyncio.get_running_loop()

    def unsupported(*args: Any) -> None:
        raise NotImplementedError

    monkeypatch.setattr(loop, "add_signal_handler", unsupported)
    worker = Worker(Store(), nothing, ["triage"])
    task = asyncio.create_task(worker.serve())
    await asyncio.sleep(0.01)
    worker.stop()
    await asyncio.wait_for(task, 5)
