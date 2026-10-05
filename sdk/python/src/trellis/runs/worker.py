"""The worker loop: claim queued runs of some agents, hand each to a handler, keep its lease.

Framework-neutral: the handler is any ``async (Job) -> ...``. It runs one claimed run (a
LangGraph graph, an OpenAI agent, plain code, a wrapped harness agent) and records how it
went through the job's fenced helpers, which name this worker so a worker whose lease lapsed
cannot write over a run another worker has since claimed::

    async def handle(job: Job) -> None:
        answer = await my_graph.ainvoke(job.record.input)
        await job.finish(RunStatus.SUCCESS, output=answer)

    async with RunsClient() as runs:
        await Worker(runs, handle, ["triage"]).serve()   # until SIGTERM or SIGINT

A claimed run is leased to this worker; a heartbeat extends the lease every third of it while
the handler runs, and a worker that dies lets the lease lapse, after which agent-runs queues
the run again as its next attempt. A heartbeat refused with ``LEASE_LOST`` (the lease lapsed
and another worker took the run, or the run ended: past its deadline or its working time)
cancels the handler: it must write nothing more. A heartbeat that says ``cancel_requested``
(someone cancelled the run: ``RunsClient.cancel``) cancels the handler too, and the worker
then finishes the run ``CANCELLED`` (:attr:`Job.cancel_requested` tells the handler why it
was cancelled). Schedules and resumed durable runs arrive the same way: as queued runs.

A handler that raises (anything but a lost lease or a cancellation) ends its run at once as
``ERROR``, the exception recorded as the contracts classify it (``AgentError.of``: its code,
category and whether it is retryable), instead of leaving the run to wait for its lease to
lapse; agent-runs puts a run whose error is retryable back on the queue for a later attempt.
Should that finish fail too (agent-runs unreachable), the lease lapses as for a dead
worker.

An idle worker asks again after a growing pause (exponential, jittered, at most
:data:`IDLE_MAX_SECONDS`), and asks at once again after it got work. :meth:`Worker.stop`
(what :meth:`Worker.serve` calls on SIGTERM or SIGINT) stops claiming and lets the runs it
holds finish for up to :data:`GRACE_SECONDS`; a run still going then is released: its handler
is cancelled with the message :data:`RELEASED`, it writes nothing, and the worker hands the
run back to the queue (``release``), so another worker runs it at once as its next attempt,
with no lapsed lease counted (when even that cannot be sent, the lease lapses as for a dead
worker). A second stop releases them at once.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import signal
import socket
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Protocol

from trellis.contracts.errors import AgentError
from trellis.contracts.runs import Interrupt, RunRecord, RunStatus
from trellis.runs.client import LEASE_SECONDS
from trellis.runs.errors import LeaseLostError
from trellis.runs.models import Claimed, Lease

log = logging.getLogger("trellis.runs.worker")

#: The most runs one worker executes at once by default (the CPU count, at most this).
MAX_DEFAULT_CONCURRENCY: Final = 8
#: An idle worker's first pause before asking for work again; doubled while the queue stays
#: empty, up to :data:`IDLE_MAX_SECONDS` (equal jitter: half of it fixed, half random).
IDLE_SECONDS: Final = 0.5
IDLE_MAX_SECONDS: Final = 10.0
#: How long a stopping worker lets the runs it holds finish before it releases them.
GRACE_SECONDS: Final = 25.0
#: The message a worker cancels a handler with when it stops before the run ends: the run
#: is released (back on the queue), not cancelled. A handler that records a cancellation as
#: the run's ending checks for it (``RELEASED in exc.args``) and writes nothing.
RELEASED: Final = "trellis:released"
#: The wait between heartbeats, and the clock a job's remaining working time runs on; names
#: of their own so tests can stand them in.
_sleep = asyncio.sleep
_clock = time.monotonic


class WorkerStore(Protocol):
    """What the worker and its jobs call: the claim, the lease and its release, and the
    fenced pause and finish. :class:`~trellis.runs.RunsClient` is one."""

    async def claim(
        self,
        worker_id: str,
        agent_ids: Sequence[str],
        *,
        lease_seconds: int = ...,
        tenant: str | None = ...,
    ) -> Claimed | None: ...

    async def heartbeat(
        self,
        run_id: str,
        worker_id: str,
        *,
        lease_seconds: int = ...,
        checkpoint: dict[str, Any] | None = ...,
        tenant: str | None = ...,
    ) -> Lease: ...

    async def release(
        self,
        run_id: str,
        worker_id: str,
        *,
        checkpoint: dict[str, Any] | None = ...,
        tenant: str | None = ...,
    ) -> RunRecord: ...

    async def pause(
        self,
        interrupt: Interrupt,
        *,
        checkpoint: dict[str, Any] | None = ...,
        worker_id: str | None = ...,
    ) -> RunRecord: ...

    async def finish(
        self,
        run_id: str,
        status: RunStatus,
        *,
        output: Any = ...,
        error: AgentError | None = ...,
        worker_id: str | None = ...,
        tenant: str | None = ...,
    ) -> RunRecord: ...


@dataclass
class _Renewed:
    """The latest lease on a job's run (the claim's, then each heartbeat's) and when it came,
    on :data:`_clock`."""

    lease: Lease | None = None
    at: float = 0.0


@dataclass(frozen=True)
class Job:
    """One claimed run, as its handler gets it: the record (``RUNNING``, with its checkpoint
    and its last resolution), the worker holding it and the lease's length, and what the
    worker learns while the handler runs (:attr:`remaining_seconds`). The helpers write as
    this worker, so a write after the lease was lost is refused (``LeaseLostError``)."""

    record: RunRecord
    worker_id: str
    lease_seconds: int
    store: WorkerStore = field(repr=False)
    _renewed: _Renewed = field(default_factory=_Renewed, repr=False, compare=False)

    @property
    def remaining_seconds(self) -> float | None:
        """The working time the run has left now, in seconds: what agent-runs said with the
        latest lease, less the time since (never below 0). ``None``: no limit. Past it
        agent-runs ends the run ``TIMEOUT`` and the next heartbeat cancels the handler, so a
        handler that bounds its own steps by it stops in time instead."""
        lease = self._renewed.lease
        if lease is None or lease.remaining_seconds is None:
            return None
        return max(0.0, lease.remaining_seconds - (_clock() - self._renewed.at))

    @property
    def cancel_requested(self) -> bool:
        """Someone asked to cancel the run (a heartbeat said so). The worker cancels the
        handler and finishes the run ``CANCELLED``; a handler that records the cancellation
        itself checks this to tell it from a lost lease."""
        lease = self._renewed.lease
        return lease is not None and lease.cancel_requested

    def _renew(self, lease: Lease) -> None:
        """Take in a lease agent-runs answered for the run (the worker calls it)."""
        self._renewed.lease, self._renewed.at = lease, _clock()

    async def checkpoint(self, checkpoint: dict[str, Any]) -> Lease:
        """Save progress (the resume journal as it stands) and extend the lease: the attempt
        after a crash resumes from it instead of repeating side effects."""
        lease = await self.store.heartbeat(
            self.record.run_id,
            self.worker_id,
            lease_seconds=self.lease_seconds,
            checkpoint=checkpoint,
            tenant=self.record.tenant_id,
        )
        self._renew(lease)
        return lease

    async def pause(
        self, interrupt: Interrupt, *, checkpoint: dict[str, Any] | None = None
    ) -> RunRecord:
        """The run waits on ``interrupt``; the lease ends with the pause."""
        return await self.store.pause(interrupt, checkpoint=checkpoint, worker_id=self.worker_id)

    async def finish(
        self, status: RunStatus, *, output: Any = None, error: AgentError | None = None
    ) -> RunRecord:
        """End the run with ``status``."""
        return await self.store.finish(
            self.record.run_id,
            status,
            output=output,
            error=error,
            worker_id=self.worker_id,
            tenant=self.record.tenant_id,
        )


#: What runs a claimed run: anything awaitable, its result ignored.
Handler = Callable[[Job], Awaitable[object]]


def default_concurrency() -> int:
    """Runs one worker executes at once when nothing says: the CPU count, from 1 to 8."""
    return max(1, min(MAX_DEFAULT_CONCURRENCY, os.cpu_count() or 1))


class Worker:
    """Claims queued runs of ``agent_ids`` from ``store`` and runs each with ``handler``,
    ``concurrency`` at a time (default: :func:`default_concurrency`). ``tenant`` is the
    tenant a platform key claims for; ``worker_id`` defaults to host, process and object."""

    def __init__(
        self,
        store: WorkerStore,
        handler: Handler,
        agent_ids: Sequence[str],
        *,
        concurrency: int | None = None,
        lease_seconds: int = LEASE_SECONDS,
        worker_id: str | None = None,
        tenant: str | None = None,
    ) -> None:
        if not agent_ids:
            raise ValueError("a worker needs at least one agent")
        chosen = default_concurrency() if concurrency is None else concurrency
        if chosen < 1:
            raise ValueError("a worker executes at least one run at a time")
        self.store = store
        self.handler = handler
        self.agent_ids = [*agent_ids]
        self.concurrency = chosen
        self.lease_seconds = lease_seconds
        self.tenant = tenant
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{id(self):x}"
        self._stopping = asyncio.Event()
        self._hurry = asyncio.Event()
        #: the runs this worker holds and their handlers, by run id (what a stop releases)
        self._held: dict[str, tuple[Job, asyncio.Future[object]]] = {}

    def stop(self) -> None:
        """Stop claiming and let the runs held finish (at most :data:`GRACE_SECONDS`); a
        second call releases them at once. ``run`` returns when they are done."""
        if self._stopping.is_set():
            self._hurry.set()
        self._stopping.set()

    async def run(self) -> None:
        """Claim and execute until stopped (or cancelled: the handlers held are then
        cancelled), ``concurrency`` runs at a time."""
        slots = asyncio.Semaphore(self.concurrency)
        running: set[asyncio.Task[None]] = set()
        idle = 0
        try:
            while await self._slot(slots):
                claimed = await self._claim()
                if claimed is None:
                    slots.release()
                    idle += 1
                    await self._idle(idle)
                    continue
                idle = 0
                task = asyncio.create_task(self._execute(claimed))
                running.add(task)
                task.add_done_callback(running.discard)
                task.add_done_callback(lambda _: slots.release())
            await self._wind_down(running)
        finally:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)

    async def serve(self) -> None:
        """:meth:`run`, stopped gracefully by SIGTERM or SIGINT (a second signal releases the
        runs held at once). Where signals cannot be handled (not the main thread, Windows),
        Ctrl-C stays ``KeyboardInterrupt``."""
        loop = asyncio.get_running_loop()
        handled: list[signal.Signals] = []
        for stop in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.add_signal_handler(stop, self.stop)
                handled.append(stop)
        try:
            await self.run()
        finally:
            for stop in handled:
                loop.remove_signal_handler(stop)

    async def run_once(self) -> bool:
        """Claim one run and execute it to its end (or pause). ``False`` when none was
        queued."""
        claimed = await self._claim()
        if claimed is None:
            return False
        await self._execute(claimed)
        return True

    # ------------------------------------------------------------------ internals
    async def _slot(self, slots: asyncio.Semaphore) -> bool:
        """A free slot, or ``False`` once the worker is told to stop."""
        if self._stopping.is_set():
            return False
        if not slots.locked():
            await slots.acquire()
            return True
        acquiring = asyncio.ensure_future(slots.acquire())
        stopping = asyncio.ensure_future(self._stopping.wait())
        try:
            await asyncio.wait({acquiring, stopping}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            got = acquiring.done()
            for waiter in (acquiring, stopping):
                waiter.cancel()
        if self._stopping.is_set():
            if got:  # a slot freed as the stop came: give it back
                slots.release()
            return False
        return True

    async def _idle(self, rounds: int) -> None:
        """Wait before asking again: longer each empty round, cut short by a stop."""
        ceiling = min(IDLE_MAX_SECONDS, IDLE_SECONDS * 2 ** min(rounds - 1, 16))
        delay = ceiling / 2 + random.uniform(0, ceiling / 2)  # jitter, not a secret
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(delay):
                await self._stopping.wait()

    async def _wind_down(self, running: set[asyncio.Task[None]]) -> None:
        """Let the runs held finish within the grace period, then release the rest."""
        if running:
            log.info(
                "worker %s stopping: %d run(s) in flight, %.0f s to finish",
                self.worker_id,
                len(running),
                GRACE_SECONDS,
            )
            hurry = asyncio.ensure_future(self._hurry.wait())
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(GRACE_SECONDS):
                    while running and not hurry.done():
                        await asyncio.wait({*running, hurry}, return_when=asyncio.FIRST_COMPLETED)
            hurry.cancel()
        released = list(self._held.values())
        for job, execution in released:
            log.warning("run %s released: another worker runs it", job.record.run_id)
            execution.cancel(RELEASED)
        await asyncio.gather(*running, return_exceptions=True)
        await asyncio.gather(*(self._release(job) for job, _ in released))

    async def _claim(self) -> Claimed | None:
        try:
            return await self.store.claim(
                self.worker_id, self.agent_ids, lease_seconds=self.lease_seconds, tenant=self.tenant
            )
        except Exception as exc:
            log.warning("claim failed: %s", exc)
            return None

    async def _execute(self, claimed: Claimed) -> None:
        """Run the handler while the lease holds; a lost lease cancels it."""
        record = claimed.run
        job = Job(record, self.worker_id, self.lease_seconds, self.store)
        job._renew(claimed.lease)
        execution = asyncio.ensure_future(self.handler(job))
        self._held[record.run_id] = (job, execution)
        heartbeat = asyncio.create_task(self._heartbeat(job, execution))
        try:
            await execution
        except asyncio.CancelledError:
            if not execution.cancelled():
                raise
            if job.cancel_requested:
                await self._end(job, RunStatus.CANCELLED)
        except LeaseLostError:
            log.warning("lease on %s lost while the handler wrote: stopped it", record.run_id)
        except Exception as exc:
            log.exception("run %s failed in the worker", record.run_id)
            await self._end(job, RunStatus.ERROR, error=AgentError.of(exc))
        finally:
            self._held.pop(record.run_id, None)
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

    async def _release(self, job: Job) -> None:
        """Hand a run its stopped handler left back to the queue; when that cannot be sent,
        its lease lapses and agent-runs takes the run back."""
        try:
            await self.store.release(job.record.run_id, self.worker_id, tenant=job.record.tenant_id)
        except Exception as refused:
            log.warning(
                "run %s could not be released (%s): its lease lapses instead",
                job.record.run_id,
                refused,
            )

    async def _end(self, job: Job, status: RunStatus, *, error: AgentError | None = None) -> None:
        """End the run for its handler: ``ERROR`` with what it raised, or ``CANCELLED`` as
        asked (a repeat when the handler recorded it). When even that is refused or cannot
        be sent, its lease lapses and agent-runs takes the run back (or cancels it)."""
        try:
            await job.finish(status, error=error)
        except Exception as refused:
            log.warning(
                "run %s could not be ended as %s (%s): its lease lapses instead",
                job.record.run_id,
                status.value,
                refused,
            )

    async def _heartbeat(self, job: Job, execution: asyncio.Future[object]) -> None:
        record = job.record
        while True:
            await _sleep(self.lease_seconds / 3)
            try:
                lease = await self.store.heartbeat(
                    record.run_id,
                    self.worker_id,
                    lease_seconds=self.lease_seconds,
                    tenant=record.tenant_id,
                )
            except LeaseLostError:
                log.warning("lease on %s lost: stopping it", record.run_id)
                execution.cancel()
                return
            except Exception as exc:
                log.warning("heartbeat for %s failed: %s", record.run_id, exc)
            else:
                job._renew(lease)
                if lease.cancel_requested:
                    log.warning("run %s cancelled: stopping it", record.run_id)
                    execution.cancel()
                    return
