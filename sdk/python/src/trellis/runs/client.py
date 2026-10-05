"""The client of agent-runs::

    runs = RunsClient()          # RUNS_URL and TRELLIS_API_KEY; or RunsClient(url, api_key=...)
    record = await runs.start(RunStart(tenant_id="acme", agent_id="triage", input={...}))
    await runs.pause(interrupt, checkpoint={...})        # waits for a person
    async for waiting in runs.iterate(status=RunStatus.PAUSED, assignee="role:procurement"):
        ...                                                # an inbox
    await runs.resume(resolution, tenant="acme")
    await runs.finish(record.run_id, RunStatus.SUCCESS, output={...}, tenant="acme")

Method names are the API's operation ids (``runs.start`` is :meth:`RunsClient.start`,
``schedules.fire`` is ``runs.schedules.fire``), so the SDK, the harness and the HTTP API share
one vocabulary. Reads by id answer ``None`` for a record that does not exist; writes raise
(``trellis.runs.errors``).

The tenant is explicit: a call whose body names it (a start, a pause, a schedule) sends that
one; any other call sends its ``tenant=`` argument, else the client's ``tenant``. A tenant key
needs neither (it names its tenant); a platform key needs one on every call.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Sequence
from typing import Any, Final, Self

import httpx
from trellis.contracts.errors import AgentError
from trellis.contracts.runs import Interrupt, InterruptResolution, RunRecord, RunStart, RunStatus
from trellis.runs._transport import (
    NO_CONTENT,
    PAGE_LIMIT,
    RETRIES,
    TIMEOUT_SECONDS,
    Transport,
    worker_params,
)
from trellis.runs.artifacts import ArtifactsAPI
from trellis.runs.errors import ConflictError, LeaseLostError
from trellis.runs.models import Claimed, Lease, Page, ResolutionEntry, RunSummary
from trellis.runs.schedules import SchedulesAPI
from trellis.runs.webhooks import WebhooksAPI

#: Where the service is when ``base_url`` is not passed, and the key when ``api_key`` is
#: not: the platform's shared names, the ones the harness reads too.
ENV_URL: Final = "RUNS_URL"
ENV_API_KEY: Final = "TRELLIS_API_KEY"
#: The local stack's address (docker compose), for a laptop with neither set.
DEFAULT_URL: Final = "http://localhost:8090"
#: A lease's length when the caller names none: the service's default.
LEASE_SECONDS: Final = 60


class RunsClient:
    """One per process. ``base_url`` defaults to ``$RUNS_URL`` (else the local stack's
    ``http://localhost:8090``) and ``api_key`` to ``$TRELLIS_API_KEY``. ``tenant`` is the
    tenant a platform key acts for when a call names none. ``timeout`` bounds each attempt;
    ``max_retries`` is how many times a call that failed on the way is sent again.
    ``http_client`` replaces the client this one would open (and is not closed by it)."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        api_key: str | None = None,
        tenant: str | None = None,
        timeout: float = TIMEOUT_SECONDS,
        max_retries: int = RETRIES,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._transport = Transport(
            base_url or os.environ.get(ENV_URL) or DEFAULT_URL,
            api_key=api_key or os.environ.get(ENV_API_KEY) or None,
            tenant=tenant,
            timeout=timeout,
            max_retries=max_retries,
            client=http_client,
        )
        #: ``artifacts.upload`` / ``artifacts.download``: payloads too large for a run's row
        self.artifacts = ArtifactsAPI(self._transport)
        #: ``schedules.create`` / ``list`` / ``get`` / ``update`` / ``delete`` / ``fire``
        self.schedules = SchedulesAPI(self._transport)
        #: ``webhooks.create`` / ``list`` / ``get`` / ``delete``: the tenant's subscriptions
        self.webhooks = WebhooksAPI(self._transport)

    # ------------------------------------------------------------------ runs
    async def start(self, start: RunStart, *, queue: bool = False) -> RunRecord:
        """Record a run ``RUNNING`` in the caller's process, or with ``queue=True`` put it
        ``QUEUED`` for a worker. Idempotent: the same ``run_id`` or ``idempotency_key``
        answers the run that exists."""
        body = {**start.model_dump(mode="json"), "queue": queue}
        data = await self._transport.json("POST", "/v1/runs", tenant=start.tenant_id, json=body)
        return RunRecord.model_validate(data)

    async def claim(
        self,
        worker_id: str,
        agent_ids: Sequence[str],
        *,
        lease_seconds: int = LEASE_SECONDS,
        tenant: str | None = None,
    ) -> Claimed | None:
        """Lease the next queued run of ``agent_ids`` to ``worker_id`` (it is now
        ``RUNNING``), or None when none may run now: the highest ``priority``, then the
        oldest, among the runs with room under their ``concurrency_key``. A platform key with
        no ``tenant`` (here or on the client) claims from every tenant's queue, the tenant
        whose workers hold the fewest runs first; the run names its tenant."""
        body = {"worker_id": worker_id, "agent_ids": [*agent_ids], "lease_seconds": lease_seconds}
        response = await self._transport.send("POST", "/v1/runs/claim", tenant=tenant, json=body)
        if response.status_code == NO_CONTENT:
            return None
        return Claimed.model_validate(response.json())

    async def heartbeat(
        self,
        run_id: str,
        worker_id: str,
        *,
        lease_seconds: int = LEASE_SECONDS,
        checkpoint: dict[str, Any] | None = None,
        tenant: str | None = None,
    ) -> Lease:
        """Extend the lease; with ``checkpoint``, also save it as the run's progress (it
        replaces the run's checkpoint; without, the one there is kept). Any ``409`` raises
        :class:`LeaseLostError`: this worker does not hold a running lease on the run."""
        body: dict[str, Any] = {"worker_id": worker_id, "lease_seconds": lease_seconds}
        if checkpoint is not None:
            body["checkpoint"] = checkpoint
        try:
            data = await self._transport.json(
                "POST", f"/v1/runs/{run_id}/heartbeat", tenant=tenant, json=body
            )
        except ConflictError as exc:
            raise LeaseLostError(
                exc.message,
                code="LEASE_LOST",
                status=exc.status,
                request_id=exc.request_id,
                details=exc.details,
            ) from exc
        return Lease.model_validate(data)

    async def release(
        self,
        run_id: str,
        worker_id: str,
        *,
        checkpoint: dict[str, Any] | None = None,
        tenant: str | None = None,
    ) -> RunRecord:
        """Let go of a run this worker holds (it is stopping): the run goes back on the queue
        at once as its next attempt, for another worker, without counting a lapsed lease;
        ``checkpoint`` saves the progress made so far first. A run whose cancel was asked
        for ends ``CANCELLED`` instead. :class:`LeaseLostError` when this worker no longer
        holds it."""
        body: dict[str, Any] = {"worker_id": worker_id}
        if checkpoint is not None:
            body["checkpoint"] = checkpoint
        data = await self._transport.json(
            "POST", f"/v1/runs/{run_id}/release", tenant=tenant, json=body
        )
        return RunRecord.model_validate(data)

    async def pause(
        self,
        interrupt: Interrupt,
        *,
        checkpoint: dict[str, Any] | None = None,
        worker_id: str | None = None,
    ) -> RunRecord:
        """The run (``interrupt.run_id``) waits on ``interrupt``, keeping ``checkpoint`` for
        whichever worker resumes it. ``worker_id`` fences the write to the lease holder."""
        body = {"interrupt": interrupt.model_dump(mode="json"), "checkpoint": checkpoint}
        data = await self._transport.json(
            "POST",
            f"/v1/runs/{interrupt.run_id}/pause",
            tenant=interrupt.tenant_id,
            json=body,
            params=worker_params(worker_id),
        )
        return RunRecord.model_validate(data)

    async def resume(
        self, resolution: InterruptResolution, *, tenant: str | None = None
    ) -> RunRecord:
        """Answer the interrupt the run waits on: ``CANCEL`` ends it, any other decision
        continues it as its next attempt. A key restricted to listed people answers only as
        one of them (``resolution.reviewer``), and only a run assigned to that person or to
        nobody: anything else raises :class:`AuthorizationError` saying why. An answer that
        does not fit the question (:mod:`trellis.runs.answers`) raises
        :class:`ValidationError` saying what does not fit. Safe to retry:
        the same ``resolution`` again answers the run as it is now; another resolution for
        an interrupt already answered (a second click) raises :class:`ConflictError`."""
        data = await self._transport.json(
            "POST",
            f"/v1/runs/{resolution.run_id}/resume",
            tenant=tenant,
            json=resolution.model_dump(mode="json"),
        )
        return RunRecord.model_validate(data)

    async def cancel(
        self, run_id: str, *, reason: str | None = None, tenant: str | None = None
    ) -> RunRecord:
        """Cancel the run, whatever its status, keeping ``reason`` with it. A queued or
        waiting run (or one in its caller's process) is ``CANCELLED`` at once; a run a worker
        holds stays ``RUNNING`` until its worker stops (its next heartbeat says
        ``cancel_requested``; :class:`Worker` cancels the handler and finishes the run
        ``CANCELLED``), or agent-runs cancels it when the lease runs out. The keys that may
        answer a run may cancel it (:class:`AuthorizationError` otherwise); an ended run
        raises :class:`ConflictError`. Safe to retry."""
        data = await self._transport.json(
            "POST", f"/v1/runs/{run_id}/cancel", tenant=tenant, json={"reason": reason}
        )
        return RunRecord.model_validate(data)

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
        """End the run with ``status`` (an ending). The same finish repeated answers the
        stored run. ``worker_id`` fences the write to the lease holder."""
        body: dict[str, Any] = {"status": status.value, "output": output}
        if error is not None:
            body["error"] = error.model_dump(mode="json")
        data = await self._transport.json(
            "POST",
            f"/v1/runs/{run_id}/finish",
            tenant=tenant,
            json=body,
            params=worker_params(worker_id),
        )
        return RunRecord.model_validate(data)

    async def get(self, run_id: str, *, tenant: str | None = None) -> RunRecord | None:
        """The full record (input, output, error, checkpoint), or None when there is none."""
        data = await self._transport.found("GET", f"/v1/runs/{run_id}", tenant=tenant)
        return None if data is None else RunRecord.model_validate(data)

    async def list(
        self,
        *,
        status: RunStatus | None = None,
        assignee: str | None = None,
        agent_id: str | None = None,
        thread_id: str | None = None,
        parent_run_id: str | None = None,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT,
        tenant: str | None = None,
    ) -> Page[RunSummary]:
        """One page of the tenant's runs, newest first. ``status=PAUSED`` with ``assignee``
        is the inbox of a person or role."""
        filters = {
            "status": status.value if status is not None else None,
            "assignee": assignee,
            "agent_id": agent_id,
            "thread_id": thread_id,
            "parent_run_id": parent_run_id,
            "cursor": cursor,
        }
        params = {name: value for name, value in filters.items() if value is not None}
        rows, after = await self._transport.page(
            "/v1/runs", tenant=tenant, params={**params, "limit": limit}
        )
        return Page[RunSummary](
            items=[RunSummary.model_validate(row) for row in rows], next_cursor=after
        )

    async def iterate(
        self,
        *,
        status: RunStatus | None = None,
        assignee: str | None = None,
        agent_id: str | None = None,
        thread_id: str | None = None,
        parent_run_id: str | None = None,
        limit: int = PAGE_LIMIT,
        tenant: str | None = None,
        max_pages: int | None = None,
    ) -> AsyncIterator[RunSummary]:
        """Every run :meth:`list` would page through, page after page; at most
        ``max_pages`` pages when given."""
        cursor: str | None = None
        pages = 0
        while True:
            page = await self.list(
                status=status,
                assignee=assignee,
                agent_id=agent_id,
                thread_id=thread_id,
                parent_run_id=parent_run_id,
                cursor=cursor,
                limit=limit,
                tenant=tenant,
            )
            for summary in page.items:
                yield summary
            pages += 1
            cursor = page.next_cursor
            if cursor is None or (max_pages is not None and pages >= max_pages):
                return

    async def resolutions(
        self,
        run_id: str,
        *,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT,
        tenant: str | None = None,
    ) -> Page[ResolutionEntry]:
        """One page of every interrupt the run paused on and how it was answered, oldest
        first (append-only)."""
        params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        rows, after = await self._transport.page(
            f"/v1/runs/{run_id}/resolutions", tenant=tenant, params=params
        )
        return Page[ResolutionEntry](
            items=[ResolutionEntry.model_validate(row) for row in rows], next_cursor=after
        )

    # ------------------------------------------------------------------ ops
    async def live(self) -> dict[str, str]:
        """The process is up (asks no dependency)."""
        return await self._transport.json("GET", "/health/live")

    async def ready(self) -> dict[str, str]:
        """The database answers; a :class:`~trellis.runs.errors.DependencyUnavailableError`
        when it does not."""
        return await self._transport.json("GET", "/health/ready")

    async def metrics(self) -> str:
        """The Prometheus exposition of the worker process that answered, as text."""
        return (await self._transport.send("GET", "/metrics")).text

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
