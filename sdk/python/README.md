# trellis-runs

Python SDK for [agent-runs](../../README.md): durable agent runs, the worker queue, the
human inbox, schedules and webhooks. It also ships a framework-neutral worker loop.

```bash
pip install trellis-runs        # imports as trellis.runs
```

It depends on `httpx`, `pydantic` and `trellis-contracts` only. It imports no agent framework
and no harness, so it plugs into whatever runs your agent: LangGraph, OpenAI Agents, the
Claude Agent SDK or plain code.

## Use it directly, or let the harness drive it

- **Let the harness drive it (Way 1)** when your agent is wrapped: with `RUNS_URL` set,
  [agent-harness](https://github.com/amitmohapatra/agent-harness)'s `h.wrap(agent)` starts,
  queues, pauses, resumes and finishes runs with this client, lists the inbox
  (`h.inbox()`), creates schedules (`agent.schedule(...)`) and runs wrapped agents on
  `Worker` (`h.worker(...)`). You write none of these calls.
- **Use it directly (Way 2)** when your own framework runs the agent and you want what
  agent-runs adds without handing over execution: a run that survives the process, a queue
  of workers, the inbox, schedules. Also when the caller is not an agent: a UI reading the
  inbox, a job creating schedules, or a webhook receiver, which uses
  [`verify_signature`](#webhooks) in both ways.

[The two ways to use Trellis](../../README.md#where-this-fits-two-ways-to-use-trellis), and
what this service does in each, are in agent-runs' README.

On this page:

- [Quickstart](#quickstart)
- [Configuration and tenants](#configuration-and-tenants)
- [The inbox](#the-inbox)
- [Schedules](#schedules)
- [Webhooks](#webhooks)
- [The worker](#the-worker)
- [Errors and retries](#errors-and-retries)

## Quickstart

The records are the `trellis-contracts` models: a run is a `RunRecord` started from a
`RunStart`, paused with an `Interrupt`, resumed with an `InterruptResolution`.

```python
from trellis.contracts.runs import (
    Interrupt,
    InterruptDecision,
    InterruptReason,
    InterruptResolution,
    RunStart,
    RunStatus,
)
from trellis.runs import RunsClient

async with RunsClient() as runs:  # RUNS_URL and TRELLIS_API_KEY from the environment
    run = await runs.start(RunStart(tenant_id="acme", agent_id="procurement", input={"sku": "A-1"}))

    # stop for a person: the run waits in role:procurement's inbox
    asked = Interrupt(
        tenant_id="acme",
        run_id=run.run_id,
        reason=InterruptReason.QUESTION,
        question="Order 12 units of A-1 from the usual supplier?",
        assignee="role:procurement",
    )
    await runs.pause(asked, checkpoint={"step": 3})

    # later, from the inbox
    answer = InterruptResolution(
        interrupt_id=asked.interrupt_id,
        run_id=run.run_id,
        decision=InterruptDecision.ANSWER,
        answer="yes",
        reviewer="user:alice",
    )
    resumed = await runs.resume(answer, tenant="acme")  # RUNNING again, attempt 2
    await runs.finish(run.run_id, RunStatus.SUCCESS, output={"order": "PO-17"}, tenant="acme")

    print(await runs.get(run.run_id, tenant="acme"))  # None for a run that does not exist
```

Method names are the API's operation ids ([docs/api.md](../../docs/api.md),
[openapi.json](../../docs/openapi.json)). Run verbs are on the client; the other resources
are grouped:

| Operation | Call | Answers |
|---|---|---|
| `runs.start` | `start(start, *, queue=False)` | `RunRecord` (the existing one for a repeated `run_id` or `idempotency_key`) |
| `runs.claim` | `claim(worker_id, agent_ids, *, lease_seconds=60)` | `Claimed` (`run`, `lease`), or `None` when nothing is queued |
| `runs.heartbeat` | `heartbeat(run_id, worker_id, *, lease_seconds=60, checkpoint=None)` | `Lease` |
| `runs.pause` | `pause(interrupt, *, checkpoint=None, worker_id=None)` | `RunRecord` (`PAUSED`) |
| `runs.resume` | `resume(resolution)` | `RunRecord` |
| `runs.finish` | `finish(run_id, status, *, output=None, error=None, worker_id=None)` | `RunRecord` |
| `runs.get` | `get(run_id)` | `RunRecord`, or `None` |
| `runs.list` | `list(*, status, assignee, agent_id, thread_id, parent_run_id, cursor, limit=50)` | `Page[RunSummary]` |
| | `iterate(..., max_pages=None)` | every `RunSummary`, page after page |
| `runs.resolutions` | `resolutions(run_id, *, cursor, limit=50)` | `Page[ResolutionEntry]` |
| `artifacts.upload` | `artifacts.upload(run_id, data, *, mime_type="application/json", worker_id=None)` | `ArtifactRef` (its SHA-256 is sent and checked) |
| `artifacts.download` | `artifacts.download(artifact_id)` | `bytes`, or `None` |
| `schedules.*` | `schedules.create(spec)`, `list(...)`, `get(id)`, `update(id, ScheduleUpdate(...))`, `delete(id)`, `fire(id, *, at=None)` | `Schedule`, `Page[Schedule]`, `Schedule` or `None`, `Schedule`, `None`, `FireResult` |
| `webhooks.*` | `webhooks.create(url, events)`, `list()`, `get(id)`, `delete(id)` | `WebhookCreated` (with the secret), `Page[Webhook]`, `Webhook` or `None`, `None` |
| `ops.*` | `live()`, `ready()`, `metrics()` | `{"status": "ok"}`, `{"status": "ok"}`, Prometheus text |

Reads by id answer `None` when the record does not exist; writes raise. Every call except
`start`, `pause` and `schedules.create` (whose bodies name the tenant) takes `tenant=`.

## Configuration and tenants

`RunsClient()` reads the platform's shared names: `RUNS_URL` for the service (the local
stack's `http://localhost:8090` when unset) and `TRELLIS_API_KEY` for the key, a key issued
by the Memory Service. Arguments win:

| Argument | Default | What it does |
|---|---|---|
| `base_url` | `$RUNS_URL`, else `http://localhost:8090` | where agent-runs is |
| `api_key` | `$TRELLIS_API_KEY` | sent as `X-API-Key` |
| `tenant` | none | the tenant a platform key acts for when a call names none |
| `timeout` | `10.0` | seconds per attempt |
| `max_retries` | `3` | how many times a call that failed on the way is sent again |
| `http_client` | its own `httpx.AsyncClient` | yours instead (the client does not close it) |

A tenant key names its tenant, so it needs no `tenant`. A platform key names the tenant on
every call (`X-Trellis-Tenant`): from the body for `start`, `pause` and `schedules.create`,
else from `tenant=`, else from the client's `tenant`. Nothing is remembered between calls.

## The inbox

A person's or a role's inbox is the paused runs assigned to them:

```python
from trellis.runs import RunsClient
from trellis.contracts.runs import RunStatus

async with RunsClient() as runs:
    page = await runs.list(status=RunStatus.PAUSED, assignee="role:procurement", limit=100)
    for summary in page.items:
        print(summary.run_id, summary.awaiting.question if summary.awaiting else None)
    if page.has_more:
        page = await runs.list(
            status=RunStatus.PAUSED, assignee="role:procurement", cursor=page.next_cursor
        )

    # or every page (at most ten here):
    async for summary in runs.iterate(status=RunStatus.PAUSED, assignee="user:alice", max_pages=10):
        ...
```

A summary carries the question (`awaiting`); read the run with `get` for its input,
checkpoint and output, and the audit trail with `resolutions`. A large payload to review (a
table, a diff) is an artifact: the interrupt's `payload_ref` is its `ArtifactRef`, and
`runs.artifacts.download(ref.artifact_id)` its bytes.

Any key of the tenant reads every inbox, but answering is checked. A key that may act for
anyone (the default) or an admin key answers any run. A key restricted to listed people
answers only as one of them (`reviewer`), and only a run assigned to that person or to
nobody, never one assigned to a group; anything else raises `AuthorizationError` saying why
([who may answer a paused run](../../README.md#who-may-answer-a-paused-run)):

```python
from trellis.contracts.runs import InterruptDecision, InterruptResolution
from trellis.runs import AuthorizationError, RunsClient

async with RunsClient(api_key=priya_key) as runs:  # a key with may_act_as=["user:priya"]
    run = await runs.get(run_id)
    answer = InterruptResolution(
        interrupt_id=run.awaiting.interrupt_id,
        run_id=run.run_id,
        decision=InterruptDecision.APPROVE,
        reviewer="priya",  # user:priya
    )
    try:
        await runs.resume(answer)
    except AuthorizationError as refused:
        print(refused.message)  # ...: the run is assigned to user:raj, not user:priya; ...
```

## Schedules

```python
from trellis.contracts.runs import ScheduleSpec
from trellis.runs import RunsClient, ScheduleUpdate

async with RunsClient() as runs:
    briefing = await runs.schedules.create(
        ScheduleSpec(
            tenant_id="acme",
            agent_id="briefing",
            name="morning briefing",
            cadence="0 8 * * 1-5",
            timezone="Europe/Berlin",
            on_behalf_of="user_ada",
            input={"topic": "inbox"},
        )
    )  # an upsert: the same agent, person, cadence and input answer the existing one
    fired = await runs.schedules.fire(briefing.schedule_id)  # fired.run_id is QUEUED
    await runs.schedules.update(briefing.schedule_id, ScheduleUpdate(enabled=False))  # pause
    await runs.schedules.update(briefing.schedule_id, ScheduleUpdate(enabled=True))  # resume
```

A fired run is queued: a [worker](#the-worker) runs it.

## Webhooks

Subscribe once per tenant; the secret is in the create's answer only:

```python
from trellis.runs import RunsClient, WebhookEvent

async with RunsClient() as runs:
    hook = await runs.webhooks.create(
        "https://ui.example/hooks/trellis", [WebhookEvent.PAUSED, WebhookEvent.FINISHED]
    )
    store_secret(hook.secret)
```

Every delivery is signed: `X-Trellis-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 keyed by
the secret over "<t>.<raw body>">`, with `X-Trellis-Event` and `X-Trellis-Delivery` (the
event id, the same on every retry). A receiver verifies the raw bytes before it parses them;
`verify_signature` refuses a signature older (or further ahead) than five minutes and never
raises on a malformed header. In a Starlette (or FastAPI) app:

```python
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from trellis.runs.webhooks import SIGNATURE_HEADER, parse_delivery, verify_signature


async def trellis_hook(request: Request) -> Response:
    body = await request.body()
    if not verify_signature(SECRET, request.headers.get(SIGNATURE_HEADER), body):
        return Response(status_code=401)
    delivery = parse_delivery(body)
    if not seen(delivery.event_id):  # at least once: drop repeats by event_id
        notify(delivery.type, delivery.data.run)  # run.paused / run.escalated / run.finished
    return Response(status_code=204)


app = Starlette(routes=[Route("/hooks/trellis", trellis_hook, methods=["POST"])])
```

A `2xx` accepts the delivery; `408`, `429`, `5xx` and an unreachable receiver are retried.
`sign(secret, timestamp, body)` is the one implementation of the scheme: agent-runs signs
with it.

## The worker

`Worker` claims queued runs of some agents and hands each to your handler, an
`async (Job) -> ...`. A `Job` carries the claimed `record` (with its `checkpoint` and
`last_resolution`), the `worker_id` and the `lease_seconds`, and writes as that worker:

```python
from trellis.contracts.runs import RunStatus
from trellis.runs import Job, RunsClient, Worker


async def handle(job: Job) -> None:
    answer = await graph.ainvoke(job.record.input)  # your framework, untouched
    await job.checkpoint({"step": "done"})  # optional: progress a retry resumes from
    await job.finish(RunStatus.SUCCESS, output=answer)


async with RunsClient() as runs:
    await Worker(runs, handle, ["triage", "billing"], concurrency=4).serve()
```

- The lease is renewed every third of `lease_seconds` (60) while the handler runs. A
  heartbeat refused with `LEASE_LOST` cancels the handler: another worker has the run, or
  it was cancelled or ran past its `deadline` (agent-runs ended it `TIMEOUT`).
- `concurrency` handlers run at once (default: the CPU count, 1 to 8). An idle worker asks
  again after 0.5 s, doubling to 10 s, jittered.
- `serve()` stops on SIGTERM or SIGINT: no new claims, the runs held get 25 s to finish,
  then are released (cancelled with the message `RELEASED`, nothing written; the lease lapses
  and another worker runs them again). A second signal releases them at once. `run()` is the
  same loop without signal handling (stop it with `stop()`), and `run_once()` claims and
  executes one run.
- `job.pause(interrupt, checkpoint=...)` and `job.finish(...)` are fenced: after the lease
  was lost they raise `LeaseLostError`. A handler that records a cancellation as the run's
  ending checks for `RELEASED in exc.args` and writes nothing then.
- `store` is anything with `claim`, `heartbeat`, `pause` and `finish` as `RunsClient` has
  them (the `WorkerStore` protocol). `tenant=` names the tenant a platform key claims for.

## Errors and retries

An error answer is an RFC 9457 problem; the client raises the class of its `code` (else of
its status). Every error has `message`, `code`, `status` (0 without a response),
`retryable`, `request_id`, `details` and `retry_after`.

| Class | `code` / status | Means |
|---|---|---|
| `RunsError` | `INTERNAL` / 500, any other | the base class |
| `AuthenticationError` | `AUTHENTICATION` / 401 | no key, or one the registry does not know |
| `AuthorizationError` | `AUTHORIZATION` / 403 | another tenant, an `on_behalf_of` the key may not act as, a paused run the key may not answer |
| `NotFoundError` | `NOT_FOUND` / 404 | no such record (reads by id answer `None` instead) |
| `ConflictError` | `CONFLICT` / 409 | an illegal transition, an answer to another interrupt |
| `LeaseLostError` | `LEASE_LOST` / 409 | the worker no longer holds the run: stop, write nothing more. **Not** a `ConflictError` |
| `ValidationError` | `VALIDATION` / 400, 405, 422 | the request is invalid |
| `PayloadTooLargeError` | `PAYLOAD_TOO_LARGE` / 413 | a `ValidationError`: a body, payload, checkpoint or artifact too large |
| `RateLimitedError` | `RATE_LIMIT` / 429 | the tenant's budget is spent for now (`retry_after`) |
| `DependencyUnavailableError` | `DEPENDENCY_UNAVAILABLE` / 502, 503, 504, or no response | the service or its database could not answer |

A call that fails on the way (no response, `429`, `502`, `503`, `504`) is sent again up to
`max_retries` times, after the `Retry-After` the service asked for (at most 30 s) or a
full-jitter backoff from 0.25 s doubling to 5 s, unless the problem says
`retryable: false`. Every write agent-runs takes is safe to repeat: a start is idempotent on
its id, a repeated pause or finish answers the stored run, a repeated resume with the same
`InterruptResolution` object answers the run as it is now (a new resolution for an
interrupt already answered, a second click, is a `ConflictError`), the same artifact bytes
are the same artifact, a schedule create is an upsert.
