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
    run = await runs.start(
        RunStart(
            tenant_id="acme", agent_id="procurement", input={"sku": "A-1"}, timeout_seconds=600
        )
    )  # at most ten minutes of work, however long it waits for people

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
| `runs.claim` | `claim(worker_id, agent_ids, *, lease_seconds=60)` | `Claimed` (`run`, `lease`): the highest `priority`, then the oldest, with room under its `concurrency_key`; or `None` when none may run now. A platform key with no tenant claims from every tenant, fairly |
| `runs.heartbeat` | `heartbeat(run_id, worker_id, *, lease_seconds=60, checkpoint=None)` | `Lease` (`remaining_seconds`, `cancel_requested`) |
| `runs.release` | `release(run_id, worker_id, *, checkpoint=None)` | `RunRecord` (`QUEUED` for another worker) |
| `runs.pause` | `pause(interrupt, *, checkpoint=None, worker_id=None)` | `RunRecord` (`PAUSED`) |
| `runs.resume` | `resume(resolution)` | `RunRecord` |
| `runs.cancel` | `cancel(run_id, *, reason=None)` | `RunRecord` (`CANCELLED`, or `RUNNING` until its worker stops) |
| `runs.finish` | `finish(run_id, status, *, output=None, error=None, worker_id=None)` | `RunRecord` (`QUEUED` when a retryable `ERROR` is retried) |
| `runs.get` | `get(run_id)` | `RunRecord`, or `None` |
| `runs.list` | `list(*, status, assignee, agent_id, thread_id, parent_run_id, top_level=False, cursor, limit=50)` | `Page[RunSummary]` (`top_level=True`: no sub-agents) |
| | `iterate(..., max_pages=None)` | every `RunSummary`, page after page |
| `runs.resolutions` | `resolutions(run_id, *, cursor, limit=50)` | `Page[ResolutionEntry]` |
| `runs.append_events` | `append_events(run_id, events, *, worker_id=None)` | `EventsAppended` (`appended`, the log's last `position`); a repeated event is stored once |
| `runs.events` | `events(run_id, *, after=0, limit=50)` | `list[RunEventEntry]` (`position`, `event`) |
| `runs.stream_events` | `stream_events(run_id, *, after=0)` | an async iterator of `RunEventEntry`, live, until the run has ended; it reconnects from the last position on its own |
| `artifacts.upload` | `artifacts.upload(run_id, data, *, mime_type="application/json", worker_id=None)` | `ArtifactRef` (its SHA-256 is sent and checked) |
| `artifacts.download` | `artifacts.download(artifact_id)` | `bytes`, or `None` |
| `schedules.*` | `schedules.create(spec)`, `list(...)`, `get(id)`, `update(id, ScheduleUpdate(...))`, `delete(id)`, `fire(id, *, at=None)` | `Schedule`, `Page[Schedule]`, `Schedule` or `None`, `Schedule`, `None`, `FireResult` |
| `webhooks.*` | `webhooks.create(url, events)`, `list()`, `get(id)`, `delete(id)`, `rotate_secret(id)` | `WebhookCreated` (with the secret), `Page[Webhook]`, `Webhook` or `None`, `None`, `WebhookCreated` (the new secret) |
| | `webhooks.deliveries(*, state=None, webhook_id=None, cursor, limit=50)`, `redeliver(delivery_id)` | `Page[DeliveryRecord]`, `DeliveryRecord` |
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

    # or every page (at most ten here), without the sub-agents a paused parent waits on:
    async for summary in runs.iterate(
        status=RunStatus.PAUSED, assignee="user:alice", top_level=True, max_pages=10
    ):
        ...
```

A summary carries the question (`awaiting`); read the run with `get` for its input,
checkpoint and output, and the audit trail with `resolutions`. A large payload to review (a
table, a diff) is an artifact: the interrupt's `payload_ref` is its `ArtifactRef`, and
`runs.artifacts.download(ref.artifact_id)` its bytes.

A question's options are plain strings or `Option(value, label, description)`
(`trellis.contracts.runs`); the answer carries the value, and with `multiple=True` a list of
values. `component` and `props` name the asker's own screen (render `ui` when you have none),
and `ui_schema` gives form widget hints for `expects`. A resolution may carry a `comment`,
and an approval of a tool call `remember="run"` (approve calls like it for the rest of the
run). `trellis.runs.answers.answer_problem(interrupt, resolution)` says, before you send it,
whether agent-runs will take the answer (it answers `422` otherwise).

## A run's events, from any replica

The worker running a run appends its `RunEvent`s; anyone with a key of the tenant reads or
follows them, whichever replica they reach:

```python
await runs.append_events(job.record.run_id, events, worker_id=job.worker_id)  # in a handler

async for entry in runs.stream_events(run_id):  # a UI backend: live, to the end of the run
    print(entry.position, entry.event.type, entry.event.data)
```

Append while the run runs, before pausing or finishing it; a retried append adds nothing.
`events(run_id, after=n)` reads the log from a position; `stream_events` yields the log and
then each new event, reconnecting from the last position it yielded.

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

**Rotating the secret** (a leak, a routine change) misses nothing:

```python
rotated = await runs.webhooks.rotate_secret(hook.webhook_id)
store_secret(rotated.secret)  # before rotated.previous_secret_expires_at (24 h by default)
```

Until `previous_secret_expires_at` every delivery is signed with both secrets
(`t=…,v1=<new>,v1=<old>`, `sign(secret, t, body, previous=old)`), and `verify_signature`
accepts a header if any `v1` matches the secret it is given: a receiver on the old secret and
one on the new both verify.

**Deliveries given up on** (7 attempts over about a quarter of an hour, or a receiver that
refused for good) are kept for seven days, dead, with the last error:

```python
from trellis.runs import DeliveryState

async with RunsClient() as runs:
    dead = await runs.webhooks.deliveries(state=DeliveryState.DEAD)
    for missed in dead.items:
        print(missed.run_id, missed.type, missed.last_error)  # e.g. "answered 503"
        await runs.webhooks.redeliver(missed.delivery_id)  # sent within a tick, same event_id
```

Outside dev agent-runs delivers only to `https` URLs whose host resolves to public addresses
(a subscription to a private, loopback or link-local one raises `ValidationError`); each
delivery checks the addresses again and connects only to one it checked, never following a
redirect, unless the operator allows private targets.

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
  it ran past its `deadline` or its working-time limit (agent-runs ended it `TIMEOUT`).
- `job.remaining_seconds` is the working time the run has left now (its `timeout_seconds`
  or the operator's maximum, the lesser, less what every attempt worked; `None` without a
  limit), kept current by every lease. The worker stops a handler still running when the
  time the claim gave is used up and finishes the run `TIMEOUT` (`run_timeout`, not
  retried); bound your own steps by it to end cleanly first. A `TimeoutError` of your own
  (a model call) still ends the run `ERROR`.
- Cancelling a run (`runs.cancel(run_id, reason=...)`, from anywhere) reaches its worker
  through the next heartbeat (`cancel_requested`): the handler is cancelled and the worker
  finishes the run `CANCELLED`. In the handler, `job.cancel_requested` tells that
  cancellation from a lost lease (record it as the ending, or let the worker do it).
- `concurrency` handlers run at once (default: the CPU count, 1 to 8). An idle worker asks
  again after 0.5 s, doubling to 10 s, jittered.
- `serve()` stops on SIGTERM or SIGINT: no new claims, the runs held get 25 s to finish,
  then are released: their handlers are cancelled with the message `RELEASED` (nothing
  written) and the worker hands each run back to the queue (`runs.release`), where another
  worker claims it at once as its next attempt, no lapse counted. Only if that cannot be
  sent does the run wait for its lease to lapse. A second signal releases them at once.
  `run()` is the same loop without signal handling (stop it with `stop()`), and
  `run_once()` claims and executes one run.
- A handler that raises ends its run at once as `ERROR`, with the exception as the run's
  `AgentError` (`AgentError.of`: its class as `code`, its text, and `retryable` as the
  contracts classify it); you need not catch anything to record a failure. A retryable one
  (a timeout, a rate limit, a dependency down) is retried by agent-runs: the run goes back
  on the queue and is claimed again after 10 s, 20 s, then 40 s; the fourth such error
  stands. Only if that finish fails does the run wait for its lease to lapse, as for a
  worker that died.
- `job.pause(interrupt, checkpoint=...)` and `job.finish(...)` are fenced: after the lease
  was lost they raise `LeaseLostError`. A handler that records a cancellation as the run's
  ending checks for `RELEASED in exc.args` and writes nothing then.
- `store` is anything with `claim`, `heartbeat`, `release`, `pause` and `finish` as
  `RunsClient` has them (the `WorkerStore` protocol). `tenant=` names the tenant a platform
  key claims for; a platform key with none serves every tenant, the one whose workers hold
  the fewest runs first, and each run's later calls name its own tenant.

## Errors and retries

An error answer is an RFC 9457 problem; the client raises the class of its `code` (else of
its status). Every error has `message`, `code`, `status` (0 without a response),
`retryable`, `request_id`, `details` and `retry_after`.

| Class | `code` / status | Means |
|---|---|---|
| `RunsError` | `INTERNAL` / 500, any other | the base class |
| `AuthenticationError` | `AUTHENTICATION` / 401 | no key, or one the registry does not know |
| `AuthorizationError` | `AUTHORIZATION` / 403 | another tenant, an `on_behalf_of` the key may not act as, a paused run the key may not answer (or a run it may not cancel) |
| `NotFoundError` | `NOT_FOUND` / 404 | no such record (reads by id answer `None` instead) |
| `ConflictError` | `CONFLICT` / 409 | an illegal transition, an answer to another interrupt, a cancel of a run that ended, a redelivery of a delivery still owed |
| `LeaseLostError` | `LEASE_LOST` / 409 | the worker no longer holds the run: stop, write nothing more. **Not** a `ConflictError` |
| `ValidationError` | `VALIDATION` / 400, 405, 422 | the request is invalid, an answer that does not fit its question included (the message says what) |
| `PayloadTooLargeError` | `PAYLOAD_TOO_LARGE` / 413 | a `ValidationError`: a body, payload, checkpoint or artifact too large |
| `RateLimitedError` | `RATE_LIMIT` / 429 | the tenant's budget is spent for now (`retry_after`) |
| `DependencyUnavailableError` | `DEPENDENCY_UNAVAILABLE` / 502, 503, 504, or no response | the service or its database could not answer |

A call that fails on the way (no response, `429`, `502`, `503`, `504`) is sent again up to
`max_retries` times, after the `Retry-After` the service asked for (at most 30 s) or a
full-jitter backoff from 0.25 s doubling to 5 s, unless the problem says
`retryable: false`. Every write agent-runs takes is safe to repeat: a start is idempotent on
its id, a repeated pause, finish or release answers the stored run, a repeated resume with
the same `InterruptResolution` object answers the run as it is now (a new resolution for an
interrupt already answered, a second click, is a `ConflictError`), a repeated cancel answers
the run as it is, the same artifact bytes are the same artifact, a schedule create is an
upsert.
