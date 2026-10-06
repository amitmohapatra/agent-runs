# agent-runs

Durable agent runs, the worker queue, the human inbox, and the schedules that start runs.
One service (it absorbed agent-schedules in 0.2.0): an API process and a ticker process over
one PostgreSQL database, with run artifacts' bytes in blob storage (a filesystem, or GCS).

This service never executes an agent. A harness (or any framework, through the
[Python SDK](#the-python-sdk): the [two ways](#where-this-fits-two-ways-to-use-trellis)) does, either in its own process (it records the run here as
`RUNNING`) or as a worker that claims `QUEUED` runs from here under a lease. This service remembers: a run that pauses for an approval at 2 a.m. is still there
at 9 a.m., a crashed worker's run goes back on the queue, a run that failed on a blip is tried
again later, a run that works too long is stopped, and a schedule fires on behalf of a person
who is not present.

Every record is a [trellis-contracts](../agent-contracts) type: a run is a `RunRecord`
started from a `RunStart`, paused with an `Interrupt`, resumed with an
`InterruptResolution`; a schedule is a `Schedule` created from a `ScheduleSpec`.

## Where this fits: two ways to use Trellis

Trellis is used in one of two ways, and each block works in both:

- **Way 1, wrapped.** `from trellis import Harness; h = Harness(); agent = h.wrap(my_agent)`.
  The harness runs your agent (LangGraph, Deep Agents, OpenAI Agents SDK, Claude Agent SDK,
  a plain function) and uses every block automatically: memory context, recording and
  feedback; durable runs, the inbox, schedules and the worker in agent-runs; governance of
  tool calls; models and MCP tools through Bifrost; evals; the AG-UI and A2A surfaces.
- **Way 2, pluggable blocks.** Keep your framework untouched and import only the blocks you
  want: `trellis.memory` (`MemoryClient`), `trellis.runs` (`RunsClient`, `Worker`,
  `webhooks.verify_signature`), `trellis.contracts` (the shared types), `bifrost_sdk` (models
  and MCP tools through Bifrost), and from the harness repo `trellis.harness.governance`
  (`Governance.from_env`, `check`, `governed`), `trellis.harness.evals` (`EvalServices`,
  `evaluate`, `judge`) and `trellis.harness.a2a.remote`.

A package shipped from its own repo is top-level `trellis.X`; anything from the harness repo
is `trellis.harness.X`. `bifrost_sdk` (pip `bifrost-sdk`) is the exception: it keeps its own,
older name.

**This package** is the durable side of a run: agent-runs keeps runs, the worker queue, the
inbox of paused runs, schedules and webhooks, and never executes an agent. `trellis.runs`
(pip `trellis-runs`, in [`sdk/python`](sdk/python/README.md)) is its Python client and a
framework-neutral worker loop.

| | What happens with agent-runs and `trellis.runs` |
|---|---|
| **Way 1, wrapped** | With `RUNS_URL` set, the harness's run store is `trellis.runs.RunsClient`: `agent.run` records the run and finishes it, `agent.start` queues it, an `ask` or an approval pauses it (the `Interrupt`, a large table or diff as an artifact, the run's journal as `checkpoint`), `agent.resume` answers it, `h.inbox()` lists the paused runs, `agent.schedule(...)` creates a schedule, and `h.worker(...)` (or `python -m trellis.harness.worker`) runs wrapped agents on `trellis.runs.Worker`. You write no runs code. Without `RUNS_URL` the harness keeps runs in its own process, and none outlives it. |
| **Way 2, pluggable** | Your framework runs the agent, untouched; you start, queue, pause and finish its runs with the SDK: |

```python
from trellis.contracts.runs import RunStart, RunStatus
from trellis.runs import Job, RunsClient, Worker

async with RunsClient() as runs:  # RUNS_URL and TRELLIS_API_KEY
    await runs.start(RunStart(tenant_id="acme", agent_id="triage", input={"ticket": 7}), queue=True)

    async def handle(job: Job) -> None:  # your framework runs the claimed run
        await job.finish(RunStatus.SUCCESS, output=await my_agent(job.record.input))

    await Worker(runs, handle, ["triage"]).serve()  # claim, heartbeat, stop on SIGTERM
```

- **Choose Way 1 when** you want durable runs, approvals, the inbox and schedules from
  `h.wrap` with no code, and a resumed run that repeats no question and no side effect (the
  harness's journal is the checkpoint).
- **Choose Way 2 when** your framework keeps its own state (a LangGraph checkpointer, an OpenAI
  Agents `RunState`) and you want only what this service adds: a run that survives the
  process, a queue of workers, an inbox, schedules. Or when the caller is not an agent at
  all: a UI reading the inbox, or a webhook receiver (`trellis.runs.webhooks.verify_signature`,
  the same in both ways).

Harness docs: [the two ways](https://github.com/amitmohapatra/agent-harness/blob/main/README.md#two-ways-to-use-trellis) · [every page](https://github.com/amitmohapatra/agent-harness/blob/main/docs/README.md) ·
blocks: [runs](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/runs.md), [contracts](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/contracts.md), [governance](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/governance.md) ·
recipes: [LangGraph](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/langgraph.md), [OpenAI Agents SDK](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/openai-agents.md),
[Claude Agent SDK](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/claude-agent-sdk.md).

## The state machine

The contracts' `RunStatus.can_become` is the only transition check; anything else is a
`409` that changes nothing. These are exactly the moves the routes and the ticker make
([docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) has what each does to the row, the sequence
diagrams and the tables):

```mermaid
stateDiagram-v2
  [*] --> RUNNING: POST /v1/runs (queue false)
  [*] --> QUEUED: POST /v1/runs (queue true), or a schedule fires

  QUEUED --> RUNNING: POST /v1/runs/claim, once available_at passed and there is room (lease to worker_id)
  QUEUED --> CANCELLED: cancel, or finish CANCELLED
  QUEUED --> TIMEOUT: finish TIMEOUT, or ticker past the run's deadline (run_deadline)

  RUNNING --> PAUSED: pause (Interrupt, checkpoint), lease released
  RUNNING --> QUEUED: ticker, lease lapsed, lease_lapses < MAX_LEASE_LAPSES (attempt + 1, after a backoff)
  RUNNING --> QUEUED: release by the lease holder (attempt + 1, no lapse counted)
  RUNNING --> QUEUED: finish ERROR, retryable, was queued, error_retries < MAX_ERROR_RETRIES (attempt + 1, after a backoff)
  RUNNING --> ERROR: ticker, lease lapsed, lease_lapses reaches MAX_LEASE_LAPSES (lease_expired)
  RUNNING --> SUCCESS: finish
  RUNNING --> PARTIAL: finish
  RUNNING --> ERROR: finish
  RUNNING --> TIMEOUT: finish, ticker past the run's deadline (run_deadline), or past its working-time limit (run_timeout)
  RUNNING --> CANCELLED: finish, cancel of a run no worker holds, or after a cancel request: ticker when the lease runs out, pause, release
  RUNNING --> REJECTED: finish

  PAUSED --> RUNNING: resume, not CANCEL, never queued (attempt + 1)
  PAUSED --> QUEUED: resume, not CANCEL, was queued (attempt + 1)
  PAUSED --> CANCELLED: resume CANCEL, cancel, or finish CANCELLED
  PAUSED --> TIMEOUT: finish TIMEOUT, ticker past the run's deadline, or past the interrupt's with no escalate_to
  PAUSED --> PAUSED: ticker past the interrupt's deadline, assignee becomes escalate_to (once)

  SUCCESS --> [*]
  PARTIAL --> [*]
  ERROR --> [*]
  TIMEOUT --> [*]
  CANCELLED --> [*]
  REJECTED --> [*]
```

## When to use what

| You want to… | Use |
|---|---|
| keep a durable record of a run your own process executes | `POST /v1/runs` (it is `RUNNING`), then `finish` |
| hand a run to a fleet of workers, surviving a worker that dies | `POST /v1/runs {queue: true}`; workers `claim`, `heartbeat` every third of the lease, and send `worker_id` on `pause`, `finish`, event appends and artifact uploads |
| put an urgent run ahead of the others | `RunStart.priority` (`-1000` to `1000`, default `0`): a claim takes the highest first, then the oldest |
| never run two runs of one conversation (or customer, or account) at once | `RunStart.concurrency_key`: at most `RUNS__RUNS__CONCURRENCY_PER_KEY` (1) of the tenant's runs sharing it are `RUNNING`; the rest wait `QUEUED`, in order |
| share one worker fleet fairly between tenants | nothing to set: a platform key's claim with no `X-Trellis-Tenant` takes from every tenant's queue, the tenant whose workers hold the fewest runs first. The operator may also cap what any one tenant holds (`RUNS__RUNS__MAX_RUNNING_PER_TENANT`) |
| show a run's progress from any replica, live | the worker appends its `RunEvent`s (`POST /v1/runs/{id}/events`); anyone reads them (`GET /v1/runs/{id}/events?after=`) or follows them as server-sent events (`GET /v1/runs/{id}/events/stream`), from whichever replica they reach |
| make a retried start harmless | the same `run_id`, or an `idempotency_key` (one run per tenant and key) |
| stop for a person's approval, answer or edit | `pause` with an `Interrupt` (`assignee`, `deadline`, `escalate_to`) and the executor's `checkpoint`; the person answers with `resume`, optionally with a `comment`, and approves similar calls for the rest of the run with `remember: "run"` (the harness keeps that promise) |
| offer labelled choices, several picks, or your own review screen | `Interrupt.options` as `{value, label, description}` objects (or plain strings), `multiple: true` (the answer is a list of values), `ui_schema` (form widget hints), `component` and `props` (your screen, `ui` the fallback). Every answer is checked against `expects` and the options, whoever collected it |
| move an unanswered question up, or give up on it | the interrupt's `deadline` and `escalate_to`: the ticker reassigns it once, else ends the run `TIMEOUT` |
| make sure a run is done by a time, whatever happens | `RunStart.deadline`: past it the ticker ends the run `TIMEOUT` (`run_deadline`, not retryable), queued, running or waiting for a person; a worker still running it is told `LEASE_LOST` and stops |
| bound how long a run may work, not counting the queue or a person's answer | `RunStart.timeout_seconds`: the ticker ends a run whose time `RUNNING`, across attempts and crashes (`worked_seconds`), passes it `TIMEOUT` (`run_timeout`); each lease says the time left (`remaining_seconds`). The operator's `RUNS__RUNS__MAX_RUN_SECONDS` bounds every run |
| show a reviewer something too big for a question (a table, a diff) | `POST /v1/runs/{id}/artifacts`, then the `ArtifactRef` as `Interrupt.payload_ref`; the UI reads `GET /v1/artifacts/{id}` |
| build a person's or a role's inbox | `GET /v1/runs?status=PAUSED&assignee=…`, with `&top_level=true` to leave out paused sub-agents (their parent is listed) |
| prove who approved what, and when | `GET /v1/runs/{id}/resolutions` (append-only) |
| cancel a run, whatever its status, saying why | `POST /v1/runs/{id}/cancel {reason}` (`RunsClient.cancel`): queued or waiting, it ends `CANCELLED` at once; held by a worker, the worker is told through its heartbeat and stops (the SDK's `Worker` does) |
| stop a worker without losing what it runs | nothing: the SDK's `Worker` lets its runs finish for 25 s, then releases them (`POST /v1/runs/{id}/release`), back on the queue at once for another worker |
| survive a model's rate limit or a dependency restarting | nothing: a queued run its worker ends `ERROR` with a retryable error is retried later, up to 3 times (10 s, 20 s, 40 s); a run kept in its caller's process is not |
| start runs on a timetable, as someone, while nobody is present | `POST /v1/schedules` (a cadence bucket or an hourly-or-slower cron, in the schedule's zone); repeat the create freely, it is an upsert. Its `timeout_seconds`, `agent_version`, `priority` and `concurrency_key` are copied into every run it fires, and its `metadata` into the run's under the fire's own keys, so a scheduled run carries everything a started one can |
| keep the database from growing forever | `RUNS__RUNS__RETENTION_DAYS`: the ticker deletes runs that ended longer ago, with their resolutions and events (unset: kept forever) |
| run a schedule now, or pause and resume it | `POST /v1/schedules/{id}/fire`; `PATCH {"enabled": false}` / `{"enabled": true}` |
| hear about pauses, escalations and endings instead of polling | `POST /v1/webhooks`; verify `X-Trellis-Signature` with the secret shown once (`trellis.runs.webhooks.verify_signature`) |
| change a webhook's secret without missing a delivery | `POST /v1/webhooks/{id}/rotate-secret`: both secrets sign for 24 h, and `verify_signature` accepts either |
| find and resend the deliveries a receiver missed | `GET /v1/webhooks/deliveries?state=dead`, then `POST /v1/webhooks/deliveries/{id}/redeliver` |
| drive all of it from Python, from any agent framework (Way 2) | the SDK, `trellis.runs`: `RunsClient` and `Worker` ([below](#the-python-sdk)) |
| have a wrapped agent use all of it with no code (Way 1) | `h.wrap(agent)` with `RUNS_URL` set ([the two ways](#where-this-fits-two-ways-to-use-trellis)) |

## The Python SDK

[`sdk/python`](sdk/python/README.md) is `trellis-runs` (imports as `trellis.runs`), the
Python client of this API, versioned with it (0.4.0). It depends on `httpx`, `pydantic` and
`trellis-contracts` only, so it plugs into LangGraph, OpenAI Agents, the Claude Agent SDK or
plain code (Way 2, [the snippet above](#where-this-fits-two-ways-to-use-trellis)) as well as
into agent-harness, whose run store it is (Way 1):

- `RunsClient`: one method per operation, named by its operation id (`runs.start` is
  `start`, `schedules.fire` is `schedules.fire`); reads by id answer `None` for a record that
  does not exist; listings answer a `Page` (`iterate` follows the `Link` pages); problems
  raise typed errors (`LeaseLostError`, `ConflictError`, …); failures on the way are
  retried, honouring `Retry-After`.
- `Worker`: the claim loop: a heartbeat every third of the lease, a lost lease cancels the
  handler, a cancel asked for (`cancel_requested` on the heartbeat's lease) cancels it and
  ends the run `CANCELLED`, a handler that raises ends its run `ERROR` at once (the exception
  as the run's `AgentError`; agent-runs retries a retryable one later), bounded concurrency,
  idle backoff, a graceful stop that releases what is still running after 25 s back to the
  queue. A handler still running when the run's working time is used up is stopped and the
  run ends `TIMEOUT` (`run_timeout`). `Job` tells the handler the working time left
  (`remaining_seconds`) and whether a cancel was asked (`cancel_requested`). A worker with a
  platform key and no tenant serves every tenant, fairly.
- `trellis.runs.webhooks`: `sign` (the service signs every delivery with it),
  `verify_signature` (any matching signature, so a receiver keeps working through a secret's
  rotation) and `parse_delivery` for a receiver.

It lives in this repository as a uv workspace member, so a change to a route and to its
client is one change; its suite (`make sdk`) checks it against `docs/openapi.json` and holds
it to 100% line and branch coverage.

## The API

Every `/v1` route needs `X-API-Key`; the ops routes do not. The OpenAPI document is
[docs/openapi.json](docs/openapi.json) (live at `/openapi.json`, `/docs`, `/redoc`). Every error is an RFC 9457
problem (`application/problem+json`) with a stable `code` (`LEASE_LOST` tells a worker to
stop; `DEPENDENCY_UNAVAILABLE` and `RATE_LIMIT` come with `Retry-After`).
[docs/api.md](docs/api.md) has every route, body, status code and problem `code`, and the
exact claim, heartbeat and resume semantics a worker implements.

| Route | What it does |
|---|---|
| `POST /v1/runs` | record a run (`RUNNING`), or queue it (`queue: true` → `QUEUED`); idempotent on run id and `idempotency_key` |
| `POST /v1/runs/claim` | lease the next queued run of `agent_ids` to `worker_id` (highest `priority`, then oldest, with room under its `concurrency_key` and its tenant's cap), or `204`; a platform key with no tenant claims from every tenant, fairly |
| `POST /v1/runs/{id}/heartbeat` | extend the lease, optionally saving a progress `checkpoint` the next attempt resumes from; the lease says the working time left and whether a cancel was asked; `409 LEASE_LOST` = stop |
| `POST /v1/runs/{id}/release` | the lease holder lets go of the run (it is stopping): back on the queue at once, as the next attempt, no lapse counted |
| `POST /v1/runs/{id}/pause` | the run waits on an `Interrupt` (assignee, deadline, escalation), keeping the executor's opaque `checkpoint` for whoever resumes it |
| `POST /v1/runs/{id}/resume` | answer it with an `InterruptResolution`; the same resolution repeated answers the run as it is now |
| `POST /v1/runs/{id}/cancel` | cancel it, whatever its status, saying why: at once, or through the heartbeat of the worker holding it; the keys that may answer it may cancel it |
| `POST /v1/runs/{id}/finish` | end it: `SUCCESS`, `PARTIAL`, `ERROR`, `TIMEOUT`, `CANCELLED`, `REJECTED` (a queued run's retryable `ERROR` is retried later instead); the same finish repeated answers the stored run |
| `GET /v1/runs/{id}` | one run, the full record |
| `GET /v1/runs/{id}/resolutions` | every interrupt the run paused on and how it was answered, oldest first (append-only audit trail) |
| `POST /v1/runs/{id}/events` | append the run's `RunEvent`s to its log while it runs, fenced like a heartbeat; a repeated event is stored once |
| `GET /v1/runs/{id}/events?after=` · `GET /v1/runs/{id}/events/stream` | the run's log past a position · followed as server-sent events (`Last-Event-ID`), ending with `event: end` once the run ended |
| `GET /v1/runs?status=PAUSED&assignee=…&top_level=true` | run summaries; with these filters, the inbox of a person or role (`top_level` leaves out sub-agents). Every listing pages with `cursor` and `limit` and a `Link: rel="next"` header |
| `POST /v1/runs/{id}/artifacts` | store a large payload (an `ask` table, a diff; ≤ 50 MiB) in blob storage and get its `ArtifactRef` for `Interrupt.payload_ref` |
| `GET /v1/artifacts/{id}` | the artifact's bytes, checksum-verified, tenant-scoped |
| `POST /v1/schedules` | create a schedule, or get the one with the same agent, `on_behalf_of`, cadence and input (an upsert) |
| `GET /v1/schedules` · `GET/PATCH/DELETE /v1/schedules/{id}` | list, read, change (`{"enabled": false}` pauses, `true` resumes), delete |
| `POST /v1/schedules/{id}/fire` | fire now |
| `POST/GET /v1/webhooks` · `GET/DELETE /v1/webhooks/{id}` | the tenant's webhook subscriptions |
| `POST /v1/webhooks/{id}/rotate-secret` | a new secret; the old one signs too for the overlap |
| `GET /v1/webhooks/deliveries?state=&webhook_id=` · `POST /v1/webhooks/deliveries/{id}/redeliver` | the deliveries owed and the dead ones; owe a dead one again |
| `GET /health/live` · `GET /health/ready` · `GET /metrics` | the process is up · the database answers · Prometheus metrics (no key) |

## The ticker

`agent-runs-ticker` is one loop (every `TICK_SECONDS`), straight against the database:

1. **Schedules.** Each due schedule is claimed with `FOR UPDATE SKIP LOCKED` and fired: its
   run is inserted `QUEUED` in the same transaction, idempotent on `(schedule_id,
   fire_time)`. A run that cannot be queued is recorded on the schedule, which backs off
   (retryable) or pauses itself (permanent, or `MAX_CONSECUTIVE_FAILURES`).
2. **Run deadlines.** A run not yet ended (`QUEUED`, `RUNNING` or `PAUSED`) past its own
   `deadline` (`RunStart.deadline`) ends in `TIMEOUT` with an `AgentError` of code
   `run_deadline` (`retryable: false`: a retry would only be later). The deadline is when
   the run must be done by, so time in the queue and time waiting for a person count. A
   worker still running it loses the run: its next heartbeat or write is `409 LEASE_LOST`,
   and the SDK's `Worker` cancels its handler.
3. **Working time.** A `RUNNING` run whose working time passed its limit ends in `TIMEOUT`
   with code `run_timeout` (not retryable). The working time is the time the run spent
   `RUNNING`, across attempts: kept on the run (`worked_seconds`) as each stretch ends, so a
   crash does not reset it, and time queued or waiting for a person does not count. The
   limit is the caller's `RunStart.timeout_seconds` or the operator's
   `RUNS__RUNS__MAX_RUN_SECONDS`, the lesser; neither set, there is none. Every lease (claim,
   heartbeat) says the time left (`remaining_seconds`), so a worker can stop in time; one
   that does not is fenced off as for a deadline.
4. **Leases.** A `RUNNING` run whose lease lapsed (its worker stopped heartbeating) goes
   back to `QUEUED` as the next attempt, after a short backoff (5 s, doubling per lapse, at
   most 1 min, jittered: a run that kills its worker is not handed straight to the next), or
   ends in `ERROR` (`lease_expired`) on its `MAX_LEASE_LAPSES`-th (5th) lapse. Only lapses
   count toward that, never a person's answers: a run reviewed ten times still survives four
   crashes. A run whose cancel was asked for ends `CANCELLED` instead.
5. **Escalation.** A `PAUSED` run past its interrupt's `deadline` moves to `escalate_to`
   (once) or ends in `TIMEOUT`, with a webhook event either way.
6. **Webhooks.** Due deliveries in the outbox are sent (one attempt each, concurrently),
   then removed, rescheduled with backoff, or, given up on, kept as dead (below).
7. **Dead deliveries.** Deliveries dead for more than `RUNS__WEBHOOKS__DEAD_RETENTION_DAYS`
   (7) are dropped.
8. **Artifacts.** Artifacts of runs that ended more than `ARTIFACT_RETENTION` (7 days) ago
   are deleted: the blob, then the record.
9. **Run retention.** Only when the operator sets `RUNS__RUNS__RETENTION_DAYS`: runs that
   ended longer ago are deleted with their resolutions and events, a run with artifacts
   still kept after them. Unset, every run is kept.

Each step is bounded per tick and safe in several replicas. A tick that fails as a whole
(the database is down) counts against a breaker; `python -m agent_runs.heartbeat` is the liveness
check (a heartbeat file touched after every tick, `RUNS__TICKER__HEARTBEAT_FILE`, one per
ticker; unset, each ticker process beats into its own file in the temp directory).

## Authentication

One scheme, one key system. `X-API-Key` is a key issued by the Memory Service; agent-runs
introspects it there (`GET {RUNS__MEMORY__URL}/v1/keys/self`, cached 60 s, refusals 10 s)
and learns the tenant it speaks for, the principal recorded as `created_by`, and the
principals it may put in `on_behalf_of`. The contract is in
[docs/api.md](docs/api.md#authentication). A platform key has no tenant of its own and names the tenant it acts
for in `X-Trellis-Tenant`; a tenant key may send that header only to agree with itself
(`403` otherwise).

### Who may answer a paused run

Any key of the tenant reads every run, lists every inbox and works the queue: `assignee` is
the filter an inbox shows, not a lock. Answering a paused run (`POST /v1/runs/{id}/resume`,
`RunsClient.resume`, a `CANCEL` included) is checked, against the run's assignee at that
moment (after any escalation), before anything is written:

1. An **admin** key (or the operator's **platform** key) answers any run.
2. A key that **may act for anyone** (`"*"` in its `may_act_as`, which is what the Memory
   Service issues by default) answers any run: the application holding it vouches for the
   `reviewer` it names.
3. A key **restricted to listed people** (`may_act_as=["user:priya"]`) answers only as one
   of them, and only a run assigned to that person or to nobody. The `reviewer` is who it
   answers as (a bare id `priya` means `user:priya`; no reviewer means the key itself). A run
   assigned to a group (`role:finance`) is refused: agent-runs cannot see who is in a group,
   so answer it with the application's key or an admin key.

| The key | Run assigned to `user:priya` | to `user:raj` | to `role:finance` | to nobody |
|---|---|---|---|---|
| admin or platform | yes | yes | yes | yes |
| `may_act_as=["*"]` (the default) | yes | yes | yes | yes |
| `may_act_as=["user:priya"]`, `reviewer="priya"` | yes | no | no | yes |
| `may_act_as=["user:priya"]`, `reviewer="raj"` | no | no | no | no |

A refusal is `403 AUTHORIZATION` (`AuthorizationError` in the SDK) whose detail says why,
for example `the run is assigned to user:raj, not user:priya; this key may act only for
user:priya`, or `the run is assigned to role:finance, a group: a key restricted to listed
people cannot answer it; answer with the application's key or an admin key`. The
`reviewer` is stored as given.

**Nothing changes if your keys may act for anyone**, as every key the Memory Service issues
does unless told otherwise. To let a person's own client (an approvals UI, a mobile app)
answer only their runs, issue that client a key restricted to them, with the tenant's admin
key:

```python
from trellis.memory import MemoryClient

async with MemoryClient(api_key=ADMIN_KEY) as admin:
    issued = await admin.tenant.keys.issue("service", "priya-approvals", may_act_as=["user:priya"])
    priya_key = issued.token  # shown once
```

```python
from trellis.contracts.runs import InterruptDecision, InterruptResolution
from trellis.runs import RunsClient

async with RunsClient(api_key=priya_key) as runs:
    await runs.resume(
        InterruptResolution(
            interrupt_id=run.awaiting.interrupt_id,
            run_id=run.run_id,
            decision=InterruptDecision.APPROVE,
            reviewer="priya",
        )
    )  # her run: answered; raj's or role:finance's: AuthorizationError
```

The rule is `answering.py`, one function. Cancelling a run (`POST /v1/runs/{id}/cancel`,
`RunsClient.cancel`) is checked by the same rule: a key may cancel a run it could answer, as
any principal it may act for; a run that is not paused is assigned to nobody.

### An answer is taken once

A paused run continues once per question, automatically safe against retries and double
clicks alike:

- **The same answer sent again** (the SDK resends a resume whose answer it lost: no
  response, `502`, `503`, `504`) answers `200` with the run as it is now and changes
  nothing: no second resolution, no second event, no second attempt. "The same" means the
  very same `InterruptResolution`, `resolved_at` included, which is set once, when the
  person answered.
- **Any other answer** to a question already answered (a second click, a second reviewer,
  the same decision made again later) is `409 CONFLICT` (`ConflictError`): the run never
  continues twice.

### An answer must fit the question

agent-runs checks every answer against what was asked before anything is written, so a
run never continues on an answer its agent cannot use. The check is
`trellis.runs.answers`, the same one the harness makes for a run it keeps in its own
process:

- An `ANSWER` must fit the interrupt's `expects` (a JSON Schema); without `expects`, an
  interrupt with `options` takes only one of them.
- An `EDIT` of a question (an interrupt with no `tool_call`) carries a `payload` that fits
  `expects`. Edited tool-call arguments are checked by the harness, which knows the tool's
  schema.
- `APPROVE`, `REJECT` and `CANCEL` carry nothing to check.

A misfit is `422 VALIDATION` (`ValidationError` in the SDK), its detail saying what does not
fit (`the answer['qty'] does not fit what was asked: 'two' is not of type 'integer'`), and
the run keeps waiting for a good answer. A question whose `expects` is not a JSON Schema at
all is refused when it is asked: the `pause` is `422`, and the run keeps running.

## Artifacts

Large review payloads never live in a run's checkpoint. The harness uploads an `ask` table
or a diff with `POST /v1/runs/{id}/artifacts` and pauses with the returned `ArtifactRef` as
`Interrupt.payload_ref`; a UI reads it with `GET /v1/artifacts/{id}`. The bytes are in the
blob store (`RUNS__BLOB__PROVIDER=filesystem` under `RUNS__BLOB__ROOT`, shared by the API
and the ticker; or `gcs` in `RUNS__BLOB__BUCKET`, with the environment's Google
credentials), written once, never overwritten, and verified against their SHA-256 on every
read. While a run works, only its lease holder adds artifacts; while it waits, only a
service key. Fencing, limits and retention are in [docs/api.md](docs/api.md#artifacts).

## Webhooks

A tenant subscribes URLs to run events (`POST /v1/webhooks`: `run.paused`,
`run.escalated`, `run.finished`); each subscription has its own secret, shown once. An event
is written to an outbox in the transaction of the run change that caused it and the ticker
sends it, retried with the service's backoff, at least once. Every delivery carries
`X-Trellis-Signature: t=<unix seconds>,v1=<hex hmac-sha256 of "<t>.<body>">`, with
`X-Trellis-Event` and `X-Trellis-Delivery`. The SDK's `trellis.runs.webhooks.sign` is the one
implementation of the scheme: the ticker signs with it, and a receiver checks with
`trellis.runs.webhooks.verify_signature` (five minutes' tolerance, constant-time compare);
[the SDK's README](sdk/python/README.md#webhooks) has a receiver. `event_id` is stable per
event, so a receiver drops repeats.

- **Rotating a secret.** `POST /v1/webhooks/{id}/rotate-secret` answers a new secret, once.
  For `RUNS__WEBHOOKS__SECRET_OVERLAP_HOURS` (24) every delivery carries a signature with
  each secret (`t=…,v1=<new>,v1=<old>`, as Stripe does) and `verify_signature` accepts any
  matching one, so receivers switch to the new secret without a missed or refused delivery;
  `previous_secret_expires_at` on the subscription says when the old one stops signing.
- **Dead deliveries.** A delivery that used its 7 attempts (15 s doubling to 10 min apart),
  or that its receiver refused for good (any `4xx` but `408` and `429`), is not deleted: it
  is kept, dead, with its `last_error`, for `RUNS__WEBHOOKS__DEAD_RETENTION_DAYS` (7).
  `GET /v1/webhooks/deliveries?state=dead` (or `&webhook_id=` for one subscription) lists
  them and `POST /v1/webhooks/deliveries/{id}/redeliver` owes one again, at once, with all
  its attempts ahead of it.
- **Where deliveries may go.** Outside `dev` a subscription's URL must be `https` and its host
  must resolve only to public addresses: not private, loopback, link-local (where cloud
  metadata lives), carrier-grade NAT, reserved or multicast (`422` when subscribed). Every
  attempt resolves the host again, once, checks every address, and connects only to an
  address it checked (in the resolver's order, the next when one refuses the connection),
  naming the host in `Host`, in TLS SNI and in the certificate check: a name that changes
  between the check and the connection (DNS rebinding) cannot send a delivery elsewhere. A
  host that now resolves to such an address is refused for good (dead), one that does not
  resolve is retried. Redirects are never followed: a `3xx` is a final answer. A deployment
  whose receivers are inside its own network sets `RUNS__WEBHOOKS__ALLOW_PRIVATE_TARGETS=true`;
  `dev` allows them unless it is `false`.

## Run it

Needs PostgreSQL, the Memory Service (the key registry, `RUNS__MEMORY__URL`), a blob store
(a directory by default; a GCS bucket in production) and a checkout of `agent-contracts`
next to this one (a path dependency).

```bash
make install                 # uv sync, trellis-contracts from ../agent-contracts
make migrate                 # alembic upgrade head (RUNS__DATABASE__URL)
uv run agent-runs            # the API on RUNS__SERVICE__PORT
uv run agent-runs-ticker     # the ticker
```

Both processes refuse to start against a database that is not at the head revision: migrate
first.

With Docker, `make up` starts all of it with `docker compose`: PostgreSQL 17 (published on
`RUNS_DB_PORT`, 5442), a one-shot `migrate`, the API (on `RUNS_PORT`, 8090) and the ticker,
sharing a blob volume; the API reaches the Memory Service on the host
(`host.docker.internal:8080`). `make image` builds the one image alone (it needs
`../agent-contracts` as the `contracts` build context); `make down` stops everything and
drops the volumes.

### Configuration

Service settings are `RUNS__*` environment variables (or a `.env` file), each also
documented in [.env.example](.env.example); every other number is a named constant in
`src/agent_runs/config/constants.py`.

| Variable | Default | Read by | Meaning |
|---|---|---|---|
| `RUNS__SERVICE__HOST` | `0.0.0.0` | API | where uvicorn binds |
| `RUNS__SERVICE__PORT` | `8090` | API | the API's port |
| `RUNS__SERVICE__ENVIRONMENT` | `dev` | API, ticker | `dev` also accepts and delivers plain-`http` webhook URLs, and private ones unless `RUNS__WEBHOOKS__ALLOW_PRIVATE_TARGETS` says otherwise; anything else only `https` to public hosts. Only `dev` and `test` may use the filesystem blob store |
| `RUNS__SERVICE__WORKERS` | one per CPU, 1–8 | API | uvicorn worker processes; each keeps its own key cache and metrics (rate-limit budgets are shared, in the database) |
| `RUNS__SERVICE__GRACEFUL_SHUTDOWN_SECONDS` | `20` | API | on `SIGTERM`, how long requests in flight may finish before they are closed |
| `RUNS__SERVICE__MAX_BODY_BYTES` | `4194304` | API | a JSON body past this is `413`, counted as it arrives (chunked too); artifacts have their own 50 MiB |
| `RUNS__SERVICE__MAX_PAYLOAD_BYTES` | `1048576` | API | a run's `input` or `output` past this (compact JSON) is `413` |
| `RUNS__RATE_LIMIT__PER_MINUTE`, `RUNS__RATE_LIMIT__BURST` | `3000`, `500` | API | each tenant's request budget, one per tenant in PostgreSQL, shared by every worker of every replica (`429` + `Retry-After` when empty); `0` per minute turns it off |
| `RUNS__MEMORY__URL` | `MEMORY_URL`, else `http://localhost:8080` | API | the Memory Service; keys are introspected at `{url}/v1/keys/self`. `MEMORY_URL` is the platform-wide name; this one wins when both are set |
| `RUNS__DATABASE__URL` | `postgresql+psycopg://memory:memory@localhost:5432/agent_runs` | API, ticker, alembic | the database |
| `RUNS__DATABASE__POOL_SIZE` | `10` | API, ticker | connections per process (per worker) |
| `RUNS__DATABASE__MAX_OVERFLOW` | `10` | API, ticker | connections opened past the pool under a burst |
| `RUNS__DATABASE__POOL_TIMEOUT_SECONDS` | `5` | API, ticker | wait for a pooled connection before answering `503` |
| `RUNS__DATABASE__POOL_RECYCLE_SECONDS` | `300` | API, ticker | a pooled connection older than this is replaced, not reused |
| `RUNS__DATABASE__POOL_PRE_PING` | `true` | API, ticker | test a pooled connection on checkout; a dead one is replaced, not handed to a request |
| `RUNS__DATABASE__CONNECT_TIMEOUT_SECONDS` | `5` | API, ticker | opening a connection to PostgreSQL |
| `RUNS__DATABASE__STATEMENT_TIMEOUT_MS` | `15000` | API, ticker | PostgreSQL cancels a statement past this (`503` here); `0` is no limit |
| `RUNS__BLOB__PROVIDER` | `filesystem` | API, ticker | `filesystem` (dev and test only) or `gcs` |
| `RUNS__BLOB__ROOT` | `.blob` | API, ticker | the filesystem store's directory (shared by both processes) |
| `RUNS__BLOB__BUCKET` | unset | API, ticker | the GCS bucket; required with `gcs` (Application Default Credentials; `STORAGE_EMULATOR_HOST` points the client at an emulator) |
| `RUNS__RUNS__MAX_RUN_SECONDS` | unset | API, ticker | the most working time any run may take (time `RUNNING`, across attempts); a run's own `timeout_seconds` may only be shorter. Unset: no platform maximum |
| `RUNS__RUNS__CONCURRENCY_PER_KEY` | `1` | API | how many of a tenant's runs sharing a `concurrency_key` may be `RUNNING` at once |
| `RUNS__RUNS__MAX_RUNNING_PER_TENANT` | unset | API | the most runs one tenant's workers may hold at once; a claim past it is `204`. Unset: no cap (fair share between tenants needs no setting) |
| `RUNS__RUNS__RETENTION_DAYS` | unset | ticker | days an ended run is kept, with its resolutions and events; unset, forever |
| `RUNS__WEBHOOKS__ALLOW_PRIVATE_TARGETS` | unset (`true` in `dev` only) | API, ticker | deliver to hosts that resolve to private, loopback or link-local addresses |
| `RUNS__WEBHOOKS__SECRET_OVERLAP_HOURS` | `24` | API | after a rotation, how long the old secret signs deliveries too; `0` the new one only |
| `RUNS__WEBHOOKS__DEAD_RETENTION_DAYS` | `7` | ticker | how long a dead delivery is kept to be redelivered |
| `RUNS__TICKER__HEARTBEAT_FILE` | unset | ticker, probe | the liveness file; unset, a per-process file in the temp directory and nothing for the probe to read |
| `RUNS__TICKER__METRICS_PORT` | unset | ticker | serve the ticker's Prometheus metrics on this port |
| `RUNS__OBSERVABILITY__LOG_LEVEL` | `INFO` | API, ticker | log level |
| `RUNS__OBSERVABILITY__LOG_JSON` | `true` | API, ticker | `false` for the console renderer |
| `RUNS_PORT`, `RUNS_DB_PORT` | `8090`, `5442` | docker compose | host ports of the API and PostgreSQL |
| `RUNS_TEST_ADMIN_URL`, `RUNS_TEST_DB`, `RUNS_TEST_GCS` | see `.env.example` | the test suite | the admin connection, the test database's name, and the opt-in fake-GCS tests |

## Develop

```bash
make lint typecheck test
make coverage                # the service's suite with line and branch coverage, failing under 95%
make sdk                     # the SDK's suite, failing under 100% line and branch coverage
make openapi                 # rewrite docs/openapi.json after changing a route or a model
```

The suite runs against the local PostgreSQL in its own database (`agent_runs_tests`, dropped
and recreated per run) and skips with a reason when there is none. The key registry is a
fake (`tests/conftest.py`, `FakeMemory`, a tiny ASGI app answering `/v1/keys/self`); the
blob store is a filesystem one per test, and the GCS adapter also runs over an in-memory
stand-in for the Google client. `RUNS_TEST_GCS=1` also runs the GCS adapter and an
end-to-end artifact test against a fake GCS server (`fsouza/fake-gcs-server`, started in
Docker on a free port and removed afterwards; needs Docker); without it those six tests
skip. Migrations live in `alembic/versions`; a test checks they build exactly the schema the
code maps.

CI (`.github/workflows/ci.yml`) runs on pushes to `main` and on pull requests: ruff, pyright
(the service and the SDK), the migrations up, down to base and up again, the suite with the
coverage floor, the SDK's suite at 100% line and branch coverage, and a diff
of `docs/openapi.json` against the document the code generates, against PostgreSQL 16 with
`agent-contracts` checked out beside this repository. The OpenAPI document embeds the
contracts' models, so it is regenerated whenever `agent-contracts` changes them.
