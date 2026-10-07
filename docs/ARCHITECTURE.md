# agent-runs architecture

How the service is put together, drawn from the code: every name below is a module, class,
method, route, column or constant in this repository (or in `trellis-contracts`, where
marked). The wire contract is [api.md](api.md); this page is the inside.

- [Among the five Trellis repos](#among-the-five-trellis-repos)
- [Components](#components)
- [The run lifecycle](#the-run-lifecycle)
- Key flows, as sequence diagrams:
  - [Start, interrupt, resume](#start-interrupt-resume) (the whole journey)
  - [Claim, lease and heartbeat](#claim-lease-and-heartbeat) (and a lapsed lease)
  - [Pause and resume, and the answer check](#pause-and-resume-and-the-answer-check)
  - [Cancel and release](#cancel-and-release)
  - [A scheduled run firing](#a-scheduled-run-firing)
  - [Webhook delivery and dead letters](#webhook-delivery-and-dead-letters)
  - [Run events and the SSE stream](#run-events-and-the-sse-stream)
- [The ticker](#the-ticker)
- [The tables](#the-tables)
- [Code map](#code-map)

The decisions behind the shape are the [ADRs](adr/README.md). Every flow below also runs as
an example against the real app, in process ([examples/](../examples/README.md)).

## Among the five Trellis repos

Trellis is five repos. This one keeps what must outlive a process: runs, the queue, the
inbox of paused runs, schedules and webhooks. It never executes an agent.

```mermaid
flowchart LR
  subgraph exec["Executors (they run agents)"]
    H["agent-harness<br/>Way 1: h.wrap(agent), h.worker(...)"]
    F["your framework<br/>Way 2: LangGraph, OpenAI Agents, ..."]
  end
  SDK["trellis.runs (sdk/python)<br/>RunsClient · Worker · webhooks"]
  R["agent-runs<br/>(this repo)"]
  C[["agent-contracts<br/>RunStart · RunRecord · Interrupt<br/>InterruptResolution · Schedule"]]
  M["agent-memory-service<br/>the key registry"]
  B["bifrost-sdk<br/>(models, MCP tools)"]
  UI["inbox UI, webhook receivers"]

  H --> SDK
  F --> SDK
  SDK -- "HTTP /v1" --> R
  R -- "GET /v1/keys/self" --> M
  R -. "types" .-> C
  SDK -. "types" .-> C
  H -. "model calls" .-> B
  R -- "signed webhooks" --> UI
  UI -- "inbox, resume" --> SDK
```

| Repo | Relation to agent-runs |
|---|---|
| [agent-harness](https://github.com/amitmohapatra/agent-harness) | with `RUNS_URL` set, its run store is `trellis.runs.RunsClient`: it records, queues, pauses, resumes and schedules runs here, and `h.worker` is a `trellis.runs.Worker` |
| [agent-contracts](https://github.com/amitmohapatra/agent-contracts) | the records on the wire: the API's bodies are its models (`RunCreate` subclasses `RunStart`), and `RunStatus.can_become` is the one transition check; pinned `>=0.6.1,<0.7` |
| [agent-memory-service](https://github.com/amitmohapatra/agent-memory-service) | the one key registry: every `X-API-Key` is introspected there (`GET /v1/keys/self`) |
| [bifrost-sdk](https://github.com/amitmohapatra/bifrost-sdk) | none: agent-runs makes no model calls |
| **agent-runs** (this repo) | the service, and its SDK `trellis.runs` (`sdk/python`) |

## Components

Two processes from one image share one PostgreSQL database and one blob store. Neither
executes an agent: a harness or any other framework does, through the Python SDK
`trellis.runs` (`sdk/python`, pip `trellis-runs`), and records here what happened. The SDK is
a uv workspace member of this repository: its `RunsClient` speaks every route, its `Worker`
is the claim loop, and its `webhooks.sign` is the one implementation of the delivery
signature, which the ticker signs with and a receiver checks with `verify_signature`.

```mermaid
flowchart LR
  subgraph clients["Clients (X-API-Key)"]
    harness["agent-harness, or any framework<br/>in-process runs and workers"]
    ui["Inbox UI / its backend"]
    sdk["trellis.runs (sdk/python)<br/>RunsClient · Worker<br/>webhooks: sign · verify_signature"]
  end

  subgraph runs["agent-runs"]
    subgraph api["API process: agent-runs (FastAPI, api/app.py)"]
      deps["api/deps.py<br/>caller() → Caller · claimer() → Claimer<br/>rate budget (api/ratelimit.py)"]
      mw["api/middleware.py<br/>request id · metrics · body cap · gzip"]
      routers["routers: runs · artifacts<br/>schedules · webhooks<br/>ops: /health/*, /metrics"]
    end
    subgraph tick["Ticker process: agent-runs-ticker (ticker.py)"]
      ticker["Ticker.tick() every TICK_SECONDS<br/>fire · time out (deadline, working time)<br/>requeue or cancel · escalate<br/>send webhooks · drop dead · purge artifacts<br/>purge ended runs (retention)"]
      sender["WebhookSender<br/>(webhooks.py, egress.py)"]
    end
    keys["KeyRegistry (keys.py)<br/>TTL cache of key answers"]
    stores["store/: RunStore · ScheduleStore<br/>WebhookStore · ArtifactStore<br/>firing.py: Firing"]
    domain["domain/: requests, errors,<br/>cadence, webhook events"]
    blobport["blob/: BlobStore port<br/>FilesystemBlobStore · GCSBlobStore"]
  end

  contracts[["trellis-contracts<br/>RunRecord · RunStart · Interrupt<br/>InterruptResolution · Schedule<br/>ScheduleSpec · ArtifactRef · RunStatus"]]
  memory["Memory Service<br/>GET /v1/keys/self"]
  pg[("PostgreSQL<br/>agent_runs · run_resolutions<br/>run_artifacts · agent_schedules<br/>webhooks · webhook_deliveries")]
  blob[("Blob storage<br/>RUNS__BLOB__ROOT or GCS bucket")]
  receivers["Webhook receivers<br/>(tenant subscriptions)"]
  migrate["migrate job<br/>alembic upgrade head"]

  harness --> sdk
  ui --> sdk
  sdk -- "HTTP /v1/runs, /v1/schedules,<br/>/v1/artifacts, /v1/webhooks" --> routers
  mw --> routers
  routers --> deps --> keys -- "introspect key" --> memory
  routers --> stores
  routers --> blobport
  ticker --> stores
  ticker --> blobport
  ticker --> sender -- "POST signed event" --> receivers
  sender -. "sign" .-> sdk
  receivers -. "verify_signature" .-> sdk
  stores --> pg
  blobport --> blob
  migrate --> pg
  domain -. "types" .-> contracts
  stores -. "types" .-> contracts
  sdk -. "types" .-> contracts
```

| Component | Where | What it owns |
|---|---|---|
| API | `api/app.py` `create_app`, `api/routers/*` | the HTTP surface |
| OpenAPI | `api/openapi.py` `custom_openapi`, `api/examples.py`, `tools/export_openapi.py` | the document every route shares (metadata, `<tag>.<function>` ids, the `ApiKeyAuth` scheme, a problem on every error status, standard headers); `docs/openapi.json` is it committed, and `tests/test_openapi.py` fails when they differ |
| Errors | `api/errors.py` `install_error_handlers`, `Problem`; `domain/errors.py` `ErrorCode` | every failure as an RFC 9457 problem: `ServiceError` subclasses with their status and `code`, FastAPI's validation and HTTP errors, a database that went away (`503`, `Retry-After`), anything else (`500`, no internals) |
| Middleware | `api/middleware.py` `RequestContextMiddleware`, `BodyLimitMiddleware`, `CompressionMiddleware` | `X-Request-ID` in, out and in every problem; request metrics by route template; the JSON body cap counted as bytes arrive; gzip, except artifact bytes |
| Rate limit | `api/ratelimit.py` `TenantRateLimiter`, `api/deps.py` `_within_budget` | one budget per tenant in PostgreSQL (`rate_limit_buckets`, a GCRA bucket moved by one upsert under the database's clock, on its own connection), shared by every worker of every replica; `429` with `Retry-After`, `X-RateLimit-*` on every counted response |
| Metrics | `observability/metrics.py` | one Prometheus registry per process: the API's `/metrics`, the ticker's `RUNS__TICKER__METRICS_PORT` |
| Caller resolution | `api/deps.py` `caller`, `Caller`; `claimer`, `Claimer` | `X-API-Key` (and `X-Trellis-Tenant` for a platform key) → the tenant and principal of the request; `require_tenant`, `require_may_act_for`. A claim (`claimer`) also takes a platform key with no tenant: it claims from every tenant's queue |
| Claim order | `store/runs.py` `RunStore.claim`, `_next_queued`, `_tenant_has_room`, `_key_has_room` | the next queued run: the tenant whose workers hold fewest runs of the claimed agents first (fair share), then `priority`, then `queued_at`, among runs with room under their `concurrency_key` (`RUNS__RUNS__CONCURRENCY_PER_KEY`) and their tenant's cap (`RUNS__RUNS__MAX_RUNNING_PER_TENANT`); the room recounted under `pg_try_advisory_xact_lock` on the key or tenant, a key another claim is counting passed over like a locked row |
| Run events | `store/runs.py` `RunStore.append_events`, `events`; `api/routers/runs.py` `stream_events`, `_follow` | each run's event log (`run_events`): appends fenced like a heartbeat, a position per event under the run's row lock, repeats (`attempt`, `sequence`) dropped; reads by position, and a server-sent event stream that polls in short transactions of its own (`EVENT_POLL_SECONDS`), keeps quiet connections open (`EVENT_KEEPALIVE_SECONDS`), ends with `end` once the run has, and lasts at most `EVENT_STREAM_SECONDS` |
| Key registry | `keys.py` `KeyRegistry`, `KeyInfo` | introspection at the Memory Service, cached 60 s (refusals 10 s), at most 10 000 keys |
| Answering | `answering.py` `require_may_answer`, `require_may_cancel` | who may answer a paused run: an admin or platform key, or one that may act for anyone, answers any run; a key restricted to listed people answers, only as one of them, a run assigned to that person or to nobody. `RunStore.resume` applies it under the row lock, to the assignee now, before writing; then `trellis.runs.answers.answer_problem` checks the answer fits the question. `RunStore.cancel` applies the same rule (as any principal the key may act for) |
| Stores | `store/runs.py`, `store/schedules.py`, `store/webhooks.py`, `store/artifacts.py` | every read and write of one table family; the caller commits |
| Firing | `firing.py` `Firing` | queueing one run for one schedule tick, in the transaction that advances the schedule |
| Ticker | `ticker.py` `Ticker` | the background loop; a `retry.Breaker` stops it hammering a dead database; `heartbeat.py` is its liveness file and probe |
| Retries | `retry.py` `backoff`, `jittered`, `Breaker` | the one retry policy: a capped doubling backoff (webhook deliveries, schedule fires, a lapsed lease's and a retryable error's requeue, the last two jittered) and the breaker |
| Webhook sender | `webhooks.py` `WebhookSender`; `domain/webhooks.py` `Attempt` | one attempt per due outbox row, signed with the SDK's `trellis.runs.webhooks.sign` (with the replaced secret too during a rotation's overlap); how it went (`Attempt`: accepted, worth another, or refused for good) decides whether `WebhookStore.settle` deletes the row, backs it off or keeps it dead |
| Egress guard | `egress.py` `public_addresses`, `pinned`, `require_public` | the SSRF guard: a subscription's host must resolve to public addresses only, checked on create (`422`) and by the sender on every attempt (a refusal is final), unless `Settings.private_webhook_targets`; the sender then connects only to the addresses it checked (`pinned`: the IP in the URL, the host in `Host` and TLS SNI, so the certificate is checked against it), and never follows a redirect |
| SDK | `sdk/python/src/trellis/runs`: `RunsClient`, `Worker`, `webhooks`, `errors`, `models` | the Python client of every route (method names are the operation ids), the framework-neutral worker loop, the delivery signature; `sdk/python/tests` holds it to 100% line and branch coverage and checks it against `docs/openapi.json` |
| Blob port | `blob/port.py` `BlobStore`, `read`; `blob/filesystem.py`, `blob/gcs.py` | create-only bytes under a key; `put_stream` writes an upload as it arrives (a temporary file, or a bounded spool for GCS), hashing as it goes; `read` verifies SHA-256 and size while streaming |
| Schema | `alembic/versions/*`, `store/tables.py` | migrations are the schema; `tests/test_schema.py` checks the mappings match them; `store/database.py` `connect` refuses a database not at the head revision |
| Engine | `store/database.py` `connect`, `ping`; `config/settings.py` `DatabaseSettings` | one pool per process, its share of the container's connection budget (`pool_plan`), with a pre-ping on checkout, a recycle window, a bounded wait for a connection, a connect timeout and a statement timeout; `ping` is the bounded readiness probe |

## The run lifecycle

The one transition check is `trellis-contracts` `RunStatus.can_become`, called by
`store/runs.py` `_move` under a row lock; a refused transition is `409` and changes nothing.
The arrows are exactly the transitions some route or ticker step makes:
`tests/test_state_machine.py` asserts every route's transition and every refusal, and
`tests/test_queue.py`, `tests/test_deadline.py`, `tests/test_working_time.py`,
`tests/test_retries.py`, `tests/test_cancel.py`, `tests/test_release.py` and
`tests/test_escalation.py` the rest.

```mermaid
stateDiagram-v2
  [*] --> RUNNING: POST /v1/runs (queue false)
  [*] --> QUEUED: POST /v1/runs (queue true), or a schedule fires

  QUEUED --> RUNNING: POST /v1/runs/claim, once available_at passed (lease to worker_id)
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

What each move does to the row (`_move`, `_requeue`, `RunStore.resume`):

- Entering `RUNNING` sets `running_since`; leaving it adds the stretch to `worked_seconds`
  and clears `running_since`, the lease (`lease_owner`, `lease_expires_at`) and
  `cancel_requested_at`. A read adds the stretch going on (`RunRecord.worked_seconds`), and
  `time_out_overworked` ends a run whose working time passed its limit (`timeout_seconds`
  or `RUNS__RUNS__MAX_RUN_SECONDS`, the lesser) `TIMEOUT` (`run_timeout`); `_lease` tells
  the worker the time left (`remaining_seconds`).
- Leaving `PAUSED` clears `awaiting`, `assignee`, `awaiting_deadline`; leaving `QUEUED`
  clears `available_at`; any ending clears `checkpoint` and starts the run's artifacts'
  retention (`expires_at = now + ARTIFACT_RETENTION`, 7 days).
- A requeue (`_requeue`) may hold the run back: `available_at = now + wait`, which the claim
  respects. A lapsed lease waits `LAPSE_RETRY_BASE` (5 s) doubling per lapse up to
  `LAPSE_RETRY_CAP` (1 min); a retried error `ERROR_RETRY_BASE` (10 s) doubling up to
  `ERROR_RETRY_CAP` (10 min); both jittered (`retry.jittered`). A release and a resume wait
  for nothing.
- `finish` with `ERROR` and a retryable error, of a run that was queued, running and not
  asked to cancel, requeues it instead of ending it (`_retried`), `error_retries + 1`, at
  most `MAX_ERROR_RETRIES` (3); the error that finally stands says how often the run was
  retried (`_counted`).
- `cancel` ends a queued, paused or unleased running run `CANCELLED` at once; a leased one
  gets `cancel_requested_at`, from which every lease is measured (`_lease`), so the run is
  cancelled within one lease: the worker finishes it, or a pause, a release or the lapse
  sweep ends it `CANCELLED` instead of pausing or requeueing it (`_cancelled_instead`).
  `cancel_reason` and `cancelled_by` keep why and who.
- "Was queued" means `queued_at` is set: the run entered the queue at least once (a
  `queue: true` start or a schedule fire), so a worker, not the original process, resumes it.
- `attempt` counts executions: `+1` on a resume that continues the run and on every requeue
  (a lapsed lease, a release, a retried error), never on a claim. `lease_lapses` counts only the lapses (`+1` on each,
  in `requeue_lapsed`), and only it decides when the ticker gives up on a run: review
  rounds are attempts, not crashes.
- Events go to the outbox in the same transaction: `run.paused` on a pause, `run.escalated`
  on an escalation, `run.finished` on any ending (`domain/webhooks.py` `event_of`). A resume
  that continues the run, and a requeue, announce nothing.
- `MAX_LEASE_LAPSES` is 5 (`config/constants.py`): the 5th lapse ends the run `ERROR`.
- A claim takes a run only with room (`RunStore.claim`): fewer `RUNNING` runs of its
  tenant share its `concurrency_key` than `RUNS__RUNS__CONCURRENCY_PER_KEY`, and fewer runs
  of its tenant are leased than `RUNS__RUNS__MAX_RUNNING_PER_TENANT`. A run kept in its
  caller's process takes its key's place while it runs, but is never held back.
- With `RUNS__RUNS__RETENTION_DAYS` set, the ticker deletes runs that ended before it
  (`RunStore.purge_ended`, `ix_runs_ended`) with their `run_resolutions` and `run_events`; a
  run whose artifacts are still kept waits for them.
- `checkpoint` is written by a pause and, as progress, by the lease holder's heartbeat or
  release; a requeue keeps it, so the next attempt's claim resumes from it; only an ending
  clears it.
- A pause, finish or release records who made it (`settled_by`, the `worker_id` or null),
  as does a finish that requeued the run for a retry; any other move clears it. A repeat of that call (the same caller, to the same status, on the same
  interrupt for a pause) is answered with the stored run and moves nothing (`_settled`),
  so a worker that lost the answer may retry. Otherwise a fenced write (`worker_id`) on a
  run whose lease the worker no longer holds is `LeaseLost` (`409 LEASE_LOST`, `_fence`),
  checked before the transition.
- A resume to a run no longer waiting on its interrupt is a repeat when the resolution kept
  for that interrupt (`run_resolutions`, read under the row lock) is the very same
  (`_answered_by`; `resolved_at` is set once per answer): the run is answered as it is and
  nothing moves. Any other is `409`. An answer that does not fit the question
  (`trellis.runs.answers.answer_problem`) is `422`, and a pause whose `expects` is not a
  JSON Schema (`schema_problem`) too, both before anything is written.

## Start, interrupt, resume

A durable run: queued, claimed by a worker, paused for a person with a large payload in an
artifact, answered from an inbox, claimed again and finished. Every request is
authenticated first (the key is introspected at the Memory Service, or taken from the
cache); that step is drawn once.

```mermaid
sequenceDiagram
  autonumber
  participant W as Worker (trellis.runs Worker)
  participant A as agent-runs API
  participant M as Memory Service
  participant DB as PostgreSQL
  participant B as Blob store
  participant T as Ticker
  participant R as Webhook receiver
  participant U as Inbox UI

  W->>A: POST /v1/runs {queue: true}
  A->>M: GET /v1/keys/self (cached 60 s)
  M-->>A: KeyInfo (tenant, principal, may_act_as, role)
  A->>DB: INSERT agent_runs QUEUED ON CONFLICT DO NOTHING
  A-->>W: 201 RunRecord (QUEUED, attempt 1)

  W->>A: POST /v1/runs/claim {worker_id, agent_ids, lease_seconds}
  A->>DB: SELECT … FOR UPDATE SKIP LOCKED: fewest held by its tenant, highest priority, oldest queued_at, with room (key, tenant cap)
  A-->>W: 200 Claimed {run RUNNING, lease}
  loop every third of the lease
    W->>A: POST /v1/runs/{id}/heartbeat {worker_id, checkpoint (progress, optional)}
    A-->>W: 200 Lease (409 LEASE_LOST = stop)
  end

  W->>A: POST /v1/runs/{id}/artifacts?worker_id= (bytes)
  A->>B: put artifacts/{artifact_id} (create-only)
  A->>DB: INSERT run_artifacts
  A-->>W: 201 ArtifactRef
  W->>A: POST /v1/runs/{id}/pause?worker_id= {interrupt (payload_ref), checkpoint}
  A->>DB: RUNNING → PAUSED, awaiting, assignee, deadline, checkpoint, then outbox run.paused
  A-->>W: 200 RunRecord (PAUSED, lease released)

  T->>DB: WebhookStore.claim_due
  T->>R: POST run.paused (X-Trellis-Signature)
  R-->>T: 2xx
  T->>DB: settle (delete the outbox row)

  U->>A: GET /v1/runs?status=PAUSED&assignee=role:procurement
  A-->>U: 200 [RunSummary]
  U->>A: GET /v1/artifacts/{artifact_id}
  A->>B: read, verified against the SHA-256
  A-->>U: 200 bytes
  U->>A: POST /v1/runs/{id}/resume {InterruptResolution APPROVE, reviewer}
  A->>DB: SELECT … FOR UPDATE: still PAUSED on this interrupt (else the same resolution kept: 200 as is, another: 409), may this key answer its assignee now (403 if not), does the answer fit (422 if not)
  A->>DB: INSERT run_resolutions, then last_resolution, then PAUSED → QUEUED, attempt 2
  A-->>U: 200 RunRecord (QUEUED)

  W->>A: POST /v1/runs/claim
  A-->>W: 200 Claimed {run with checkpoint and last_resolution, lease}
  W->>A: POST /v1/runs/{id}/finish?worker_id= {status: SUCCESS, output}
  A->>DB: RUNNING → SUCCESS, checkpoint cleared, artifacts expire in 7 days, then outbox run.finished
  A-->>W: 200 RunRecord (SUCCESS)
```

An in-process run is the same without the queue: `POST /v1/runs` records it `RUNNING`, the
pause takes no `worker_id`, and a resume that continues it moves it to `RUNNING` for the
process that resumes it.

## Claim, lease and heartbeat

A worker leases one queued run at a time and keeps the lease alive; every write it makes
names its `worker_id` and is fenced to the lease holder. A worker that stops heartbeating
loses the run to the queue, with its checkpoint, after a backoff.

```mermaid
sequenceDiagram
  autonumber
  participant W1 as worker-a (trellis.runs Worker)
  participant A as agent-runs API
  participant DB as PostgreSQL
  participant T as Ticker
  participant W2 as worker-b

  W1->>A: POST /v1/runs/claim {worker_id, agent_ids, lease_seconds (5 to 3600, default 60)}
  A->>DB: _next_queued: FOR UPDATE SKIP LOCKED, available_at passed,<br/>tenant with fewest held first, then priority, then oldest queued_at,<br/>room under concurrency_key and the tenant cap (advisory lock while counting)
  alt nothing may run now
    A-->>W1: 204
  else a run
    A->>DB: QUEUED → RUNNING, lease_owner, lease_expires_at, running_since
    A-->>W1: 200 Claimed {run (checkpoint, last_resolution), lease (remaining_seconds, cancel_requested)}
  end
  loop every third of the lease
    W1->>A: POST /v1/runs/{id}/heartbeat {worker_id, lease_seconds, checkpoint?}
    A->>DB: FOR UPDATE: RUNNING and lease_owner = worker_id (_fence), extend, save checkpoint
    A-->>W1: 200 Lease {expires_at, remaining_seconds, cancel_requested}
  end
  Note over W1: worker-a dies: no more heartbeats
  T->>DB: requeue_lapsed: RUNNING with lease_expires_at past
  alt lease_lapses + 1 < MAX_LEASE_LAPSES (5)
    T->>DB: RUNNING → QUEUED, attempt + 1, checkpoint kept,<br/>available_at = now + 5 s doubling per lapse (at most 1 min, jittered)
  else the 5th lapse
    T->>DB: RUNNING → ERROR (lease_expired), outbox run.finished
  end
  W1->>A: (back from a pause) POST /v1/runs/{id}/finish?worker_id=worker-a
  A-->>W1: 409 LEASE_LOST: stop working the run (the SDK's Worker cancels its handler)
  W2->>A: POST /v1/runs/claim, once available_at has passed
  A-->>W2: 200 Claimed {run, attempt 2, the checkpoint worker-a saved}
```

The same fence ends a run whose time is up. Past the run's own `deadline` the ticker ends it
`TIMEOUT` (`run_deadline`); past its working-time limit, `TIMEOUT` (`run_timeout`). The worker
learns from its next heartbeat or write (`409 LEASE_LOST`), and every lease says the working
time left (`remaining_seconds`) so a worker can stop in time.
[examples/02_queue_claim_heartbeat.py](../examples/02_queue_claim_heartbeat.py) runs this flow.

## Pause and resume, and the answer check

A run pauses on one `Interrupt`; a person answers it from an inbox. The answer is checked
under the run's row lock, in this order, before anything is written. A question that
nobody answers in time is escalated once, or times out.

```mermaid
sequenceDiagram
  autonumber
  participant X as Executor (worker or in-process)
  participant A as agent-runs API
  participant DB as PostgreSQL
  participant U as Inbox UI (a person's key)
  participant T as Ticker

  X->>A: POST /v1/runs/{id}/pause?worker_id= {interrupt, checkpoint}
  A->>A: schema_problem(interrupt.expects): not a JSON Schema → 422, the run keeps running
  A->>DB: FOR UPDATE, fence, RUNNING → PAUSED: awaiting, assignee, awaiting_deadline,<br/>checkpoint, lease released, outbox run.paused
  A-->>X: 200 RunRecord (PAUSED)
  U->>A: GET /v1/runs?status=PAUSED&assignee=user:priya
  A-->>U: 200 [RunSummary with awaiting]
  U->>A: POST /v1/runs/{id}/resume InterruptResolution {decision, answer, reviewer, comment}
  A->>DB: SELECT … FOR UPDATE
  alt not PAUSED on this interrupt any more
    alt the resolution kept for it is this very one (_answered_by)
      A-->>U: 200 the run as it is now: no second answer, event or attempt
    else
      A-->>U: 409 CONFLICT: the question was already answered
    end
  else require_may_answer(key, the assignee now) refuses
    A-->>U: 403 AUTHORIZATION, saying why
  else answer_problem(interrupt, resolution): it does not fit expects or the options
    A-->>U: 422 VALIDATION, saying what does not fit, the run keeps waiting
  else accepted
    A->>DB: INSERT run_resolutions, last_resolution,<br/>PAUSED → RUNNING (in process) or QUEUED (was queued), attempt + 1<br/>(CANCEL: → CANCELLED, outbox run.finished)
    A-->>U: 200 RunRecord
  end
  opt nobody answered by the interrupt's deadline
    T->>DB: escalate: assignee = escalate_to (once), outbox run.escalated,<br/>or, with no escalate_to, PAUSED → TIMEOUT, outbox run.finished
  end
```

Who may answer is the rule in [the README](../README.md#who-may-answer-a-paused-run) and
[api.md](api.md#who-may-answer-a-paused-run); the answer check is `trellis.runs.answers`, the
same one the harness makes for a run it keeps in its own process.
[examples/03_pause_resume_answer_check.py](../examples/03_pause_resume_answer_check.py) runs
every branch.

## Cancel and release

A cancel ends a run that nobody holds at once and asks a worker that holds one to stop. A
release is a worker letting go of a run because it is stopping itself.

```mermaid
sequenceDiagram
  autonumber
  participant C as Caller (UI, harness)
  participant A as agent-runs API
  participant DB as PostgreSQL
  participant W as Worker (holds the run)
  participant T as Ticker

  C->>A: POST /v1/runs/{id}/cancel {reason}
  A->>A: require_may_cancel: a key may cancel a run it could answer
  alt QUEUED, PAUSED, or RUNNING with no lease (in process)
    A->>DB: → CANCELLED, cancel_reason, cancelled_by, outbox run.finished
    A-->>C: 200 RunRecord (CANCELLED)
  else RUNNING under a worker's lease
    A->>DB: cancel_requested_at = now
    A-->>C: 200 RunRecord (RUNNING, the cancel asked)
    W->>A: POST /v1/runs/{id}/heartbeat
    A-->>W: 200 Lease {cancel_requested: true}
    W->>W: the SDK's Worker cancels the handler
    W->>A: POST /v1/runs/{id}/finish?worker_id= {status: CANCELLED}
    A-->>W: 200 RunRecord (CANCELLED)
    Note over A,T: a pause or release of that run ends it CANCELLED instead,<br/>and if the worker is gone the ticker does when the lease runs out
  end

  Note over W: SIGTERM: Worker.stop() claims no more and lets handlers run for 25 s
  W->>A: POST /v1/runs/{id}/release {worker_id, checkpoint?}
  A->>DB: fence, RUNNING → QUEUED, attempt + 1, no lapse counted, available at once
  A-->>W: 200 RunRecord (QUEUED): the next claim, by any worker, resumes from the checkpoint
```

[examples/04_cancel_and_release.py](../examples/04_cancel_and_release.py) runs both.

## A scheduled run firing

A schedule is created once (an upsert on its identity) and the ticker fires it from then
on, as its `on_behalf_of`, while nobody is present. The run it queues is built from the
stored schedule and nothing else, and carries everything a started run can: the schedule's
`timeout_seconds`, `agent_version`, `priority` and `concurrency_key`, and its `metadata`
under the fire's own keys, which win on conflict (`firing.py`; contracts ADR 0007). `POST /v1/schedules/{id}/fire` takes the
same path (`Firing.fire`) for one schedule, on demand.

```mermaid
sequenceDiagram
  autonumber
  participant O as Owner (harness or UI)
  participant A as agent-runs API
  participant T as Ticker
  participant DB as PostgreSQL
  participant W as Worker

  O->>A: POST /v1/schedules ScheduleSpec (cadence, timezone, on_behalf_of, input)
  A->>DB: INSERT agent_schedules ON CONFLICT (identity) DO NOTHING
  A-->>O: 201 Schedule armed at next_fire_at (200 with the existing one)

  loop every TICK_SECONDS (5 s), up to SWEEP_BATCH fires per tick
    T->>DB: ScheduleStore.claim_due: enabled, next_fire_at ≤ now,<br/>retry_after unset or passed, FOR UPDATE SKIP LOCKED
    alt nothing due
      DB-->>T: no row, the fire step ends
    else a schedule is due
      DB-->>T: the schedule, locked for this transaction
      T->>DB: SAVEPOINT, then INSERT agent_runs QUEUED, idempotency_key "schedule_id@fire_time",<br/>on_behalf_of, input, timeout_seconds, agent_version, priority, concurrency_key copied,<br/>metadata = the schedule's, then schedule_id, schedule_name, fire_time, created_by (these win)
      alt the run is queued (or already was for this tick)
        T->>DB: record_success: last_fired_at, last_run_id, failures 0,<br/>next_fire_at = next occurrence after max(fire_time, now)
      else the database refuses it (DBAPIError)
        T->>DB: ROLLBACK TO SAVEPOINT, then record_failure: consecutive_failures + 1, last_error
        Note over T,DB: permanent error, or MAX_CONSECUTIVE_FAILURES (3): enabled = false<br/>retryable: retry_after = now + 5 min doubling, at most 1 h
      end
      T->>DB: COMMIT
    end
  end

  W->>A: POST /v1/runs/claim
  A-->>W: 200 Claimed (metadata: the schedule's, then schedule_id, schedule_name, fire_time, created_by)
```

Two tickers on one tick both see the schedule; `SKIP LOCKED` gives it to one, and the
idempotency key would make a repeat find the same run anyway
(`tests/test_ticker.py::test_two_tickers_on_one_tick_queue_one_run`). `next_fire_at` only
moves forward past now, so a long outage fires each schedule once, not once per missed tick.

## Webhook delivery and dead letters

A run change writes its event to an outbox in the same transaction, one row per subscription
of the tenant that wants it, and the ticker delivers it at least once. A delivery the
receiver refuses for good, or one that used its attempts, is kept dead to be redelivered.

```mermaid
sequenceDiagram
  autonumber
  participant X as Executor or person
  participant A as agent-runs API
  participant DB as PostgreSQL
  participant T as Ticker (WebhookSender)
  participant R as Receiver
  participant O as Operator (a tenant key)

  O->>A: POST /v1/webhooks {url, events}
  A-->>O: 201 WebhookCreated (the secret, shown once)
  X->>A: pause, finish, cancel, resume CANCEL, ...
  A->>DB: the run change + INSERT webhook_deliveries (outbox), one transaction
  loop every tick (5 s)
    T->>DB: WebhookStore.claim_due (FOR UPDATE SKIP LOCKED, leased for 20 s)
    T->>T: https only (http in dev), the host resolved now, every address public,<br/>unless private targets are allowed (egress.py)
    T->>R: POST, connected only to a checked address (Host, SNI, cert name the host)<br/>X-Trellis-Signature: t=..,v1=<new>[,v1=<old> during a rotation]
    alt 2xx
      T->>DB: settle: delete the row
    else 408, 429, 5xx or unreachable, attempts left (7 in all)
      T->>DB: next_attempt_at = now + 15 s doubling (at most 10 min)
    else any other answer (a 3xx too), a private address, or the 7th attempt
      T->>DB: dead: dead_at, last_error (kept RUNS__WEBHOOKS__DEAD_RETENTION_DAYS, 7)
    end
  end
  R->>R: verify_signature(secret, header, raw body), drop repeats by event_id
  O->>A: GET /v1/webhooks/deliveries?state=dead
  A-->>O: 200 [DeliveryRecord with last_error]
  O->>A: POST /v1/webhooks/deliveries/{id}/redeliver
  A->>DB: pending again, all its attempts ahead of it (409 if still owed)
  O->>A: POST /v1/webhooks/{id}/rotate-secret
  A-->>O: 200 the new secret, once: both sign for RUNS__WEBHOOKS__SECRET_OVERLAP_HOURS (24)
```

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

The payload, headers and events are in [api.md](api.md#events-and-delivery);
[examples/07_webhooks_and_dead_letters.py](../examples/07_webhooks_and_dead_letters.py) runs
a delivery, a dead letter, a redelivery and a rotation.

## Run events and the SSE stream

The executor appends a run's `RunEvent`s while it runs; any replica serves them, by position
or as server-sent events, so a UI that reconnects to another replica misses nothing.

```mermaid
sequenceDiagram
  autonumber
  participant X as Executor (worker or in-process)
  participant A1 as agent-runs replica 1
  participant DB as PostgreSQL
  participant A2 as agent-runs replica 2
  participant UI as UI (SDK stream_events)

  X->>A1: POST /v1/runs/{id}/events?worker_id= {events (1 to 500)}
  A1->>DB: FOR UPDATE: RUNNING, fenced like a heartbeat,<br/>a position per event, a repeat (attempt, sequence) dropped
  A1-->>X: 200 {appended, position}
  UI->>A2: GET /v1/runs/{id}/events/stream (Last-Event-ID: last position seen)
  loop every EVENT_POLL_SECONDS (0.5 s), in short transactions
    A2->>DB: events past the position
    A2-->>UI: id: <position>, data: RunEventEntry
    A2-->>UI: a keepalive comment after EVENT_KEEPALIVE_SECONDS (15 s) of quiet
  end
  X->>A1: POST /v1/runs/{id}/finish
  A2->>DB: the run has ended and every event was sent
  A2-->>UI: event: end
  Note over UI,A2: a stream lasts at most EVENT_STREAM_SECONDS (300 s),<br/>the SDK reopens from the last position, on any replica
  UI->>A2: GET /v1/runs/{id}/events?after=&limit= (the same log, by page)
```

[examples/08_events_and_sse.py](../examples/08_events_and_sse.py) appends, pages and follows
a run's events.

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

## The tables

Eight tables, built by the nineteen Alembic revisions in `alembic/versions` (head
`9d5e0f1a2b3c`) and mapped in `store/tables.py`. Solid lines are foreign keys; the dotted
line is the logical link a schedule fire leaves (no foreign key: a run outlives the schedule
that fired it).

```mermaid
erDiagram
  agent_runs ||--o{ run_resolutions : "answered interrupts"
  agent_runs ||--o{ run_artifacts : "artifacts"
  agent_runs ||--o{ run_events : "event log (ON DELETE CASCADE)"
  webhooks ||--o{ webhook_deliveries : "outbox (ON DELETE CASCADE)"
  agent_schedules |o..o{ agent_runs : "fires (last_run_id, run metadata.schedule_id)"

  agent_runs {
    varchar run_id PK
    varchar tenant_id UK "uq_runs_tenant_idempotency with idempotency_key"
    varchar agent_id
    varchar status "RunStatus"
    varchar parent_run_id
    varchar thread_id
    varchar user_id
    varchar workspace_id
    varchar on_behalf_of
    jsonb input
    jsonb output
    jsonb error "AgentError"
    jsonb awaiting "the Interrupt a PAUSED run waits on"
    jsonb last_resolution "the latest InterruptResolution"
    jsonb checkpoint "executor resume state, cleared on ending"
    varchar assignee "from awaiting, for the inbox"
    timestamptz awaiting_deadline "from awaiting, for escalation"
    int attempt "executions: resumes and requeues add one"
    int lease_lapses "only lapsed leases; MAX_LEASE_LAPSES fails the run"
    int error_retries "requeues after a retryable error; MAX_ERROR_RETRIES"
    timestamptz deadline "RunStart.deadline, for the deadline sweep"
    float timeout_seconds "RunStart.timeout_seconds: the working-time limit"
    float worked_seconds "RUNNING stretches that ended"
    timestamptz running_since "set exactly while RUNNING"
    varchar agent_version "RunStart.agent_version"
    int priority "RunStart.priority: claim order, higher first"
    varchar concurrency_key "RunStart.concurrency_key: runs sharing it run a few at a time"
    varchar idempotency_key UK
    jsonb run_metadata
    timestamptz queued_at "set once the run entered the queue"
    timestamptz available_at "a queued run is claimed only after it (a backoff)"
    varchar lease_owner
    timestamptz lease_expires_at
    varchar settled_by "worker_id of the pause, finish or release that made the state"
    timestamptz cancel_requested_at "a held run asked to stop"
    varchar cancel_reason
    varchar cancelled_by "the principal who cancelled"
    timestamptz created_at
    timestamptz updated_at
  }

  run_resolutions {
    varchar resolution_id PK "stable_id(run_id, interrupt_id)"
    varchar run_id FK
    varchar tenant_id
    varchar interrupt_id
    varchar decision "InterruptDecision"
    varchar reviewer
    jsonb interrupt "as asked"
    jsonb resolution "as answered"
    int attempt "the attempt that paused"
    timestamptz resolved_at
    timestamptz recorded_at
  }

  run_events {
    varchar run_id PK, FK
    bigint position PK "1, 2, ...: assigned under the run's row lock"
    varchar tenant_id
    int attempt UK "uq_run_events_sequence with run_id and sequence"
    int sequence UK
    jsonb event "the RunEvent"
    timestamptz recorded_at
  }

  rate_limit_buckets {
    varchar bucket PK "the tenant, or key:<key_id> for a platform claim"
    timestamptz tat "when the budget would be full again (GCRA)"
  }

  run_artifacts {
    varchar artifact_id PK
    varchar run_id FK, UK "uq_run_artifacts_content with checksum"
    varchar tenant_id
    varchar blob_key "artifacts/{artifact_id}"
    varchar mime
    bigint size
    varchar checksum UK "sha256:hex"
    timestamptz created_at
    timestamptz expires_at "run ended + ARTIFACT_RETENTION"
  }

  agent_schedules {
    varchar schedule_id PK
    varchar tenant_id UK "uq_schedules_identity"
    varchar agent_id UK
    varchar name "a label, not unique"
    varchar cadence UK "bucket or cron, at most hourly"
    varchar timezone
    jsonb input
    varchar input_sha256 UK "SHA-256 of canonical JSON input"
    varchar on_behalf_of UK "NOT NULL"
    varchar workspace_id
    varchar created_by "the key's principal"
    boolean enabled
    float timeout_seconds "copied into every fired run"
    varchar agent_version "copied into every fired run"
    int priority "copied into every fired run"
    varchar concurrency_key "copied into every fired run"
    timestamptz next_fire_at
    timestamptz last_fired_at
    varchar last_run_id
    int consecutive_failures
    jsonb last_error
    timestamptz retry_after "backoff gate after a retryable failure"
    jsonb schedule_metadata "under the fire's keys in every fired run"
    timestamptz created_at
    timestamptz updated_at
  }

  webhooks {
    varchar webhook_id PK
    varchar tenant_id
    varchar url
    varchar_array events "run.paused, run.escalated, run.finished"
    varchar secret "whsec_..., shown once"
    varchar previous_secret "the rotated-out secret, signing too until it expires"
    timestamptz previous_secret_expires_at
    varchar created_by
    timestamptz created_at
  }

  webhook_deliveries {
    varchar delivery_id PK "stable_id(event_id, webhook_id)"
    varchar webhook_id FK
    jsonb payload "the event envelope"
    int attempts
    timestamptz next_attempt_at
    text last_error "what the last attempt met"
    timestamptz dead_at "given up on; null while owed"
    timestamptz created_at
  }
```

Every listing is a keyset page (`store/paging.py`): ordered by `created_at` (or
`recorded_at`) and the id, fetched one row past the page, the next page starting strictly
after the last row returned. The indexes each serve one query:

| Index | Table | Serves |
|---|---|---|
| `ix_runs_tenant_created` | `agent_runs (tenant_id, created_at)` | `GET /v1/runs`, newest first |
| `ix_runs_parent` | `agent_runs (parent_run_id)` | `GET /v1/runs?parent_run_id=` |
| `ix_runs_inbox` | `agent_runs (tenant_id, status, assignee, created_at)` | the inbox, `status=PAUSED&assignee=` |
| `ix_runs_queue` | `agent_runs (tenant_id, agent_id, queued_at) WHERE status = 'QUEUED'` | `RunStore.claim` |
| `ix_runs_lease` | `agent_runs (status, lease_expires_at) WHERE lease_expires_at IS NOT NULL` | `RunStore.requeue_lapsed` |
| `ix_runs_escalation` | `agent_runs (awaiting_deadline) WHERE status = 'PAUSED' AND awaiting_deadline IS NOT NULL` | `RunStore.escalate_overdue` |
| `ix_runs_deadline` | `agent_runs (deadline) WHERE status IN ('QUEUED', 'RUNNING', 'PAUSED') AND deadline IS NOT NULL` | `RunStore.time_out_past_deadline` |
| `ix_runs_working` | `agent_runs (running_since) WHERE status = 'RUNNING'` | `RunStore.time_out_overworked` |
| `ix_runs_concurrency` | `agent_runs (tenant_id, concurrency_key) WHERE status = 'RUNNING' AND concurrency_key IS NOT NULL` | `RunStore.claim`: the runs sharing a key |
| `ix_runs_leased` | `agent_runs (tenant_id, agent_id) WHERE status = 'RUNNING' AND lease_owner IS NOT NULL` | `RunStore.claim`: what each tenant's workers hold (fair share, the cap) |
| `ix_runs_ended` | `agent_runs (updated_at) WHERE status IN (the endings)` | `RunStore.purge_ended` |
| `run_events` primary key | `run_events (run_id, position)` | `RunStore.events`, the stream |
| `ix_run_resolutions_run` | `run_resolutions (tenant_id, run_id, recorded_at)` | `GET /v1/runs/{id}/resolutions` |
| `ix_run_artifacts_expiry` | `run_artifacts (expires_at) WHERE expires_at IS NOT NULL` | `ArtifactStore.expired` |
| `ix_schedules_tenant_created` | `agent_schedules (tenant_id, created_at)` | `GET /v1/schedules`, newest first |
| `ix_schedules_due` | `agent_schedules (next_fire_at) WHERE enabled` | `ScheduleStore.claim_due` |
| `ix_webhooks_tenant` | `webhooks (tenant_id)` | listing, `WebhookStore.announce` |
| `ix_webhook_deliveries_due` | `webhook_deliveries (next_attempt_at) WHERE dead_at IS NULL` | `WebhookStore.claim_due` |
| `ix_webhook_deliveries_webhook` | `webhook_deliveries (webhook_id)` | the cascade on unsubscribe, `GET /v1/webhooks/deliveries?webhook_id=` |
| `ix_webhook_deliveries_dead` | `webhook_deliveries (dead_at) WHERE dead_at IS NOT NULL` | `WebhookStore.drop_dead` |

The migrations, oldest first: `1f6242bb21de` initial runs table, `7c1d2e3f4a5b` queue, lease
and inbox, `8d2e3f4a5b6c` schedules, `9e3f4a5b6c7d` run checkpoint, `a0f4b5c6d7e8` schedule
identity, `b1a5c6d7e8f9` webhook subscriptions, `c2b6d7e8f9a0` run artifacts,
`d3c8e9f0a1b2` run resolutions, `e4d9f0a1b2c3` run `settled_by`, `f5e0a1b2c3d4` lease lapses
and the run deadline sweep, `1a7f2b3c4d5e` run working time, `2b8a3c4d5e6f` run retries,
`3c9b4d5e6f7a` run cancel, `4d0c5e6f7a8b` webhook dead letters and secret rotation,
`5e1f6a7b8c9d` schedules' run limits, `6a2b7c8d9e0f` run priority and concurrency key,
`7b3c8d9e0f1a` shared rate-limit budgets, `8c4d9e0f1a2b` run events, `9d5e0f1a2b3c` run
retention, `0a6b1c2d3e4f` schedules' run priority and concurrency key. Each has a downgrade; CI runs upgrade, downgrade to base and upgrade again.

## Code map

```
src/agent_runs/
  __main__.py          agent-runs: uvicorn on RUNS__SERVICE__HOST:PORT, RUNS__SERVICE__WORKERS
                       processes (one per CPU, 1 to 8), graceful shutdown
  ticker.py            agent-runs-ticker: Ticker, run(), main()
  heartbeat.py         the ticker's liveness file; python -m agent_runs.heartbeat is the probe
  keys.py              KeyRegistry, KeyInfo: who an X-API-Key is
  firing.py            Firing: one schedule tick → one queued run
  webhooks.py          WebhookSender: delivering the outbox (signed with trellis.runs.webhooks.sign)
  egress.py            public_addresses(), pinned(), require_public(): where a webhook may go
  answering.py         require_may_answer(), require_may_cancel(): who may answer or cancel a run
  retry.py             backoff(), jittered(), Breaker: the one retry policy
  api/app.py           create_app(): routers, middleware, error handlers, health routes
  api/errors.py        Problem, install_error_handlers(): every error as a problem
  api/middleware.py    request context (id, metrics), body cap, compression
  api/pagination.py    cursor in, Link: rel="next" out
  api/openapi.py       the OpenAPI document: metadata, ids, problems, headers
  api/examples.py      a request example for every body
  api/ratelimit.py     TenantRateLimiter: one budget per tenant, in PostgreSQL
  api/routers/ops.py   /health/live, /health/ready, /metrics
  api/deps.py          Caller, caller(), Claimer, claimer(), session(): who is calling (a
                       claim may span tenants), a session per request
  api/routers/         runs.py, artifacts.py, schedules.py, webhooks.py
  domain/              runs.py, schedules.py, webhooks.py (request and answer models),
                       cadence.py (validate_cadence, next_fire_at), errors.py (status, ErrorCode)
  store/               tables.py (the mappings), runs.py, schedules.py, webhooks.py,
                       artifacts.py, database.py (connect + the head-revision check),
                       paging.py (Page, page_of: keyset pages)
  blob/                port.py (BlobStore, read), filesystem.py, gcs.py, open_blob_store()
  config/              settings.py (RUNS__* deployment facts), constants.py (design decisions)
  tools/               export_openapi.py: docs/openapi.json (make openapi)
  observability/       logging.py (structlog, JSON or console), metrics.py (Prometheus)

sdk/python/src/trellis/runs/      (trellis-runs, a workspace member; no trellis/__init__.py)
  client.py            RunsClient: the run verbs (cancel and release among them), list/iterate,
                       append_events/events/stream_events, live/ready/metrics
  artifacts.py         ArtifactsAPI: upload (with its SHA-256), download
  schedules.py         SchedulesAPI: create, list, get, update, delete, fire
  webhooks.py          WebhooksAPI (rotate_secret, deliveries, redeliver too); sign,
                       verify_signature, parse_delivery, the header names
  worker.py            Worker, Job, WorkerStore, RELEASED, OUT_OF_TIME: the claim loop (a
                       handler is stopped at the run's working time)
  models.py            RunSummary, Lease, Claimed, ResolutionEntry, RunEventEntry, EventsAppended,
                       ScheduleUpdate, FireResult,
                       Webhook*, DeliveryRecord, DeliveryState, WebhookDelivery, Page
  errors.py            RunsError and its classes, error_from_problem
  _transport.py        the key and tenant headers, retries, Retry-After, Link paging, the
                       server-sent event reader
```
