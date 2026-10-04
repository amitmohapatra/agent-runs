# agent-runs architecture

How the service is put together, drawn from the code: every name below is a module, class,
method, route, column or constant in this repository (or in `trellis-contracts`, where
marked). The wire contract is [api.md](api.md); this page is the inside.

- [Components](#components)
- [The run lifecycle](#the-run-lifecycle)
- [Start, interrupt, resume](#start-interrupt-resume)
- [A scheduled run firing](#a-scheduled-run-firing)
- [The tables](#the-tables)
- [Code map](#code-map)

## Components

Two processes from one image share one PostgreSQL database and one blob store. Neither
executes an agent: the harness does, and records here what happened.

```mermaid
flowchart LR
  subgraph clients["Clients (X-API-Key)"]
    harness["agent-harness<br/>HttpRuns: in-process runs<br/>and trellis workers"]
    ui["Inbox UI / its backend"]
  end

  subgraph runs["agent-runs"]
    subgraph api["API process: agent-runs (FastAPI, api/app.py)"]
      deps["api/deps.py<br/>caller() → Caller"]
      mw["api/middleware.py<br/>request id · metrics · body cap · gzip"]
      routers["routers: runs · artifacts<br/>schedules · webhooks<br/>ops: /health/*, /metrics"]
    end
    subgraph tick["Ticker process: agent-runs-ticker (ticker.py)"]
      ticker["Ticker.tick() every TICK_SECONDS<br/>fire · requeue · escalate<br/>send webhooks · purge artifacts"]
      sender["WebhookSender<br/>(webhooks.py)"]
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

  harness -- "HTTP /v1/runs, /v1/schedules,<br/>/v1/artifacts" --> routers
  ui -- "HTTP inbox, resume,<br/>artifacts, webhooks" --> routers
  mw --> routers
  routers --> deps --> keys -- "introspect key" --> memory
  routers --> stores
  routers --> blobport
  ticker --> stores
  ticker --> blobport
  ticker --> sender -- "POST signed event" --> receivers
  stores --> pg
  blobport --> blob
  migrate --> pg
  domain -. "types" .-> contracts
  stores -. "types" .-> contracts
  harness -. "types" .-> contracts
```

| Component | Where | What it owns |
|---|---|---|
| API | `api/app.py` `create_app`, `api/routers/*` | the HTTP surface |
| OpenAPI | `api/openapi.py` `custom_openapi`, `api/examples.py`, `tools/export_openapi.py` | the document every route shares (metadata, `<tag>.<function>` ids, the `ApiKeyAuth` scheme, a problem on every error status, standard headers); `docs/openapi.json` is it committed, and `tests/test_openapi.py` fails when they differ |
| Errors | `api/errors.py` `install_error_handlers`, `Problem`; `domain/errors.py` `ErrorCode` | every failure as an RFC 9457 problem: `ServiceError` subclasses with their status and `code`, FastAPI's validation and HTTP errors, a database that went away (`503`, `Retry-After`), anything else (`500`, no internals) |
| Middleware | `api/middleware.py` `RequestContextMiddleware`, `BodyLimitMiddleware`, `CompressionMiddleware` | `X-Request-ID` in, out and in every problem; request metrics by route template; the JSON body cap counted as bytes arrive; gzip, except artifact bytes |
| Rate limit | `api/ratelimit.py` `TenantRateLimiter`, `api/deps.py` `_within_budget` | a token bucket per tenant per worker process; `429` with `Retry-After`, `X-RateLimit-*` on every counted response |
| Metrics | `observability/metrics.py` | one Prometheus registry per process: the API's `/metrics`, the ticker's `RUNS__TICKER__METRICS_PORT` |
| Caller resolution | `api/deps.py` `caller`, `Caller` | `X-API-Key` (and `X-Trellis-Tenant` for a platform key) → the tenant and principal of the request; `require_tenant`, `require_may_act_for` |
| Key registry | `keys.py` `KeyRegistry`, `KeyInfo` | introspection at the Memory Service, cached 60 s (refusals 10 s), at most 10 000 keys |
| Stores | `store/runs.py`, `store/schedules.py`, `store/webhooks.py`, `store/artifacts.py` | every read and write of one table family; the caller commits |
| Firing | `firing.py` `Firing` | queueing one run for one schedule tick, in the transaction that advances the schedule |
| Ticker | `ticker.py` `Ticker` | the background loop; a `retry.Breaker` stops it hammering a dead database; `heartbeat.py` is its liveness file and probe |
| Webhook sender | `webhooks.py` `WebhookSender`, `sign` | one signed attempt per due outbox row |
| Blob port | `blob/port.py` `BlobStore`, `read`; `blob/filesystem.py`, `blob/gcs.py` | create-only bytes under a key; `put_stream` writes an upload as it arrives (a temporary file, or a bounded spool for GCS), hashing as it goes; `read` verifies SHA-256 and size while streaming |
| Schema | `alembic/versions/*`, `store/tables.py` | migrations are the schema; `tests/test_schema.py` checks the mappings match them; `store/database.py` `connect` refuses a database not at the head revision |
| Engine | `store/database.py` `connect`, `ping`; `config/settings.py` `DatabaseSettings` | one pool per process with a pre-ping on checkout, a recycle window, a bounded wait for a connection, a connect timeout and a statement timeout; `ping` is the bounded readiness probe |

## The run lifecycle

The one transition check is `trellis-contracts` `RunStatus.can_become`, called by
`store/runs.py` `_move` under a row lock; a refused transition is `409` and changes nothing.
The arrows are exactly the transitions some route or ticker step makes:
`tests/test_state_machine.py` asserts every route's transition and every refusal, and
`tests/test_queue.py` and `tests/test_escalation.py` the ticker's.

```mermaid
stateDiagram-v2
  [*] --> RUNNING: POST /v1/runs (queue false)
  [*] --> QUEUED: POST /v1/runs (queue true), or a schedule fires

  QUEUED --> RUNNING: POST /v1/runs/claim (lease to worker_id)
  QUEUED --> CANCELLED: finish CANCELLED
  QUEUED --> TIMEOUT: finish TIMEOUT

  RUNNING --> PAUSED: pause (Interrupt, checkpoint), lease released
  RUNNING --> QUEUED: ticker, lease lapsed and attempt < MAX_ATTEMPTS (attempt + 1)
  RUNNING --> ERROR: ticker, lease lapsed and attempt ≥ MAX_ATTEMPTS (lease_expired)
  RUNNING --> SUCCESS: finish
  RUNNING --> PARTIAL: finish
  RUNNING --> ERROR: finish
  RUNNING --> TIMEOUT: finish
  RUNNING --> CANCELLED: finish
  RUNNING --> REJECTED: finish

  PAUSED --> RUNNING: resume, not CANCEL, never queued (attempt + 1)
  PAUSED --> QUEUED: resume, not CANCEL, was queued (attempt + 1)
  PAUSED --> CANCELLED: resume CANCEL, or finish CANCELLED
  PAUSED --> TIMEOUT: finish TIMEOUT, or ticker past deadline with no escalate_to
  PAUSED --> PAUSED: ticker past deadline, assignee becomes escalate_to (once)

  SUCCESS --> [*]
  PARTIAL --> [*]
  ERROR --> [*]
  TIMEOUT --> [*]
  CANCELLED --> [*]
  REJECTED --> [*]
```

What each move does to the row (`_move`, `_requeue`, `RunStore.resume`):

- Leaving `RUNNING` clears the lease (`lease_owner`, `lease_expires_at`); leaving `PAUSED`
  clears `awaiting`, `assignee`, `awaiting_deadline`; any ending clears `checkpoint` and
  starts the run's artifacts' retention (`expires_at = now + ARTIFACT_RETENTION`, 7 days).
- "Was queued" means `queued_at` is set: the run entered the queue at least once (a
  `queue: true` start or a schedule fire), so a worker, not the original process, resumes it.
- `attempt` counts executions: `+1` on a resume that continues the run and on a lapsed
  lease's requeue, never on a claim.
- Events go to the outbox in the same transaction: `run.paused` on a pause, `run.escalated`
  on an escalation, `run.finished` on any ending (`domain/webhooks.py` `event_of`). A resume
  that continues the run, and a requeue, announce nothing.
- `MAX_ATTEMPTS` is 5 (`config/constants.py`).
- `checkpoint` is written by a pause and, as progress, by the lease holder's heartbeat; a
  requeue keeps it, so the next attempt's claim resumes from it; only an ending clears it.
- A pause or finish records who made it (`settled_by`, the `worker_id` or null); any other
  move clears it. A repeat of that call (the same caller, to the same status, on the same
  interrupt for a pause) is answered with the stored run and moves nothing (`_settled`),
  so a worker that lost the answer may retry. Otherwise a fenced write (`worker_id`) on a
  run whose lease the worker no longer holds is `LeaseLost` (`409 LEASE_LOST`, `_fence`),
  checked before the transition.

## Start, interrupt, resume

A durable run: queued, claimed by a worker, paused for a person with a large payload in an
artifact, answered from an inbox, claimed again and finished. Every request is
authenticated first (the key is introspected at the Memory Service, or taken from the
cache); that step is drawn once.

```mermaid
sequenceDiagram
  autonumber
  participant W as Worker (agent-harness)
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
  A->>DB: SELECT … FOR UPDATE SKIP LOCKED, oldest queued_at
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
  U->>A: POST /v1/runs/{id}/resume {InterruptResolution APPROVE}
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

## A scheduled run firing

A schedule is created once (an upsert on its identity) and the ticker fires it from then
on, as its `on_behalf_of`, while nobody is present. `POST /v1/schedules/{id}/fire` takes the
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
      T->>DB: SAVEPOINT, then INSERT agent_runs QUEUED,<br/>idempotency_key "schedule_id@fire_time"
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
  A-->>W: 200 Claimed (metadata: schedule_id, schedule_name, fire_time, created_by)
```

Two tickers on one tick both see the schedule; `SKIP LOCKED` gives it to one, and the
idempotency key would make a repeat find the same run anyway
(`tests/test_ticker.py::test_two_tickers_on_one_tick_queue_one_run`). `next_fire_at` only
moves forward past now, so a long outage fires each schedule once, not once per missed tick.

## The tables

Six tables, built by the nine Alembic revisions in `alembic/versions` (head
`e4d9f0a1b2c3`) and mapped in `store/tables.py`. Solid lines are foreign keys; the dotted
line is the logical link a schedule fire leaves (no foreign key: a run outlives the schedule
that fired it).

```mermaid
erDiagram
  agent_runs ||--o{ run_resolutions : "answered interrupts"
  agent_runs ||--o{ run_artifacts : "artifacts"
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
    int attempt
    timestamptz deadline "RunStart.deadline"
    varchar idempotency_key UK
    jsonb run_metadata
    timestamptz queued_at "set once the run entered the queue"
    varchar lease_owner
    timestamptz lease_expires_at
    varchar settled_by "worker_id of the pause or finish that made the state"
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
    timestamptz next_fire_at
    timestamptz last_fired_at
    varchar last_run_id
    int consecutive_failures
    jsonb last_error
    timestamptz retry_after "backoff gate after a retryable failure"
    jsonb schedule_metadata
    timestamptz created_at
    timestamptz updated_at
  }

  webhooks {
    varchar webhook_id PK
    varchar tenant_id
    varchar url
    varchar_array events "run.paused, run.escalated, run.finished"
    varchar secret "whsec_..., shown once"
    varchar created_by
    timestamptz created_at
  }

  webhook_deliveries {
    varchar delivery_id PK "stable_id(event_id, webhook_id)"
    varchar webhook_id FK
    jsonb payload "the event envelope"
    int attempts
    timestamptz next_attempt_at
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
| `ix_run_resolutions_run` | `run_resolutions (tenant_id, run_id, recorded_at)` | `GET /v1/runs/{id}/resolutions` |
| `ix_run_artifacts_expiry` | `run_artifacts (expires_at) WHERE expires_at IS NOT NULL` | `ArtifactStore.expired` |
| `ix_schedules_tenant_created` | `agent_schedules (tenant_id, created_at)` | `GET /v1/schedules`, newest first |
| `ix_schedules_due` | `agent_schedules (next_fire_at) WHERE enabled` | `ScheduleStore.claim_due` |
| `ix_webhooks_tenant` | `webhooks (tenant_id)` | listing, `WebhookStore.announce` |
| `ix_webhook_deliveries_due` | `webhook_deliveries (next_attempt_at)` | `WebhookStore.claim_due` |
| `ix_webhook_deliveries_webhook` | `webhook_deliveries (webhook_id)` | the cascade on unsubscribe |

The migrations, oldest first: `1f6242bb21de` initial runs table, `7c1d2e3f4a5b` queue, lease
and inbox, `8d2e3f4a5b6c` schedules, `9e3f4a5b6c7d` run checkpoint, `a0f4b5c6d7e8` schedule
identity, `b1a5c6d7e8f9` webhook subscriptions, `c2b6d7e8f9a0` run artifacts,
`d3c8e9f0a1b2` run resolutions, `e4d9f0a1b2c3` run `settled_by`. Each has a downgrade; CI runs upgrade, downgrade to base and
upgrade again.

## Code map

```
src/agent_runs/
  __main__.py          agent-runs: uvicorn on RUNS__SERVICE__HOST:PORT, RUNS__SERVICE__WORKERS
                       processes (one per CPU, 1 to 8), graceful shutdown
  ticker.py            agent-runs-ticker: Ticker, run(), main()
  heartbeat.py         the ticker's liveness file; python -m agent_runs.heartbeat is the probe
  keys.py              KeyRegistry, KeyInfo: who an X-API-Key is
  firing.py            Firing: one schedule tick → one queued run
  webhooks.py          WebhookSender, sign: delivering the outbox
  retry.py             backoff(), Breaker: the one retry policy
  api/app.py           create_app(): routers, middleware, error handlers, health routes
  api/errors.py        Problem, install_error_handlers(): every error as a problem
  api/middleware.py    request context (id, metrics), body cap, compression
  api/pagination.py    cursor in, Link: rel="next" out
  api/openapi.py       the OpenAPI document: metadata, ids, problems, headers
  api/examples.py      a request example for every body
  api/ratelimit.py     TenantRateLimiter: a token bucket per tenant, per process
  api/routers/ops.py   /health/live, /health/ready, /metrics
  api/deps.py          Caller, caller(), session(): who is calling, a session per request
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
```
