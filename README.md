# agent-runs

Durable agent runs, the worker queue, the human inbox, and the schedules that start runs.
One service (it absorbed agent-schedules in 0.2.0): an API process and a ticker process over
one PostgreSQL database, with run artifacts' bytes in blob storage (a filesystem, or GCS).

This service never executes an agent. A harness does, either in its own process (it records
the run here as `RUNNING`) or as a `trellis worker` that claims `QUEUED` runs from here under
a lease. This service remembers: a run that pauses for an approval at 2 a.m. is still there
at 9 a.m., a crashed worker's run goes back on the queue, and a schedule fires on behalf of a
person who is not present.

Every record is a [trellis-contracts](../agent-contracts) type: a run is a `RunRecord`
started from a `RunStart`, paused with an `Interrupt`, resumed with an
`InterruptResolution`; a schedule is a `Schedule` created from a `ScheduleSpec`.

## The state machine

The contracts' `RunStatus.can_become` is the only transition check; anything else is a
`409` that changes nothing. These are exactly the moves the routes and the ticker make
([docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) has what each does to the row, the sequence
diagrams and the tables):

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

## When to use what

| You want to… | Use |
|---|---|
| keep a durable record of a run your own process executes | `POST /v1/runs` (it is `RUNNING`), then `finish` |
| hand a run to a fleet of workers, surviving a worker that dies | `POST /v1/runs {queue: true}`; workers `claim`, `heartbeat` every third of the lease, and send `worker_id` on `pause`, `finish` and artifact uploads |
| make a retried start harmless | the same `run_id`, or an `idempotency_key` (one run per tenant and key) |
| stop for a person's approval, answer or edit | `pause` with an `Interrupt` (`assignee`, `deadline`, `escalate_to`) and the executor's `checkpoint`; the person answers with `resume` |
| move an unanswered question up, or give up on it | the interrupt's `deadline` and `escalate_to`: the ticker reassigns it once, else ends the run `TIMEOUT` |
| show a reviewer something too big for a question (a table, a diff) | `POST /v1/runs/{id}/artifacts`, then the `ArtifactRef` as `Interrupt.payload_ref`; the UI reads `GET /v1/artifacts/{id}` |
| build a person's or a role's inbox | `GET /v1/runs?status=PAUSED&assignee=…` |
| prove who approved what, and when | `GET /v1/runs/{id}/resolutions` (append-only) |
| cancel a run that is queued or waiting | `finish` with `CANCELLED` (or `resume` with `CANCEL`, recorded as an answer) |
| start runs on a timetable, as someone, while nobody is present | `POST /v1/schedules` (a cadence bucket or an hourly-or-slower cron, in the schedule's zone); repeat the create freely, it is an upsert |
| run a schedule now, or pause and resume it | `POST /v1/schedules/{id}/fire`; `PATCH {"enabled": false}` / `{"enabled": true}` |
| hear about pauses, escalations and endings instead of polling | `POST /v1/webhooks`; verify `X-Trellis-Signature` with the secret shown once |

## The API

Every `/v1` route needs `X-API-Key`; the health routes do not. Every error is an RFC 9457
problem (`application/problem+json`) with a stable `code` (`LEASE_LOST` tells a worker to
stop; `DEPENDENCY_UNAVAILABLE` and `RATE_LIMIT` come with `Retry-After`).
[docs/api.md](docs/api.md) has every route, body, status code and problem `code`, and the
exact claim, heartbeat and resume semantics a worker implements.

| Route | What it does |
|---|---|
| `POST /v1/runs` | record a run (`RUNNING`), or queue it (`queue: true` → `QUEUED`); idempotent on run id and `idempotency_key` |
| `POST /v1/runs/claim` | lease the oldest queued run of `agent_ids` to `worker_id`, or `204` |
| `POST /v1/runs/{id}/heartbeat` | extend the lease; `409 LEASE_LOST` = stop |
| `POST /v1/runs/{id}/pause` | the run waits on an `Interrupt` (assignee, deadline, escalation), keeping the executor's opaque `checkpoint` for whoever resumes it |
| `POST /v1/runs/{id}/resume` | answer it with an `InterruptResolution` |
| `POST /v1/runs/{id}/finish` | end it: `SUCCESS`, `PARTIAL`, `ERROR`, `TIMEOUT`, `CANCELLED`, `REJECTED`; the same finish repeated answers the stored run |
| `GET /v1/runs/{id}` | one run, the full record |
| `GET /v1/runs/{id}/resolutions` | every interrupt the run paused on and how it was answered, oldest first (append-only audit trail) |
| `GET /v1/runs?status=PAUSED&assignee=…` | run summaries; with these filters, the inbox of a person or role |
| `POST /v1/runs/{id}/artifacts` | store a large payload (an `ask` table, a diff; ≤ 50 MiB) in blob storage and get its `ArtifactRef` for `Interrupt.payload_ref` |
| `GET /v1/artifacts/{id}` | the artifact's bytes, checksum-verified, tenant-scoped |
| `POST /v1/schedules` | create a schedule, or get the one with the same agent, `on_behalf_of`, cadence and input (an upsert) |
| `GET /v1/schedules` · `GET/PATCH/DELETE /v1/schedules/{id}` | list, read, change (`{"enabled": false}` pauses, `true` resumes), delete |
| `POST /v1/schedules/{id}/fire` | fire now |
| `POST/GET /v1/webhooks` · `DELETE /v1/webhooks/{id}` | the tenant's webhook subscriptions |
| `GET /health/live` · `GET /health/ready` | the process is up · the database answers (no key) |

## The ticker

`agent-runs-ticker` is one loop (every `TICK_SECONDS`), straight against the database:

1. **Schedules.** Each due schedule is claimed with `FOR UPDATE SKIP LOCKED` and fired: its
   run is inserted `QUEUED` in the same transaction, idempotent on `(schedule_id,
   fire_time)`. A run that cannot be queued is recorded on the schedule, which backs off
   (retryable) or pauses itself (permanent, or `MAX_CONSECUTIVE_FAILURES`).
2. **Leases.** A `RUNNING` run whose lease lapsed goes back to `QUEUED` as the next attempt,
   or ends in `ERROR` after `MAX_ATTEMPTS`.
3. **Escalation.** A `PAUSED` run past its interrupt's `deadline` moves to `escalate_to`
   (once) or ends in `TIMEOUT`, with a webhook event either way.
4. **Webhooks.** Due deliveries in the outbox are sent (one attempt each, concurrently),
   then removed, or rescheduled with backoff.
5. **Artifacts.** Artifacts of runs that ended more than `ARTIFACT_RETENTION` (7 days) ago
   are deleted: the blob, then the record.

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
sends it, retried with the service's backoff, at least once. The envelope and the signature
are the Memory Service's: `X-Trellis-Signature: t=<unix seconds>,v1=<hex hmac-sha256 of
"<t>.<body>">`, with `X-Trellis-Event` and `X-Trellis-Delivery`, so one receiver verifies
both with `trellis.memory.webhooks.verify_signature`; `event_id` is stable per event, so a
receiver drops repeats.

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
| `RUNS__SERVICE__ENVIRONMENT` | `dev` | API, ticker | `dev` also accepts and delivers plain-`http` webhook URLs; anything else only `https` |
| `RUNS__MEMORY__URL` | `http://localhost:8080` | API | the Memory Service; keys are introspected at `{url}/v1/keys/self` |
| `RUNS__DATABASE__URL` | `postgresql+psycopg://memory:memory@localhost:5432/agent_runs` | API, ticker, alembic | the database |
| `RUNS__DATABASE__POOL_SIZE` | `10` | API, ticker | connections per process |
| `RUNS__BLOB__PROVIDER` | `filesystem` | API, ticker | `filesystem` or `gcs` |
| `RUNS__BLOB__ROOT` | `.blob` | API, ticker | the filesystem store's directory (shared by both processes) |
| `RUNS__BLOB__BUCKET` | unset | API, ticker | the GCS bucket; required with `gcs` (Application Default Credentials; `STORAGE_EMULATOR_HOST` points the client at an emulator) |
| `RUNS__TICKER__HEARTBEAT_FILE` | unset | ticker, probe | the liveness file; unset, a per-process file in the temp directory and nothing for the probe to read |
| `RUNS__OBSERVABILITY__LOG_LEVEL` | `INFO` | API, ticker | log level |
| `RUNS__OBSERVABILITY__LOG_JSON` | `true` | API, ticker | `false` for the console renderer |
| `RUNS_PORT`, `RUNS_DB_PORT` | `8090`, `5442` | docker compose | host ports of the API and PostgreSQL |
| `RUNS_TEST_ADMIN_URL`, `RUNS_TEST_DB`, `RUNS_TEST_GCS` | see `.env.example` | the test suite | the admin connection, the test database's name, and the opt-in fake-GCS tests |

## Develop

```bash
make lint typecheck test
make coverage                # the suite with line and branch coverage, failing under 95%
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

CI (`.github/workflows/ci.yml`) runs on pushes to `main` and on pull requests: ruff, pyright,
the migrations up, down to base and up again, and the suite with the coverage floor, against
PostgreSQL 16 with `agent-contracts` checked out beside this repository.
