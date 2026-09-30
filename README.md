# agent-runs

Durable agent runs, the worker queue, the human inbox, and the schedules that start runs.
One service (it absorbed agent-schedules in 0.2.0): an API process and a ticker process over
one PostgreSQL database.

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
`409`.

```mermaid
stateDiagram-v2
  [*] --> QUEUED: POST /v1/runs {queue: true} · a schedule fires
  [*] --> RUNNING: POST /v1/runs
  QUEUED --> RUNNING: claim (a worker, under a lease)
  RUNNING --> QUEUED: lease lapsed (ticker, attempt + 1)
  RUNNING --> PAUSED: pause (Interrupt, checkpoint)
  PAUSED --> RUNNING: resume (in-process run, attempt + 1)
  PAUSED --> QUEUED: resume (queued run, attempt + 1)
  PAUSED --> PAUSED: deadline passed, escalate_to (ticker)
  PAUSED --> TIMEOUT: deadline passed, nobody to escalate to (ticker)
  PAUSED --> CANCELLED: resume CANCEL · finish
  QUEUED --> CANCELLED: finish
  RUNNING --> SUCCESS
  RUNNING --> PARTIAL
  RUNNING --> ERROR: finish · lease lapsed MAX_ATTEMPTS times
  RUNNING --> TIMEOUT
  RUNNING --> CANCELLED
  RUNNING --> REJECTED
```

## The API

All routes need `X-Api-Key`. [docs/api.md](docs/api.md) has every route, body and status
code, and the exact claim, heartbeat and resume semantics a worker implements.

| Route | What it does |
|---|---|
| `POST /v1/runs` | record a run (`RUNNING`), or queue it (`queue: true` → `QUEUED`); idempotent on run id and `idempotency_key` |
| `POST /v1/runs/claim` | lease the oldest queued run of `agent_ids` to `worker_id`, or `204` |
| `POST /v1/runs/{id}/heartbeat` | extend the lease; `409` = lease lost, stop |
| `POST /v1/runs/{id}/pause` | the run waits on an `Interrupt` (assignee, deadline, escalation), keeping the executor's opaque `checkpoint` for whoever resumes it |
| `POST /v1/runs/{id}/resume` | answer it with an `InterruptResolution` |
| `POST /v1/runs/{id}/finish` | end it: `SUCCESS`, `PARTIAL`, `ERROR`, `TIMEOUT`, `CANCELLED`, `REJECTED` |
| `GET /v1/runs/{id}` · `GET /v1/runs/{id}/lineage` | one run · it and its ancestors |
| `GET /v1/runs?status=PAUSED&assignee=…` | the inbox of a person or role |
| `POST/GET /v1/schedules` · `GET/PATCH/DELETE /v1/schedules/{id}` | schedules |
| `POST /v1/schedules/{id}/pause` · `/resume` · `/fire` | stop, restart, fire now |

## The ticker

`agent-runs-ticker` is one loop (every `TICK_SECONDS`), straight against the database:

1. **Schedules.** Each due schedule is claimed with `FOR UPDATE SKIP LOCKED` and fired: its
   run is inserted `QUEUED` in the same transaction, idempotent on `(schedule_id,
   fire_time)`. A run that cannot be queued is recorded on the schedule, which backs off
   (retryable) or pauses itself (permanent, or `MAX_CONSECUTIVE_FAILURES`).
2. **Leases.** A `RUNNING` run whose lease lapsed goes back to `QUEUED` as the next attempt,
   or ends in `ERROR` after `MAX_ATTEMPTS`.
3. **Escalation.** A `PAUSED` run past its interrupt's `deadline` moves to `escalate_to`
   (once) or ends in `TIMEOUT`, with a webhook either way.

Each step is bounded per tick and safe in several replicas. A tick that fails as a whole
(the database is down) counts against a breaker; `python -m agent_runs.heartbeat` is the liveness
check (a heartbeat file touched after every tick).

## Authentication

One scheme. `X-Api-Key` is the caller; its credential (in `RUNS__SERVICE__API_KEYS`) names
the tenant it speaks for, the principal recorded as `created_by`, and the principals it may
put in `on_behalf_of`. A platform key has no tenant of its own and names the tenant it acts
for in `X-Trellis-Tenant`; a tenant key may send that header only to agree with itself
(`403` otherwise).

## Webhooks

A run started (or scheduled) with a `webhook_url` is announced when it pauses
(`run.paused`), is escalated (`run.escalated`) or ends (`run.finished`). The envelope and the
signature are the Memory Service's: `X-Trellis-Signature: t=<unix seconds>,v1=<hex
hmac-sha256 of "<t>.<body>">`, with `X-Trellis-Event` and `X-Trellis-Delivery`, so one
receiver verifies both with `trellis.memory.webhooks.verify_signature`. Deliveries are
retried with the service's backoff, off the request path, at least once; `event_id` is
stable per event, so a receiver drops repeats.

## Run it

Needs PostgreSQL and a checkout of `agent-contracts` next to this one (a path dependency).

```bash
make install                 # uv sync, trellis-contracts from ../agent-contracts
make migrate                 # alembic upgrade head (RUNS__DATABASE__URL)
uv run agent-runs            # the API on RUNS__SERVICE__PORT
uv run agent-runs-ticker     # the ticker
make up                      # or all of it in docker compose: postgres, migrate, api, ticker
```

Configuration is `RUNS__*` environment variables, each documented in
[.env.example](.env.example); every other number is a named constant in
`src/agent_runs/config/constants.py`.

## Develop

```bash
make lint typecheck test
```

The suite runs against the local PostgreSQL in its own database (`agent_runs_tests`, dropped
and recreated per run) and skips with a reason when there is none. Migrations live in
`alembic/versions`; a test checks they build exactly the schema the code maps.
