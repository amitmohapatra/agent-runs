# agent-runs API (0.2.0)

Every route needs `X-Api-Key`. A platform key (credential with `tenant_id: null`) also sends
`X-Trellis-Tenant: <tenant>`; a tenant key may send it only with its own tenant. Bodies are
JSON; the record types are `trellis.contracts.runs` models, serialised as pydantic does.
Errors answer `{"detail": ...}`.

| Status | Means |
|---|---|
| `400` | a platform key named no tenant |
| `401` | unknown or missing `X-Api-Key` |
| `403` | the body or header names another tenant, or `on_behalf_of` the key may not act as |
| `404` | no such run or schedule in this tenant |
| `409` | the record is not in a state that allows it: an illegal transition, an answer to another interrupt, a lease that is no longer the caller's, a taken run id, a schedule update onto another schedule's identity |
| `413` | a pause's `checkpoint` is larger than 1 MiB of compact JSON |
| `422` | the body is invalid (including the contracts' own validators) |
| `503` | a schedule fire could not queue its run (recorded on the schedule) |

## Runs

### `POST /v1/runs` → `201 RunRecord` (`200` for a repeat)

Body: `RunStart` plus `queue`.

```json
{"tenant_id": "acme", "agent_id": "triage", "run_id": "run_…", "parent_run_id": null,
 "thread_id": null, "user_id": null, "workspace_id": null, "on_behalf_of": null,
 "input": {}, "deadline": null, "idempotency_key": null, "webhook_url": null,
 "metadata": {}, "queue": false}
```

Only `tenant_id` and `agent_id` are required; `run_id` is minted when absent. `queue: false`
records a run already `RUNNING` in the caller's process; `queue: true` puts it `QUEUED` for a
worker. Idempotent: a start whose `run_id`, or whose `(tenant_id, idempotency_key)`, already
exists answers `200` with the existing run (a run id held by another tenant is `409`).
`on_behalf_of` must be a principal the key may act as.

### `POST /v1/runs/claim` → `200 Claimed` or `204`

```json
{"worker_id": "w-1", "agent_ids": ["triage", "billing"], "lease_seconds": 60}
```

`lease_seconds` is 5–3600 (default 60); `agent_ids` 1–100. Takes the oldest `QUEUED` run
(by `queued_at`) of those agents in the tenant, sets it `RUNNING` and leases it:

```json
{"run": {…RunRecord…}, "lease": {"run_id": "run_…", "worker_id": "w-1", "expires_at": "…"}}
```

Claims use `SELECT … FOR UPDATE SKIP LOCKED`: concurrent claimers never receive the same run
and never wait on each other. `attempt` is not changed by a claim; it counts executions and
was already incremented when the run was put back on the queue. `204` means nothing is
queued for those agents; poll again later.

### `POST /v1/runs/{id}/heartbeat` → `200 Lease`

```json
{"worker_id": "w-1", "lease_seconds": 60}
```

Extends the lease to `now + lease_seconds`. `409` means the lease is no longer this
worker's (it lapsed and the run was re-queued, possibly claimed by another worker) or the
run is no longer `RUNNING` (it was cancelled or finished): **stop working the run and do not
write to it**. Heartbeat well inside the lease (every third of it).

### `POST /v1/runs/{id}/pause?worker_id=` → `200 RunRecord`

Body: the `Interrupt` the run waits on, whose `tenant_id` and `run_id` are this run's (`422`
otherwise), and optionally the executor's `checkpoint`:

```json
{"interrupt": {"interrupt_id": "int_…", "tenant_id": "acme", "run_id": "run_…",
               "reason": "APPROVAL", "question": "Create PO for 12 000 EUR?", "ui": "approve",
               "expects": null, "options": [], "payload": null, "payload_ref": null,
               "tool_call": {…}, "assignee": "role:procurement",
               "deadline": "2026-10-01T09:00:00Z", "escalate_to": "role:finance-leads"},
 "checkpoint": {"asks": {…}, "tools": {…}, "framework": {…}}}
```

`RUNNING → PAUSED`. The interrupt is kept as `awaiting`; `assignee` and `deadline` are
indexed for the inbox and the escalation sweep. Announced as `run.paused`. The lease ends:
a worker pausing a run lets go of it.

`checkpoint` is any JSON object, opaque to the service: the executor's resume journal
(answered asks, completed tool outputs) and the framework's own resume state (a LangGraph
interrupt id, a serialized OpenAI `RunState`). It is returned as `RunRecord.checkpoint` on
every read, resume and claim, so whichever worker resumes the run repeats no side effect.
Each pause replaces it (omitted means `null`); any ending (`finish`, a `CANCEL` answer, a
`TIMEOUT`, a run failed after `MAX_ATTEMPTS`) clears it. Larger than 1 MiB as compact JSON
(`MAX_CHECKPOINT_BYTES`) is `413`, and nothing changes. Heartbeats do not carry one.

`worker_id` (query, optional, also on `finish`) fences the write: when given, the write is
refused with `409` unless that worker still holds the run's lease. Workers always send it,
so a worker whose lease lapsed cannot write over the run another worker has since claimed.

### `POST /v1/runs/{id}/resume` → `200 RunRecord`

Body: an `InterruptResolution` for the interrupt the run waits on:

```json
{"interrupt_id": "int_…", "run_id": "run_…", "decision": "APPROVE",
 "answer": null, "reviewer": "user:alice", "payload": null}
```

`decision` is `ANSWER | APPROVE | REJECT | EDIT | CANCEL` (`EDIT` carries `payload`). The
resolution is kept as `last_resolution`, and:

- `CANCEL` ends the run: `PAUSED → CANCELLED`.
- any other decision continues it as the next attempt (`attempt + 1`):
  - a run that was ever queued (it came from the queue or a schedule) goes back to
    `QUEUED` and a worker claims it again, finding the answer in `run.last_resolution`;
  - a run recorded in process goes to `RUNNING`, for the process that resumes it.

`409` when the run is not `PAUSED` (a second answer) or waits on a different
`interrupt_id`; `422` when `run_id` names another run. Announced as `run.finished` only for
`CANCEL`.

### `POST /v1/runs/{id}/finish?worker_id=` → `200 RunRecord`

```json
{"status": "ERROR", "output": null,
 "error": {"code": "ToolFailed", "category": "TOOL", "message": "…", "retryable": false}}
```

`status` must be an ending (`SUCCESS | PARTIAL | ERROR | TIMEOUT | CANCELLED | REJECTED`);
`error` only on `ERROR | TIMEOUT | REJECTED`. From `RUNNING` any ending; from `QUEUED` or
`PAUSED` only `CANCELLED` or `TIMEOUT` (cancelling a queued or waiting run). A second finish
is `409`. Announced as `run.finished`.

### Reads

- `GET /v1/runs/{id}` → `RunRecord`, the full record (input, output, error, checkpoint).
- `GET /v1/runs?status=&assignee=&agent_id=&thread_id=&parent_run_id=&limit=` →
  `[RunSummary]`, newest first, `limit` 1–500 (default 50). The inbox is
  `status=PAUSED&assignee=role:procurement`.

```json
[{"run_id": "run_…", "agent_id": "triage", "status": "PAUSED",
  "awaiting": {…Interrupt…}, "assignee": "role:procurement",
  "deadline": null, "updated_at": "2026-09-30T08:00:00Z"}]
```

`awaiting` is the interrupt a paused run waits on (`null` otherwise; its own `deadline` is
when the answer is due), `assignee` whose inbox it is in, `deadline` the run's own deadline
(`RunStart.deadline`). Nothing else is in a summary; read the run for the rest.

## Schedules

A schedule fires runs as `on_behalf_of`, the person who set it, while nobody is present.
Creating one needs a key that may act as that principal; editing, pausing, resuming, firing
or deleting one needs the same of the schedule's stored `on_behalf_of` (`404` before `403`).

### `POST /v1/schedules` → `201 Schedule` (`200` for an existing one)

Body: a `ScheduleSpec` (`created_by` is refused; it is the key's principal):

```json
{"tenant_id": "acme", "agent_id": "briefing", "name": "morning briefing",
 "cadence": "0 8 * * 1-5", "timezone": "Europe/Berlin", "on_behalf_of": "user_ada",
 "input": {"topic": "inbox"}, "workspace_id": null, "enabled": true, "metadata": {}}
```

`cadence` is `hourly | daily | weekly | weekdays | manual` (local midnight; weekly on Monday;
`manual` never fires on its own) or a cron expression firing at most once an hour.

**An upsert.** A schedule's identity is `(tenant_id, agent_id, on_behalf_of, cadence,
input_sha256)`, where `cadence` is the normalised expression and `input_sha256` is the
lowercase hex SHA-256 of the UTF-8 bytes of the input as canonical JSON (Python's
`json.dumps(input, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`; no input is
`null`). A create whose identity is new answers `201` with the new schedule, armed for its
next occurrence; one whose identity exists answers `200` with the existing schedule,
**unchanged** (its name, timezone, metadata and enabled flag stay as they are; change them
with `PATCH`). Concurrent creates of one identity make one schedule. `name` is a label and
need not be unique. This is the one idempotency mechanism: a client repeats the create and
needs no name, no `409` handling and no follow-up `PATCH`.

### `GET /v1/schedules?enabled=&agent_id=&limit=` · `GET /v1/schedules/{id}` · `DELETE /v1/schedules/{id}` (`204`)

### `PATCH /v1/schedules/{id}` → `Schedule`

Any of `agent_id, name, cadence, timezone, input, workspace_id, enabled, metadata`
(`metadata` is merged). `tenant_id` and `on_behalf_of` are refused (`422`). Changing the
cadence or zone re-arms the next fire. A change that would give the schedule the identity of
another one is `409`.

Pausing and resuming are `PATCH`es: `{"enabled": false}` pauses; `{"enabled": true}` on a
paused schedule resumes it, clearing an auto-pause (failure count, last error, backoff) and
firing from the next occurrence it can still honour, never the backlog.

### `POST /v1/schedules/{id}/fire` → `FireResult`

Body (optional): `{"at": "2026-10-01T06:00:00Z"}`, an instant that has arrived (`422` for one
more than a minute ahead, or without an offset). Without it, the fire is for the tick the
schedule is due for, else for now. Queues the run (`QUEUED`, `idempotency_key =
"<schedule_id>@<fire_time UTC ISO>"`, metadata `schedule_id`, `schedule_name`, `fire_time`,
`created_by`) and advances the schedule:

```json
{"schedule_id": "sch_…", "run_id": "run_…", "fire_time": "…",
 "idempotency_key": "sch_…@2026-10-01T06:00:00+00:00", "schedule": {…Schedule…}}
```

A repeat for the same instant returns the same run. `409` when the schedule is paused.
`503` when the run could not be queued; the detail says what the schedule did about it:

```json
{"detail": {"message": "…", "consecutive_failures": 1, "auto_paused": false, "error": {…}}}
```

## Webhooks

`POST <webhook_url>` with

```json
{"event_id": "whd_…", "type": "run.paused", "tenant_id": "acme", "workspace_id": null,
 "occurred_at": "…", "data": {"run": {…RunRecord…}}}
```

and headers `X-Trellis-Event: <type>`, `X-Trellis-Delivery: <event_id>`,
`X-Trellis-Signature: t=<unix seconds>,v1=<hex hmac-sha256 over "<t>.<raw body>">` (with
`RUNS__WEBHOOKS__SIGNING_SECRET`). Types: `run.paused`, `run.escalated`, `run.finished`. A
`2xx` accepts; `408`, `429` and `5xx` are retried (4 attempts, doubling backoff); any other
answer is final. `https` only outside dev.

## Ops

`GET /health/live` · `GET /health/ready` (the database answers). The ticker's probe is
`python -m agent_runs.heartbeat`.
