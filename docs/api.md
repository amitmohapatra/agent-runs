# agent-runs API (0.4.0)

Every `/v1` route needs `X-API-Key` (header names are case-insensitive: `X-Api-Key` is the
same header), a key issued by the Memory Service (the one key registry; see
[Authentication](#authentication)). A platform key (`tenant_id: null`) also sends
`X-Trellis-Tenant: <tenant>`; a tenant key may send it only with its own tenant. The health
routes (`/health/live`, `/health/ready`) and FastAPI's own `/docs` and `/openapi.json` need
no key. Bodies are JSON; the record types are `trellis.contracts.runs` models, serialised as
pydantic does.

The machine-readable contract is [openapi.json](openapi.json) (OpenAPI 3.1, generated from
the code and checked against it by the suite and CI; `make openapi` rewrites it), served live
at `/openapi.json` with `/docs` (Swagger UI) and `/redoc`. Operation ids are
`<tag>.<function>` (`runs.start`, `runs.list`, `schedules.fire`, …); every operation documents
its error statuses with the `Problem` schema, and every request body has an example.

The Python client is the SDK in [`sdk/python`](../sdk/python/README.md), `trellis.runs`
(pip `trellis-runs`, versioned with this API): `RunsClient` has one method per operation,
named by its operation id (`runs.start` is `RunsClient.start`, `schedules.fire` is
`RunsClient.schedules.fire`), raises each problem `code` below as its own error class
(`LEASE_LOST` is `LeaseLostError`, a sibling of `ConflictError`, not a kind of it), and
retries what is safe to retry; its `Worker` implements the claim, heartbeat and release
semantics of [Runs](#runs).

What changed on the wire in each version, and what a caller must do about it:
[CHANGELOG.md](../CHANGELOG.md). Which versions of the other repos go with this one:
[versioning.md](versioning.md).

Every response carries `X-Request-ID`: the caller's when it sent one that is an id (a letter
or digit, then letters, digits and `._:-`, at most 200 characters), else a generated
`req_…`.

## Errors

Every error is an RFC 9457 problem, `Content-Type: application/problem+json`, in the Memory
Service's shape, so one client reads both services:

```json
{"type": "urn:trellis:problem:lease-lost", "title": "Lease lost", "status": 409,
 "detail": "worker w-1 does not hold the lease on run run_…",
 "instance": "/v1/runs/run_…/finish", "code": "LEASE_LOST", "retryable": false,
 "request_id": "req_…", "details": {}}
```

`code` is the stable category to branch on (`type` is its URN, `title` its fixed words);
`detail` is this occurrence and never echoes a submitted value; `retryable: true` means the
same request may succeed later, and then a `429` or `503` carries `Retry-After` (seconds).
`details` is structured context: `errors` (each `{loc, msg, type}`) for a `422`, the
schedule's state for a failed fire, `differing` for a reused idempotency key.

| Status | `code` | Means |
|---|---|---|
| `400` | `VALIDATION` | a platform key named no tenant |
| `401` | `AUTHENTICATION` | missing `X-API-Key`, or one the key registry does not know (or revoked, expired) |
| `403` | `AUTHORIZATION` | the registry refuses the key (a suspended tenant), the body or header names another tenant, `on_behalf_of` the key may not act as, an answer to a paused run (or a cancel of a run) the key may not give ([who may](#who-may-answer-a-paused-run)), or an artifact for a paused run from a key whose role is not `service` |
| `404` | `NOT_FOUND` | no such run, schedule, webhook or artifact in this tenant (or no such route) |
| `405` | `VALIDATION` | the route does not take this method (`Allow` lists the ones it does) |
| `409` | `LEASE_LOST` | a worker's fenced write (`worker_id`: heartbeat, release, pause, finish, artifact upload) on a run whose lease it no longer holds: the lease lapsed and the run went back on the queue, or the run was paused, cancelled, finished, or ended past its deadline or working time. **Stop working the run.** |
| `409` | `CONFLICT` | any other state conflict: an illegal transition, an answer to another interrupt, a cancel of a run that ended, a run id that cannot be used, an idempotency key reused with a different start, a schedule update onto another schedule's identity, a fire of a paused schedule, a 21st webhook, a redelivery of a delivery still owed |
| `413` | `PAYLOAD_TOO_LARGE` | a JSON body past `RUNS__SERVICE__MAX_BODY_BYTES` (4 MiB), a run's `input` or `output` past `RUNS__SERVICE__MAX_PAYLOAD_BYTES` (1 MiB), a `checkpoint` past 1 MiB, an artifact past 50 MiB (see [Limits](#limits)) |
| `422` | `VALIDATION` | the body or query is invalid (including the contracts' own validators), a cursor this listing did not issue, or a webhook URL this deployment does not deliver to (scheme, or a host that does not resolve or resolves to a private address) |
| `429` | `RATE_LIMIT` | the tenant's request budget is spent for now; retryable after `Retry-After` (see [Limits](#limits)) |
| `500` | `INTERNAL` | a fault here (an artifact's stored bytes no longer match their checksum, a statement the database refuses, anything unanticipated); the detail says nothing about internals |
| `503` | `DEPENDENCY_UNAVAILABLE` | PostgreSQL did not answer (no connection, a connection lost, no pooled connection free in time, a statement past its timeout), the key registry (Memory Service) could not be asked, or a schedule fire could not queue its run (recorded on the schedule); retryable, with `Retry-After: 5` (a fire that paused its schedule is not retryable) |

## Pages and locations

Every listing (`GET /v1/runs`, `/v1/runs/{id}/resolutions`, `/v1/schedules`,
`/v1/webhooks`, `/v1/webhooks/deliveries`) takes `cursor` and `limit` (1–500, default 50) and answers a bare JSON array
with `Link: <url>; rel="next"` (RFC 8288) exactly when there is a next page; the URL is this
request's with `cursor` set, so the filters and the limit carry over. The cursor is opaque
(base64url JSON of the position the listing is ordered by: `created_at` and the id, which
never change, so a record written between two pages is neither skipped nor repeated); one
this listing did not issue is `422`. Without `cursor`, the first page.

A run's event log (`GET /v1/runs/{id}/events`) is the one exception: it is read by
position (`after`, the last `position` seen, and `limit`), the number its stream
(`…/events/stream`) sends as each event's `id`, so a reader moves between the two freely.

Every create that answers `201` says where the new record lives: `Location: /v1/runs/{id}`,
`/v1/schedules/{id}`, `/v1/webhooks/{id}`, `/v1/artifacts/{id}`. A repeat answered `200`
carries none.

## Limits

- **Body.** A JSON body past `RUNS__SERVICE__MAX_BODY_BYTES` (default 4 MiB) is `413`: at
  once when `Content-Length` says so, else as soon as the bytes that arrived pass it (a
  chunked body is counted too), before the route has it all.
- **Run payloads.** A run's `input` (`POST /v1/runs`) or `output` (`finish`) past
  `RUNS__SERVICE__MAX_PAYLOAD_BYTES` (default 1 MiB) of compact JSON is `413`: both live in
  the run's row and in every read of it. Anything larger belongs in an artifact.
- **Checkpoints** are at most 1 MiB (`MAX_CHECKPOINT_BYTES`), **artifacts** at most 50 MiB
  (`MAX_ARTIFACT_BYTES`), streamed to the blob store as they arrive.
- **Rate.** Each tenant's `/v1` requests draw on one budget, refilled at
  `RUNS__RATE_LIMIT__PER_MINUTE` (default 3000) a minute and holding at most
  `RUNS__RATE_LIMIT__BURST` (500). Every counted response carries `X-RateLimit-Limit` (the
  budget a minute) and `X-RateLimit-Remaining`; an empty budget is `429 RATE_LIMIT` with
  `Retry-After` (seconds until a request is allowed again). The budget is kept in
  PostgreSQL (`rate_limit_buckets`, one row per tenant, moved by one upsert per request
  under the database's clock), so every worker of every replica draws on the same one: the
  limit is the limit, however many replicas run. A platform key claiming from every tenant
  draws on its own budget (`key:<key_id>`). `0` turns it off.
- **Compression.** A response of 1 KiB or more is gzipped for a client that sends
  `Accept-Encoding: gzip`, except artifact bytes, served as stored.

## Every route

`auth` below is the answers any `/v1` route may give before it looks at the request:
`400`, `401`, `403`, `429` (see [Limits](#limits)), `503` (see
[Authentication](#authentication)); any route with a body may also answer `413`. The
sections after the table have the bodies and the exact semantics.

| Method and path | Body / query | Success | Errors besides `auth` |
|---|---|---|---|
| `POST /v1/runs` | `RunStart` + `queue` | `201 RunRecord` (`200` repeat) | `403` tenant or `on_behalf_of`, `409` run id unusable or idempotency key reused with another start, `422` |
| `POST /v1/runs/claim` | `{worker_id, agent_ids, lease_seconds}` (a platform key may omit `X-Trellis-Tenant`) | `200 Claimed`, `204` nothing queued that may run now | `422` |
| `POST /v1/runs/{id}/heartbeat` | `{worker_id, lease_seconds, checkpoint?}` | `200 Lease` | `404`, `409 LEASE_LOST` lease lost or run not `RUNNING`, `422` |
| `POST /v1/runs/{id}/release` | `{worker_id, checkpoint?}` | `200 RunRecord` (`QUEUED`; a repeat answers the run) | `404`, `409 LEASE_LOST` not the lease holder or run not `RUNNING`, `413`, `422` |
| `POST /v1/runs/{id}/pause` | `{interrupt, checkpoint}`, `?worker_id=` | `200 RunRecord` (`PAUSED`; a repeat answers the stored run) | `404`, `409` not `RUNNING` (`CONFLICT`) or not the lease holder (`LEASE_LOST`), `413`, `422` interrupt of another run |
| `POST /v1/runs/{id}/resume` | `InterruptResolution` | `200 RunRecord` | `403` the key may not answer this run ([who may](#who-may-answer-a-paused-run)), `404`, `409` not `PAUSED` or another interrupt, `422` another run |
| `POST /v1/runs/{id}/cancel` | `{reason?}` | `200 RunRecord` (`CANCELLED`, or `RUNNING` with the cancel asked; a repeat answers the run) | `403` the key may not cancel it, `404`, `409` ended, `422` |
| `POST /v1/runs/{id}/finish` | `{status, output, error}`, `?worker_id=` | `200 RunRecord` (a repeat answers the stored run; a retried `ERROR` answers it `QUEUED`) | `404`, `409` illegal ending (`CONFLICT`) or not the lease holder (`LEASE_LOST`), `422` not an ending, `error` on a non-failure |
| `GET /v1/runs/{id}` | | `200 RunRecord` | `404` |
| `GET /v1/runs/{id}/resolutions` | `?cursor=&limit=` | `200 [ResolutionEntry]` | `404`, `422` |
| `POST /v1/runs/{id}/events` | `{events: [RunEvent]}` (1–500), `?worker_id=` | `200 {appended, position}` | `404`, `409` not `RUNNING` or leased (`CONFLICT`) or not the lease holder (`LEASE_LOST`), `413`, `422` an event of another run |
| `GET /v1/runs/{id}/events` | `?after=&limit=` | `200 [RunEventEntry]` | `404`, `422` |
| `GET /v1/runs/{id}/events/stream` | `?after=`, `Last-Event-ID` | `200 text/event-stream` | `404`, `422` |
| `GET /v1/runs` | `?status=&assignee=&agent_id=&thread_id=&parent_run_id=&top_level=&cursor=&limit=` | `200 [RunSummary]` | `422` |
| `POST /v1/runs/{id}/artifacts` | raw bytes, `Content-Type`, `?worker_id=&checksum=` | `201 ArtifactRef` (`200` repeat) | `403` paused run, non-service key, `404`, `409`, `413`, `422` empty or checksum mismatch |
| `GET /v1/artifacts/{artifact_id}` | | `200` the bytes | `404`, `500` corrupt |
| `POST /v1/schedules` | `ScheduleSpec` | `201 Schedule` (`200` existing) | `403` tenant or `on_behalf_of`, `409` identity deleted mid-create (rare), `422` |
| `GET /v1/schedules` | `?enabled=&agent_id=&cursor=&limit=` | `200 [Schedule]` | `422` |
| `GET /v1/schedules/{id}` | | `200 Schedule` | `404` |
| `PATCH /v1/schedules/{id}` | `ScheduleUpdate` | `200 Schedule` | `403` `on_behalf_of`, `404`, `409` another schedule's identity, `422` |
| `DELETE /v1/schedules/{id}` | | `204` | `403` `on_behalf_of`, `404` |
| `POST /v1/schedules/{id}/fire` | optional `{at}` | `200 FireResult` | `403` `on_behalf_of`, `404`, `409` paused, `422` `at` ahead or naive, `503` not queued |
| `POST /v1/webhooks` | `{url, events}` | `201 WebhookCreated` | `409` 20 already, `422` (also a URL this deployment does not deliver to) |
| `GET /v1/webhooks` | `?cursor=&limit=` | `200 [Webhook]` | `422` |
| `GET /v1/webhooks/{id}` | | `200 Webhook` | `404` |
| `DELETE /v1/webhooks/{id}` | | `204` | `404` |
| `POST /v1/webhooks/{id}/rotate-secret` | | `200 WebhookCreated` (the new secret) | `404` |
| `GET /v1/webhooks/deliveries` | `?state=&webhook_id=&cursor=&limit=` | `200 [DeliveryRecord]` | `422` |
| `POST /v1/webhooks/deliveries/{id}/redeliver` | | `200 DeliveryRecord` | `404`, `409` still owed |
| `GET /health/live` | no key | `200 {"status": "ok"}` | |
| `GET /health/ready` | no key | `200 {"status": "ok"}` | `503` the database does not answer within 3 s |
| `GET /metrics` | no key | `200` Prometheus text | |

## Authentication

agent-runs keeps no keys. It asks the Memory Service who a key is and caches the answer.

### The introspection contract: `GET {RUNS__MEMORY__URL}/v1/keys/self`

`RUNS__MEMORY__URL`, else the platform-wide `MEMORY_URL`. One kept-alive connection pool
(at most 100 connections, 20 idle ones kept 30 s), 2 s to connect, 3 s in all.

Request: the caller's key, unchanged, as the Memory Service authenticates any call:

```
GET /v1/keys/self
X-API-Key: <the key agent-runs was sent>
```

Answers (anything else, a timeout of 3 s or an unreachable service is `503` here, not cached):

| Status | Body | agent-runs answers |
|---|---|---|
| `200` | `KeyInfo` (below) | the request proceeds as that key |
| `401` | any | `401`: unknown, revoked or expired key |
| `403` | any | `403`: a key the registry refuses (a suspended tenant) |

```json
{"key_id": "key_…", "tenant_id": "acme", "principal": "svc:harness", "role": "service",
 "may_act_as": ["*"]}
```

| Field | Type | Meaning |
|---|---|---|
| `key_id` | string | the key's id (never the secret) |
| `tenant_id` | string or `null` | the tenant the key speaks for; `null` for a platform key, which then names the tenant per request in `X-Trellis-Tenant` |
| `principal` | string | who the caller is: recorded as `created_by` on schedules and webhooks, and a principal the key may always act as |
| `role` | string | the registry's role (`platform`, `admin`, `service`, …): an `admin` or `platform` key answers any paused run ([who may](#who-may-answer-a-paused-run)); only a `service` key adds an artifact to a paused run |
| `may_act_as` | array of strings | the principals the key may put in `on_behalf_of` (a run then executes as them) and answer paused runs as (`reviewer`); `"*"` is any principal of the tenant; empty means only `principal` |

Other fields are ignored. A missing required field is a malformed answer (`503`).

Caching (per process, keyed by the SHA-256 of the key; the key itself is not kept): a `200`
answer for 60 s (`KEY_CACHE_SECONDS`, so a revocation takes effect here within a minute), a
`401`/`403` answer for 10 s (`KEY_NEGATIVE_CACHE_SECONDS`), at most 10 000 keys (least
recently used evicted).

## Runs

### `POST /v1/runs` → `201 RunRecord` (`200` for a repeat)

Body: `RunStart` plus `queue`.

```json
{"tenant_id": "acme", "agent_id": "triage", "run_id": "run_…", "parent_run_id": null,
 "thread_id": null, "user_id": null, "workspace_id": null, "on_behalf_of": null,
 "input": {}, "deadline": null, "timeout_seconds": 600, "idempotency_key": null,
 "agent_version": "2026.10.05-3f2a1c", "priority": 0, "concurrency_key": null,
 "metadata": {}, "queue": false}
```

Only `tenant_id` and `agent_id` are required; `run_id` is minted when absent. `queue: false`
records a run already `RUNNING` in the caller's process; `queue: true` puts it `QUEUED` for a
worker. Idempotent: a start whose `run_id`, or whose `(tenant_id, idempotency_key)`, already
exists answers `200` with the existing run. A run id held by another tenant is `409
CONFLICT` that says only that the id cannot be used, not that or by whom it is held. An
`idempotency_key` repeated with a different start (any of `agent_id`, `parent_run_id`,
`thread_id`, `user_id`, `workspace_id`, `on_behalf_of`, `input`, `deadline`,
`timeout_seconds`, `priority`, `concurrency_key`, `metadata`, `queue`; not `run_id`, which is minted when absent, nor
`agent_version`, which a retry from a newer deploy may change) is `409 CONFLICT` with
`details.differing` naming the fields, and no run. `agent_version` (optional) is kept as the
first start gave it: which code started the run. `on_behalf_of` must be a principal the key
may act as.

`deadline` (optional) is when the run must be done by. Nothing needs to watch it: within a
tick of it passing, the ticker ends a run that has not ended (`QUEUED`, `RUNNING` or
`PAUSED`: time in the queue and time waiting for a person count) as `TIMEOUT`, with the
error `{"code": "run_deadline", "category": "TIMEOUT", "retryable": false}`, announced as
`run.finished`. A worker still running it gets `409 LEASE_LOST` on its next heartbeat or
write and must stop. An interrupt's own `deadline` (below) is separate: it escalates or
ends one wait; the run's ends the run.

`timeout_seconds` (optional, > 0) is the most **working time** the run may take: the time
it spends `RUNNING`, across every attempt, not time queued or waiting for a person. The run
keeps it as `worked_seconds` (each `RUNNING` stretch added as it ends, the one going on
counted on every read), so a worker crash does not reset the clock. Within a tick of the
run working past its limit, the ticker ends it `TIMEOUT` with
`{"code": "run_timeout", "category": "TIMEOUT", "retryable": false}`, announced as
`run.finished`, and its worker is fenced off as for a deadline. The limit is
`timeout_seconds` or the operator's `RUNS__RUNS__MAX_RUN_SECONDS`, the lesser; with neither
there is none. Both a deadline and a limit may be set. Every lease answers the working time
left (`remaining_seconds`), so a worker stops in time (the SDK's `Worker` stops its handler
there and finishes the run `TIMEOUT` with the same `run_timeout` error).

`priority` (`-1000` to `1000`, default `0`) and `concurrency_key` (optional, 1–200
characters) say how a queued run waits its turn ([the claim](#post-v1runsclaim--200-claimed-or-204)):
higher priority first, and at most `RUNS__RUNS__CONCURRENCY_PER_KEY` (default 1) of the
tenant's runs sharing a key `RUNNING` at once, the others waiting `QUEUED`. A run recorded
`RUNNING` in its caller's process takes its key's place too (it runs), but is never held
back: only a claim waits.

### `POST /v1/runs/claim` → `200 Claimed` or `204`

```json
{"worker_id": "w-1", "agent_ids": ["triage", "billing"], "lease_seconds": 60}
```

`lease_seconds` is 5–3600 (default 60); `agent_ids` 1–100; `worker_id` 1–200 characters
(surrounding whitespace dropped, blank refused). Takes the next `QUEUED` run of those agents
in the tenant, sets it `RUNNING` and leases it. The next run is, among the available ones
(no retry's backoff holding it back) **with room**, the one with the highest `priority`,
then the oldest (`queued_at`). A run has room when fewer than
`RUNS__RUNS__CONCURRENCY_PER_KEY` (default 1) runs of its tenant sharing its
`concurrency_key` are `RUNNING`, and its tenant's workers hold fewer runs (`RUNNING` with a
lease) than `RUNS__RUNS__MAX_RUNNING_PER_TENANT` (unset: no cap). The room is counted again
under a transaction lock on the key (and on the tenant, when capped), so two claims at once
never both take the last place; a key or tenant another claim is counting at that moment is
passed over for this claim, as a locked row is.

**Fair share.** A platform key that sends no `X-Trellis-Tenant` claims from every tenant's
queue: the run is taken from the tenant whose workers hold the fewest runs of these agents
(then by priority and age), so a tenant takes more of a shared fleet only while no tenant
holding fewer has work waiting. Nothing is set for it. The claimed run names its tenant
(`run.tenant_id`); the worker's heartbeat, pause, finish and appends for it send that tenant
in `X-Trellis-Tenant` (the SDK's `Worker` does). Such a claim draws on the platform key's own
rate budget.

```json
{"run": {…RunRecord…}, "lease": {"run_id": "run_…", "worker_id": "w-1", "expires_at": "…"}}
```

`lease.remaining_seconds` is the working time the run has left (`null`: no limit), and
`lease.cancel_requested` is always `false` on a claim.

Claims use `SELECT … FOR UPDATE SKIP LOCKED`: concurrent claimers never receive the same run
and never wait on each other. A run held back by a retry's backoff is not claimed until it
has passed. `attempt` is not changed by a claim; it counts executions and was already
incremented when the run was put back on the queue. A lease nobody extends in time lapses:
within a tick the ticker puts the run back on the queue as the next attempt, claimable
after a short backoff (5 s, doubling per lapse, at most 1 min, jittered), and on its 5th
lapse (`MAX_LEASE_LAPSES`) ends it `ERROR` with code `lease_expired` instead. Lapses are
counted on their own, not by `attempt`, so answering a run many times never brings it closer
to failing. `204` means nothing is queued for those agents that may run now (none queued,
none available yet, or none with room); poll again later.

### `POST /v1/runs/{id}/heartbeat` → `200 Lease`

```json
{"worker_id": "w-1", "lease_seconds": 60,
 "checkpoint": {"tools": {"call_1": {"output": "PO-17 created"}}}}
```

Extends the lease to `now + lease_seconds`. **Progress checkpoints:** `checkpoint`
(optional) is saved on the run as it stands, replacing the one there (omitted: the run's
checkpoint is kept as it is), with the pause checkpoint's bound (`413 PAYLOAD_TOO_LARGE`
past 1 MiB of compact JSON, nothing saved). It comes back as `RunRecord.checkpoint` on the
next claim and on every read, so when this worker dies the next attempt resumes from it and
repeats no side effect the journal records. Only the lease holder saves one (`409
LEASE_LOST` otherwise, nothing saved); sending the same checkpoint again is harmless. A
worker saves progress after each side-effecting step, on the heartbeat it sends anyway. `409 LEASE_LOST` means the lease is no longer
this worker's (it lapsed and the run was re-queued, possibly claimed by another worker) or
the run is no longer `RUNNING` (it was cancelled or finished): **stop working the run and do
not write to it**. Heartbeat well inside the lease (every third of it).

The answer is the `Lease`:

```json
{"run_id": "run_…", "worker_id": "w-1", "expires_at": "…", "remaining_seconds": 412.5,
 "cancel_requested": false}
```

`remaining_seconds` is the working time the run has left (`null` without a limit): stop
before it runs out. `cancel_requested: true` means someone cancelled the run
([below](#post-v1runsidcancel--200-runrecord)): stop working it and finish it `CANCELLED`.
From the cancel on, the lease runs from the cancel, not from the heartbeat, so it is no
longer extended: when it runs out the ticker cancels the run itself.

### `POST /v1/runs/{id}/release` → `200 RunRecord`

```json
{"worker_id": "w-1", "checkpoint": {"tools": {"call_1": {"output": "PO-17 created"}}}}
```

A worker that is stopping lets go of a run it could not finish (the SDK's `Worker` does so
for the runs it still holds when its 25 s shutdown grace ends): `RUNNING → QUEUED` at once
as the next attempt (`attempt + 1`), for another worker, without counting a lapsed lease
(nothing crashed) and without a backoff. `checkpoint` (optional, the heartbeat's bound) is
saved first as the run's progress; absent, the run's checkpoint is kept. A run whose cancel
was asked for ends `CANCELLED` instead. Only the lease holder releases (`409 LEASE_LOST`
otherwise, nothing saved); the same worker's repeat answers the run as it is.

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

`interrupt.expects`, when given, must be a JSON Schema: one that is not is `422`, the detail
saying why (`expects is not a valid JSON Schema: …`), and the run is not paused. Answers are
checked against it on `resume`.

`RUNNING → PAUSED`. The interrupt is kept as `awaiting`; `assignee` and `deadline` are
indexed for the inbox and the escalation sweep. Announced as `run.paused`. The lease ends:
a worker pausing a run lets go of it.

`checkpoint` is any JSON object, opaque to the service: the executor's resume journal
(answered asks, completed tool outputs) and the framework's own resume state (a LangGraph
interrupt id, a serialized OpenAI `RunState`). It is returned as `RunRecord.checkpoint` on
every read, resume and claim, so whichever worker resumes the run repeats no side effect.
Each pause replaces it (omitted means `null`); any ending (`finish`, `cancel`, a `CANCEL`
answer, a `TIMEOUT`, a run past its deadline or its working-time limit, a run failed on its
`MAX_LEASE_LAPSES`-th lapsed lease) clears it; a requeue (a lapse, a release, a retried
error) keeps it for the next attempt. Larger than 1 MiB as compact JSON
(`MAX_CHECKPOINT_BYTES`) is `413`, and nothing changes. A heartbeat may save one too, as
progress (below).

`worker_id` (query, optional, also on `finish`) fences the write: when given, the write is
refused with `409 LEASE_LOST` unless that worker still holds the run's lease. Workers always
send it, so a worker whose lease lapsed cannot write over the run another worker has since
claimed.

**A repeated pause is not an error.** A pause of a run that is already `PAUSED` on the same
`interrupt_id` by the same caller (the same `worker_id`, or none both times) answers `200`
with the run as stored and changes nothing (no second `run.paused`): a worker that never saw
the answer to its pause retries it. Another interrupt, or another worker, is `409`.

### `GET /v1/runs/{id}/resolutions` → `200 [ResolutionEntry]`

Every interrupt the run paused on and how it was answered, oldest first, a page at a time:
`{"interrupt": Interrupt, "resolution": InterruptResolution, "attempt": 1, "recorded_at": "…"}`.
Append-only: `last_resolution` is only the latest of these. A row exists exactly when the
resume took effect; a refused resume (`403`, `409`, `422`) leaves none. `404` for a run the caller's
tenant does not hold.

### `POST /v1/runs/{id}/resume` → `200 RunRecord`

Body: an `InterruptResolution` for the interrupt the run waits on:

```json
{"interrupt_id": "int_…", "run_id": "run_…", "decision": "APPROVE",
 "answer": null, "reviewer": "user:alice", "payload": null}
```

`decision` is `ANSWER | APPROVE | REJECT | EDIT | CANCEL` (`EDIT` carries `payload`).
`comment` (optional, any decision) is the reviewer's remark, kept with the resolution.
`remember` is `once` (default) or `run`: an `APPROVE` of a tool call that also approves calls
like it for the rest of the run; the harness, which sees the calls, keeps that promise, and
this service keeps the record.
**The answer must fit the question** (`trellis.runs.answers`, checked before anything is
written): an `ANSWER` fits the interrupt's `expects`, or, with no `expects`, picks among its
`options` when it has some: one option's `value` (never its `label`), or with `multiple` a
list of distinct values. An answer the asker's own screen (`component`) collected is held to
the same. An `EDIT` of a question (no `tool_call`) carries a `payload` that fits `expects`;
`APPROVE`, `REJECT` and `CANCEL` carry nothing to check, and `remember: "run"` is refused for
an interrupt that has no tool call. A misfit is `422
VALIDATION` whose detail says what does not fit, and the run keeps waiting. The
resolution is kept as `last_resolution`, appended to the run's resolution history in the
same transaction (`GET /v1/runs/{id}/resolutions`), and:

- `CANCEL` ends the run: `PAUSED → CANCELLED`.
- any other decision continues it as the next attempt (`attempt + 1`):
  - a run that was ever queued (it came from the queue or a schedule) goes back to
    `QUEUED` and a worker claims it again, finding the answer in `run.last_resolution`;
  - a run recorded in process goes to `RUNNING`, for the process that resumes it.

**A retried resume is not an error.** The very same resolution sent again (every field,
`resolved_at` included: it is set once, when the answer is made, so a client's retry after
a lost answer sends it unchanged) answers `200` with the run as it is now, even after the
run moved on, and changes nothing: no second resolution, no event, no attempt.

`409` for any other answer to an interrupt already answered (a second click or a second
reviewer: its `resolved_at` differs), or when the run is not `PAUSED` or waits on a
different `interrupt_id`; `422` when `run_id` names another run or the answer does not fit
the question; `403` when the key may not answer it (below), checked first. Announced as `run.finished` only for `CANCEL`.

#### Who may answer a paused run

Checked before anything is written, against the run's `assignee` at that moment (after any
escalation); a `CANCEL` is an answer like any other. Principals compare as `kind:id`, a bare
id being a user's (`priya` is `user:priya`); `reviewer` is stored as given.

| The key ([`KeyInfo`](#authentication)) | May answer |
|---|---|
| `role` `admin` or `platform` | any run |
| `may_act_as` holds `"*"` (the Memory Service's default) | any run: the application vouches for the `reviewer` it names |
| any other (restricted to listed principals) | as one of them (`reviewer`; none is the key's own `principal`), and only a run assigned to that principal or to nobody; never a run assigned to a group (`role:…`, any kind but `user`, `agent`, `key`) |

The refusal is `403 AUTHORIZATION`, its detail one of:

```
this key may not act for user:raj; it may act only for user:priya
the run is assigned to user:raj, not user:priya; this key may act only for user:priya
the run is assigned to role:finance, a group: a key restricted to listed people cannot answer it; answer with the application's key or an admin key
```

Nothing else is restricted this way but cancelling (below): every key of the tenant reads
every run, lists every inbox (`assignee` is a filter, not a lock) and works the queue.

### `POST /v1/runs/{id}/cancel` → `200 RunRecord`

```json
{"reason": "the customer withdrew the request"}
```

Cancels the run, whatever its status, keeping `reason` (optional, at most 1000 characters)
and the principal of the key that asked with it:

- `QUEUED` or `PAUSED`, or `RUNNING` with no worker holding it (a run kept in its caller's
  process): `CANCELLED` at once, announced as `run.finished`; the checkpoint is cleared and
  the artifacts' retention starts. The caller's process gets `409` on its next write.
- `RUNNING` and held by a worker: the run stays `RUNNING` with its cancel asked. The
  worker's next heartbeat answers `cancel_requested: true` and no longer extends the lease;
  the worker stops and finishes the run `CANCELLED` (the SDK's `Worker` cancels the handler
  and does it). If the run is still running when the lease runs out (the worker died or
  ignored it), the ticker cancels it, never requeues it. A pause or a release of it ends it
  `CANCELLED` too, and a retryable `ERROR` is not retried.
- Ended: `409 CONFLICT`, except a repeat (below).

Who may cancel: a key that may answer the run ([who may](#who-may-answer-a-paused-run)),
answering as any principal it may act for, checked against the run's assignee now (a run
that is not paused is assigned to nobody); `403` otherwise, before anything is written. A
cancel already asked of a worker answers the run as it is (the first reason stands), and so
does a cancel of a run this principal already cancelled for the same reason (a retried
request).

### `POST /v1/runs/{id}/finish?worker_id=` → `200 RunRecord`

```json
{"status": "ERROR", "output": null,
 "error": {"code": "ToolFailed", "category": "TOOL", "message": "…", "retryable": false}}
```

`status` must be an ending (`SUCCESS | PARTIAL | ERROR | TIMEOUT | CANCELLED | REJECTED`);
`error` only on `ERROR | TIMEOUT | REJECTED`. From `RUNNING` any ending; from `QUEUED` or
`PAUSED` only `CANCELLED` or `TIMEOUT` (cancelling a queued or waiting run). Announced as
`run.finished`.

**A retryable error is retried, later.** A run that came from the queue (a worker runs it)
and is finished `ERROR` with `error.retryable: true` does not end: it goes back to `QUEUED`
as the next attempt (`attempt + 1`), claimable after a jittered backoff (10 s, then 20 s,
then 40 s; `MAX_ERROR_RETRIES` 3 times at most), and the answer is the requeued run; no
event is announced. The next such error after the third retry stands, and so does any error
once the run was retried, its message ending `(after 2 of 3 retries)`. A run kept in its
caller's process is never retried here, nor one whose cancel was asked for. The worker's
repeat of such a finish answers the requeued run.

**A repeated finish is not an error.** A finish of a run that already ended with the same
`status`, by the same caller (the same `worker_id`, or none both times), answers `200` with
the run as stored (the first `output` and `error`) and changes nothing (no second
`run.finished`). A different `status` is `409 CONFLICT` (`409 LEASE_LOST` when a
`worker_id` was sent); another worker is `409 LEASE_LOST`.

### Reads

- `GET /v1/runs/{id}` → `RunRecord`, the full record (input, output, error, checkpoint,
  `worked_seconds` counting the stretch it is running now).
- `GET /v1/runs?status=&assignee=&agent_id=&thread_id=&parent_run_id=&top_level=&cursor=&limit=` →
  `[RunSummary]`, newest first, paged (`Link`). The inbox is
  `status=PAUSED&assignee=role:procurement`; `top_level=true` keeps only runs with no
  `parent_run_id`, so a paused sub-agent is not listed next to the parent that waits on it.

```json
[{"run_id": "run_…", "agent_id": "triage", "status": "PAUSED",
  "awaiting": {…Interrupt…}, "assignee": "role:procurement",
  "deadline": null, "updated_at": "2026-09-30T08:00:00Z"}]
```

`awaiting` is the interrupt a paused run waits on (`null` otherwise; its own `deadline` is
when the answer is due), `assignee` whose inbox it is in, `deadline` the run's own deadline
(`RunStart.deadline`). Nothing else is in a summary; read the run for the rest.

### Events

A run's events (`RunEvent`: AG-UI's vocabulary plus `CONTEXT_LOADED` and `INTERRUPT`) are
kept in the run's log, so any replica serves any run's events.

- `POST /v1/runs/{id}/events?worker_id=` with `{"events": [RunEvent, …]}` (1–500) →
  `{"appended": 2, "position": 7}`. Only while the run is `RUNNING`, fenced as a heartbeat
  is: by the worker holding its lease (`worker_id`), or, when no worker holds it (a run in
  its caller's process), by a call that names none; `409` otherwise (`LEASE_LOST` for a
  worker). Each event must name this run and tenant (`422`). Each takes the next `position`
  (1, 2, …), assigned under the run's row lock, so positions are never committed out of
  order. An event already logged (the same `attempt` and `sequence`) is not added again: a
  retried append is safe. Append before pausing or finishing: the log takes nothing after.
- `GET /v1/runs/{id}/events?after=&limit=` → `[{"position": 3, "event": RunEvent}]`, the
  events past `after` (default 0), oldest first.
- `GET /v1/runs/{id}/events/stream?after=` → `text/event-stream`: the events past `after`
  (or the `Last-Event-ID` header, whichever is larger), then each new one as it is appended
  to any replica, as `id: <position>`, `event: <type>`, `data: <the entry as JSON>`. Once
  the run has ended and its last event was sent: `event: end` with `data: {"status": …}`, and
  the stream closes. A paused run's stream stays open. A comment (`: keepalive`) every 15 s
  keeps idle connections open; after 5 minutes the service closes the stream without `end`,
  and the client reconnects with the last position (an `EventSource` does on its own; the
  SDK's `stream_events` does). Each look for new events is a short read of its own, every
  0.5 s: a stream holds no database connection while it waits.

The log is deleted with its run ([run retention](#run-retention)).

## Artifacts

A payload too large for a checkpoint or an interrupt (an `ask` table, a diff, a report) is
uploaded as a **run artifact**: its bytes go to blob storage (`RUNS__BLOB__PROVIDER`:
filesystem or GCS), its record to PostgreSQL, and the run carries only the returned
`ArtifactRef`, typically as `Interrupt.payload_ref`. Checkpoints stay small.

### `POST /v1/runs/{id}/artifacts?worker_id=&checksum=` → `201 ArtifactRef` (`200` for a repeat)

The body is the artifact's raw bytes; `Content-Type` is its mime type (`application/json`
for an `ask` table; `application/octet-stream` when absent). At most 50 MiB
(`MAX_ARTIFACT_BYTES`), `413` past it, counted as the body arrives when there is no
`Content-Length`; an empty body is `422`. The bytes are streamed to the blob store as they
arrive (to a temporary file, or a spool for GCS), never held whole in memory; a refused or
broken upload leaves nothing behind. `checksum` (optional, `sha256:<hex>`) is what the
caller computed: `422` unless the bytes that arrived match.

```json
{"artifact_id": "art_…", "type": "blob", "uri": "/v1/artifacts/art_…",
 "mime_type": "application/json", "checksum": "sha256:9f86d0…", "size_bytes": 48213,
 "created_at": "2026-09-30T08:00:00Z", "metadata": {"run_id": "run_…"}}
```

Who may add one:

- the run is `RUNNING`: a leased run (claimed from the queue) only with its lease holder's
  `worker_id` (`409 CONFLICT` without it, `409 LEASE_LOST` with another worker's); a run
  recorded in the caller's process has no lease and takes no `worker_id` (`409 LEASE_LOST`
  with one);
- the run is `PAUSED`: only a key whose registry role is `service` (a harness, or the UI
  backend uploading a reviewer's corrected table); any other key is `403`;
- any other status: `409` (`LEASE_LOST` when a `worker_id` was sent, else `CONFLICT`). An
  unknown run (or another tenant's) is `404`.

The same bytes uploaded to the same run again return the first artifact with `200` (a
retried upload stores nothing twice).

### `GET /v1/artifacts/{artifact_id}` → `200` the bytes

Streams the bytes with the artifact's `Content-Type`, `Content-Length`,
`ETag: "sha256:<hex>"` and `Cache-Control: private, max-age=31536000, immutable` (an
artifact never changes under its id). `If-None-Match` naming the ETag (or `*`; weak
comparison) is `304` with no body. The bytes are verified against the recorded SHA-256 as they are
read, and the last chunk is held back until they match: corrupted bytes are `500` (or a
response cut short), never served whole. `404` for another tenant's artifact, or one
already deleted.

### Retention

When a run ends (`finish`, `cancel`, a `CANCEL` answer, a `TIMEOUT`, `ERROR` after
`MAX_LEASE_LAPSES`), its artifacts get `expires_at = ended + 7 days` (`ARTIFACT_RETENTION`);
until then they are still readable. The ticker deletes each expired artifact's blob, then its record; a blob
that cannot be deleted keeps its record for the next tick.

### Run retention

Runs are kept forever unless the operator sets `RUNS__RUNS__RETENTION_DAYS`. Then the
ticker deletes the runs that ended (`SUCCESS`, `PARTIAL`, `ERROR`, `TIMEOUT`, `CANCELLED`,
`REJECTED`) longer ago than that, with their resolutions and their event log; a run whose
artifacts are still kept is deleted after them. A deleted run is `404` like any other.

## Schedules

A schedule fires runs as `on_behalf_of`, the person who set it, while nobody is present.
Creating one needs a key that may act as that principal; editing, pausing, resuming, firing
or deleting one needs the same of the schedule's stored `on_behalf_of` (`404` before `403`).

### `POST /v1/schedules` → `201 Schedule` (`200` for an existing one)

Body: a `ScheduleSpec` (`created_by` is refused; it is the key's principal):

```json
{"tenant_id": "acme", "agent_id": "briefing", "name": "morning briefing",
 "cadence": "0 8 * * 1-5", "timezone": "Europe/Berlin", "on_behalf_of": "user_ada",
 "input": {"topic": "inbox"}, "workspace_id": null, "enabled": true,
 "timeout_seconds": 600, "agent_version": "2026.10.05-3f2a1c", "priority": 0,
 "concurrency_key": null, "metadata": {}}
```

`timeout_seconds`, `agent_version`, `priority` and `concurrency_key` (optional) are copied
into the `RunStart` of every run the schedule fires: its working-time limit, which code set
the schedule up, and how its runs wait their turn on the queue (as on `POST /v1/runs`).
`metadata` is copied into each fired run's `metadata`, under the fire's own keys
(`schedule_id`, `schedule_name`, `fire_time`, `created_by`), which win on conflict; a
scheduled run so carries whatever a caller keeps there for a started one.

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

### `GET /v1/schedules?enabled=&agent_id=&cursor=&limit=` · `GET /v1/schedules/{id}` · `DELETE /v1/schedules/{id}` (`204`)

The listing is this tenant's schedules, newest first, paged (`Link`), filtered by `enabled`
and `agent_id` when given. Any key of the tenant may list and read a schedule;
deleting one needs a key that may act as its `on_behalf_of` (`404` before `403`).

### `PATCH /v1/schedules/{id}` → `Schedule`

Any of `agent_id, name, cadence, timezone, input, workspace_id, enabled, timeout_seconds,
agent_version, priority, concurrency_key, metadata` (`metadata` is merged; `null` removes a
limit, a version or a concurrency key; `priority` takes a number, not `null`). `tenant_id` and `on_behalf_of` are refused (`422`). Changing the
cadence or zone re-arms the next fire. A change that would give the schedule the identity of
another one is `409`.

Pausing and resuming are `PATCH`es: `{"enabled": false}` pauses; `{"enabled": true}` on a
paused schedule resumes it, clearing an auto-pause (failure count, last error, backoff) and
firing from the next occurrence it can still honour, never the backlog.

### `POST /v1/schedules/{id}/fire` → `FireResult`

Body (optional): `{"at": "2026-10-01T06:00:00Z"}`, an instant that has arrived (`422` for one
more than a minute ahead, or without an offset). Without it, the fire is for the tick the
schedule is due for, else for now. Queues the run (`QUEUED`, `idempotency_key =
"<schedule_id>@<fire_time UTC ISO>"`, the schedule's limits, version, priority and
concurrency key, and its metadata under `schedule_id`, `schedule_name`, `fire_time`,
`created_by`) and advances the schedule:

```json
{"schedule_id": "sch_…", "run_id": "run_…", "fire_time": "…",
 "idempotency_key": "sch_…@2026-10-01T06:00:00+00:00", "schedule": {…Schedule…}}
```

A repeat for the same instant returns the same run. `409` when the schedule is paused.
`503 DEPENDENCY_UNAVAILABLE` when the run could not be queued; `details` says what the
schedule did about it (`retryable` is false once the schedule paused itself):

```json
{"type": "urn:trellis:problem:dependency-unavailable", "status": 503,
 "code": "DEPENDENCY_UNAVAILABLE", "detail": "schedule sch_… could not queue its run",
 "retryable": true, "details": {"consecutive_failures": 1, "auto_paused": false,
 "error": {…AgentError…}}, …}
```

## Webhooks

Notifications are **tenant subscriptions**: a URL and the run events it wants. There is no
per-run or per-schedule URL (`webhook_url` in a `RunStart` or `ScheduleSpec` is `422`).

### `POST /v1/webhooks` → `201 WebhookCreated`

```json
{"url": "https://ui.example/hooks/trellis", "events": ["run.paused", "run.finished"]}
```

`events` is a non-empty subset of `run.paused`, `run.escalated`, `run.finished` (duplicates
dropped, answered sorted). `url` is absolute `https` (`http` too when
`RUNS__SERVICE__ENVIRONMENT=dev`), else `422`. Its host must resolve, and only to public
addresses: not private, loopback, link-local (cloud metadata), carrier-grade NAT, reserved or
multicast, an IPv4-mapped IPv6 address judged as its IPv4 one; else `422` saying which
address. The check is skipped where private targets are allowed
(`RUNS__WEBHOOKS__ALLOW_PRIVATE_TARGETS=true`, and in `dev` unless it is `false`), and made
again for every delivery, which then connects only to an address it checked (below). At most
20 subscriptions per tenant (`409`).

```json
{"webhook_id": "wh_…", "url": "https://ui.example/hooks/trellis",
 "events": ["run.finished", "run.paused"], "created_by": "user_ada",
 "created_at": "2026-09-30T08:00:00Z", "secret": "whsec_…"}
```

`secret` signs every delivery to this subscription. **It is in this answer only** (and in a
rotation's); a lost or leaked secret is replaced by rotating it.

### `GET /v1/webhooks?cursor=&limit=` → `[Webhook]` · `GET /v1/webhooks/{id}` → `Webhook` · `DELETE /v1/webhooks/{id}` → `204`

The listing and the read are the same shape without `secret`; the listing is oldest first,
paged (`Link`). Deleting drops the
deliveries still owed to the subscription, and its dead ones. Another tenant's id is `404`.
`previous_secret_expires_at` is, after a rotation, when the replaced secret stops signing
(`null` before any rotation).

### `POST /v1/webhooks/{id}/rotate-secret` → `200 WebhookCreated`

No body. A new secret for the subscription, in this answer only. For
`RUNS__WEBHOOKS__SECRET_OVERLAP_HOURS` (24) from now, every delivery carries a signature with
each secret, `X-Trellis-Signature: t=<t>,v1=<hex with the new>,v1=<hex with the old>`, and
`verify_signature` accepts any matching `v1`: update the receiver's secret within that window
and no delivery is refused. A rotation within the window replaces the older secret at once
(only the last two sign). `0` hours signs with the new secret only.

### `GET /v1/webhooks/deliveries?state=&webhook_id=&cursor=&limit=` → `[DeliveryRecord]`

This tenant's deliveries, newest first, paged (`Link`): `state=pending` the ones still
owed, `state=dead` the ones given up on, `webhook_id` one subscription's.

```json
[{"delivery_id": "dlv_…", "webhook_id": "wh_…", "event_id": "whd_…", "type": "run.finished",
  "run_id": "run_…", "state": "dead", "attempts": 7, "last_error": "answered 503",
  "next_attempt_at": null, "dead_at": "…", "created_at": "…"}]
```

A delivery dies when it used its 7 attempts, or at once when it was refused for good (an
answer other than `2xx`, `408`, `429` or `5xx`; an `http` URL outside dev; a host that now
resolves to a private address). `last_error` says what the last attempt met (`answered 503`,
`unreachable: …`, `refused: …`). Dead deliveries are kept for
`RUNS__WEBHOOKS__DEAD_RETENTION_DAYS` (7), then the ticker drops them.

### `POST /v1/webhooks/deliveries/{id}/redeliver` → `200 DeliveryRecord`

No body. Owes a dead delivery again: `pending`, due now (sent within a tick), with all its
attempts ahead of it, signed with the subscription's secrets as they are now; the event and
its `event_id` are unchanged. A delivery still owed is `409` (it is being tried already);
another tenant's is `404`.

### Events and delivery

| Event | When |
|---|---|
| `run.paused` | a run pauses (`/pause`) |
| `run.escalated` | the ticker moves an overdue interrupt to `escalate_to` |
| `run.finished` | a run ends: `/finish` (not a retried `ERROR`), `/cancel`, a `CANCEL` answer, an interrupt `TIMEOUT`, a run past its own `deadline` or its working-time limit, a lease lapsed `MAX_LEASE_LAPSES` times, a cancelled run's lease running out, a pause or release of a run whose cancel was asked |

The event is written to an outbox in the same transaction as the run change, one row per
subscription of the tenant that wants it, and the ticker sends it (so within one tick,
5 s): `POST <url>` with

```json
{"event_id": "whd_…", "type": "run.paused", "tenant_id": "acme", "workspace_id": null,
 "occurred_at": "…", "data": {"run": {…RunSummary…}}}
```

and headers `X-Trellis-Event: <type>`, `X-Trellis-Delivery: <event_id>`,
`X-Trellis-Signature: t=<unix seconds>,v1=<hex hmac-sha256 keyed by the subscription's
secret over "<t>.<raw body>">` (and a second `v1=` keyed by the replaced secret during a
rotation's overlap), made by the SDK's `trellis.runs.webhooks.sign`. A receiver
checks it over the raw bytes with `trellis.runs.webhooks.verify_signature(secret, header,
body)` (it refuses a timestamp more than 300 s from its clock and compares in constant time)
and reads the body with `parse_delivery`. `event_id` is the same on every
retry. A `2xx` accepts; `408`, `429`, `5xx` and an unreachable receiver are retried (7
attempts, 15 s doubling to at most 10 min); any other answer is final, a redirect (`3xx`)
included: it is never followed. Where private targets are not allowed, each attempt
resolves the host once, refuses the delivery for good when any address is not public, and
connects only to the addresses it checked, in order (the next when one refuses the
connection), with `Host`, TLS SNI and the certificate check naming the host: a name that
resolves elsewhere a moment later (DNS rebinding) is never reached. A delivery given up on
is kept, dead, to be redelivered (above). At least once: receivers drop repeats by
`event_id` and read the run for anything the summary lacks.

## Ops

`GET /health/live` (asks no dependency) · `GET /health/ready` (the database answers within
3 s; else `503`) · `GET /metrics` (Prometheus: `runs_http_requests_total` and
`runs_http_request_seconds` by method, route template and status, `runs_claims_total` by
outcome, `runs_rate_limited_total`, `runs_db_pool_connections` by state). The ticker serves
`runs_ticker_ticks_total` (by outcome), `runs_ticker_swept_total` (by step: `fired`,
`timed_out`, `overworked`, `requeued`, `escalated`, `sent`, `dropped`, `purged`),
`runs_webhook_dead_total` (deliveries given up on) and the pool gauges on `RUNS__TICKER__METRICS_PORT` when it is set. Each process has its own registry: with
several workers a scrape sees one worker's share. The ticker's probe is
`python -m agent_runs.heartbeat`, which reads
`RUNS__TICKER__HEARTBEAT_FILE` (set per ticker; compose sets it in the ticker container).
