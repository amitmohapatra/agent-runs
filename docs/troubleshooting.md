# Troubleshooting and FAQ

Every error is an RFC 9457 problem with a stable `code`; the SDK raises each as its own class.
The full table of statuses and codes is in [api.md](api.md#errors). Here is what each usually
means in practice, and what to do.

## Problem codes

| Status, `code` | SDK error | Usual cause | What to do |
|---|---|---|---|
| `401 AUTHENTICATION` | `AuthenticationError` | no `X-API-Key`, or one the Memory Service does not know (revoked, expired, mistyped) | set `TRELLIS_API_KEY` (or `RunsClient(api_key=)`) to a key the Memory Service issued; check `RUNS__MEMORY__URL` points at the registry that issued it |
| `400 VALIDATION` (no tenant) | `ValidationError` | a platform key sent no `X-Trellis-Tenant` | pass `tenant=` to the call or `RunsClient(tenant=)` |
| `403 AUTHORIZATION` | `AuthorizationError` | the tenant is suspended; the body names another tenant; `on_behalf_of` is someone the key may not act for; an answer or cancel the key may not give; an artifact for a paused run from a non-`service` key | the detail says which. For answers, see [who may answer](../README.md#who-may-answer-a-paused-run) |
| `404 NOT_FOUND` | `NotFoundError` (reads by id answer `None`) | the record is in another tenant, or was purged by retention | check the key's tenant; `RUNS__RUNS__RETENTION_DAYS` deletes ended runs |
| `409 LEASE_LOST` | `LeaseLostError` | the worker's lease lapsed (no heartbeat within it), or the run was cancelled, paused, finished, or ended past its deadline or working time | **stop working the run**; the SDK's `Worker` cancels the handler. Heartbeat every third of the lease; raise `lease_seconds` for long steps |
| `409 CONFLICT` | `ConflictError` | an illegal transition (finishing a run that ended), a second answer to a question, a cancel of an ended run, an idempotency key reused with another start, a fire of a paused schedule, a 21st webhook, a redelivery of a delivery still owed | read the run (`GET /v1/runs/{id}`): it has moved on. A resend of the very same resolution or finish is not a conflict |
| `413 PAYLOAD_TOO_LARGE` | `PayloadTooLargeError` | a body past 4 MiB, a run's `input` or `output` past 1 MiB, a `checkpoint` past 1 MiB, an artifact past 50 MiB | put large payloads in an artifact (`artifacts.upload`) and pass its `ArtifactRef` |
| `422 VALIDATION` | `ValidationError` | the body fails the contracts' validators; an answer that does not fit the interrupt's `expects` or options; a pause whose `expects` is no JSON Schema; a webhook URL this deployment does not deliver to | the detail says what does not fit; the run keeps waiting for a good answer |
| `429 RATE_LIMIT` | `RateLimitedError` | the tenant's request budget is spent | the SDK waits `Retry-After` and retries; raise `RUNS__RATE_LIMIT__PER_MINUTE` or `BURST` |
| `503 DEPENDENCY_UNAVAILABLE` | `DependencyUnavailableError` | PostgreSQL did not answer (or no pooled connection was free in time, or a statement passed its timeout), or the Memory Service could not be asked | the SDK retries; check the database and `RUNS__MEMORY__URL`, and `RUNS__DATABASE__CONNECTION_BUDGET` under load |
| `500 INTERNAL` | `RunsError` | a fault here, such as an artifact whose bytes no longer match their checksum | the response's `request_id` finds it in the logs |

## Symptoms

**The API or the ticker refuses to start: the database is not at the head revision.** Run
`make migrate` (`alembic upgrade head`) against `RUNS__DATABASE__URL` first. `make up` runs
the migration as a one-shot service.

**`RUNS__BLOB__BUCKET is required in 'prod'`.** Outside `dev` and `test` artifacts need a
GCS bucket: set `RUNS__BLOB__BUCKET` (the filesystem blob store is for a single machine).

**A queued run is never claimed.** One of these:

* no worker claims its `agent_id` (`Worker(..., ["the-agent-id"])`);
* its `available_at` has not passed: it is waiting out a retry backoff (5 s doubling after a
  lapse, 10 s doubling after a retryable error);
* another run with the same `concurrency_key` is `RUNNING` (`RUNS__RUNS__CONCURRENCY_PER_KEY`,
  default 1);
* the tenant holds `RUNS__RUNS__MAX_RUNNING_PER_TENANT` runs already.

A claim then answers `204`.

**A run went back to `QUEUED` by itself.** Its worker's lease lapsed (the worker died or did
not heartbeat in time), it was released by a stopping worker, or it finished `ERROR` with a
retryable error and is being retried (up to 3 times). `attempt` counts these.

**A run ended `ERROR` with `lease_expired`.** Its lease lapsed five times
(`MAX_LEASE_LAPSES`): something kills its worker. Look at the worker's logs for that run.

**A run ended `TIMEOUT` with `run_deadline` or `run_timeout`.** Past its `deadline` (queue and
waiting time count), or past its working-time limit (`timeout_seconds`, or
`RUNS__RUNS__MAX_RUN_SECONDS`; only time `RUNNING` counts).

**A paused run timed out or changed assignee.** The interrupt's `deadline` passed: the ticker
moves it to `escalate_to` once, or ends it `TIMEOUT` when there is none.

**A cancelled run is still `RUNNING`.** A worker holds it: the cancel is delivered through its
next heartbeat (`cancel_requested`), and the SDK's `Worker` ends it `CANCELLED`. If the worker
is gone, the ticker ends it when the lease runs out.

**Webhooks never arrive.** In order:

1. Is the ticker running (`python -m agent_runs.heartbeat` with `RUNS__TICKER__HEARTBEAT_FILE`)?
2. Is the subscription for that event (`run.paused`, `run.escalated`, `run.finished`)? A resume
   that continues a run, and a requeue, send nothing.
3. List `GET /v1/webhooks/deliveries?state=dead`: `last_error` says why (`answered 404`, a
   private address, a host that does not resolve), and `redeliver` sends it again.
4. Outside `dev`, a URL must be `https` and resolve to public addresses only, unless
   `RUNS__WEBHOOKS__ALLOW_PRIVATE_TARGETS=true`.

**`verify_signature` returns `False`.** Verify the raw body bytes before any JSON parsing, with
the subscription's secret; the receiver's clock must be within 5 minutes. After a rotation
either secret verifies for the overlap (24 h by default).

**The event stream stops after five minutes.** By design (`EVENT_STREAM_SECONDS`); the SDK's
`stream_events` reopens from the last position, on any replica.

**The tests or the examples skip or fail with "no PostgreSQL".** They need the admin URL
(`RUNS_TEST_ADMIN_URL`, default `postgresql://memory:memory@localhost:5432/postgres`) to
create their own databases. `make up` publishes PostgreSQL on port 5442, so point the
variable there, or use a local PostgreSQL on 5432.

**`docs/openapi.json` differs in CI.** A route or a contracts model changed: run
`make openapi` and commit the document.

## FAQ

**Does agent-runs run my agent?** No. A harness or your own worker runs it; this service keeps
the record, the queue, the inbox, the schedules and the webhooks.

**Can I use it without the harness?** Yes, with `trellis.runs` (Way 2): see the
[SDK's README](../sdk/python/README.md) and the [examples](../examples/README.md).

**Does `assignee` stop others from seeing a paused run?** No. Every key of the tenant reads
every run; `assignee` is what an inbox filters on, and what a restricted key may answer.

**Are webhooks exactly once?** At least once. Drop repeats by `event_id`.
