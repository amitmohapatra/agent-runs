# Changelog

What changed in each version of `agent-runs` and its SDK `trellis-runs` (they are versioned
together), and what a caller must do about it. Which versions of the other Trellis repos go
with which: [docs/versioning.md](docs/versioning.md). Why: [docs/adr/](docs/adr/README.md).

## Unreleased

* **Run payloads are written by reference.** A run's input, output, the payload its pause
  waits on, its answers and its checkpoint go to the blob store before any row refers to
  them; reads answer the same JSON (a webhook's body the same bytes). A checkpoint may be a
  list of segments the worker uploads (`POST /v1/runs/{run_id}/payloads`, read back with
  `GET /v1/payloads/{sha256}`; SDK `artifacts.put_payload` / `get_payload`), so a save sends
  only what changed. An ended run's events become one object (the ticker). Artifact retention
  is a setting, `RUNS__BLOB__RETENTION_DAYS` (7, as before). Old rows keep working; nothing
  to do. Set `RUNS__RUNS__RETENTION_DAYS` to bound what is kept.
* **Run payloads can be read by reference (expand step).** Every payload column gets a
  nullable `*_ref` (a `PayloadRef` to a zstd-compressed JSON object in the blob store, keyed by its
  SHA-256 under the tenant, hash-sharded as the Memory Service's archive is), and every reader (a run, the listing, resolutions, events, a
  webhook's body) takes either, answering the same JSON. Nothing writes references yet. A
  ticker sweep deletes payload objects no row refers to once a day old. Migration
  `a7c1d2e3f4b5` (adds columns and two tables). SDK: `trellis.runs.payloads` (the format).
  Nothing to do; roll back by redeploying the previous release.
* **One run of a schedule at a time, spread over the minute.** A tick while the schedule's
  last run is queued, running or paused is skipped (the schedule waits for its next
  occurrence), so workers down across several ticks run it once on recovery. Each schedule
  fires a stable 0 to 59 s after its tick (a hash of its id), so `next_fire_at` carries those
  seconds. Nothing to do.
* **A schedule set from a chat fires into that chat.** A schedule whose `metadata` names a
  `thread_id` (a string) queues each run with that `thread_id`, so the run is a turn of the
  conversation (its reply in the thread's history, its runs in `GET /v1/runs?thread_id=`).
  `RunSummary.thread_id` (listings and the webhook envelope) says which conversation a run is
  a turn of, so one listing serves many open chats. `GET /v1/schedules?on_behalf_of=` lists
  the schedules acting for one person (SDK: `schedules.list(on_behalf_of=)`). Nothing to do.
* **Listing one thread's runs, or one user's, reads an index.** `GET /v1/runs?thread_id=` (a
  conversation's turns, its scheduled runs among them) was served by `ix_runs_tenant_created`
  filtering every newer run of the tenant, or by a scan for an old thread: at 100k runs a
  sequential scan (9.5 ms). `ix_runs_thread` (tenant, thread, `created_at`) serves it in
  0.15 ms; `ix_runs_user` does the same for a user's runs (0.19 ms for 22k rows filtered),
  the listing the `background-agents` branch adds.
  *Migration:* `alembic upgrade head` (`2c8d3e4f5a6b`: two indexes; `downgrade` drops them).
* **The run inbox and a run's usage moved to the `background-agents` branch** (#20 reverted
  from `main`, never released): `POST`/`GET /v1/runs/{id}/messages`, `RunRecord.usage` and
  `GET /v1/runs?user_id=` continue there with the harness's background agents.
  **Requires `trellis-contracts>=0.6.2,<0.8`**: 0.7.0, the contracts without the run inbox, is
  admitted. *Migration:* a database that ran `1b7c2d3e4f5a` goes back first
  (`alembic downgrade 0a6b1c2d3e4f`, with the code of #20), then runs this release.
* **SDK `Worker`: one attempt of a run at a time, and none past its lease.** A handler whose
  lease no heartbeat could renew for the lease's length (agent-runs unreachable) is stopped as
  for a lost lease, writing nothing more: agent-runs may already have given the run to another
  worker. A run the worker claims again while it still works the earlier attempt stops that
  attempt first (both name the worker, so the service's fence cannot tell them apart). Before,
  both attempts went on side by side: with agent-runs down 75 s, a run's writes were done twice
  (14 of 25, live). Nothing to do.

* **Requires `trellis-contracts>=0.6.2`**, whose `ErrorSource` has `acp`: the harness's
  adapter for agents spoken to over the Agent Client Protocol (Kimi Code) records its failed
  runs under it, and a run stores that error as it is. `docs/openapi.json` lists the value.
  *Migration:* none; install the 0.6.2 contracts with this release.

Breaking configuration changes: three kinds of setting are gone, because the service can work
them out or because they were never a deployment's choice. A variable left set is ignored, not
an error; set the replacement where one is named.

* **The blob store follows `RUNS__BLOB__BUCKET`.** `RUNS__BLOB__PROVIDER` is removed:
  artifacts go to GCS exactly when `RUNS__BLOB__BUCKET` is set, and to the directory
  `RUNS__BLOB__ROOT` otherwise (still `dev` and `test` only: elsewhere `Settings` refuses to
  start without a bucket, naming `RUNS__BLOB__BUCKET`). *Migration:* drop
  `RUNS__BLOB__PROVIDER`; keep `RUNS__BLOB__BUCKET` for GCS. A deployment that set a bucket
  with `RUNS__BLOB__PROVIDER=filesystem` now writes to that bucket: unset the bucket to stay
  on the filesystem.
* **One connection budget replaces the pool settings.** `RUNS__DATABASE__POOL_SIZE` and
  `RUNS__DATABASE__MAX_OVERFLOW` are replaced by `RUNS__DATABASE__CONNECTION_BUDGET`, the
  connections one container may open across all its processes (the Memory Service's design):
  each process (an API worker; the ticker is one) takes `budget // processes`, half kept open
  and half for a burst. Unset, a process opens 10 + 10, as before. *Migration:* drop both;
  to keep a custom size set `CONNECTION_BUDGET` to `(POOL_SIZE + MAX_OVERFLOW) × workers`.
* **The pool's protections are constants.** `RUNS__DATABASE__POOL_PRE_PING` (always on),
  `POOL_TIMEOUT_SECONDS` (5), `POOL_RECYCLE_SECONDS` (300), `CONNECT_TIMEOUT_SECONDS` (5)
  and `STATEMENT_TIMEOUT_MS` (15000) are removed; their values are the `DB_*` constants in
  `config/constants.py`, unchanged. *Migration:* drop them.

## 0.4.1 (2026-10-06)

* **SDK `Worker`: a handler that honours `job.remaining_seconds` ends its own run.** The
  worker's working-time clock fired at the same instant as a handler's own timeout on
  `remaining_seconds`, and was set first, so the handler's ending (its `TIMEOUT`, its events)
  was always cancelled midway and the worker's `run_timeout` stood instead. The worker now
  waits `WORKING_TIME_GRACE_SECONDS` (1 s) past the working time before it stops a handler:
  one that bounds itself by `remaining_seconds` finishes the run itself in that second, and
  one that ignores it is still stopped and the run ended `TIMEOUT` (`run_timeout`), a second
  later than before. The service is unchanged; nothing for a caller to do.
* **Schedules carry everything a started run can** (trellis-contracts 0.6.1, its ADR 0007).
  A schedule also takes `priority` and `concurrency_key`, copied into every run it fires,
  and its `metadata` goes into each fired run's metadata under the fire's own keys
  (`schedule_id`, `schedule_name`, `fire_time`, `created_by`), which win on conflict. A
  `PATCH` changes them like any other field. Requires `trellis-contracts>=0.6.1`.
* **Documentation.** A "Start here" README; the run lifecycle, the ticker and webhook
  delivery live once, in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), which gains the
  five-repo view and sequence diagrams for claim, lease and heartbeat, pause and resume with
  the answer check, cancel and release, webhook delivery and dead letters, and run events
  over SSE; [configuration](docs/configuration.md), [troubleshooting](docs/troubleshooting.md),
  [versioning](docs/versioning.md), this changelog (the version notes moved here from
  `docs/api.md`) and four [ADRs](docs/adr/README.md).
* **Examples.** Nine scripts in [examples/](examples/README.md) run the real app, ticker and
  SDK in process (`make examples`); CI runs them, and a link check (`make links`).

## 0.4.0 (2026-10-05)

Adds to the wire (trellis-contracts 0.6, its ADR 0006):

* Interrupts offer labelled options (`{value, label, description}`, plain strings still
  valid), several picks (`multiple`), form widget hints (`ui_schema`) and the asker's own
  screen (`component`, `props`), and every answer is checked against them. A resolution
  carries a `comment` and how far an approval reaches (`remember`).
* A run has a `priority` and a `concurrency_key` that a claim honours; a platform key may
  claim from every tenant (a fair share); the operator may cap a tenant's running runs
  (`RUNS__RUNS__MAX_RUNNING_PER_TENANT`).
* A run's events are kept and served from any replica (`POST`/`GET /v1/runs/{id}/events`,
  `GET …/events/stream`). `GET /v1/runs` takes `top_level`.
* A schedule's `timeout_seconds` and `agent_version` go into every run it fires.
* The rate limit is one budget per tenant, shared by every replica (in PostgreSQL), and an
  operator may set a run retention (`RUNS__RUNS__RETENTION_DAYS`).

Every new field has a default that keeps the old behaviour. A run store on 0.3 refuses a body
that sets one (the contracts' models refuse unknown fields), so the service moves first.

## 0.3.2 (2026-10-05)

Adds to the wire without changing a shape (trellis-contracts 0.5.1):

* A run's working-time limit (`RunStart.timeout_seconds`, and
  `RUNS__RUNS__MAX_RUN_SECONDS`), its working time (`RunRecord.worked_seconds`) and
  `agent_version`; the lease's `remaining_seconds` and `cancel_requested`.
* `POST /v1/runs/{id}/cancel` and `POST /v1/runs/{id}/release`.
* A queued run's retryable `ERROR` is retried later, and a lapsed lease is requeued after a
  backoff.
* Webhook dead letters (`GET /v1/webhooks/deliveries`, `…/redeliver`), secret rotation
  (`POST /v1/webhooks/{id}/rotate-secret`, two signatures during the overlap) and the address
  guard on subscription URLs.

## 0.3.1 (2026-10-05)

Keeps every shape and changes what happens:

* A run's own `deadline` is enforced: the ticker ends it `TIMEOUT`.
* Only lapsed leases count toward failing a run, not answers.
* The very same resolution repeated answers the run instead of `409`.
* An answer that does not fit its question, and a question whose `expects` is no JSON
  Schema, are `422`.
* The SDK's `Worker` ends a run whose handler raised as `ERROR` at once.

## 0.3.0 (2026-10-04)

Changed the wire in place (its consumers are the platform's own repositories):

* Errors are RFC 9457 problems instead of `{"detail": …}`.
* Listings page with `cursor` and `Link`.
* The limits, the rate limit and the repeat semantics are new.
* The OpenAPI document has stable operation ids (`<tag>.<function>`), one security scheme
  and a problem on every error status, and is committed as `docs/openapi.json`.

Also since 0.2.0 (made while the version still read 0.2.0):

* A pause carries the executor's checkpoint for the worker that resumes the run.
* One key system: `X-API-Key` is introspected at the Memory Service.
* Webhooks are tenant subscriptions delivered from an outbox.
* A schedule create is an upsert on its identity; pause and resume are a `PATCH`.
* Listings return run summaries.
* Run artifacts in blob storage; every answer kept in `run_resolutions`, append-only.

## 0.2.0 (2026-09-30)

* Runs are the contracts' records (`RunStart`, `RunRecord`, `Interrupt`,
  `InterruptResolution`): a worker queue with leases, an assignee inbox and escalation.
* Schedules moved in from agent-schedules: one service, an API and a ticker process from one
  image.

## 0.1.0 (2026-09-21)

* Durable agent runs over PostgreSQL, with webhooks, migrations, Docker and structured
  logging.
