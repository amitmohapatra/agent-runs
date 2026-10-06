# Changelog

What changed in each version of `agent-runs` and its SDK `trellis-runs` (they are versioned
together), and what a caller must do about it. Which versions of the other Trellis repos go
with which: [docs/versioning.md](docs/versioning.md). Why: [docs/adr/](docs/adr/README.md).

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
