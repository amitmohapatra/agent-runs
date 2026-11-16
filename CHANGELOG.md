# Changelog

What changed in each version of `agent-runs` and its SDK `trellis-runs` (they are versioned
together), and what a caller must do about it. Which versions of the other Trellis repos go
with which: [docs/versioning.md](docs/versioning.md). Why: [docs/adr/](docs/adr/README.md).

## Unreleased

* **A tenant's claim no longer reads its whole queue.** Every claim read all the queued runs
  of the worker's agents and sorted them to take one: `ix_runs_queue` held them by age, and
  the claim's order starts with the fair share between tenants. Measured with
  `EXPLAIN ANALYZE` on one tenant's queue (PostgreSQL 16, JIT off): 37 ms a claim at 10,000
  queued runs and 400 ms at 100,000 (790 ms when every run has a `concurrency_key`), a sort
  spilling to disk. `ix_runs_queue` now holds the queue in claim order (`priority`
  descending, then `queued_at`; migration `d0f5a6b7c8e9`), and a claim for one tenant, among
  whose runs the fair share decides nothing, is ordered by priority and age alone: it reads
  the first run with room and stops, 0.1 ms at either depth. The order runs are claimed in
  is unchanged. A platform key's claim across tenants (no `X-Trellis-Tenant`) still sorts
  every queued run of its agents (470 ms at 100,000), and so does a tenant's claim for
  several `agent_ids` at once (80 ms). Run `alembic upgrade head` (`make migrate`) before starting
  this version; the index is rebuilt in the migration's transaction, which blocks writes to
  `agent_runs` while it runs.
* **One way to build a `RunStore`.** `RunStore(session, limits)` takes the deployment's
  `RUNS__LIMITS__*` (`Settings.limits`) and has no defaults: most routes, the ticker's
  retention and compaction steps and `Firing` built one without them, which no method read
  yet, so the next one that did would have ignored the configured limits. Routes get the
  store from one dependency (`api/deps.py` `Runs`), `Firing` is handed it, and `Ticker` takes
  `limits=` (was `max_run_seconds=` and `run_retention=`). Nothing to do: no route or setting
  changes.
* **A page of an ended run's events reads a page's worth, not the whole log.** The ticker
  compacted an ended run's event log into one object, so every page of
  `GET /v1/runs/{id}/events`, and every 500 events of its stream, downloaded and decoded the
  whole log: reading a log of n events cost n²/500 of them. A log is now compacted into
  objects of 500 events (`EVENT_CHUNK`), and a page reads the one it starts in and at most
  the next. `agent_runs.events_ref` lists the objects (`{"chunks": [{"through", "ref"}]}`);
  a log compacted before is still one object and is read as before, so there is no
  migration. Deploy the API before the ticker: an API replica of the previous version
  cannot read a log the new ticker compacted.
* **A sweep reads none of its runs' payloads.** Each of the ticker's five run sweeps returned
  full records, reading the input, output, last answer, checkpoint and question payload of up
  to 100 runs from the blob store, only to announce them, and looked each run's
  subscriptions up on its own: up to about 500 object reads and 200 statements a sweep. A
  sweep's runs now come back without their payloads, and `WebhookStore.announce` takes them
  all at once (three statements however many). A swept run whose payload object is lost no
  longer fails its sweep. The events delivered are unchanged. Nothing to do.
* **A fire the blob store cannot take is recorded on its schedule.** Only a database error
  failed a fire; a blob store that did not answer when the run's input was written escaped,
  nothing was recorded, and the same schedule was tried first, and failed, on every tick
  while the schedules due after it never fired. It is now a failed fire like any other:
  `last_error` (`Unavailable`, category `DEPENDENCY`, retryable), `consecutive_failures`,
  the backoff, the auto-pause after three, and `503` with the schedule's state in `details`
  from `POST /v1/schedules/{id}/fire` (it was a `503` naming the payload store). Nothing to
  do.
* **What a payload route holds is bounded.** `POST /v1/payloads/read` took up to 256 SHA-256s,
  the same one as often as sent, read every object whole and copied them into one answer:
  256 names of one 50 MiB object made one replica hold about 12.8 GB, twice. Now an object
  named twice is read once (and still answered at each place asked), the bytes asked for are
  at most 64 MiB together (`413 PAYLOAD_TOO_LARGE` past it, before anything is read from the
  store), and the answer is written frame by frame, not copied into one body.
  `POST /v1/runs/{id}/payloads` refuses (`413`) an object whose header says it decompresses
  to more than 64 MiB (`MAX_PAYLOAD_DECODED_BYTES`) before decoding it to check it; it was
  decoded up to 512 MiB. The SDK's `artifacts.get_payloads` asks for more than one read
  answers (`trellis.runs.payloads.MAX_READ_OBJECTS`, `MAX_READ_BYTES`) in several requests,
  so a caller of it has nothing to do; a client that posts to `/v1/payloads/read` itself
  must split a read past 64 MiB. `payloads.decoded_size` is new.
* **A burst of answers or fires no longer runs the pool dry.** `POST /v1/runs/{id}/resume` and
  a schedule's fire register their payloads while their session holds a connection and a row
  lock, and the registry took a second connection from the same pool: about 20 at once (the
  default pool) each held one connection, waited 5 s for another and answered `503`. The
  payload registry now has a pool of its own (`store/database.py` `registry_engine`), taken
  out of the same budget: of a process's connections a tenth (at least one) are the
  registry's, so the default is 9 kept + 9 burst for sessions and 2 for the registry (was
  10 + 10), and `RUNS__DATABASE__CONNECTION_BUDGET` still bounds everything the container
  opens. Nothing to do; `runs_db_pool_connections` reports the sessions' pool.
* **One failing ticker step no longer stops the others.** Every step of a tick ran in one
  `try` that caught only database and OS errors, so anything else one step raised (a webhook
  delivery whose payload object was lost, a stored interrupt this build cannot read) ended the
  tick there, and, the same rows coming first again, every later step for every tenant, for
  good. Each step now runs on its own: what it raises that is no outage is logged
  (`ticker.step_failed`) and counted in the new `runs_ticker_step_failures_total{step}`, and
  the steps after it run. Only an outage (the database or its pool not answering, an OS
  error) fails the tick as a whole and counts against the breaker. A delivery whose payload
  is lost is kept dead (`last_error` says "missing or corrupt") instead of failing the claim
  of the others, and a paused run past its interrupt's deadline whose stored interrupt cannot
  be read ends `TIMEOUT` with code `interrupt_unreadable`. Alert on any increase of the new
  counter.
* **A checkpoint kept past a run's end stays readable.** A run finished or cancelled with
  `keep_checkpoint` lost its link to the checkpoint's object when it ended, so the sweep
  deleted the object after 24 hours while the record still named it. The link is now kept
  for such a run. Nothing to do.
* An artifact row deleted between a duplicate upload's insert and its read answers
  `409 CONFLICT` instead of failing an assertion.

* **An answer is held to its question's `format`s.** `trellis.runs.answers.answer_problem` (what
  `POST /v1/runs/{id}/resume` checks, and the harness for a run it keeps in its process) now
  validates `expects` with the schema draft's own format checker (`jsonschema`, already a
  dependency): an answer to a `{"type": "string", "format": "date"}` question that is not an
  ISO 8601 calendar date (`"tomorrow"`, `"2026-13-40"`, `""`) is `422 VALIDATION` ("… is not a
  'date'"), as any misfit is, and the run keeps waiting. The formats enforced are one named
  set, `trellis.runs.answers.ANSWER_FORMATS` (`date`, `email`, `ipv4`, `ipv6`, `regex`,
  `uuid`: those the standard library checks), whatever optional libraries are installed, so
  two servers never disagree on an answer; any other format (`date-time`, `time`, `hostname`,
  `uri`, …) is not checked anywhere.
* **A run's place in its key's line is on the wire, and a key's line can be listed.** The run
  record and the run summary carry `placed_at` (read-only, optional; `trellis-contracts`
  `RunRecord.placed_at`): when the run was started, or when its latest answer was recorded —
  the place agent-runs decides under the key's lock. Null for a run with no
  `concurrency_key`. `GET /v1/runs` (and the SDK's `list`/`iterate`) takes `concurrency_key`:
  only that key's runs, in the caller's tenant; with `status=RUNNING` or `QUEUED` it is served
  by `ix_runs_concurrency`, each place read from `ix_run_resolutions_run` (plan kept in
  `tests/test_plans.py`). Additive: an older client ignores the field and never sends the
  filter. The harness orders a conversation's turns by `placed_at` and lists its line by its
  key, so it no longer reads every answer of every run in the line, nor another user's runs
  on a thread of the same name.
* **A key's runs are placed in the order they commit, and a queued one holds its place.** A run
  with a `concurrency_key` is created in its place among the key's runs: under the key's
  transaction lock (the one claims count the key's room under), in the one statement that
  follows it, its `created_at` is set just after the latest of the key's running and queued
  runs when its clock is behind — and never changes. A run continued after its answer takes
  its place again the same way (its answer's `recorded_at`). A claim takes a queued run only
  while no run placed before it runs; one kept in its caller's process placed after it no
  longer holds it back. `ix_runs_concurrency` holds the queued runs too (migration
  `c9e4f5a6b7d8`). The harness answers a conversation's messages one at a time in this order.
  The wire is unchanged.
* **A key's run kept in its caller's process is born to be held.** Started `RUNNING` with a
  `concurrency_key`, it must be held (a heartbeat) within `RESUME_GRACE` (60 s), as a run
  continued after its answer must: the ticker ends one nobody ever held `ERROR`
  (`holder_lost`), so a process killed between the start and its first heartbeat leaves no run
  its key's line waits behind. A caller that starts keyed runs in its own process and never
  heartbeats them: heartbeat them, or start them without a key.
* **Who may hold a run.** A hold decides when a run kept in its caller's process ends, so a
  heartbeat or a release on one takes a key that administers the tenant, may act for anyone,
  or may act for the person the run is for (`on_behalf_of`, else its user): `403` otherwise.
  A hold lasts at most 300 s whatever `lease_seconds` asks, and a refused hold no longer
  names the holder.
* **Requires `trellis-contracts>=0.7.1,<0.8`** (the service and the SDK; was `>=0.6.2`).
  agent-runs keeps interrupt resolutions, and a harness on 0.7.1 may send `remember: "always"`,
  which an older contracts package refuses to read: the store takes 0.7.1 before any harness
  sends it, as the contracts' changelog asks. `uv.lock` updated (`uv lock`). Install
  trellis-contracts 0.7.1 (agent-contracts `main`) beside this repository.
* **`docs/openapi.json` on trellis-contracts 0.7.1.** CI checks agent-contracts' `main` out
  beside this repository, which is 0.7.1 (`InterruptRemember.ALWAYS`), so the committed
  document generated on 0.7.0 failed the "OpenAPI document is current" step and
  `tests/test_openapi.py`. Regenerated; the only difference is `remember: "always"` in
  `InterruptResolution`.
* **A run kept in its caller's process can be held, and is ended when its holder stops.** A run
  started `RUNNING` (never queued) had no lease, so one whose process crashed stayed `RUNNING`
  forever unless it had a `deadline` or the service a `RUNS__LIMITS__MAX_RUN_SECONDS`. Now the
  existing `POST /v1/runs/{run_id}/heartbeat` (SDK `heartbeat`), sent for such a run, holds it
  for its `worker_id` for `lease_seconds`; once the hold lapses the ticker ends the run `ERROR`
  with code `holder_lost` (category `TIMEOUT`, not retryable), never queueing it. A hold fences
  none of the caller's writes, takes no worker's place, and a pause lets go of it. No request or
  answer schema changes: such a heartbeat was a `409 LEASE_LOST` before, so no caller sends one
  unless it means to hold. Migration `b8d3e4f5a6c7` adds `agent_runs.holder` and `held_until`
  (null: nothing holds the run, as before) and `ix_runs_held`. Nothing to do.
* **An answered run nothing continues is ended.** A run held when it paused, answered after
  the process that held it stopped, ran in nobody's process and stayed `RUNNING`. Its resume
  now gives whoever continues it `RESUME_GRACE` (60 s) to hold it, after which the ticker ends
  it `ERROR` (`holder_lost`, "nobody took the run up after its answer"). A run never held is
  unaffected.
* **A holder can let go of a hold.** `POST /v1/runs/{run_id}/release` by the holder of a run
  kept in its caller's process (a 409 before) clears the hold; the run stays `RUNNING`, unheld
  (a helper whose parent run waits for a person).
* **Built on trellis-contracts 0.7.0.** `docs/openapi.json` no longer lists the run inbox
  names 0.7.0 removed (`InterruptReason.PAUSE`, `RunRecord.usage`, `RunMessage`); the service
  never used them. Requires `trellis-contracts>=0.6.2,<0.8`, as before. Nothing to do.
* **Storage is reference-only; no migration (dev databases reset).** Every run payload (input,
  output, answers, the question's payload, checkpoints) lives in the blob store and the rows
  hold references only: migration `a7c1d2e3f4b5` replaces `agent_runs.input`, `output`,
  `last_resolution`, `checkpoint` and `run_resolutions.interrupt`, `resolution` with their
  `*_ref` columns. There is no inline read path and no `agent-runs-payloads` command. A
  checkpoint is bounded by `RUNS__SERVICE__MAX_PAYLOAD_BYTES` (1 MiB), as `input` and `output`
  are (`MAX_CHECKPOINT_BYTES` is gone). Reset a dev database:
  `alembic downgrade base && alembic upgrade head`.
* **The `runs` settings group is now `limits`.** `RUNS__LIMITS__MAX_RUN_SECONDS`,
  `CONCURRENCY_PER_KEY`, `MAX_RUNNING_PER_TENANT` and `RETENTION_DAYS` replace the
  `RUNS__RUNS__*` names (`Settings.limits`). For this release the old names still work, each
  with a one-line warning at startup naming its new name, and the new name wins if both are set.
  The next release removes the old names.
* **A segment's save is four statements.** The new segment is checked stored and linked in one
  statement, and the run's row is written once (the links before it, no autoflush between).
  Nothing to do.
* **A checkpoint's segments read back in one request.** `POST /v1/payloads/read` answers
  several payload objects at once (read from the store in parallel, each verified; SDK
  `artifacts.get_payloads`), so a resume is one round trip. A save that adds a segment checks
  and links only the new one, its cost independent of the run's length. Small objects are
  read in one call (`BlobStore.get`, `read_whole`). Nothing to do.
* **Run payloads are written by reference.** A run's input, output, the payload its pause
  waits on, its answers and its checkpoint go to the blob store before any row refers to
  them; reads answer the same JSON (a webhook's body the same bytes). A checkpoint may be a
  list of segments the worker uploads (`POST /v1/runs/{run_id}/payloads`, read back with
  `GET /v1/payloads/{sha256}`; SDK `artifacts.put_payload` / `get_payload`), so a save sends
  only what changed. An ended run's events become one object (the ticker). Artifact retention
  is a setting, `RUNS__BLOB__RETENTION_DAYS` (7, as before). Set
  `RUNS__LIMITS__RETENTION_DAYS` to bound what is kept.
* **Run payloads are stored by reference.** Every payload has a `*_ref` (a `PayloadRef` to a
  zstd-compressed JSON object in the blob store, keyed by its SHA-256 under the tenant,
  hash-sharded as the Memory Service's archive is), read by a run, the listing, resolutions,
  events and a webhook's body. A ticker sweep deletes payload objects no row refers to once a
  day old. Migration `a7c1d2e3f4b5` (the `*_ref` columns and two tables). SDK:
  `trellis.runs.payloads` (the format).
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
