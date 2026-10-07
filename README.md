# agent-runs

Durable agent runs, the worker queue, the human inbox, and the schedules that start runs.
One service (it absorbed agent-schedules in 0.2.0): an API process and a ticker process over
one PostgreSQL database, with run artifacts' bytes in blob storage (a filesystem, or GCS).

This service never executes an agent. A harness (or any framework, through the
[Python SDK](sdk/python/README.md): the [two ways](#where-this-fits-two-ways-to-use-trellis)) does, either in its own process (it records the run here as
`RUNNING`) or as a worker that claims `QUEUED` runs from here under a lease. This service remembers: a run that pauses for an approval at 2 a.m. is still there
at 9 a.m., a crashed worker's run goes back on the queue, a run that failed on a blip is tried
again later, a run that works too long is stopped, and a schedule fires on behalf of a person
who is not present.

Every record is a [trellis-contracts](https://github.com/amitmohapatra/agent-contracts) type: a run is a `RunRecord`
started from a `RunStart`, paused with an `Interrupt`, resumed with an
`InterruptResolution`; a schedule is a `Schedule` created from a `ScheduleSpec`.

## Start here

1. **See it work, in one command.** `make install && make examples` runs nine scripts against
   the real service in this process (the API, the ticker and the SDK; the local PostgreSQL in
   a database of its own, no other network), from one run to webhooks and the event stream
   ([examples/](examples/README.md)).
2. **Run the service**: `make up` (Docker), or `make migrate` and two processes
   ([Run it](#run-it)).
3. **Drive it from Python** with the SDK, `trellis.runs`
   ([Where this fits](#where-this-fits-two-ways-to-use-trellis), [the SDK](sdk/python/README.md)).
4. **Pick the feature** you need in [When to use what](#when-to-use-what).
5. **Go deeper** in the [documentation](#documentation): the architecture and its sequence
   diagrams, every route, every setting, troubleshooting, versions and the ADRs.

## Where this fits: two ways to use Trellis

Trellis is five repos: **agent-harness** runs your agent, **agent-runs** (this one) keeps its
runs durable, **agent-memory-service** gives agents memory and issues the API keys this
service checks, **agent-contracts** holds the types on this API's wire, and **bifrost-sdk**
reaches models and MCP tools. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#among-the-five-trellis-repos)
draws how they connect. Each block works in both ways of using Trellis:

- **Way 1, wrapped.** `from trellis import Harness; h = Harness(); agent = h.wrap(my_agent)`.
  The harness runs your agent (LangGraph, Deep Agents, OpenAI Agents SDK, Claude Agent SDK,
  a plain function) and uses this service through the SDK automatically.
- **Way 2, pluggable blocks.** Keep your framework untouched and use `trellis.runs`
  (`RunsClient`, `Worker`, `webhooks.verify_signature`) yourself.

| | What happens with agent-runs and `trellis.runs` |
|---|---|
| **Way 1, wrapped** | With `RUNS_URL` set, the harness's run store is `trellis.runs.RunsClient`: `agent.run` records the run and finishes it, `agent.start` queues it, an `ask` or an approval pauses it (the `Interrupt`, a large table or diff as an artifact, the run's journal as `checkpoint`), `agent.resume` answers it, `h.inbox()` lists the paused runs, `agent.schedule(...)` creates a schedule, and `h.worker(...)` (or `python -m trellis.harness.worker`) runs wrapped agents on `trellis.runs.Worker`. You write no runs code. Without `RUNS_URL` the harness keeps runs in its own process, and none outlives it. |
| **Way 2, pluggable** | Your framework runs the agent, untouched; you start, queue, pause and finish its runs with the SDK: |

```python
from trellis.contracts.runs import RunStart, RunStatus
from trellis.runs import Job, RunsClient, Worker

async with RunsClient() as runs:  # RUNS_URL and TRELLIS_API_KEY
    await runs.start(RunStart(tenant_id="acme", agent_id="triage", input={"ticket": 7}), queue=True)

    async def handle(job: Job) -> None:  # your framework runs the claimed run
        await job.finish(RunStatus.SUCCESS, output=await my_agent(job.record.input))

    await Worker(runs, handle, ["triage"]).serve()  # claim, heartbeat, stop on SIGTERM
```

- **Choose Way 1 when** you want durable runs, approvals, the inbox and schedules from
  `h.wrap` with no code, and a resumed run that repeats no question and no side effect (the
  harness's journal is the checkpoint).
- **Choose Way 2 when** your framework keeps its own state (a LangGraph checkpointer, an OpenAI
  Agents `RunState`) and you want only what this service adds: a run that survives the
  process, a queue of workers, an inbox, schedules. Or when the caller is not an agent at
  all: a UI reading the inbox, or a webhook receiver (`trellis.runs.webhooks.verify_signature`,
  the same in both ways).

Harness docs: [the two ways](https://github.com/amitmohapatra/agent-harness/blob/main/README.md#two-ways-to-use-trellis) ·
[every page](https://github.com/amitmohapatra/agent-harness/blob/main/docs/README.md) ·
blocks: [runs](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/runs.md),
[governance](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/governance.md) ·
recipes: [LangGraph](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/langgraph.md),
[OpenAI Agents SDK](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/openai-agents.md),
[Claude Agent SDK](https://github.com/amitmohapatra/agent-harness/blob/main/docs/blocks/claude-agent-sdk.md).

## When to use what

| You want to… | Use |
|---|---|
| keep a durable record of a run your own process executes | `POST /v1/runs` (it is `RUNNING`), then `finish` |
| hand a run to a fleet of workers, surviving a worker that dies | `POST /v1/runs {queue: true}`; workers `claim`, `heartbeat` every third of the lease, and send `worker_id` on `pause`, `finish`, event appends and artifact uploads |
| put an urgent run ahead of the others | `RunStart.priority` (`-1000` to `1000`, default `0`): a claim takes the highest first, then the oldest |
| never run two runs of one conversation (or customer, or account) at once | `RunStart.concurrency_key`: at most `RUNS__RUNS__CONCURRENCY_PER_KEY` (1) of the tenant's runs sharing it are `RUNNING`; the rest wait `QUEUED`, in order |
| share one worker fleet fairly between tenants | nothing to set: a platform key's claim with no `X-Trellis-Tenant` takes from every tenant's queue, the tenant whose workers hold the fewest runs first. The operator may also cap what any one tenant holds (`RUNS__RUNS__MAX_RUNNING_PER_TENANT`) |
| show a run's progress from any replica, live | the worker appends its `RunEvent`s (`POST /v1/runs/{id}/events`); anyone reads them (`GET /v1/runs/{id}/events?after=`) or follows them as server-sent events (`GET /v1/runs/{id}/events/stream`), from whichever replica they reach |
| make a retried start harmless | the same `run_id`, or an `idempotency_key` (one run per tenant and key) |
| stop for a person's approval, answer or edit | `pause` with an `Interrupt` (`assignee`, `deadline`, `escalate_to`) and the executor's `checkpoint`; the person answers with `resume`, optionally with a `comment`, and approves similar calls for the rest of the run with `remember: "run"` (the harness keeps that promise) |
| offer labelled choices, several picks, or your own review screen | `Interrupt.options` as `{value, label, description}` objects (or plain strings), `multiple: true` (the answer is a list of values), `ui_schema` (form widget hints), `component` and `props` (your screen, `ui` the fallback). Every answer is checked against `expects` and the options, whoever collected it |
| move an unanswered question up, or give up on it | the interrupt's `deadline` and `escalate_to`: the ticker reassigns it once, else ends the run `TIMEOUT` |
| make sure a run is done by a time, whatever happens | `RunStart.deadline`: past it the ticker ends the run `TIMEOUT` (`run_deadline`, not retryable), queued, running or waiting for a person; a worker still running it is told `LEASE_LOST` and stops |
| bound how long a run may work, not counting the queue or a person's answer | `RunStart.timeout_seconds`: the ticker ends a run whose time `RUNNING`, across attempts and crashes (`worked_seconds`), passes it `TIMEOUT` (`run_timeout`); each lease says the time left (`remaining_seconds`). The operator's `RUNS__RUNS__MAX_RUN_SECONDS` bounds every run |
| show a reviewer something too big for a question (a table, a diff) | `POST /v1/runs/{id}/artifacts`, then the `ArtifactRef` as `Interrupt.payload_ref`; the UI reads `GET /v1/artifacts/{id}` |
| build a person's or a role's inbox | `GET /v1/runs?status=PAUSED&assignee=…`, with `&top_level=true` to leave out paused sub-agents (their parent is listed) |
| prove who approved what, and when | `GET /v1/runs/{id}/resolutions` (append-only) |
| cancel a run, whatever its status, saying why | `POST /v1/runs/{id}/cancel {reason}` (`RunsClient.cancel`): queued or waiting, it ends `CANCELLED` at once; held by a worker, the worker is told through its heartbeat and stops (the SDK's `Worker` does) |
| stop a worker without losing what it runs | nothing: the SDK's `Worker` lets its runs finish for 25 s, then releases them (`POST /v1/runs/{id}/release`), back on the queue at once for another worker |
| survive a model's rate limit or a dependency restarting | nothing: a queued run its worker ends `ERROR` with a retryable error is retried later, up to 3 times (10 s, 20 s, 40 s); a run kept in its caller's process is not |
| start runs on a timetable, as someone, while nobody is present | `POST /v1/schedules` (a cadence bucket or an hourly-or-slower cron, in the schedule's zone); repeat the create freely, it is an upsert. Its `timeout_seconds`, `agent_version`, `priority` and `concurrency_key` are copied into every run it fires, and its `metadata` into the run's under the fire's own keys, so a scheduled run carries everything a started one can |
| keep the database from growing forever | `RUNS__RUNS__RETENTION_DAYS`: the ticker deletes runs that ended longer ago, with their resolutions and events (unset: kept forever) |
| run a schedule now, or pause and resume it | `POST /v1/schedules/{id}/fire`; `PATCH {"enabled": false}` / `{"enabled": true}` |
| hear about pauses, escalations and endings instead of polling | `POST /v1/webhooks`; verify `X-Trellis-Signature` with the secret shown once (`trellis.runs.webhooks.verify_signature`) |
| change a webhook's secret without missing a delivery | `POST /v1/webhooks/{id}/rotate-secret`: both secrets sign for 24 h, and `verify_signature` accepts either |
| find and resend the deliveries a receiver missed | `GET /v1/webhooks/deliveries?state=dead`, then `POST /v1/webhooks/deliveries/{id}/redeliver` |
| drive all of it from Python, from any agent framework (Way 2) | the SDK, `trellis.runs`: `RunsClient` and `Worker` ([its README](sdk/python/README.md)) |
| have a wrapped agent use all of it with no code (Way 1) | `h.wrap(agent)` with `RUNS_URL` set ([the two ways](#where-this-fits-two-ways-to-use-trellis)) |

How a run moves between its statuses, and what each move does:
[the run lifecycle](docs/ARCHITECTURE.md#the-run-lifecycle). Every route, body and status
code: [docs/api.md](docs/api.md).

## Who may answer a paused run

Every `/v1` route needs an `X-API-Key` issued by the Memory Service, which agent-runs
introspects there ([the contract](docs/api.md#authentication)). A platform key names the
tenant it acts for in `X-Trellis-Tenant`.

Any key of the tenant reads every run, lists every inbox and works the queue: `assignee` is
the filter an inbox shows, not a lock. Answering a paused run (`POST /v1/runs/{id}/resume`,
`RunsClient.resume`, a `CANCEL` included) is checked, against the run's assignee at that
moment (after any escalation), before anything is written:

1. An **admin** key (or the operator's **platform** key) answers any run.
2. A key that **may act for anyone** (`"*"` in its `may_act_as`, which is what the Memory
   Service issues by default) answers any run: the application holding it vouches for the
   `reviewer` it names.
3. A key **restricted to listed people** (`may_act_as=["user:priya"]`) answers only as one
   of them, and only a run assigned to that person or to nobody. The `reviewer` is who it
   answers as (a bare id `priya` means `user:priya`; no reviewer means the key itself). A run
   assigned to a group (`role:finance`) is refused: agent-runs cannot see who is in a group,
   so answer it with the application's key or an admin key.

| The key | Run assigned to `user:priya` | to `user:raj` | to `role:finance` | to nobody |
|---|---|---|---|---|
| admin or platform | yes | yes | yes | yes |
| `may_act_as=["*"]` (the default) | yes | yes | yes | yes |
| `may_act_as=["user:priya"]`, `reviewer="priya"` | yes | no | no | yes |
| `may_act_as=["user:priya"]`, `reviewer="raj"` | no | no | no | no |

A refusal is `403 AUTHORIZATION` (`AuthorizationError` in the SDK) whose detail says why,
for example `the run is assigned to user:raj, not user:priya; this key may act only for
user:priya`, or `the run is assigned to role:finance, a group: a key restricted to listed
people cannot answer it; answer with the application's key or an admin key`. The
`reviewer` is stored as given.

**Nothing changes if your keys may act for anyone**, as every key the Memory Service issues
does unless told otherwise. To let a person's own client (an approvals UI, a mobile app)
answer only their runs, issue that client a key restricted to them, with the tenant's admin
key:

```python
from trellis.memory import MemoryClient

async with MemoryClient(api_key=ADMIN_KEY) as admin:
    issued = await admin.tenant.keys.issue("service", "priya-approvals", may_act_as=["user:priya"])
    priya_key = issued.token  # shown once
```

```python
from trellis.contracts.runs import InterruptDecision, InterruptResolution
from trellis.runs import RunsClient

async with RunsClient(api_key=priya_key) as runs:
    await runs.resume(
        InterruptResolution(
            interrupt_id=run.awaiting.interrupt_id,
            run_id=run.run_id,
            decision=InterruptDecision.APPROVE,
            reviewer="priya",
        )
    )  # her run: answered; raj's or role:finance's: AuthorizationError
```

The rule is `answering.py`, one function. Cancelling a run (`POST /v1/runs/{id}/cancel`,
`RunsClient.cancel`) is checked by the same rule: a key may cancel a run it could answer, as
any principal it may act for; a run that is not paused is assigned to nobody.

Two more checks come with every answer, before anything is written: **an answer is taken
once** (the very same resolution resent is answered with the run as it is now; any other
answer to a question already answered is `409 CONFLICT`), and **an answer must fit the
question** (its `expects` and options: `422 VALIDATION` otherwise, and the run keeps
waiting). [The sequence](docs/ARCHITECTURE.md#pause-and-resume-and-the-answer-check) shows
the order; [the resume route](docs/api.md#post-v1runsidresume--200-runrecord) the details.

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

Every setting, its default, whether it applies without being set, and an example:
[docs/configuration.md](docs/configuration.md). The ones a deployment sets first are
`RUNS__DATABASE__URL`, `RUNS__MEMORY__URL` (or `MEMORY_URL`), `RUNS__SERVICE__ENVIRONMENT`
and, outside `dev`, `RUNS__BLOB__PROVIDER=gcs` with `RUNS__BLOB__BUCKET`.

## Documentation

| Page | What it answers |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Its place among the five repos, the components, the run lifecycle, sequence diagrams (start to finish; claim, lease and heartbeat; pause and resume with the answer check; cancel and release; a schedule firing; webhook delivery and dead letters; events and SSE), the ticker, the tables, the code map |
| [docs/api.md](docs/api.md) · [docs/openapi.json](docs/openapi.json) | Every route, body, status and problem `code`; the OpenAPI 3.1 document (served live at `/openapi.json`, `/docs`, `/redoc`) |
| [sdk/python/README.md](sdk/python/README.md) | The Python SDK, `trellis.runs`: the client, the worker, the inbox, schedules, webhooks |
| [examples/](examples/README.md) | Nine runnable scripts, in process, simplest first |
| [docs/configuration.md](docs/configuration.md) | Every setting: default, automatic or not, example; the SDK's and the test suite's variables |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Problem codes and operational symptoms, their causes and fixes; FAQ |
| [docs/versioning.md](docs/versioning.md) | Which versions of the five repos go together; how the API and the schema change |
| [CHANGELOG.md](CHANGELOG.md) | What each version changed |
| [docs/adr/](docs/adr/README.md) | Why: leases and fencing, the ticker owns time, webhooks live here, schedules merged in |

## Develop

```bash
make lint typecheck test
make coverage                # the service's suite with line and branch coverage, failing under 95%
make sdk                     # the SDK's suite, failing under 100% line and branch coverage
make examples                # every example, in process (the local PostgreSQL)
make links                   # every relative Markdown link and anchor resolves
make openapi                 # rewrite docs/openapi.json after changing a route or a model
```

```bash
make lint typecheck test
make coverage                # the service's suite with line and branch coverage, failing under 95%
make sdk                     # the SDK's suite, failing under 100% line and branch coverage
make openapi                 # rewrite docs/openapi.json after changing a route or a model
```

The suite runs against the local PostgreSQL in its own database (`agent_runs_tests`, dropped
and recreated per run) and skips with a reason when there is none. The key registry is a
fake (`tests/conftest.py`, `FakeMemory`, a tiny ASGI app answering `/v1/keys/self`); the
blob store is a filesystem one per test, and the GCS adapter also runs over an in-memory
stand-in for the Google client and against a fake GCS server (`fsouza/fake-gcs-server`,
started in Docker on a free port and removed afterwards, so the suite needs Docker), with an
end-to-end artifact test on it. Migrations live in `alembic/versions`; a test checks they
build exactly the schema the code maps.

CI (`.github/workflows/ci.yml`) runs on pushes to `main` and on pull requests: ruff, pyright
(the service and the SDK), the migrations up, down to base and up again, the suite with the
coverage floor, the SDK's suite at 100% line and branch coverage, the examples, the link
check, and a diff
of `docs/openapi.json` against the document the code generates, against PostgreSQL 16 with
`agent-contracts` checked out beside this repository. The OpenAPI document embeds the
contracts' models, so it is regenerated whenever `agent-contracts` changes them.
