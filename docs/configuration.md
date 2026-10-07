# Configuration

Every setting of the service, from `src/agent_runs/config/settings.py`. Settings are
environment variables with one prefix, `RUNS__`, and `__` between levels (or a `.env` file in
the working directory); [.env.example](../.env.example) lists them with their defaults. Every
other number (lease bounds, retry backoffs, page sizes, webhook attempts) is a named constant
in `src/agent_runs/config/constants.py`, not a setting.

**Automatic?** says what happens when you set nothing: **yes** means the default is right
for a deployment as well as a laptop; **derived** means it is computed from something else;
**set it** means the default only suits a laptop, so a deployment sets it.

## A working example

A laptop needs nothing beyond PostgreSQL and the Memory Service on their default ports. A
production deployment sets at least these:

```bash
RUNS__SERVICE__ENVIRONMENT=prod
RUNS__DATABASE__URL=postgresql+psycopg://runs:${DB_PASSWORD}@db.internal:5432/agent_runs
RUNS__MEMORY__URL=https://memory.internal
RUNS__BLOB__PROVIDER=gcs
RUNS__BLOB__BUCKET=acme-agent-runs-artifacts
RUNS__TICKER__HEARTBEAT_FILE=/run/agent-runs/ticker.beat
RUNS__RUNS__MAX_RUN_SECONDS=3600
RUNS__RUNS__RETENTION_DAYS=90
```

`Settings` refuses to start with the filesystem blob store outside `dev` and `test`, and with
`gcs` and no bucket.

## Service (`RUNS__SERVICE__*`)

| Variable | Default | Automatic? | Read by | What it does | Example |
|---|---|---|---|---|---|
| `RUNS__SERVICE__HOST` | `0.0.0.0` | yes | API | where uvicorn binds | `127.0.0.1` |
| `RUNS__SERVICE__PORT` | `8090` | yes | API | the API's port | `8080` |
| `RUNS__SERVICE__ENVIRONMENT` | `dev` | **set it** | API, ticker | `dev` also accepts and delivers plain-`http` webhook URLs, and private ones unless `RUNS__WEBHOOKS__ALLOW_PRIVATE_TARGETS` says otherwise; anything else only `https` to public hosts. Only `dev` and `test` may use the filesystem blob store | `prod` |
| `RUNS__SERVICE__WORKERS` | one per CPU, 1 to 8 | derived | API | uvicorn worker processes; each keeps its own key cache and metrics (rate-limit budgets are shared, in the database) | `4` |
| `RUNS__SERVICE__GRACEFUL_SHUTDOWN_SECONDS` | `20` | yes | API | on `SIGTERM`, how long requests in flight may finish before they are closed | `30` |
| `RUNS__SERVICE__MAX_BODY_BYTES` | `4194304` (4 MiB) | yes | API | a JSON body past this is `413`, counted as it arrives (chunked too); artifacts have their own 50 MiB | `8388608` |
| `RUNS__SERVICE__MAX_PAYLOAD_BYTES` | `1048576` (1 MiB) | yes | API | a run's `input` or `output` past this (compact JSON) is `413` | `2097152` |

## The Memory Service (`RUNS__MEMORY__*`)

| Variable | Default | Automatic? | Read by | What it does | Example |
|---|---|---|---|---|---|
| `RUNS__MEMORY__URL` | `MEMORY_URL`, else `http://localhost:8080` | **set it** (or `MEMORY_URL`) | API | the Memory Service, the one key registry: every `X-API-Key` is introspected at `{url}/v1/keys/self`. `MEMORY_URL` is the platform-wide name; this one wins when both are set | `https://memory.internal` |

## Database (`RUNS__DATABASE__*`)

| Variable | Default | Automatic? | Read by | What it does | Example |
|---|---|---|---|---|---|
| `RUNS__DATABASE__URL` | `postgresql+psycopg://memory:memory@localhost:5432/agent_runs` | **set it** | API, ticker, alembic | the database; both processes refuse to start on one that is not at the head revision (`make migrate`) | `postgresql+psycopg://runs:…@db:5432/agent_runs` |
| `RUNS__DATABASE__POOL_SIZE` | `10` | yes | API, ticker | connections per process (per worker) | `20` |
| `RUNS__DATABASE__MAX_OVERFLOW` | `10` | yes | API, ticker | connections opened past the pool under a burst | `5` |
| `RUNS__DATABASE__POOL_TIMEOUT_SECONDS` | `5` | yes | API, ticker | wait for a pooled connection before answering `503` | `2` |
| `RUNS__DATABASE__POOL_RECYCLE_SECONDS` | `300` | yes | API, ticker | a pooled connection older than this is replaced, not reused | `120` |
| `RUNS__DATABASE__POOL_PRE_PING` | `true` | yes | API, ticker | test a pooled connection on checkout; a dead one is replaced, not handed to a request | `true` |
| `RUNS__DATABASE__CONNECT_TIMEOUT_SECONDS` | `5` | yes | API, ticker | opening a connection to PostgreSQL | `3` |
| `RUNS__DATABASE__STATEMENT_TIMEOUT_MS` | `15000` | yes | API, ticker | PostgreSQL cancels a statement past this (`503` here); `0` is no limit | `5000` |

## Blob storage, for artifacts (`RUNS__BLOB__*`)

| Variable | Default | Automatic? | Read by | What it does | Example |
|---|---|---|---|---|---|
| `RUNS__BLOB__PROVIDER` | `filesystem` | **set it** | API, ticker | `filesystem` (dev and test only) or `gcs` | `gcs` |
| `RUNS__BLOB__ROOT` | `.blob` | yes (dev) | API, ticker | the filesystem store's directory, shared by both processes | `/var/lib/agent-runs/blobs` |
| `RUNS__BLOB__BUCKET` | unset | **set it** with `gcs` | API, ticker | the GCS bucket (Application Default Credentials; `STORAGE_EMULATOR_HOST` points the client at an emulator) | `acme-agent-runs-artifacts` |

## Runs (`RUNS__RUNS__*`)

| Variable | Default | Automatic? | Read by | What it does | Example |
|---|---|---|---|---|---|
| `RUNS__RUNS__MAX_RUN_SECONDS` | unset: no platform maximum | yes | API, ticker | the most working time any run may take (time `RUNNING`, across attempts); a run's own `timeout_seconds` may only be shorter | `3600` |
| `RUNS__RUNS__CONCURRENCY_PER_KEY` | `1` | yes | API | how many of a tenant's runs sharing a `concurrency_key` may be `RUNNING` at once | `2` |
| `RUNS__RUNS__MAX_RUNNING_PER_TENANT` | unset: no cap | yes | API | the most runs one tenant's workers may hold at once; a claim past it is `204`. Fair share between tenants needs no setting | `50` |
| `RUNS__RUNS__RETENTION_DAYS` | unset: kept forever | yes | ticker | days an ended run is kept, with its resolutions and events | `90` |

## Webhooks (`RUNS__WEBHOOKS__*`)

| Variable | Default | Automatic? | Read by | What it does | Example |
|---|---|---|---|---|---|
| `RUNS__WEBHOOKS__ALLOW_PRIVATE_TARGETS` | unset: `true` in `dev` only | derived | API, ticker | deliver to hosts that resolve to private, loopback or link-local addresses (receivers inside the deployment's network) | `true` |
| `RUNS__WEBHOOKS__SECRET_OVERLAP_HOURS` | `24` | yes | API | after a rotation, how long the old secret signs deliveries too; `0` the new one only | `48` |
| `RUNS__WEBHOOKS__DEAD_RETENTION_DAYS` | `7` | yes | ticker | how long a dead delivery is kept to be redelivered | `14` |

## Rate limit (`RUNS__RATE_LIMIT__*`)

| Variable | Default | Automatic? | Read by | What it does | Example |
|---|---|---|---|---|---|
| `RUNS__RATE_LIMIT__PER_MINUTE` | `3000` | yes | API | each tenant's request budget, refilled per minute, one per tenant in PostgreSQL and shared by every worker of every replica (`429` + `Retry-After` when empty); `0` turns it off | `6000` |
| `RUNS__RATE_LIMIT__BURST` | `500` | yes | API | the most requests the budget holds at once | `1000` |

## Ticker (`RUNS__TICKER__*`)

| Variable | Default | Automatic? | Read by | What it does | Example |
|---|---|---|---|---|---|
| `RUNS__TICKER__HEARTBEAT_FILE` | unset: a per-process file in the temp directory | **set it** for a liveness probe | ticker, probe | the file the ticker touches every tick and `python -m agent_runs.heartbeat` reads | `/run/agent-runs/ticker.beat` |
| `RUNS__TICKER__METRICS_PORT` | unset: none | yes | ticker | serve the ticker's Prometheus metrics on this port | `9100` |

## Observability (`RUNS__OBSERVABILITY__*`)

| Variable | Default | Automatic? | Read by | What it does | Example |
|---|---|---|---|---|---|
| `RUNS__OBSERVABILITY__LOG_LEVEL` | `INFO` | yes | API, ticker | log level | `WARNING` |
| `RUNS__OBSERVABILITY__LOG_JSON` | `true` | yes | API, ticker | `false` for the console renderer | `false` |

## Outside the service

| Variable | Default | Read by | What it does |
|---|---|---|---|
| `RUNS_URL` | `http://localhost:8090` | the SDK (`RunsClient()`), the harness | where agent-runs is; `RunsClient(base_url=)` wins |
| `TRELLIS_API_KEY` | unset | the SDK, the harness | the `X-API-Key` the SDK sends; `RunsClient(api_key=)` wins. Never logged |
| `MEMORY_URL` | unset | the API (when `RUNS__MEMORY__URL` is unset), the harness, the memory SDK | the Memory Service, by its platform-wide name |
| `RUNS_PORT`, `RUNS_DB_PORT` | `8090`, `5442` | `docker compose` (`make up`) | host ports of the API and PostgreSQL |
| `RUNS_TEST_ADMIN_URL` | `postgresql://memory:memory@localhost:5432/postgres` | the test suite, the examples | the admin connection that creates their databases |
| `RUNS_TEST_DB` | `agent_runs_tests` | the test suite | the test database's name (dropped and recreated per run) |
| `RUNS_EXAMPLES_DB` | `agent_runs_examples` | the examples | the examples' database's name (dropped and recreated per example) |

The SDK's own defaults (a 10 s timeout, 3 retries, a 60 s lease, a 25 s graceful stop) are
constructor arguments and constants, listed in
[the SDK's README](../sdk/python/README.md#configuration-and-tenants).
