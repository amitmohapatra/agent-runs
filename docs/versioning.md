# Versioning and compatibility

## What goes with what

`agent-runs` (the service) and `trellis-runs` (its SDK, `sdk/python`) are one version: they
change in the same commit, and the SDK's suite checks every model it sends or parses against
the service's committed `docs/openapi.json`. The pins below are copied from each package's
`pyproject.toml` on `main`.

| Package | Version | Relation |
|---|---|---|
| `agent-runs` and `trellis-runs` | **0.4.1** | this repo |
| `trellis-contracts` (agent-contracts) | **0.6.1** | required by both: `>=0.6.1,<0.7` (schedules' `priority`, `concurrency_key` and `metadata` need 0.6.1) |
| `trellis-harness` (agent-harness) | **0.4.0** | requires `trellis-runs>=0.4.0`; its run store is `RunsClient` when `RUNS_URL` is set |
| agent-memory-service | **0.3.0** | the key registry: `GET /v1/keys/self` ([the contract](api.md#the-introspection-contract-get-runs__memory__urlv1keysself)) |
| bifrost-sdk | **0.3.0** | none |
| PostgreSQL | 16 in CI, 17 in `docker compose` | `FOR UPDATE SKIP LOCKED`, advisory locks |
| Python | `>=3.12` | |

## The rules

* **The wire.** Until 1.0 the API's consumers are the platform's own repositories, and they
  move together. Additions come with defaults that keep the old behaviour. A field another
  service would refuse (the contracts' written records refuse unknown fields) ships in the
  service first, then in the callers. The version notes say for each release what changed
  and what a caller must do ([CHANGELOG.md](../CHANGELOG.md)).
* **The OpenAPI document** (`docs/openapi.json`) is the contract a client is built against.
  CI fails when it differs from what the code generates, so it changes only in the commit
  that changes a route or a model (`make openapi`).
* **The schema.** Migrations are the schema (`alembic/versions`, head `9d5e0f1a2b3c`). Every
  migration has a downgrade, and CI runs up, down to base, and up again. Both processes
  refuse to start on a database that is not at the head revision, so a deploy runs
  `make migrate` (or the compose `migrate` job) before the new processes start.
* **The SDK.** `trellis-runs` is versioned with the service; a client on an older minor may
  meet fields it does not know in what it reads (it ignores them) but must not send fields an
  older service does not know.

## Where changes are recorded

* [CHANGELOG.md](../CHANGELOG.md): what each version changed.
* [adr/](adr/README.md): why the service is shaped as it is.
* [agent-contracts' ADRs](https://github.com/amitmohapatra/agent-contracts/blob/main/docs/adr/README.md):
  the records on the wire.
