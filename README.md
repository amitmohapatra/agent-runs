# agent-runs

Durable agent runs: start, pause for a human, resume, cancel, replay.

A run is the unit of work a user can leave and come back to. The harness executes; this
service remembers — so a run that pauses for an approval at 2am is still there at 9am, and a
crashed worker does not lose the answer a human already gave.

## The state machine

```
PENDING ──► RUNNING ──► SUCCEEDED
               │  ▲         
               │  └── resume(answer)
               ▼
            PAUSED ──► CANCELLED / FAILED
```

`PAUSED` is the interesting state: it carries the question that was asked and the schema of
the answer expected, so a UI can render an approval without knowing anything about the agent
that raised it.

## Guarantees

- **Idempotent starts.** A start is keyed per tenant; replaying the same key returns the
  existing run rather than creating a second one.
- **One writer at a time.** State transitions take `SELECT … FOR UPDATE` on the run row, so
  two workers racing to finish the same run cannot interleave.
- **Tenant-bound credentials.** An API key is bound to the tenant it was issued for;
  presenting a valid key for someone else's tenant is a 403, not a read.

## Run it

```bash
uv sync
uv run uvicorn agent_runs.api.app:app --port 8095
```

## Tests

```bash
uv run pytest
```
