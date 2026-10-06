# ADR 0002: The ticker owns time

**Status:** accepted · **Date:** 2026-09-30 (recorded 2026-10-06) · **Version:** 0.2.0,
extended in every release since

## Context
Much of what this service does happens because time passed, not because a request arrived:
a schedule is due, a lease lapsed, a run passed its deadline or its working-time limit, a
question went unanswered past its deadline, a webhook delivery is due again, an artifact or a
run is past its retention. Doing these in the API process (timers per request, background
tasks per worker) would run them once per uvicorn worker and replica, lose them on a
restart, and mix slow sweeps into request latency.

## Decision
- **One loop, its own process**: `agent-runs-ticker`, every `TICK_SECONDS` (5 s), straight
  against the database. It fires due schedules, times out runs (deadline, working time),
  requeues or cancels runs whose lease lapsed, escalates or times out unanswered
  interrupts, delivers webhooks, drops old dead deliveries, and purges artifacts and, when
  configured, ended runs.
- **Safe in several replicas**: each step claims its rows with `FOR UPDATE SKIP LOCKED` and is
  bounded per tick (`SWEEP_BATCH`); a schedule fire is idempotent on
  `(schedule_id, fire_time)`.
- **The API never waits for time.** It records instants (`lease_expires_at`, `available_at`,
  `deadline`, `next_fire_at`) and the ticker acts on them.
- **A tick takes "now" as an argument** (`Ticker.tick(now=)`), so tests and examples move the
  clock instead of sleeping.
- A failing tick counts against a breaker, and a heartbeat file
  (`RUNS__TICKER__HEARTBEAT_FILE`) is the liveness probe.

## Consequences
- Everything time-driven happens within one tick (5 s), whatever the API's load.
- Several tickers may run for availability without doing anything twice.
- A run past its deadline is ended even if no request ever mentions it again.
- [ARCHITECTURE.md](../ARCHITECTURE.md#the-ticker) lists the steps in order.
