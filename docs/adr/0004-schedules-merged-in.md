# ADR 0004: Schedules live in agent-runs, and a fire is a queued run

**Status:** accepted · **Date:** 2026-09-30 (recorded 2026-10-06) · **Version:** 0.2.0,
amended in 0.4.0 and by trellis-contracts 0.6.1

## Context
Schedules were a separate service, agent-schedules, that called agent-runs to start each
run. Two services, two databases and an HTTP call per fire meant a fire could succeed in one
and fail in the other: a schedule marked fired whose run never existed, or a run started
twice when the call was retried. Its own worker and its own queue duplicated what agent-runs
already had.

## Decision
- **One service.** agent-runs absorbed agent-schedules: `POST /v1/schedules` and the rest,
  fired by the ticker ([ADR 0002](0002-the-ticker-owns-time.md)).
- **A fire is a queued run**, inserted in the same transaction that advances the schedule,
  idempotent on `(schedule_id, fire_time)`, so a fire either happened with its run or not at
  all.
- **The run is built from the stored schedule alone**, above all its `on_behalf_of`, fixed
  when the schedule was made: no request can make a schedule fire as someone else.
- **A scheduled run carries everything a started one can** (0.4.0 and contracts 0.6.1): the
  schedule's `timeout_seconds`, `agent_version`, `priority` and `concurrency_key`, and its
  `metadata` under the fire's own keys, which win.
- A create is an upsert on the schedule's identity; a fire that cannot queue its run backs
  off, or pauses the schedule after `MAX_CONSECUTIVE_FAILURES`.

## Consequences
- Scheduled runs are ordinary queued runs: the same workers, inbox, webhooks and limits.
- A long outage fires each schedule once, not once per missed tick.
- [ARCHITECTURE.md](../ARCHITECTURE.md#a-scheduled-run-firing) draws a fire.
