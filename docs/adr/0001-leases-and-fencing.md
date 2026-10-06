# ADR 0001: Workers hold runs under a lease, and every write is fenced to the holder

**Status:** accepted · **Date:** 2026-09-30 (recorded 2026-10-06) · **Version:** 0.2.0,
amended in 0.3.1 and 0.3.2

## Context
A queued run is executed by whichever worker claims it, and workers die: a deploy, an OOM, a
lost node. The run must not be lost with the worker, and it must not be executed twice at
once either: a second worker resuming a run while the first, merely slow, still writes to it
would repeat side effects and overwrite its result. A lock held in a worker's memory cannot
outlive the worker, and a distributed lock service would be one more thing to run.

## Decision
- **A claim is a lease.** `POST /v1/runs/claim` takes the next queued run with
  `FOR UPDATE SKIP LOCKED` and records `lease_owner` (the `worker_id`) and
  `lease_expires_at`. The worker extends it with a heartbeat every third of the lease, which
  may also save a progress `checkpoint`.
- **Every write by a worker is fenced.** Heartbeat, release, pause, finish, event appends and
  artifact uploads name the `worker_id`; under the row lock, a write by anyone but the lease
  holder of a `RUNNING` run is `409 LEASE_LOST`, which means "stop working the run". A repeat
  of a write already made is answered with the stored run, so a worker that lost an answer may
  retry.
- **A lapsed lease requeues the run**, as its next attempt, with its checkpoint, after a
  jittered backoff (5 s doubling, at most 1 min). Only lapses count toward giving up
  (`MAX_LEASE_LAPSES`, 5, then `ERROR lease_expired`); a person's answers never do (0.3.1).
- **A stopping worker releases** what it holds (0.3.2): back on the queue at once, no lapse
  counted.

## Consequences
- At most one worker writes to a run at a time, without any lock outside PostgreSQL.
- A crash costs one lease length plus the backoff, and the next attempt resumes from the last
  checkpoint rather than from the start.
- Cancels, deadlines and working-time limits reuse the same fence: the worker learns from its
  next heartbeat or write.
- The SDK's `Worker` implements the loop (heartbeat, cancel the handler on `LEASE_LOST`,
  release on stop); [ARCHITECTURE.md](../ARCHITECTURE.md#claim-lease-and-heartbeat) draws it.
