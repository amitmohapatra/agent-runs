# ADR 0003: Notifications are webhooks sent from here, not from the harness

**Status:** accepted · **Date:** 2026-09-30 (recorded 2026-10-06) · **Version:** 0.2.0,
extended in 0.3.2

## Context
People need to hear that a run paused for them, was escalated, or ended. The harness once
had notifiers of its own (a Slack and an SMTP sender) and runs carried a `webhook_url`. A
notifier in the executor only fires if the executor is alive at that moment: a pause the
ticker escalates at 3 a.m., or a run the ticker times out, has no executor at all. And each
executor would need each team's channel credentials.

## Decision
- **The tenant subscribes URLs** to run events (`POST /v1/webhooks`: `run.paused`,
  `run.escalated`, `run.finished`); runs carry no `webhook_url` (trellis-contracts dropped
  it).
- **An outbox**: the event is written in the same transaction as the run change that caused
  it, one row per subscription that wants it, and the ticker delivers it at least once, with
  retries (7 attempts, 15 s doubling to 10 min).
- **Signed**: `X-Trellis-Signature` (HMAC-SHA256 over timestamp and body), made by the SDK's
  `trellis.runs.webhooks.sign`; receivers check it with `verify_signature`. A rotation signs
  with both secrets for an overlap (0.3.2).
- **Kept when given up on**: a dead delivery is listed and can be redelivered (0.3.2).
- **Guarded**: outside `dev` a URL must be `https` to public addresses only, checked on
  subscribe and again on every attempt, with the connection pinned to the checked address.

## Consequences
- A notification is sent whether or not any executor is alive, and exactly when the state
  changed (no state change without its event, no event without its state change).
- Slack, email or an inbox UI is a receiver of the tenant's own; the platform holds no
  channel credentials.
- Receivers must drop repeats by `event_id` (at least once).
- [ARCHITECTURE.md](../ARCHITECTURE.md#webhook-delivery-and-dead-letters) draws the flow.
