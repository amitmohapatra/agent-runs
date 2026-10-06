# Examples

Nine scripts, simplest first. Each runs **in process**: the real agent-runs app (through
`httpx.ASGITransport`), the real ticker, and the real SDK `trellis.runs`, set up by
[`_local.py`](_local.py). There is no port, no Memory Service (a fake key registry answers),
no blob bucket (a temporary directory) and no webhook receiver on the network (an
`httpx.MockTransport`). Each script asserts what it shows, so exit 0 is a passing check.

The one thing they need is a PostgreSQL, the same one the test suite uses: each script
creates its own database (`agent_runs_examples`), migrates it to head, and drops it next time.
The admin URL is `RUNS_TEST_ADMIN_URL` (default
`postgresql://memory:memory@localhost:5432/postgres`; with `make up`, point it at port 5442).

```bash
make examples                                        # all of them, as CI does
uv run python examples/03_pause_resume_answer_check.py  # one
```

| # | Script | What it shows | Read with |
|---|---|---|---|
| 01 | [01_start_and_finish.py](01_start_and_finish.py) | a run your own process executes: `start`, an idempotent repeat, `finish`, `get`, `list` | [When to use what](../README.md#when-to-use-what) |
| 02 | [02_queue_claim_heartbeat.py](02_queue_claim_heartbeat.py) | `start(queue=True)`, `priority`, `claim`, `heartbeat` with a checkpoint, `LeaseLostError`, a lapsed lease requeued by the ticker, claimed again with its checkpoint | [Claim, lease and heartbeat](../docs/ARCHITECTURE.md#claim-lease-and-heartbeat) |
| 03 | [03_pause_resume_answer_check.py](03_pause_resume_answer_check.py) | `pause` with labelled options and an assignee, the inbox, a misfit answer (422), a key that may not answer (403), the answer, a resend, a second click (409), the audit trail | [Pause and resume, and the answer check](../docs/ARCHITECTURE.md#pause-and-resume-and-the-answer-check) |
| 04 | [04_cancel_and_release.py](04_cancel_and_release.py) | `cancel` of a queued run and of a held one (`cancel_requested`), `release` on shutdown | [Cancel and release](../docs/ARCHITECTURE.md#cancel-and-release) |
| 05 | [05_worker_loop.py](05_worker_loop.py) | the SDK's `Worker` around a handler: finish, a retryable error retried later, `job.checkpoint` and `job.pause`, an approval that sends the run back to the queue | [the SDK: the worker](../sdk/python/README.md#the-worker) |
| 06 | [06_schedule_fire.py](06_schedule_fire.py) | `schedules.create` (an upsert), `fire` on demand, a ticker fire; the run carries `priority`, `concurrency_key`, `timeout_seconds` and the schedule's `metadata` under the fire's keys | [A scheduled run firing](../docs/ARCHITECTURE.md#a-scheduled-run-firing) |
| 07 | [07_webhooks_and_dead_letters.py](07_webhooks_and_dead_letters.py) | a subscription, a signed delivery checked with `verify_signature`, a dead letter, `redeliver`, a secret rotation signed with both secrets | [Webhook delivery and dead letters](../docs/ARCHITECTURE.md#webhook-delivery-and-dead-letters) |
| 08 | [08_events_and_sse.py](08_events_and_sse.py) | `append_events` (a repeat stored once), `events` by position, `stream_events` (SSE) to the end | [Run events and the SSE stream](../docs/ARCHITECTURE.md#run-events-and-the-sse-stream) |
| 09 | [09_artifacts.py](09_artifacts.py) | `artifacts.upload`, a `REVIEW` interrupt with the `payload_ref`, `download` verified against its checksum | [api.md: artifacts](../docs/api.md#artifacts) |

**Against a running service.** The SDK calls are the same: drop `local_service()`, build
`RunsClient()` (it reads `RUNS_URL` and `TRELLIS_API_KEY`), and let a real ticker keep time
instead of `local.tick(...)`.
