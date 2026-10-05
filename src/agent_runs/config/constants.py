"""Design decisions, named once. Deployment facts (URLs, secrets, ports, pool sizes) are
settings (``settings.py``); nothing here differs between deployments."""

from __future__ import annotations

from datetime import timedelta
from typing import Final

# --------------------------------------------------------------------------- the wire

#: The caller's credential. It names the tenant it speaks for, unless it is a platform key.
#: Spelled as the Memory Service spells it; header names are case-insensitive, so
#: ``X-Api-Key`` is the same header.
HEADER_API_KEY: Final = "X-API-Key"
#: The tenant a platform key acts for. A tenant key may send it only to agree with itself.
HEADER_TENANT: Final = "X-Trellis-Tenant"
#: The caller's request id, echoed on every response and quoted in every problem; a client
#: that sends none (or one that is not an id) gets a generated one.
HEADER_REQUEST_ID: Final = "X-Request-ID"
#: Seconds a client waits before repeating a request a dependency could not serve (a 503):
#: long enough for a database failover or a restarted key registry to come back, short
#: enough that a worker's lease outlives a couple of retries.
RETRY_AFTER_SECONDS: Final = 5
#: The longest the readiness probe waits for the database before answering 503.
READY_TIMEOUT_SECONDS: Final = 3.0

#: How long an introspected key is trusted before the registry is asked again: the longest
#: a revoked key keeps working here.
KEY_CACHE_SECONDS: Final = 60.0
#: How long a refused key stays refused without asking again.
KEY_NEGATIVE_CACHE_SECONDS: Final = 10.0
KEY_INTROSPECTION_TIMEOUT_SECONDS: Final = 3.0
#: Opening a connection to the registry, within the timeout above.
KEY_CONNECT_TIMEOUT_SECONDS: Final = 2.0
#: Connections to the registry: introspection is one small GET per uncached key, so a few
#: kept alive (30 s, under the usual 60 s idle timeout of a load balancer) carry it.
KEY_MAX_CONNECTIONS: Final = 100
KEY_MAX_KEEPALIVE: Final = 20
KEY_KEEPALIVE_SECONDS: Final = 30.0
#: Keys cached per process (least recently used goes first).
MAX_CACHED_KEYS: Final = 10_000

#: A page of runs or schedules.
DEFAULT_PAGE: Final = 50
MAX_PAGE: Final = 500

#: The largest executor checkpoint a pause may carry, serialized as compact JSON. A resume
#: journal and a framework's resume state fit well inside it; anything bigger belongs in an
#: artifact the checkpoint refers to.
MAX_CHECKPOINT_BYTES: Final = 1024 * 1024

# --------------------------------------------------------------------------- artifacts

#: The largest artifact one upload may carry (an ``ask`` table, a diff, a report).
MAX_ARTIFACT_BYTES: Final = 50 * 1024 * 1024
#: How long a run's artifacts outlive the run: a reviewer can still open what a finished run
#: asked about; after that the ticker deletes them.
ARTIFACT_RETENTION: Final = timedelta(days=7)
#: The role of the key (the Memory Service's registry) that may add an artifact to a paused
#: run: the tenant's service principal (a harness or its UI backend), not a user or admin.
PAUSED_ARTIFACT_ROLE: Final = "service"
#: The unit a blob is read and streamed in.
BLOB_CHUNK_BYTES: Final = 1024 * 1024

# --------------------------------------------------------------------------- the queue

#: A lease a worker may ask for. Short enough that a dead worker's run is back on the queue
#: within minutes; long enough that a heartbeat every third of it is cheap.
MIN_LEASE_SECONDS: Final = 5
MAX_LEASE_SECONDS: Final = 3600
DEFAULT_LEASE_SECONDS: Final = 60
#: Lapsed leases after which a run is failed instead of re-queued: a run that kills every
#: worker that claims it would otherwise take the fleet down in turn. Only lapses count, not
#: attempts: a person's answer starts an attempt too, and review rounds are not crashes.
MAX_LEASE_LAPSES: Final = 5
#: The wait before a run whose lease lapsed may be claimed again, doubling per lapse up to the
#: cap (jittered): a run that kills its worker is not handed straight to the next one.
LAPSE_RETRY_BASE: Final = timedelta(seconds=5)
LAPSE_RETRY_CAP: Final = timedelta(minutes=1)
#: Times a queued run that its worker ended ``ERROR`` with a retryable error goes back on the
#: queue before the error stands: a blip (a model's rate limit, a dependency restarting) is
#: retried, a failure that keeps coming back is not retried forever.
MAX_ERROR_RETRIES: Final = 3
#: The wait before each of those retries, doubling up to the cap (jittered): 10 s, 20 s, 40 s.
ERROR_RETRY_BASE: Final = timedelta(seconds=10)
ERROR_RETRY_CAP: Final = timedelta(minutes=10)

#: Runs of one tenant sharing a ``concurrency_key`` that may be RUNNING at once, unless the
#: deployment says otherwise (``RUNS__RUNS__CONCURRENCY_PER_KEY``): one, so a thread's second
#: message waits for its first run.
CONCURRENCY_PER_KEY: Final = 1

# --------------------------------------------------------------------------- the ticker

#: How often the ticker comes round. The worst-case lateness of a schedule, a run past its
#: deadline, a lapsed lease and an overdue interrupt.
TICK_SECONDS: Final = 5.0
#: Rows each sweep handles per tick; the rest wait for the next one, so one tick is bounded.
SWEEP_BATCH: Final = 100
#: Consecutive failed ticks (the database, not one schedule) before the ticker backs off.
BREAKER_THRESHOLD: Final = 5
BREAKER_COOLDOWN: Final = timedelta(minutes=2)
#: The heartbeat file is touched after every tick; the probe calls the loop dead past this.
HEARTBEAT_MAX_AGE_SECONDS: Final = 60.0

# --------------------------------------------------------------------------- schedules

#: Consecutive failed fires before a schedule pauses itself. Three is "not a blip".
MAX_CONSECUTIVE_FAILURES: Final = 3
#: The wait after a retryable failed fire, doubling per failure up to the cap.
FIRE_RETRY_BASE: Final = timedelta(minutes=5)
FIRE_RETRY_CAP: Final = timedelta(hours=1)
#: How far ahead of the clock a fire may claim to be for: a replica's clock a little ahead,
#: not a typo'd year that would retire the schedule.
MAX_FIRE_SKEW: Final = timedelta(minutes=1)

# --------------------------------------------------------------------------- webhooks

#: Subscriptions per tenant: the fan-out of one event is bounded.
MAX_WEBHOOKS_PER_TENANT: Final = 20
WEBHOOK_TIMEOUT_SECONDS: Final = 10.0
#: How long a ticker holds a delivery it is sending; past it another ticker may send it.
WEBHOOK_LEASE: Final = timedelta(seconds=2 * WEBHOOK_TIMEOUT_SECONDS)
#: Attempts per delivery, including the first, spaced by the one backoff helper (15 s,
#: 30 s, 1 min, 2 min, 4 min, 8 min): about a quarter of an hour of receiver downtime.
WEBHOOK_ATTEMPTS: Final = 7
WEBHOOK_RETRY_BASE: Final = timedelta(seconds=15)
WEBHOOK_RETRY_CAP: Final = timedelta(minutes=10)
#: Receiver answers worth another attempt: a 5xx, or the receiver asking for time.
WEBHOOK_RETRYABLE: Final = frozenset({408, 429, 500, 502, 503, 504})
#: How long a delivery given up on is kept, dead, for a tenant to list and redeliver, unless
#: the deployment says otherwise (``RUNS__WEBHOOKS__DEAD_RETENTION_DAYS``).
WEBHOOK_DEAD_RETENTION: Final = timedelta(days=7)
