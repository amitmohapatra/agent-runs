"""Design decisions, named once. Deployment facts (URLs, secrets, ports, pool sizes) are
settings (``settings.py``); nothing here differs between deployments."""

from __future__ import annotations

from datetime import timedelta
from typing import Final

# --------------------------------------------------------------------------- the wire

#: The caller's credential. It names the tenant it speaks for, unless it is a platform key.
HEADER_API_KEY: Final = "X-Api-Key"
#: The tenant a platform key acts for. A tenant key may send it only to agree with itself.
HEADER_TENANT: Final = "X-Trellis-Tenant"
#: Webhook headers, spelled as the Memory Service spells them so one receiver verifies both.
HEADER_SIGNATURE: Final = "X-Trellis-Signature"
HEADER_EVENT: Final = "X-Trellis-Event"
HEADER_DELIVERY: Final = "X-Trellis-Delivery"

#: A page of runs or schedules.
DEFAULT_PAGE: Final = 50
MAX_PAGE: Final = 500
#: How many ancestors a lineage walks before it stops: a cycle cannot exist (a parent is
#: written before its child), but a bound keeps the one recursive query honest.
MAX_LINEAGE: Final = 100

#: The largest executor checkpoint a pause may carry, serialized as compact JSON. A resume
#: journal and a framework's resume state fit well inside it; anything bigger belongs in an
#: artifact the checkpoint refers to.
MAX_CHECKPOINT_BYTES: Final = 1024 * 1024

# --------------------------------------------------------------------------- the queue

#: A lease a worker may ask for. Short enough that a dead worker's run is back on the queue
#: within minutes; long enough that a heartbeat every third of it is cheap.
MIN_LEASE_SECONDS: Final = 5
MAX_LEASE_SECONDS: Final = 3600
DEFAULT_LEASE_SECONDS: Final = 60
#: Attempts after which a run whose lease keeps lapsing is failed instead of re-queued: a run
#: that kills every worker that claims it would otherwise take the fleet down in turn.
MAX_ATTEMPTS: Final = 5

# --------------------------------------------------------------------------- the ticker

#: How often the ticker comes round. The worst-case lateness of a schedule, a lapsed lease
#: and an overdue interrupt.
TICK_SECONDS: Final = 5.0
#: Rows each sweep handles per tick; the rest wait for the next one, so one tick is bounded.
SWEEP_BATCH: Final = 100
#: Consecutive failed ticks (the database, not one schedule) before the ticker backs off.
BREAKER_THRESHOLD: Final = 5
BREAKER_COOLDOWN: Final = timedelta(minutes=2)
#: Touched after every tick; the container probe calls the loop dead past the max age.
HEARTBEAT_PATH: Final = "/tmp/agent-runs-ticker.heartbeat"  # container-local
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

WEBHOOK_TIMEOUT_SECONDS: Final = 10.0
#: Attempts per notification, including the first, spaced by the one backoff helper.
WEBHOOK_ATTEMPTS: Final = 4
WEBHOOK_RETRY_BASE: Final = timedelta(seconds=1)
WEBHOOK_RETRY_CAP: Final = timedelta(seconds=30)
#: Receiver answers worth another attempt: a 5xx, or the receiver asking for time.
WEBHOOK_RETRYABLE: Final = frozenset({408, 429, 500, 502, 503, 504})
