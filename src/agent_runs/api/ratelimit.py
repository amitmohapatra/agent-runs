"""Per-tenant rate limiting: a token bucket per tenant, in this process's memory.

Every ``/v1`` request of a tenant takes one token from that tenant's bucket, which holds at
most ``burst`` tokens and refills at ``per_minute`` a minute. An empty bucket is a 429
problem with ``Retry-After`` (seconds until a token is back); every counted response carries
``X-RateLimit-Limit`` (the budget a minute) and ``X-RateLimit-Remaining`` (tokens left).

Per process on purpose: agent-runs has no shared cache, and a limiter that asked PostgreSQL
would put a write on every request of the database it protects. So each worker of each
replica keeps its own buckets, and the budget a tenant really gets is ``per_minute`` times
the number of workers: a guard against a runaway client (a worker loop claiming in a tight
loop), not a quota. The tenant is the authenticated one, so a flood of bad keys never
reaches here (the key cache answers those).
"""

from __future__ import annotations

import math
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from agent_runs.config.settings import RateLimitSettings

HEADER_LIMIT: Final = "X-RateLimit-Limit"
HEADER_REMAINING: Final = "X-RateLimit-Remaining"
#: Buckets kept per process (least recently used goes first): a full bucket costs nothing
#: to forget, since a forgotten tenant starts full again.
MAX_BUCKETS: Final = 10_000
_SECONDS_PER_MINUTE: Final = 60.0


@dataclass(frozen=True)
class Decision:
    allowed: bool
    limit: int
    remaining: int
    #: whole seconds until the next token, when refused
    retry_after: int

    def headers(self) -> dict[str, str]:
        return {HEADER_LIMIT: str(self.limit), HEADER_REMAINING: str(self.remaining)}


class TenantRateLimiter:
    def __init__(
        self, settings: RateLimitSettings, *, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._per_minute = settings.per_minute
        self._burst = float(settings.burst)
        self._rate = settings.per_minute / _SECONDS_PER_MINUTE
        self._clock = clock
        #: tenant -> (tokens, when they were counted)
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()

    @property
    def enabled(self) -> bool:
        return self._per_minute > 0

    def take(self, tenant_id: str) -> Decision:
        """Take one token from the tenant's bucket, if there is one."""
        now = self._clock()
        tokens, then = self._buckets.get(tenant_id, (self._burst, now))
        tokens = min(self._burst, tokens + (now - then) * self._rate)
        allowed = tokens >= 1.0
        if allowed:
            tokens -= 1.0
        self._buckets[tenant_id] = (tokens, now)
        self._buckets.move_to_end(tenant_id)
        while len(self._buckets) > MAX_BUCKETS:
            self._buckets.popitem(last=False)
        wait = 0 if allowed else max(1, math.ceil((1.0 - tokens) / self._rate))
        return Decision(allowed, self._per_minute, math.floor(tokens), wait)
