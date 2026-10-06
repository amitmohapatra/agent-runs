"""Per-tenant rate limiting, shared by every worker of every replica: one bucket per tenant,
kept in PostgreSQL.

Every ``/v1`` request of a tenant takes one request from that tenant's budget, which holds at
most ``burst`` requests and refills at ``per_minute`` a minute. An empty budget is a 429
problem with ``Retry-After`` (seconds until a request is allowed again); every counted
response carries ``X-RateLimit-Limit`` (the budget a minute) and ``X-RateLimit-Remaining``
(requests left now).

The budget is a GCRA bucket (the token bucket, kept as one time): a row per tenant holds the
instant its budget would be full again (``tat``). Taking a request moves it one interval
(``60 / per_minute`` seconds) later, refused when that would put it more than the burst
ahead of now. One statement does it, an upsert on the tenant's row under the database's own
clock, so concurrent requests on any replica draw on the one budget and no replica's clock
matters. That costs one small write per request, on its own connection, outside the
request's transaction (a refused request leaves no trace beyond its 429). The tenant is the
authenticated one, so a flood of bad keys never reaches here (the key cache answers those).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import timedelta
from typing import Final

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_runs.config.settings import RateLimitSettings
from agent_runs.store.tables import RateLimitRow

HEADER_LIMIT: Final = "X-RateLimit-Limit"
HEADER_REMAINING: Final = "X-RateLimit-Remaining"
_SECONDS_PER_MINUTE: Final = 60.0
#: Seconds compared on the database's microsecond clock: a budget exactly full is full.
_PRECISION: Final = 6


@dataclass(frozen=True)
class Decision:
    allowed: bool
    limit: int
    remaining: int
    #: whole seconds until a request is allowed again, when refused
    retry_after: int

    def headers(self) -> dict[str, str]:
        return {HEADER_LIMIT: str(self.limit), HEADER_REMAINING: str(self.remaining)}


class TenantRateLimiter:
    """The tenants' budgets, in ``engine``'s ``rate_limit_buckets``."""

    def __init__(self, settings: RateLimitSettings, engine: AsyncEngine) -> None:
        self._per_minute = settings.per_minute
        self._burst = settings.burst
        self._engine = engine

    @property
    def enabled(self) -> bool:
        return self._per_minute > 0

    async def take(self, tenant_id: str) -> Decision:
        """Take one request from the tenant's budget, if there is one left."""
        interval = _SECONDS_PER_MINUTE / self._per_minute
        tolerance = interval * (self._burst - 1)
        clock = func.now()
        due = func.greatest(RateLimitRow.tat, clock)
        took = (
            insert(RateLimitRow)
            .values(bucket=tenant_id, tat=clock + timedelta(seconds=interval))
            .on_conflict_do_update(
                index_elements=[RateLimitRow.bucket],
                set_={"tat": due + timedelta(seconds=interval)},
                where=due - clock <= timedelta(seconds=tolerance),
            )
            .returning(RateLimitRow.tat)
            .cte("took")
        )
        # the CTE's write is not visible to the outer query: ``ahead`` is the budget before
        taken = select(func.extract("epoch", took.c.tat - clock)).scalar_subquery()
        ahead = (
            select(func.extract("epoch", RateLimitRow.tat - clock))
            .where(RateLimitRow.bucket == tenant_id)
            .scalar_subquery()
        )
        async with self._engine.begin() as conn:
            row = (await conn.execute(select(taken, ahead))).one()
        if row[0] is not None:
            left = round(self._burst - float(row[0]) / interval, _PRECISION)
            return Decision(True, self._per_minute, max(0, math.floor(left)), 0)
        wait = round(float(row[1]) - tolerance, _PRECISION)
        return Decision(False, self._per_minute, 0, max(1, math.ceil(wait)))
