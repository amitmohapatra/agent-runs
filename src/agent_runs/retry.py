"""The one retry policy: a capped doubling backoff, and a breaker for a loop whose
dependency is down. Webhook deliveries, failed schedule fires and the ticker all use these.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import structlog

log = structlog.get_logger(__name__)


def backoff(base: timedelta, failures: int, *, cap: timedelta) -> timedelta:
    """The wait after the ``failures``-th consecutive failure: ``base * 2**(failures-1)``,
    never more than ``cap``."""
    return min(base * 2 ** max(failures - 1, 0), cap)


@dataclass
class Breaker:
    """Stop calling a dependency that keeps failing; after ``cooldown`` let one call through
    to find out whether it is back (half-open). A failed trial restarts the cooldown."""

    threshold: int
    cooldown: timedelta
    failures: int = field(default=0, init=False)
    opened_at: datetime | None = field(default=None, init=False)

    @property
    def is_open(self) -> bool:
        return self.opened_at is not None

    def allows(self, now: datetime) -> bool:
        return self.opened_at is None or now - self.opened_at >= self.cooldown

    def record_success(self) -> None:
        if self.opened_at is not None:
            log.info("breaker.closed", after_failures=self.failures)
        self.failures = 0
        self.opened_at = None

    def record_failure(self, now: datetime) -> None:
        self.failures += 1
        if self.opened_at is not None or self.failures >= self.threshold:
            if self.opened_at is None:
                log.warning("breaker.opened", failures=self.failures)
            self.opened_at = now
