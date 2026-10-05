"""SDK exceptions mapped from agent-runs' RFC 9457 problems.

The names and the shape are the memory SDK's (``trellis.memory.errors``), so one ``except``
reads both services. Branch on the class, or on ``code`` (the problem's stable category).

:class:`LeaseLostError` is **not** a :class:`ConflictError`: it tells a worker to stop
working the run and write nothing more, while a conflict is a refused transition the caller
may reconcile (by reading the run again). Code that catches ``ConflictError`` to re-read a
run must not swallow a lost lease, so the two are siblings under :class:`RunsError`.
"""

from __future__ import annotations

from typing import Any, Final


class RunsError(Exception):
    """Base SDK error. ``code`` mirrors the problem's code; ``status`` is the HTTP status (0
    when no response arrived); ``retryable`` says whether the same call may succeed later;
    ``retry_after`` is the seconds the service asked for (``Retry-After``), when it said;
    ``request_id`` is what to quote to whoever runs the service; ``details`` is the
    problem's structured context."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "INTERNAL",
        status: int = 500,
        retryable: bool = False,
        request_id: str | None = None,
        details: dict[str, Any] | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.status = status
        self.retryable = retryable
        self.request_id = request_id
        self.details = details or {}
        self.retry_after = retry_after

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(code={self.code}, status={self.status}, "
            f"message={self.message!r})"
        )


class AuthenticationError(RunsError):
    """401: no key, or one the key registry does not know (or revoked, expired)."""


class AuthorizationError(RunsError):
    """403: the key may not do this (another tenant, an ``on_behalf_of`` it may not act as,
    a paused run it may not answer)."""


class NotFoundError(RunsError):
    """404: no such run, schedule, webhook or artifact in this tenant."""


class ConflictError(RunsError):
    """409 ``CONFLICT``: the record is not in a state that allows the call (an illegal
    transition, an answer to another interrupt, a run id that cannot be used)."""


class LeaseLostError(RunsError):
    """409 ``LEASE_LOST``: the worker no longer holds the run. Stop working it and write
    nothing more; another worker runs it again."""


class ValidationError(RunsError):
    """400, 405 or 422: the request is invalid."""


class PayloadTooLargeError(ValidationError):
    """413: the body, a run's input or output, a checkpoint or an artifact is larger than
    the service accepts; never retryable as is."""


class RateLimitedError(RunsError):
    """429: the tenant's request budget is spent for now; ``retry_after`` says how long."""


class DependencyUnavailableError(RunsError):
    """503 (or 502, 504, or no response at all): the service or what it needs could not
    answer. Retryable unless the problem says otherwise."""


#: The class of each problem code; a code not here is classed by the status.
BY_CODE: Final[dict[str, type[RunsError]]] = {
    "AUTHENTICATION": AuthenticationError,
    "AUTHORIZATION": AuthorizationError,
    "NOT_FOUND": NotFoundError,
    "CONFLICT": ConflictError,
    "LEASE_LOST": LeaseLostError,
    "VALIDATION": ValidationError,
    "PAYLOAD_TOO_LARGE": PayloadTooLargeError,
    "RATE_LIMIT": RateLimitedError,
    "DEPENDENCY_UNAVAILABLE": DependencyUnavailableError,
}
#: The class and code an error status stands for when its body names no code (a proxy or a
#: load balancer answered instead of the service). Any other status is the base class.
BY_STATUS: Final[dict[int, tuple[type[RunsError], str]]] = {
    400: (ValidationError, "VALIDATION"),
    401: (AuthenticationError, "AUTHENTICATION"),
    403: (AuthorizationError, "AUTHORIZATION"),
    404: (NotFoundError, "NOT_FOUND"),
    405: (ValidationError, "VALIDATION"),
    409: (ConflictError, "CONFLICT"),
    413: (PayloadTooLargeError, "PAYLOAD_TOO_LARGE"),
    422: (ValidationError, "VALIDATION"),
    429: (RateLimitedError, "RATE_LIMIT"),
    502: (DependencyUnavailableError, "DEPENDENCY_UNAVAILABLE"),
    503: (DependencyUnavailableError, "DEPENDENCY_UNAVAILABLE"),
    504: (DependencyUnavailableError, "DEPENDENCY_UNAVAILABLE"),
}
#: The answers that mean "try again": too many requests, and a gateway or service in trouble.
RETRY_STATUSES: Final = frozenset({429, 502, 503, 504})


def error_from_problem(
    status: int,
    body: Any,
    *,
    where: str,
    text: str = "",
    request_id: str | None = None,
    retry_after: float | None = None,
) -> RunsError:
    """The typed exception for an error answer: by the problem's ``code``, else by the
    status, keeping its words, its details and whether it may be retried (the problem's
    ``retryable``; without one, whether the status is one that means "try again").
    ``where`` names the call (``"POST /v1/runs"``); ``text`` is the body when it is not a
    problem; ``request_id`` is the response's header, used when the problem names none."""
    problem: dict[str, Any] = body if isinstance(body, dict) else {}
    status_cls, status_code = BY_STATUS.get(status, (RunsError, "INTERNAL"))
    named = problem.get("code")
    code = named if isinstance(named, str) and named else None
    cls = BY_CODE.get(code, status_cls) if code else status_cls
    retryable = problem.get("retryable")
    details = problem.get("details")
    detail = problem.get("detail") or problem.get("title") or text[:300]
    named_request = problem.get("request_id")
    return cls(
        f"agent-runs {where}: HTTP {status}{f' {code}' if code else ''}: {detail}",
        code=code or status_code,
        status=status,
        retryable=retryable if isinstance(retryable, bool) else status in RETRY_STATUSES,
        request_id=named_request if isinstance(named_request, str) else request_id,
        details=details if isinstance(details, dict) else {},
        retry_after=retry_after,
    )
