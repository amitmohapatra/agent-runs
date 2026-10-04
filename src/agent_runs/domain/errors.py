"""What can go wrong, each with the HTTP status it is and the stable ``code`` a client acts
on. Raised by the stores and the firing path; one exception handler in the app turns them
into RFC 9457 problems (``api/errors.py``), so no route translates errors by hand.

``code`` is the Memory Service's vocabulary (one client maps both services' errors) plus
``LEASE_LOST``: a worker writing to a run whose lease it no longer holds must stop working
it, which is a different thing to do from any other conflict.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from agent_runs.config.constants import RETRY_AFTER_SECONDS

_BAD_REQUEST = 400
_UNAUTHORIZED = 401
_FORBIDDEN = 403
_NOT_FOUND = 404
_CONFLICT = 409
_TOO_LARGE = 413
_UNPROCESSABLE = 422
_TOO_MANY = 429
_UNAVAILABLE = 503


class ErrorCode(StrEnum):
    """The problem's ``code``: stable, machine-readable, one per way of failing."""

    #: the request is malformed or names something it may not (400, 422)
    VALIDATION = "VALIDATION"
    #: no ``X-API-Key``, or one the key registry does not know (401)
    AUTHENTICATION = "AUTHENTICATION"
    #: a known key that may not do this (403)
    AUTHORIZATION = "AUTHORIZATION"
    #: no such record in this tenant (404)
    NOT_FOUND = "NOT_FOUND"
    #: the record is not in a state that allows it, or a limit is reached (409)
    CONFLICT = "CONFLICT"
    #: the worker no longer holds the run's lease: stop working the run (409)
    LEASE_LOST = "LEASE_LOST"
    #: the body, or a part of it with its own bound, is too large (413)
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
    #: the tenant's request budget is spent for now (429, ``Retry-After``)
    RATE_LIMIT = "RATE_LIMIT"
    #: PostgreSQL or the key registry (Memory Service) did not answer (503, ``Retry-After``)
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
    #: a fault of this service; the detail says nothing about its internals (500)
    INTERNAL = "INTERNAL"


class ServiceError(Exception):
    """One failure: ``detail`` is the sentence for this occurrence, ``details`` structured
    context, ``headers`` any the response must carry (``Retry-After`` is added for a
    retryable 429 or 503)."""

    status_code = 500
    code = ErrorCode.INTERNAL
    retryable = False

    def __init__(
        self,
        detail: str,
        *,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        retryable: bool | None = None,
    ) -> None:
        super().__init__(detail)
        self.detail = detail
        self.details = details or {}
        self.headers = dict(headers or {})
        if retryable is not None:
            self.retryable = retryable


class BadRequest(ServiceError):
    status_code = _BAD_REQUEST
    code = ErrorCode.VALIDATION


class Unauthorized(ServiceError):
    status_code = _UNAUTHORIZED
    code = ErrorCode.AUTHENTICATION


class Forbidden(ServiceError):
    status_code = _FORBIDDEN
    code = ErrorCode.AUTHORIZATION


class NotFound(ServiceError):
    status_code = _NOT_FOUND
    code = ErrorCode.NOT_FOUND


class Conflict(ServiceError):
    """Well formed, but the record is not in a state that allows it: an illegal transition,
    an answer to another question, a taken name, a reused idempotency key. Usually a lost
    race or a repeat."""

    status_code = _CONFLICT
    code = ErrorCode.CONFLICT


class LeaseLost(Conflict):
    """A worker fencing its write (``worker_id``) on a run whose lease it no longer holds:
    the lease lapsed and the run went back on the queue, or the run was paused, cancelled or
    finished. The worker must stop working the run."""

    code = ErrorCode.LEASE_LOST


class TooLarge(ServiceError):
    status_code = _TOO_LARGE
    code = ErrorCode.PAYLOAD_TOO_LARGE


class Unprocessable(ServiceError):
    status_code = _UNPROCESSABLE
    code = ErrorCode.VALIDATION


class RateLimited(ServiceError):
    """The tenant's request budget is spent for now; a token is back in ``retry_after``
    seconds."""

    status_code = _TOO_MANY
    code = ErrorCode.RATE_LIMIT
    retryable = True

    def __init__(
        self,
        detail: str,
        *,
        retry_after: int,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(detail, details=details, headers=headers)
        self.retry_after = retry_after


class Unavailable(ServiceError):
    """A dependency did not answer; the same request may succeed in ``retry_after``
    seconds."""

    status_code = _UNAVAILABLE
    code = ErrorCode.DEPENDENCY_UNAVAILABLE
    retryable = True

    def __init__(
        self,
        detail: str,
        *,
        details: dict[str, Any] | None = None,
        retryable: bool | None = None,
        retry_after: int = RETRY_AFTER_SECONDS,
    ) -> None:
        super().__init__(detail, details=details, retryable=retryable)
        self.retry_after = retry_after


def field_errors(errors: list[Any]) -> list[dict[str, Any]]:
    """Pydantic's errors as a problem's ``details.errors``: where, what and which rule, never
    the value that failed (it may be a secret, or a megabyte)."""
    return [
        {"loc": [str(p) for p in e.get("loc", ())], "msg": e.get("msg"), "type": e.get("type")}
        for e in errors
    ]
