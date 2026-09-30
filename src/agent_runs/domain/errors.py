"""What can go wrong, each with the HTTP status it is. Raised by the stores and the firing
path; one exception handler in the app turns them into responses, so no route translates
errors by hand."""

from __future__ import annotations

from typing import Any

_BAD_REQUEST = 400
_UNAUTHORIZED = 401
_FORBIDDEN = 403
_NOT_FOUND = 404
_CONFLICT = 409
_TOO_LARGE = 413
_UNPROCESSABLE = 422
_UNAVAILABLE = 503

#: Why a ``webhook_url`` (a field the contracts still carry) is refused.
WEBHOOK_URL_REFUSED = (
    "webhook_url is not supported: notifications are tenant subscriptions, POST /v1/webhooks"
)


class ServiceError(Exception):
    status_code = 500

    def __init__(self, detail: str | dict[str, Any]) -> None:
        super().__init__(detail if isinstance(detail, str) else detail.get("message", ""))
        self.detail = detail


class BadRequest(ServiceError):
    status_code = _BAD_REQUEST


class Unauthorized(ServiceError):
    status_code = _UNAUTHORIZED


class Forbidden(ServiceError):
    status_code = _FORBIDDEN


class NotFound(ServiceError):
    status_code = _NOT_FOUND


class Conflict(ServiceError):
    """Well formed, but the record is not in a state that allows it: an illegal transition,
    an answer to another question, a lease that is no longer the caller's, a taken name.
    Usually a lost race or a repeat."""

    status_code = _CONFLICT


class TooLarge(ServiceError):
    status_code = _TOO_LARGE


class Unprocessable(ServiceError):
    status_code = _UNPROCESSABLE


class Unavailable(ServiceError):
    status_code = _UNAVAILABLE
