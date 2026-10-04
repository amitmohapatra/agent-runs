"""RFC 9457 problem details and the exception handlers that produce them.

Every error response is ``application/problem+json`` with this shape (the Memory Service's,
so one client reads both)::

    {"type": "urn:trellis:problem:lease-lost", "title": "Lease lost", "status": 409,
     "detail": "worker w1 does not hold the lease on run run_…", "instance": "/v1/runs/…",
     "code": "LEASE_LOST", "retryable": false, "request_id": "req_…", "details": {}}

``code`` is the stable category (``domain.errors.ErrorCode``) and ``type`` its URN. A
retryable 429 or 503 carries ``Retry-After`` in seconds. One builder serves the handlers
below and the responses the middleware writes before a route runs (413, 429), so there is
exactly one error shape on the wire.
"""

from __future__ import annotations

from typing import Any, Final

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from starlette.exceptions import HTTPException as StarletteHTTPException

from agent_runs.config.constants import HEADER_REQUEST_ID, RETRY_AFTER_SECONDS
from agent_runs.domain.errors import ErrorCode, ServiceError, Unavailable, field_errors

log = structlog.get_logger(__name__)

PROBLEM_MEDIA_TYPE: Final = "application/problem+json"
PROBLEM_TYPE_PREFIX: Final = "urn:trellis:problem:"
RETRY_AFTER: Final = "Retry-After"

#: One title per category: the same words for every occurrence, as RFC 9457 asks.
PROBLEM_TITLES: Final[dict[ErrorCode, str]] = {
    ErrorCode.VALIDATION: "Invalid request",
    ErrorCode.AUTHENTICATION: "Authentication required",
    ErrorCode.AUTHORIZATION: "Not permitted",
    ErrorCode.NOT_FOUND: "Not found",
    ErrorCode.CONFLICT: "Conflict",
    ErrorCode.LEASE_LOST: "Lease lost",
    ErrorCode.PAYLOAD_TOO_LARGE: "Payload too large",
    ErrorCode.RATE_LIMIT: "Too many requests",
    ErrorCode.DEPENDENCY_UNAVAILABLE: "A dependency is unavailable",
    ErrorCode.INTERNAL: "Internal error",
}

#: What a bare HTTP status (a route that does not exist, a method it does not take, a body
#: past the cap) is, as a category.
_STATUS_CODES: Final[dict[int, ErrorCode]] = {
    401: ErrorCode.AUTHENTICATION,
    403: ErrorCode.AUTHORIZATION,
    404: ErrorCode.NOT_FOUND,
    409: ErrorCode.CONFLICT,
    413: ErrorCode.PAYLOAD_TOO_LARGE,
    429: ErrorCode.RATE_LIMIT,
    503: ErrorCode.DEPENDENCY_UNAVAILABLE,
}
_RETRYABLE_STATUSES: Final = frozenset({429, 503})
_SERVER_ERROR = 500
_UNPROCESSABLE = 422


def problem_type(code: ErrorCode) -> str:
    """``urn:trellis:problem:<code in kebab case>``: a stable identifier, not a URL to fetch."""
    return PROBLEM_TYPE_PREFIX + code.value.lower().replace("_", "-")


class Problem(BaseModel):
    """RFC 9457 problem details with the extension members a client acts on."""

    model_config = ConfigDict(frozen=True)

    type: str = Field(
        description="URN of the category: urn:trellis:problem:<code in kebab case>.",
        examples=["urn:trellis:problem:lease-lost"],
    )
    title: str = Field(
        description="Short summary of the category; the same for every occurrence.",
        examples=["Lease lost"],
    )
    status: int = Field(description="The HTTP status code.", examples=[409])
    detail: str = Field(
        description="What went wrong in this occurrence. Never echoes a submitted value.",
        examples=["worker w1 does not hold the lease on run run_01J8"],
    )
    instance: str = Field(description="The request path.", examples=["/v1/runs/run_01J8/finish"])
    code: ErrorCode = Field(
        description="Stable machine-readable category. LEASE_LOST: the worker no longer holds "
        "the run's lease and must stop working it; CONFLICT: any other state conflict; "
        "DEPENDENCY_UNAVAILABLE and RATE_LIMIT are retryable after Retry-After."
    )
    retryable: bool = Field(description="Whether the same request may succeed if repeated.")
    request_id: str | None = Field(
        default=None, description="The request's X-Request-ID, for support tickets and logs."
    )
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Category-specific structured context: `errors` (each with `loc`, `msg`, "
        "`type`) for a 422, the schedule's state for a failed fire, the budget for a 429.",
    )


class ProblemResponse(JSONResponse):
    media_type = PROBLEM_MEDIA_TYPE


def build_problem(
    *,
    code: ErrorCode,
    detail: str,
    status: int,
    retryable: bool,
    instance: str,
    request_id: str | None,
    details: dict[str, Any] | None = None,
) -> Problem:
    return Problem(
        type=problem_type(code),
        title=PROBLEM_TITLES[code],
        status=status,
        detail=detail,
        instance=instance,
        code=code,
        retryable=retryable,
        request_id=request_id,
        details=details or {},
    )


def problem_response(problem: Problem, *, headers: dict[str, str] | None = None) -> ProblemResponse:
    return ProblemResponse(
        status_code=problem.status, content=problem.model_dump(mode="json"), headers=headers
    )


def request_id_of(scope_state: dict[str, Any] | None) -> str | None:
    """The id the request middleware recorded (``None`` before it ran)."""
    return (scope_state or {}).get("request_id")


def _problem(
    request: Request,
    *,
    code: ErrorCode,
    detail: str,
    status: int,
    retryable: bool,
    details: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    retry_after: int = RETRY_AFTER_SECONDS,
) -> ProblemResponse:
    sent = dict(headers or {})
    if retryable and status in _RETRYABLE_STATUSES:
        sent.setdefault(RETRY_AFTER, str(retry_after))
    request_id = request_id_of(request.scope.get("state"))
    if request_id is not None:
        sent.setdefault(HEADER_REQUEST_ID, request_id)
    return problem_response(
        build_problem(
            code=code,
            detail=detail,
            status=status,
            retryable=retryable,
            instance=request.url.path,
            request_id=request_id,
            details=details,
        ),
        headers=sent,
    )


def is_unavailable(exc: BaseException) -> bool:
    """A database failure worth a retry: no connection, a connection lost or invalidated
    mid-statement, no pooled connection free in time, a statement cancelled by its timeout.
    Anything else the database refuses is a fault of the request or of this service."""
    if isinstance(exc, PoolTimeout | OperationalError | InterfaceError):
        return True
    return isinstance(exc, DBAPIError) and exc.connection_invalidated


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ServiceError)
    async def _service_error(request: Request, exc: ServiceError) -> ProblemResponse:
        if exc.status_code >= _SERVER_ERROR:
            log.error("request.failed", code=exc.code, detail=exc.detail, path=request.url.path)
        return _problem(
            request,
            code=exc.code,
            detail=exc.detail,
            status=exc.status_code,
            retryable=exc.retryable,
            details=exc.details,
            headers=exc.headers,
            retry_after=getattr(exc, "retry_after", RETRY_AFTER_SECONDS),
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError) -> ProblemResponse:
        return _problem(
            request,
            code=ErrorCode.VALIDATION,
            detail="the request is invalid",
            status=_UNPROCESSABLE,
            retryable=False,
            details={"errors": field_errors(list(exc.errors()))},
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(request: Request, exc: StarletteHTTPException) -> ProblemResponse:
        status = exc.status_code
        code = _STATUS_CODES.get(
            status, ErrorCode.INTERNAL if status >= _SERVER_ERROR else ErrorCode.VALIDATION
        )
        return _problem(
            request,
            code=code,
            detail=str(exc.detail),
            status=status,
            retryable=status in _RETRYABLE_STATUSES,
            headers=dict(exc.headers or {}),  # 405's Allow, a 429's budget
        )

    async def _database_error(request: Request, exc: Exception) -> ProblemResponse:
        if is_unavailable(exc):
            log.warning("request.database_unavailable", error=type(exc).__name__)
            unavailable = Unavailable("the database is unavailable")
            return await _service_error(request, unavailable)
        log.error("request.database_error", error=type(exc).__name__, path=request.url.path)
        return _problem(
            request,
            code=ErrorCode.INTERNAL,
            detail="internal error",
            status=_SERVER_ERROR,
            retryable=False,
        )

    app.add_exception_handler(DBAPIError, _database_error)
    app.add_exception_handler(PoolTimeout, _database_error)

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> ProblemResponse:
        # Starlette's outermost ServerErrorMiddleware runs this one, outside every other
        # middleware: the request id goes into the log line and the headers by hand.
        log.exception(
            "request.unhandled",
            path=request.url.path,
            request_id=request_id_of(request.scope.get("state")),
        )
        return _problem(
            request,
            code=ErrorCode.INTERNAL,
            detail="internal error",
            status=_SERVER_ERROR,
            retryable=False,
        )


#: Examples of each status, for the OpenAPI document (``api/openapi.py``).
ERROR_EXAMPLES: Final[dict[int, tuple[ErrorCode, str, bool]]] = {
    400: (ErrorCode.VALIDATION, "a platform key names the tenant in X-Trellis-Tenant", False),
    401: (ErrorCode.AUTHENTICATION, "missing X-API-Key", False),
    403: (ErrorCode.AUTHORIZATION, "X-Trellis-Tenant is not the tenant of this api key", False),
    404: (ErrorCode.NOT_FOUND, "no run run_01J8", False),
    409: (ErrorCode.CONFLICT, "run run_01J8 cannot move from SUCCESS to CANCELLED", False),
    413: (ErrorCode.PAYLOAD_TOO_LARGE, "the body is larger than 4194304 bytes", False),
    422: (ErrorCode.VALIDATION, "the request is invalid", False),
    429: (ErrorCode.RATE_LIMIT, "this tenant's request budget is spent", True),
    500: (ErrorCode.INTERNAL, "internal error", False),
    503: (ErrorCode.DEPENDENCY_UNAVAILABLE, "the database is unavailable", True),
}


def problem_example(status: int, *, instance: str, detail: str | None = None) -> dict[str, Any]:
    code, default, retryable = ERROR_EXAMPLES[status]
    details: dict[str, Any] = {}
    if status == _UNPROCESSABLE:
        missing = {"loc": ["body", "agent_id"], "msg": "Field required", "type": "missing"}
        details = {"errors": [missing]}
    return build_problem(
        code=code,
        detail=detail or default,
        status=status,
        retryable=retryable,
        instance=instance,
        request_id="req_01J8ZQ4Y6V9W3X2K7M5N0P1R2S",
        details=details,
    ).model_dump(mode="json")
