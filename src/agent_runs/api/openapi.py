"""The OpenAPI document: metadata, the one security scheme, the problem schema on every error
response, the standard response headers, and stable operation ids (``<tag>.<function>``).

Routes declare what is particular to them (summary, description, a ``409``'s meaning, request
examples); everything every route shares is attached here, so it cannot be forgotten on one.
``docs/openapi.json`` is this document as committed; ``tests/test_openapi.py`` fails when it
and the code disagree, and ``python -m agent_runs.tools.export_openapi`` rewrites it.
"""

from __future__ import annotations

from typing import Any, Final

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi.routing import APIRoute

from agent_runs.api.errors import PROBLEM_MEDIA_TYPE, RETRY_AFTER, Problem, problem_example
from agent_runs.api.pagination import LINK
from agent_runs.api.ratelimit import HEADER_LIMIT, HEADER_REMAINING
from agent_runs.config.constants import HEADER_REQUEST_ID

TITLE: Final = "agent-runs"

DESCRIPTION: Final = """
Durable agent runs, the worker queue, the human inbox, and the schedules that start runs.
This service never executes an agent: a harness does, in its own process or as a worker
claiming queued runs, and records here what happened.

### Authentication
Every `/v1` route needs `X-API-Key` (any case), a key issued by the Memory Service, which
agent-runs introspects there (`GET /v1/keys/self`, cached 60 s). A platform key names the
tenant it acts for in `X-Trellis-Tenant`; a tenant key may send that header only with its own
tenant. The `ops` routes need no key.

Reading is tenant-wide for every key. Answering a paused run (`resume`) and cancelling a run
(`cancel`) are not: an admin or platform key, or one that may act for anyone (`*` in
`may_act_as`, the default), answers or cancels any run; a key restricted to listed people only
a run assigned to one of them or to nobody (never one assigned to a group).

### Errors
Every error is an RFC 9457 problem (`application/problem+json`, schema `Problem`): branch on
`code`. `LEASE_LOST` (409) tells a worker it no longer holds the run's lease: stop working the
run. `CONFLICT` (409) is any other state conflict. `DEPENDENCY_UNAVAILABLE` (503) and
`RATE_LIMIT` (429) are retryable after `Retry-After` seconds.

### Retries
Starting a run is idempotent on its `run_id` and its `idempotency_key`; a repeated `pause`,
`finish` or `release` by the caller that made it answers the stored run, a repeated `resume`
with the very same resolution answers the run as it is now, and a repeated `cancel` answers the
run as it is; claims and heartbeats are safe to repeat. A client may retry transport errors,
429, 502, 503 and 504. agent-runs retries on its own too: a queued run its worker ends `ERROR`
with a retryable error goes back on the queue after a backoff (at most 3 times), a run whose
lease lapsed after a short one, and a webhook delivery takes up to 7 attempts before it is kept
as dead.

### Pages
Every listing takes `cursor` and `limit` and answers `Link: <url>; rel="next"` when there is
more; the bodies are bare arrays.

### Limits
JSON bodies are at most 4 MiB (`413`), a run's `input` and `output` 1 MiB each, a checkpoint
1 MiB, an artifact 50 MiB. Each tenant's requests draw on a per-process token bucket
(`X-RateLimit-Limit`, `X-RateLimit-Remaining`; `429` when empty).
"""

TAGS: Final[list[dict[str, Any]]] = [
    {
        "name": "runs",
        "description": "Recording runs, the worker queue (claim, heartbeat with progress "
        "checkpoints and the working time left, release), pausing for a person and resuming, "
        "cancelling, finishing, the inbox and each run's answered interrupts.",
    },
    {
        "name": "artifacts",
        "description": "Payloads too large for a checkpoint or an interrupt (an ask table, a "
        "diff), stored in blob storage, checksum-verified, tenant-scoped.",
    },
    {
        "name": "schedules",
        "description": "When runs start, on whose behalf, while nobody is present: an upsert "
        "on the schedule's identity, pause and resume, fire now.",
    },
    {
        "name": "webhooks",
        "description": "The tenant's subscriptions to run events (paused, escalated, finished), "
        "each signed with its own secret (rotated with an overlap); the deliveries owed to "
        "them, and the dead ones to redeliver.",
    },
    {"name": "ops", "description": "Liveness, readiness and Prometheus metrics; no key."},
]

CONTACT: Final = {"name": "agent-runs", "url": "https://github.com/amitmohapatra/agent-runs"}
LICENSE: Final = {"name": "Proprietary"}
SERVERS: Final = [{"url": "/", "description": "this host"}]

#: What each error status means on any route that can answer it; a route's own
#: ``responses`` entry (a 409's particular meaning) takes precedence.
ERROR_DESCRIPTIONS: Final[dict[int, str]] = {
    400: "VALIDATION: a platform key named no tenant in X-Trellis-Tenant.",
    401: "AUTHENTICATION: no X-API-Key, or one the key registry does not know.",
    403: "AUTHORIZATION: the key registry refuses the key, or the request names a tenant or "
    "on_behalf_of the key may not act for.",
    404: "NOT_FOUND: no such record in this tenant.",
    409: "CONFLICT: the record is not in a state that allows this.",
    413: "PAYLOAD_TOO_LARGE: the body, or a part of it with its own bound, is too large.",
    422: "VALIDATION: the body, a parameter or a cursor is invalid; details.errors says where.",
    429: "RATE_LIMIT: the tenant's request budget is spent; retry after Retry-After seconds.",
    500: "INTERNAL: a fault of this service.",
    503: "DEPENDENCY_UNAVAILABLE: PostgreSQL or the key registry did not answer; retry after "
    "Retry-After seconds.",
}
_CREATED: Final = 201
_FIRST_ERROR: Final = 400
_UNPROCESSABLE: Final = 422
_RETRYABLE: Final = frozenset({429, 503})
#: What any authenticated route may answer before it looks at the request.
AUTH_STATUSES: Final[tuple[int, ...]] = (400, 401, 403, 422, 429, 503)

RESPONSE_HEADERS: Final[dict[str, str]] = {
    HEADER_REQUEST_ID: "The request id: the caller's when it sent one that is an id, else a "
    "generated one. Quoted in every problem.",
    HEADER_LIMIT: "The tenant's request budget a minute; present when the rate limiter "
    "counted the request.",
    HEADER_REMAINING: "Requests left in the tenant's bucket; present when the rate limiter "
    "counted the request.",
    RETRY_AFTER: "Seconds to wait before repeating the request (on a retryable 429 or 503).",
    LINK: 'RFC 8288: rel="next" names the next page of the listing; present exactly when '
    "there is one.",
    "Location": "Where the record this request created lives.",
}


def operation_id(route: APIRoute) -> str:
    """``<tag>.<function>``: stable across path edits and readable in generated clients."""
    return f"{route.tags[0]}.{route.name}" if route.tags else route.name


def conflict(description: str) -> dict[int | str, dict[str, Any]]:
    """A route's own 409, described: what the conflict means there."""
    return {409: {"description": description}}


def _ref(name: str) -> dict[str, str]:
    return {"$ref": f"#/components/headers/{name}"}


def _problem_content(status: int, path: str) -> dict[str, Any]:
    return {
        PROBLEM_MEDIA_TYPE: {
            "schema": {"$ref": f"#/components/schemas/{Problem.__name__}"},
            "example": problem_example(status, instance=path),
        }
    }


def _statuses(path: str, op: dict[str, Any]) -> list[int]:
    """The error statuses this operation can answer, besides any it declares itself."""
    if not path.startswith("/v1/"):
        return [503] if path == "/health/ready" else []
    statuses: list[int] = list(AUTH_STATUSES)
    if "{" in path:
        statuses.append(404)
    if "requestBody" in op:
        statuses.append(413)
    return statuses


def _document(path: str, method: str, op: dict[str, Any]) -> None:
    """Every error response a problem with its meaning and example, the standard headers on
    every response, ``Link`` on a listing's, ``Location`` on a create's."""
    responses: dict[str, Any] = op.setdefault("responses", {})
    for status in _statuses(path, op):
        responses.setdefault(str(status), {})
    paged = any(p.get("name") == "cursor" for p in op.get("parameters", []))
    for status, response in responses.items():
        code = int(status)
        headers = response.setdefault("headers", {})
        headers[HEADER_REQUEST_ID] = _ref(HEADER_REQUEST_ID)
        if path.startswith("/v1/"):
            headers[HEADER_LIMIT] = _ref(HEADER_LIMIT)
            headers[HEADER_REMAINING] = _ref(HEADER_REMAINING)
        if code >= _FIRST_ERROR:
            if code == _UNPROCESSABLE or not response.get("description"):
                response["description"] = ERROR_DESCRIPTIONS[code]
            response["content"] = _problem_content(code, path)
            if code in _RETRYABLE:
                headers[RETRY_AFTER] = _ref(RETRY_AFTER)
        elif paged:
            headers[LINK] = _ref(LINK)
        if code == _CREATED and method == "post":
            headers["Location"] = _ref("Location")


def _drop_unused_schemas(schema: dict[str, Any]) -> None:
    """FastAPI's own validation error schemas, no longer referenced once every 422 is a
    problem."""
    schemas = schema.get("components", {}).get("schemas", {})
    for name in ("HTTPValidationError", "ValidationError"):
        schemas.pop(name, None)


def custom_openapi(app: FastAPI) -> dict[str, Any]:
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=TITLE,
        version=app.version,
        summary="Durable agent runs, the worker queue, the human inbox and schedules.",
        description=DESCRIPTION,
        routes=app.routes,
        tags=TAGS,
        servers=SERVERS,
        contact=CONTACT,
        license_info=LICENSE,
    )
    components = schema.setdefault("components", {})
    schemas = components.setdefault("schemas", {})
    # no route names Problem as a response model (it would be documented as
    # application/json too), so its schema and the enum it references are added here
    problem = Problem.model_json_schema(ref_template="#/components/schemas/{model}")
    for name, definition in problem.pop("$defs", {}).items():
        schemas.setdefault(name, definition)
    schemas[Problem.__name__] = problem
    components["headers"] = {
        name: {"description": text, "schema": {"type": "string"}}
        for name, text in RESPONSE_HEADERS.items()
    }
    for path, methods in schema.get("paths", {}).items():
        for method, op in methods.items():
            _document(path, method, op)
    _drop_unused_schemas(schema)
    app.openapi_schema = schema
    return schema
