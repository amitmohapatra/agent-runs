"""Request middleware, as plain ASGI callables (no ``BaseHTTPMiddleware``: it costs a task
and a stream pair per request for an API this one does not need). A request meets them in
this order:

``RequestContextMiddleware`` names the request (the caller's ``X-Request-ID`` when it is an
id, else a generated one, kept in the request state for every problem and echoed on every
response), counts it in the metrics by route template and status, and writes the rate-limit
headers the caller dependency decided on.

``BodyLimitMiddleware`` refuses a JSON body past ``RUNS__SERVICE__MAX_BODY_BYTES``: at once
when ``Content-Length`` says so, else when the bytes that arrived (a chunked body has no
length) pass it, before the route has it all. Artifact uploads stream to the blob store and
count their own bytes against ``MAX_ARTIFACT_BYTES``.

``CompressionMiddleware`` gzips responses of 1 KiB and more for a client that accepts it,
except artifact bytes: they are served as stored, with the length and checksum they have.
"""

from __future__ import annotations

import asyncio
import re
import time
from typing import Final

from starlette.datastructures import Headers, MutableHeaders
from starlette.exceptions import HTTPException
from starlette.middleware.gzip import GZipMiddleware
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from trellis.contracts.ids import new_id

from agent_runs.api.errors import build_problem, problem_response
from agent_runs.config.constants import HEADER_REQUEST_ID
from agent_runs.domain.errors import ErrorCode
from agent_runs.observability.metrics import http_request_seconds, http_requests_total

#: The id alphabet shared with the Memory Service: a letter or digit, then letters, digits
#: and ``._:-``, at most 200 characters.
_ID: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:\-]{0,199}$")
#: The artifact upload route: its body is bytes of its own bound, streamed to the blob store.
ARTIFACT_UPLOAD: Final = re.compile(r"^/v1/runs/[^/]+/artifacts$")
#: The artifact download route: its bytes are never recompressed.
ARTIFACT_DOWNLOAD_PREFIX: Final = "/v1/artifacts/"
GZIP_MINIMUM_BYTES: Final = 1024
_GZIP_LEVEL: Final = 5
_TOO_LARGE: Final = 413
_CLIENT_CLOSED: Final = 499
_SERVER_ERROR: Final = 500


def request_id(headers: Headers) -> str:
    value = headers.get(HEADER_REQUEST_ID)
    return value if value is not None and _ID.match(value) else new_id("req_")


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        rid = request_id(Headers(scope=scope))
        state = scope.setdefault("state", {})
        state["request_id"] = rid
        started = time.perf_counter()
        recorded = False

        def record(status: int) -> None:
            nonlocal recorded
            recorded = True
            route = getattr(scope.get("route"), "path", None) or "unmatched"
            http_requests_total.labels(scope["method"], route, str(status)).inc()
            elapsed = time.perf_counter() - started
            http_request_seconds.labels(scope["method"], route).observe(elapsed)

        async def send_with_context(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers.setdefault(HEADER_REQUEST_ID, rid)
                for name, value in state.get("ratelimit", {}).items():
                    headers.setdefault(name, value)
                record(message["status"])
            await send(message)

        try:
            await self.app(scope, receive, send_with_context)
        except BaseException as exc:
            # nothing answered yet: count what the outermost error handler will answer (a
            # cancelled request, a client that went away, as 499: nothing failed here)
            if not recorded:
                record(_CLIENT_CLOSED if isinstance(exc, asyncio.CancelledError) else _SERVER_ERROR)
            raise


class BodyLimitMiddleware:
    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or ARTIFACT_UPLOAD.match(scope["path"]):
            await self.app(scope, receive, send)
            return
        limit = self.max_body_bytes
        detail = f"the body is larger than {limit} bytes"
        declared = Headers(scope=scope).get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > limit:
            problem = build_problem(
                code=ErrorCode.PAYLOAD_TOO_LARGE,
                detail=detail,
                status=_TOO_LARGE,
                retryable=False,
                instance=scope["path"],
                request_id=scope.get("state", {}).get("request_id"),
            )
            await problem_response(problem)(scope, receive, send)
            return
        seen = 0

        async def counted() -> Message:
            nonlocal seen
            message = await receive()
            if message["type"] == "http.request":
                seen += len(message.get("body", b""))
                if seen > limit:
                    # raised inside the route's read of its body, so the app's handler for
                    # HTTP errors answers it as a 413 problem
                    raise HTTPException(_TOO_LARGE, detail=detail)
            return message

        await self.app(scope, counted, send)


class CompressionMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app
        self.gzip = GZipMiddleware(app, minimum_size=GZIP_MINIMUM_BYTES, compresslevel=_GZIP_LEVEL)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope["path"].startswith(ARTIFACT_DOWNLOAD_PREFIX):
            await self.app(scope, receive, send)
            return
        await self.gzip(scope, receive, send)
