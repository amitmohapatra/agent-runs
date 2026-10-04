"""Request middleware, as plain ASGI callables (no ``BaseHTTPMiddleware``: it costs a task
and a stream pair per request for an API this one does not need).

``RequestContextMiddleware`` names the request: the caller's ``X-Request-ID`` when it is an
id, else a generated one, kept in the request state (every problem quotes it) and echoed on
every response.
"""

from __future__ import annotations

import re
from typing import Final

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from trellis.contracts.ids import new_id

from agent_runs.config.constants import HEADER_REQUEST_ID

#: The id alphabet shared with the Memory Service: a letter or digit, then letters, digits
#: and ``._:-``, at most 200 characters.
_ID: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:\-]{0,199}$")


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
        scope.setdefault("state", {})["request_id"] = rid

        async def send_with_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message).setdefault(HEADER_REQUEST_ID, rid)
            await send(message)

        await self.app(scope, receive, send_with_id)
