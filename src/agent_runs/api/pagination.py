"""Cursor pagination, as the Memory Service pages (its ``api/pagination.py``): every listing
takes ``cursor`` (opaque, copied from the previous page) and ``limit``, and answers
``Link: <url>; rel="next"`` (RFC 8288) exactly when a next page exists. The listings here
answer bare arrays, so the header is the whole of it.

The cursor is base64url JSON of the keyset the store ordered by (``store/paging.py``). It is
checked on the way in, so a malformed or foreign cursor is a 422 and never a database error,
and it is not signed: it only names a position, and every query is scoped by the caller's
tenant anyway.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Final

from fastapi import Query, Request, Response

from agent_runs.config.constants import DEFAULT_PAGE, MAX_PAGE
from agent_runs.domain.errors import Unprocessable

LINK: Final = "Link"
CURSOR_MAX_CHARS: Final = 1024
_NOT_OURS: Final = "not a cursor this listing issued"

CursorQuery = Annotated[
    str | None,
    Query(
        max_length=CURSOR_MAX_CHARS,
        description="Opaque position of the next page, copied from the previous response's "
        '`Link: <...>; rel="next"`. Omit for the first page.',
    ),
]
LimitQuery = Annotated[
    int,
    Query(ge=1, le=MAX_PAGE, description=f"Items per page, 1 to {MAX_PAGE}."),
]
DEFAULT_LIMIT: Final = DEFAULT_PAGE


def encode_cursor(position: Mapping[str, Any]) -> str:
    raw = json.dumps(dict(position), sort_keys=True, separators=(",", ":"), default=_text)
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _text(value: Any) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value)


def decode_cursor(cursor: str | None, *, fields: Mapping[str, type]) -> dict[str, Any] | None:
    """The position a cursor names, with exactly ``fields`` as members converted to their
    declared types (``str`` or a timezone-aware ``datetime``), else 422."""
    if not cursor:
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        position = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        if not isinstance(position, dict) or set(position) != set(fields):
            raise ValueError("not this listing's cursor")
        return {name: _member(position[name], kind) for name, kind in fields.items()}
    except (binascii.Error, UnicodeDecodeError, ValueError, TypeError) as exc:
        raise Unprocessable(
            "invalid cursor: copy it from the previous page's Link header",
            details={"errors": [{"loc": ["query", "cursor"], "msg": _NOT_OURS, "type": "cursor"}]},
        ) from exc


def _member(value: Any, kind: type) -> Any:
    if not isinstance(value, str):
        raise TypeError("a cursor member is a string")
    if kind is datetime:
        instant = datetime.fromisoformat(value)
        if instant.tzinfo is None:
            raise ValueError("a cursor instant is timezone-aware")
        return instant
    return value


def link_next(request: Request, response: Response, after: Mapping[str, Any] | None) -> None:
    """``Link: <this URL with cursor=…>; rel="next"`` when there is a next page."""
    if after is not None:
        url = request.url.include_query_params(cursor=encode_cursor(after))
        response.headers[LINK] = f'<{url}>; rel="next"'
