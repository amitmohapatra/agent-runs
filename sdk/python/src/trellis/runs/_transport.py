"""HTTP transport: the key and tenant headers, retries, and problems as typed errors.

Every call is retried — up to ``max_retries`` times, with exponential backoff and full
jitter, or after the ``Retry-After`` the service sent (at most
:data:`RETRY_AFTER_MAX_SECONDS`) — when it failed on the way: a transport error, ``429``,
``502``, ``503``, ``504``, unless the problem says ``retryable: false``. That is safe for
every write agent-runs takes: a start is idempotent on its id (or its idempotency key), a
repeated pause or finish from the same worker with the same status answers the stored record,
the same artifact bytes are the same artifact, a schedule create is an upsert, a fire for one
tick queues one run; a claim whose answer was lost leaves its run leased and unworked until
the lease lapses and agent-runs queues it again. An error answer is read from its problem
document by its ``code`` (:func:`~trellis.runs.errors.error_from_problem`).
"""

from __future__ import annotations

import asyncio
import random
import re
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Final

import httpx
from trellis.runs.errors import (
    RETRY_STATUSES,
    DependencyUnavailableError,
    NotFoundError,
    RunsError,
    error_from_problem,
)

#: The caller's credential, a key issued by the Memory Service.
API_KEY_HEADER: Final = "X-API-Key"
#: The tenant a platform key acts for (a tenant key may send it only with its own tenant).
TENANT_HEADER: Final = "X-Trellis-Tenant"
REQUEST_ID_HEADER: Final = "X-Request-ID"
#: How long one attempt may take before it counts as failed (and is retried).
TIMEOUT_SECONDS: Final = 10.0
#: Retries of a call that failed on the way, after the first attempt.
RETRIES: Final = 3
#: The backoff ceiling of the first retry, doubled for each one after it (full jitter: the
#: wait is uniform between 0 and the ceiling), and the highest ceiling.
BACKOFF_SECONDS: Final = 0.25
BACKOFF_MAX_SECONDS: Final = 5.0
#: The longest ``Retry-After`` honoured.
RETRY_AFTER_MAX_SECONDS: Final = 30.0
NO_CONTENT: Final = 204
#: Items a listing page asks for when the caller names no limit (the service allows 1-500).
PAGE_LIMIT: Final = 50
#: How long an event stream may stay silent before it counts as broken: the service sends a
#: comment every 15 s on a quiet stream.
STREAM_IDLE_SECONDS: Final = 60.0
#: The wait between attempts; a name of its own so tests can stand it in.
_sleep = asyncio.sleep


class Transport:
    """One ``httpx.AsyncClient`` and the retry policy. ``tenant`` is the default
    ``X-Trellis-Tenant`` (a platform key's tenant); a call that names one sends that."""

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None,
        tenant: str | None,
        timeout: float,
        max_retries: int,
        client: httpx.AsyncClient | None,
    ) -> None:
        headers = {
            "User-Agent": "trellis-runs-python",
            "Accept": "application/json, application/problem+json",
        }
        if api_key:
            headers[API_KEY_HEADER] = api_key
        if client is None:
            client = httpx.AsyncClient(
                base_url=base_url.rstrip("/"), headers=headers, timeout=timeout
            )
            self._owns_client = True
        else:
            client.headers.update(headers)
            # an injected client is the caller's; one without a base URL gets this one
            if not str(client.base_url):
                client.base_url = base_url.rstrip("/")
            self._owns_client = False
        self._client = client
        self.tenant = tenant
        self.max_retries = max_retries

    async def send(
        self,
        method: str,
        path: str,
        *,
        tenant: str | None = None,
        headers: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """The answer of the first attempt worth returning; an error answer is raised as its
        typed error (see the module's docstring for what is retried)."""
        sent = dict(headers or {})
        if tenant := tenant or self.tenant:
            sent[TENANT_HEADER] = tenant
        retry = 0
        while True:
            try:
                response = await self._client.request(method, path, headers=sent, **kwargs)
            except httpx.TransportError as exc:
                if retry >= self.max_retries:
                    raise DependencyUnavailableError(
                        f"agent-runs {method} {path} unreachable: {type(exc).__name__}: {exc}",
                        code="DEPENDENCY_UNAVAILABLE",
                        status=0,
                        retryable=True,
                    ) from exc
                delay = backoff(retry, None)
            except httpx.HTTPError as exc:
                raise RunsError(
                    f"agent-runs {method} {path} failed: {type(exc).__name__}: {exc}", status=0
                ) from exc
            else:
                if not response.is_error:
                    return response
                error = refusal(response)
                if not _again(response, error) or retry >= self.max_retries:
                    raise error
                delay = backoff(retry, response.headers.get("retry-after"))
            retry += 1
            await _sleep(delay)

    async def json(self, method: str, path: str, **kwargs: Any) -> Any:
        """The decoded body of a successful answer."""
        return (await self.send(method, path, **kwargs)).json()

    async def found(self, method: str, path: str, **kwargs: Any) -> Any | None:
        """A read by id: the decoded body, or None when there is no such record (404)."""
        try:
            return await self.json(method, path, **kwargs)
        except NotFoundError:
            return None

    async def page(self, path: str, **kwargs: Any) -> tuple[Any, str | None]:
        """A listing's body and the cursor of its next page (``Link: rel="next"``)."""
        response = await self.send("GET", path, **kwargs)
        return response.json(), next_cursor(response.headers.get("link"))

    async def events(
        self, path: str, *, tenant: str | None = None, params: Mapping[str, Any] | None = None
    ) -> AsyncIterator[tuple[str, str]]:
        """A server-sent event stream's events as ``(event, data)``, ids and comments left out;
        an error answer is raised as its typed error. Not retried: the caller reconnects."""
        headers = {"Accept": "text/event-stream"}
        if tenant := tenant or self.tenant:
            headers[TENANT_HEADER] = tenant
        timeout = httpx.Timeout(TIMEOUT_SECONDS, read=STREAM_IDLE_SECONDS)
        async with self._client.stream(
            "GET", path, headers=headers, params=params, timeout=timeout
        ) as response:
            if response.is_error:
                await response.aread()
                raise refusal(response)
            name, data = "message", ""
            async for line in response.aiter_lines():
                if line.startswith("event: "):
                    name = line.removeprefix("event: ")
                elif line.startswith("data: "):
                    data = line.removeprefix("data: ")
                elif not line and data:
                    yield name, data
                    name, data = "message", ""

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def refusal(response: httpx.Response) -> RunsError:
    """The error an error answer means (see :func:`~trellis.runs.errors.error_from_problem`)."""
    try:
        body = response.json()
    except ValueError:
        body = None
    return error_from_problem(
        response.status_code,
        body,
        where=f"{response.request.method} {response.request.url.path}",
        text=response.text,
        request_id=response.headers.get(REQUEST_ID_HEADER),
        retry_after=retry_after(response.headers.get("retry-after")),
    )


def _again(response: httpx.Response, error: RunsError) -> bool:
    """Whether an error answer is worth another attempt: a status that means "try again",
    unless the service said it is not (a fire that paused its schedule)."""
    return response.status_code in RETRY_STATUSES and error.retryable


def backoff(retry: int, retry_after_header: str | None) -> float:
    """How long to wait before retry number ``retry + 1``: the service's ``Retry-After``
    (seconds or an HTTP date, at most :data:`RETRY_AFTER_MAX_SECONDS`) when it sent one,
    else full jitter under an exponentially growing ceiling."""
    asked = retry_after(retry_after_header)
    if asked is not None:
        return min(asked, RETRY_AFTER_MAX_SECONDS)
    ceiling = min(BACKOFF_MAX_SECONDS, BACKOFF_SECONDS * 2**retry)
    return random.uniform(0, ceiling)  # jitter, not a secret


def retry_after(value: str | None) -> float | None:
    """Seconds a ``Retry-After`` asks for: delta-seconds or an HTTP date; None when absent or
    unreadable."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:  # "-0000": UTC, by RFC 5322
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - datetime.now(UTC)).total_seconds())


_NEXT_LINK: Final = re.compile(r'<([^>]+)>\s*;\s*rel="?next"?')


def next_cursor(link: str | None) -> str | None:
    """The ``cursor`` of a ``Link`` header's ``rel="next"`` target; None on the last page."""
    for part in (link or "").split(","):
        match = _NEXT_LINK.search(part)
        if match is not None:
            cursor = httpx.URL(match.group(1)).params.get("cursor")
            return cursor or None
    return None


def worker_params(worker_id: str | None) -> dict[str, str]:
    """The ``worker_id`` query that fences a write, when the call names a worker."""
    return {"worker_id": worker_id} if worker_id else {}
