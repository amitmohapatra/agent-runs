"""Request-scoped dependencies: a session, and who is calling."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from agent_runs.config.settings import Credential, Settings

_UNAUTHORIZED = 401
_FORBIDDEN = 403


async def session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessions() as s:
        yield s


def caller(
    request: Request,
    x_tenant_id: Annotated[str | None, Header()] = None,
    x_api_key: Annotated[str | None, Header()] = None,
) -> Credential:
    """Who is calling, resolved from the credential rather than from a header.

    The API key is checked on every request, in every environment. It used to be checked
    only when ``auth_mode == "trusted_dev"``, which meant the startup check's own advice —
    use another mode outside dev — turned authentication off, and a typo in the mode name
    did the same thing silently.

    The credential, not ``X-Tenant-Id``, decides the tenant. A header may still be sent, but
    only to *agree*: a key issued to one tenant naming another is a 403, not a way to read a
    stranger's paused runs and the questions they are asking.
    """
    cfg: Settings = request.app.state.settings
    credential = cfg.service.api_keys.get(x_api_key) if x_api_key else None
    if credential is None:
        raise HTTPException(_UNAUTHORIZED, "unknown api key")
    if x_tenant_id is not None and x_tenant_id != credential.tenant_id:
        raise HTTPException(
            _FORBIDDEN, "X-Tenant-Id is not the tenant this credential is issued to"
        )
    return credential


Caller = Annotated[Credential, Depends(caller)]


def tenant(who: Caller) -> str:
    """The calling tenant.

    Every query in the store is scoped by this. It is a dependency rather than a parameter
    so that no route can forget it — a runs service that leaks across tenants leaks the
    questions other people's paused runs are waiting on.
    """
    return who.tenant_id


Session = Annotated[AsyncSession, Depends(session)]
Tenant = Annotated[str, Depends(tenant)]
