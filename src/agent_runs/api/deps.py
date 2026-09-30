"""Request-scoped dependencies: a session, and who is calling."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from agent_runs.config.constants import HEADER_API_KEY, HEADER_TENANT
from agent_runs.config.settings import Credential, Settings
from agent_runs.domain.errors import BadRequest, Forbidden, Unauthorized


@dataclass(frozen=True)
class Caller:
    """The authenticated caller and the one tenant this request acts in."""

    credential: Credential
    tenant_id: str

    @property
    def principal(self) -> str:
        return self.credential.principal

    def require_tenant(self, tenant_id: str) -> None:
        """A body naming a tenant must name this one: it cannot widen the credential."""
        if tenant_id != self.tenant_id:
            raise Forbidden("tenant_id is not the tenant this request acts in")

    def require_may_act_for(self, principal: str | None) -> None:
        """``on_behalf_of`` makes a run execute as someone; only a credential allowed to act
        as them may name them."""
        if principal is not None and not self.credential.may_act_for(principal):
            raise Forbidden(f"this credential may not act on behalf of {principal!r}")


async def session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessions() as s:
        yield s


def caller(
    request: Request,
    api_key: Annotated[str | None, Header(alias=HEADER_API_KEY)] = None,
    tenant: Annotated[str | None, Header(alias=HEADER_TENANT)] = None,
) -> Caller:
    """One scheme: ``X-Api-Key`` is the caller and names its tenant. A platform key (no
    tenant of its own) names the tenant it acts for in ``X-Trellis-Tenant``; a tenant key may
    send that header only to agree with itself."""
    settings: Settings = request.app.state.settings
    credential = settings.service.api_keys.get(api_key) if api_key else None
    if credential is None:
        raise Unauthorized("unknown api key")
    if credential.tenant_id is None:
        if not tenant:
            raise BadRequest(f"a platform key names the tenant in {HEADER_TENANT}")
        return Caller(credential, tenant)
    if tenant is not None and tenant != credential.tenant_id:
        raise Forbidden(f"{HEADER_TENANT} is not the tenant of this api key")
    return Caller(credential, credential.tenant_id)


Session = Annotated[AsyncSession, Depends(session)]
Who = Annotated[Caller, Depends(caller)]
