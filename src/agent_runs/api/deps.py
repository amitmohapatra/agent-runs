"""Request-scoped dependencies: a session, and who is calling."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header, Request, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from agent_runs.api.ratelimit import TenantRateLimiter
from agent_runs.config.constants import HEADER_API_KEY, HEADER_TENANT
from agent_runs.domain.errors import BadRequest, Forbidden, RateLimited, Unauthorized
from agent_runs.keys import KeyInfo, KeyRegistry
from agent_runs.observability.metrics import rate_limited_total


@dataclass(frozen=True)
class Caller:
    """The authenticated caller and the one tenant this request acts in."""

    credential: KeyInfo
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


#: The one scheme, as OpenAPI describes it. ``auto_error=False``: a missing key is this
#: service's own 401 problem, not FastAPI's 403.
API_KEY = APIKeyHeader(
    name=HEADER_API_KEY,
    scheme_name="ApiKeyAuth",
    description="A key issued by the Memory Service (its key registry). Header names are "
    "case-insensitive: X-Api-Key is the same header.",
    auto_error=False,
)


async def caller(
    request: Request,
    api_key: Annotated[str | None, Security(API_KEY)] = None,
    tenant: Annotated[
        str | None,
        Header(
            alias=HEADER_TENANT,
            description="The tenant this request acts in. Required with a platform key "
            "(one with no tenant of its own); a tenant key may send it only with its own "
            "tenant (403 otherwise).",
            max_length=128,
        ),
    ] = None,
) -> Caller:
    """One scheme: ``X-API-Key`` is the caller and names its tenant. A platform key (no
    tenant of its own) names the tenant it acts for in ``X-Trellis-Tenant``; a tenant key may
    send that header only to agree with itself. The key is introspected at the Memory
    Service's key registry (cached)."""
    if not api_key:
        raise Unauthorized(f"missing {HEADER_API_KEY}")
    keys: KeyRegistry = request.app.state.keys
    credential = await keys.resolve(api_key)
    if credential.tenant_id is None:
        if not tenant:
            raise BadRequest(f"a platform key names the tenant in {HEADER_TENANT}")
        acting = Caller(credential, tenant)
    elif tenant is not None and tenant != credential.tenant_id:
        raise Forbidden(f"{HEADER_TENANT} is not the tenant of this api key")
    else:
        acting = Caller(credential, credential.tenant_id)
    _within_budget(request, acting.tenant_id)
    return acting


def _within_budget(request: Request, tenant_id: str) -> None:
    """Take one of the tenant's tokens (``api/ratelimit.py``), or refuse with 429. The
    budget headers go on the response either way (the request middleware writes them)."""
    limiter: TenantRateLimiter = request.app.state.limiter
    if not limiter.enabled:
        return
    decision = limiter.take(tenant_id)
    request.state.ratelimit = decision.headers()
    if not decision.allowed:
        rate_limited_total.inc()
        raise RateLimited(
            "this tenant's request budget is spent; retry after Retry-After seconds",
            retry_after=decision.retry_after,
            headers=decision.headers(),
            details={"limit_per_minute": decision.limit},
        )


Session = Annotated[AsyncSession, Depends(session)]
Who = Annotated[Caller, Depends(caller)]
