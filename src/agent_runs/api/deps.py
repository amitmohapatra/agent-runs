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


@dataclass(frozen=True)
class Claimer:
    """Who claims, and from which queue: the tenant's, or with a platform key that names no
    tenant, every tenant's (``tenant_id`` None), shared fairly between them."""

    credential: KeyInfo
    tenant_id: str | None


ApiKey = Annotated[str | None, Security(API_KEY)]
TenantHeader = Annotated[
    str | None,
    Header(
        alias=HEADER_TENANT,
        description="The tenant this request acts in. Required with a platform key (one with "
        "no tenant of its own), except on a claim, where leaving it out claims from every "
        "tenant's queue; a tenant key may send it only with its own tenant (403 otherwise).",
        max_length=128,
    ),
]


async def _acting(request: Request, api_key: str | None, tenant: str | None) -> Claimer:
    """The key, introspected at the Memory Service's key registry (cached), and the tenant
    it acts in: its own, or the one a platform key names (``None`` when it names none). A
    tenant key may send ``X-Trellis-Tenant`` only to agree with itself."""
    if not api_key:
        raise Unauthorized(f"missing {HEADER_API_KEY}")
    keys: KeyRegistry = request.app.state.keys
    credential = await keys.resolve(api_key)
    if credential.tenant_id is None:
        return Claimer(credential, tenant or None)
    if tenant is not None and tenant != credential.tenant_id:
        raise Forbidden(f"{HEADER_TENANT} is not the tenant of this api key")
    return Claimer(credential, credential.tenant_id)


async def caller(request: Request, api_key: ApiKey = None, tenant: TenantHeader = None) -> Caller:
    """One scheme: ``X-API-Key`` is the caller and names its tenant. A platform key (no
    tenant of its own) names the tenant it acts for in ``X-Trellis-Tenant``."""
    acting = await _acting(request, api_key, tenant)
    if acting.tenant_id is None:
        raise BadRequest(f"a platform key names the tenant in {HEADER_TENANT}")
    await _within_budget(request, acting.tenant_id)
    return Caller(acting.credential, acting.tenant_id)


async def claimer(request: Request, api_key: ApiKey = None, tenant: TenantHeader = None) -> Claimer:
    """The caller of a claim: as :func:`caller`, except that a platform key may name no
    tenant, and then claims from every tenant's queue (its budget is the key's own)."""
    acting = await _acting(request, api_key, tenant)
    await _within_budget(request, acting.tenant_id or f"key:{acting.credential.key_id}")
    return acting


async def _within_budget(request: Request, tenant_id: str) -> None:
    """Take one of the tenant's tokens (``api/ratelimit.py``), or refuse with 429. The
    budget headers go on the response either way (the request middleware writes them)."""
    limiter: TenantRateLimiter = request.app.state.limiter
    if not limiter.enabled:
        return
    decision = await limiter.take(tenant_id)
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
Claiming = Annotated[Claimer, Depends(claimer)]
