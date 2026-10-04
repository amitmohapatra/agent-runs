"""Who an ``X-API-Key`` is: asked of the Memory Service, the one key registry.

agent-runs keeps no keys. It introspects each key it sees with ``GET {memory}/v1/keys/self``
(the key in ``X-API-Key``, as for any memory call) and caches the answer per key: a known
key for ``KEY_CACHE_SECONDS``, a refused one for ``KEY_NEGATIVE_CACHE_SECONDS``, so a
revocation takes effect within a minute and a flood of bad keys costs the registry one call
per key per interval. Only a hash of each key is held. The contract is in docs/api.md.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from hashlib import sha256

import httpx
import structlog
from pydantic import BaseModel, ConfigDict, ValidationError

from agent_runs.config.constants import (
    HEADER_API_KEY,
    KEY_CACHE_SECONDS,
    KEY_INTROSPECTION_TIMEOUT_SECONDS,
    KEY_NEGATIVE_CACHE_SECONDS,
    MAX_CACHED_KEYS,
)
from agent_runs.domain.errors import Forbidden, ServiceError, Unauthorized, Unavailable

log = structlog.get_logger(__name__)

INTROSPECTION_PATH = "/v1/keys/self"
_UNAUTHORIZED = 401
_FORBIDDEN = 403


class KeyInfo(BaseModel):
    """The introspection answer. The key *is* the caller: it names the tenant it speaks for
    (``None`` for a platform key, which then names the tenant in ``X-Trellis-Tenant``), the
    principal recorded as ``created_by``, and the principals it may make runs execute as
    (``on_behalf_of``; ``"*"`` is any). ``role`` is the registry's (``platform``, ``admin``,
    ``service``, …), carried for the record."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    key_id: str
    tenant_id: str | None
    principal: str
    role: str
    may_act_as: tuple[str, ...] = ()

    @property
    def platform(self) -> bool:
        return self.tenant_id is None

    def may_act_for(self, principal: str) -> bool:
        return principal == self.principal or "*" in self.may_act_as or principal in self.may_act_as


class KeyRegistry:
    """The Memory Service's key registry, behind a bounded TTL cache."""

    def __init__(
        self,
        memory_url: str,
        *,
        client: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client or httpx.AsyncClient(timeout=KEY_INTROSPECTION_TIMEOUT_SECONDS)
        self._url = memory_url.rstrip("/") + INTROSPECTION_PATH
        self._clock = clock
        #: sha256(key) -> (expires at, the answer): a KeyInfo, or the refusal to raise
        self._cache: OrderedDict[str, tuple[float, KeyInfo | ServiceError]] = OrderedDict()

    async def resolve(self, api_key: str) -> KeyInfo:
        """The key's identity. ``Unauthorized`` for a key the registry does not know (or
        revoked, or expired), ``Forbidden`` for one it refuses (a suspended tenant),
        ``Unavailable`` when the registry cannot be asked (never cached)."""
        digest = sha256(api_key.encode()).hexdigest()
        now = self._clock()
        cached = self._cache.get(digest)
        if cached is not None and cached[0] > now:
            self._cache.move_to_end(digest)
            answer = cached[1]
        else:
            answer = await self._introspect(api_key)
            ttl = KEY_CACHE_SECONDS if isinstance(answer, KeyInfo) else KEY_NEGATIVE_CACHE_SECONDS
            self._cache[digest] = (now + ttl, answer)
            self._cache.move_to_end(digest)
            while len(self._cache) > MAX_CACHED_KEYS:
                self._cache.popitem(last=False)
        if isinstance(answer, ServiceError):
            raise answer
        return answer

    async def _introspect(self, api_key: str) -> KeyInfo | ServiceError:
        try:
            response = await self._client.get(self._url, headers={HEADER_API_KEY: api_key})
        except httpx.HTTPError as exc:
            log.warning("keys.registry_unreachable", error=str(exc))
            raise Unavailable("the key registry (memory service) is unreachable") from exc
        if response.status_code == _UNAUTHORIZED:
            return Unauthorized("unknown api key")
        if response.status_code == _FORBIDDEN:
            return Forbidden("the key registry refuses this api key")
        if not response.is_success:
            log.warning("keys.registry_failed", status=response.status_code)
            raise Unavailable(f"the key registry (memory service) answered {response.status_code}")
        try:
            return KeyInfo.model_validate_json(response.content)
        except ValidationError as exc:
            log.warning("keys.registry_malformed", error=str(exc))
            raise Unavailable("the key registry (memory service) answered malformed") from exc

    async def aclose(self) -> None:
        await self._client.aclose()
