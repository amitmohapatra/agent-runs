"""One authentication scheme for every route: ``X-Api-Key`` is the caller and names its
tenant, as the Memory Service's key registry says (``GET /v1/keys/self``, cached);
``X-Trellis-Tenant`` is how a platform key names the tenant it acts for, and a tenant key may
send it only to agree. What a key may make a run execute as (``on_behalf_of``) is bound to
the key too."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from agent_runs.domain.errors import Forbidden, Unauthorized, Unavailable
from agent_runs.keys import KeyInfo
from tests.conftest import KEYS, SUSPENDED_KEY, FakeMemory, started


async def test_an_unknown_or_missing_key_is_refused(client, app) -> None:
    assert (await client.get("/v1/runs", headers={"X-Api-Key": "nope"})).status_code == 401
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://runs") as anonymous:
        assert (await anonymous.get("/v1/runs")).status_code == 401


async def test_the_key_decides_the_tenant_not_the_header(client, other_tenant) -> None:
    run = (await client.post("/v1/runs", json=started())).json()
    stolen = await client.get(f"/v1/runs/{run['run_id']}", headers={"X-Trellis-Tenant": "globex"})
    assert stolen.status_code == 403
    agreeing = await client.get(f"/v1/runs/{run['run_id']}", headers={"X-Trellis-Tenant": "acme"})
    assert agreeing.status_code == 200
    assert (
        await other_tenant.get("/v1/runs", headers={"X-Trellis-Tenant": "acme"})
    ).status_code == 403


async def test_a_platform_key_acts_for_the_tenant_it_names(client, platform) -> None:
    mine = (await client.post("/v1/runs", json=started())).json()
    assert (await platform.get(f"/v1/runs/{mine['run_id']}")).status_code == 200
    created = await platform.post("/v1/runs", json=started())
    assert created.status_code == 201


async def test_a_platform_key_that_names_no_tenant_is_refused(platform) -> None:
    assert (await platform.get("/v1/runs", headers={"X-Trellis-Tenant": ""})).status_code == 400


async def test_a_body_cannot_widen_the_tenant(client) -> None:
    assert (await client.post("/v1/runs", json=started(tenant_id="globex"))).status_code == 403


async def test_on_behalf_of_is_bound_to_the_key(narrow) -> None:
    """A run executes as ``on_behalf_of``; naming someone the key may not act as would be
    the escalation this whole binding exists to stop."""
    assert (
        await narrow.post("/v1/runs", json=started(on_behalf_of="user_root"))
    ).status_code == 403
    assert (await narrow.post("/v1/runs", json=started(on_behalf_of="user_bob"))).status_code == 201


async def test_a_key_the_registry_refuses_is_forbidden(client) -> None:
    assert (await client.get("/v1/runs", headers={"X-Api-Key": SUSPENDED_KEY})).status_code == 403


async def test_a_registry_that_is_down_is_a_503_not_a_401(client, memory) -> None:
    memory.status = 502
    response = await client.get("/v1/runs", headers={"X-Api-Key": "fresh-key"})
    assert response.status_code == 503
    assert (response.json()["code"], response.json()["retryable"]) == (
        "DEPENDENCY_UNAVAILABLE",
        True,
    )
    assert response.headers["retry-after"] == "5"


# ------------------------------------------------------------------ the registry client


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def test_the_answer_is_the_contract() -> None:
    info = await FakeMemory().registry().resolve("platform-key")
    assert info == KeyInfo(
        key_id="key_platform",
        tenant_id=None,
        principal="svc_worker",
        role="platform",
        may_act_as=("*",),
    )
    assert info.platform and info.may_act_for("anyone")


async def test_unknown_fields_in_the_answer_are_ignored() -> None:
    memory = FakeMemory({"k": {**KEYS["narrow-key"], "workspace_id": "ws1", "expires_at": None}})
    info = await memory.registry().resolve("k")
    assert (info.principal, info.may_act_for("user_bob"), info.may_act_for("x")) == (
        "user_bob",
        True,
        False,
    )


async def test_a_known_key_is_cached_for_a_minute() -> None:
    memory, clock = FakeMemory(), Clock()
    keys = memory.registry(clock=clock)
    await keys.resolve("dev-key")
    clock.now += 59
    await keys.resolve("dev-key")
    assert memory.asked == ["dev-key"]
    clock.now += 2
    await keys.resolve("dev-key")
    assert memory.asked == ["dev-key", "dev-key"]


async def test_a_revoked_key_stops_working_when_its_cache_entry_expires() -> None:
    memory, clock = FakeMemory(), Clock()
    keys = memory.registry(clock=clock)
    await keys.resolve("dev-key")
    del memory.keys["dev-key"]
    await keys.resolve("dev-key")  # still cached
    clock.now += 61
    with pytest.raises(Unauthorized):
        await keys.resolve("dev-key")


async def test_a_refused_key_is_cached_briefly() -> None:
    memory, clock = FakeMemory(), Clock()
    keys = memory.registry(clock=clock)
    for _ in range(3):
        with pytest.raises(Unauthorized):
            await keys.resolve("nope")
    with pytest.raises(Forbidden):
        await keys.resolve(SUSPENDED_KEY)
    assert memory.asked == ["nope", SUSPENDED_KEY]
    clock.now += 11
    memory.keys["nope"] = KEYS["dev-key"]  # issued since
    assert (await keys.resolve("nope")).tenant_id == "acme"


async def test_a_failed_introspection_is_not_cached() -> None:
    memory = FakeMemory()
    keys = memory.registry()
    memory.status = 500
    with pytest.raises(Unavailable):
        await keys.resolve("dev-key")
    memory.status = None
    assert (await keys.resolve("dev-key")).principal == "user_ada"


async def test_a_malformed_answer_or_an_unreachable_registry_is_unavailable() -> None:
    with pytest.raises(Unavailable):
        await FakeMemory({"k": {"tenant_id": "acme"}}).registry().resolve("k")
    from agent_runs.keys import KeyRegistry

    unreachable = KeyRegistry("http://127.0.0.1:9")
    with pytest.raises(Unavailable):
        await unreachable.resolve("dev-key")
    await unreachable.aclose()


async def test_the_cache_is_bounded(monkeypatch) -> None:
    monkeypatch.setattr("agent_runs.keys.MAX_CACHED_KEYS", 2)
    memory = FakeMemory()
    keys = memory.registry()
    for key in ("dev-key", "other-key", "narrow-key", "dev-key"):
        await keys.resolve(key)
    assert memory.asked == ["dev-key", "other-key", "narrow-key", "dev-key"]


async def test_the_key_goes_to_the_registry_as_x_api_key(app, memory) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://runs", headers={"X-Api-Key": "dev-key"}
    ) as c:
        assert (await c.get("/v1/runs")).status_code == 200
    assert memory.asked == ["dev-key"]
