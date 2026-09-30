"""One authentication scheme for every route: ``X-Api-Key`` is the caller and names its
tenant; ``X-Trellis-Tenant`` is how a platform key names the tenant it acts for, and a tenant
key may send it only to agree. What a key may make a run execute as (``on_behalf_of``) is
bound to the key too."""

from __future__ import annotations

import pytest

from agent_runs.config.settings import Credential, ServiceSettings, Settings, WebhookSettings
from tests.conftest import started


async def test_an_unknown_or_missing_key_is_refused(client, app) -> None:
    assert (await client.get("/v1/runs", headers={"X-Api-Key": "nope"})).status_code == 401
    from httpx import ASGITransport, AsyncClient

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


def test_a_deployment_with_no_keys_is_refused() -> None:
    with pytest.raises(ValueError, match="api_keys is empty"):
        Settings(service=ServiceSettings(api_keys={})).check()


def test_the_development_key_cannot_survive_into_production() -> None:
    with pytest.raises(ValueError, match="development credential"):
        Settings(
            service=ServiceSettings(environment="prod"),
            webhooks=WebhookSettings(signing_secret="x"),
        ).check()


def test_unsigned_webhooks_are_refused_outside_dev() -> None:
    keys = {"k": Credential(tenant_id="acme", principal="svc")}
    with pytest.raises(ValueError, match="signing_secret"):
        Settings(service=ServiceSettings(environment="prod", api_keys=keys)).check()
    Settings(
        service=ServiceSettings(environment="prod", api_keys=keys),
        webhooks=WebhookSettings(signing_secret="s3cret"),
    ).check()


def test_credentials_parse_from_the_environment(monkeypatch) -> None:
    monkeypatch.setenv(
        "RUNS__SERVICE__API_KEYS",
        '{"k1": {"tenant_id": "acme", "principal": "svc", "may_act_as": ["*"]},'
        ' "k2": {"tenant_id": null, "principal": "platform"}}',
    )
    keys = Settings().service.api_keys
    assert keys["k1"].may_act_for("anyone")
    assert keys["k2"].platform and not keys["k2"].may_act_for("anyone")
