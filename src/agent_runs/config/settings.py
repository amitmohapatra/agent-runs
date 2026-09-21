"""Configuration. Everything has a default that works on a laptop; nothing has a default
that is wrong in production.

One prefix, ``RUNS__``, with ``__`` between levels — the same shape the Memory Service uses,
so an operator learns it once.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseModel):
    url: str = "postgresql+psycopg://memory:memory@localhost:5432/agent_runs"
    pool_size: int = 10
    echo: bool = False


class ObservabilitySettings(BaseModel):
    #: OTLP export is off until an endpoint is given. A service that tries to export to
    #: nowhere spends its startup retrying a connection that will never succeed.
    otel_enabled: bool = False
    otel_endpoint: str | None = None
    log_level: str = "INFO"
    log_json: bool = True


class Credential(BaseModel):
    """What one API key is allowed to be.

    A key here is not a password that unlocks the service; it *is* the caller. It names the
    tenant it speaks for, and that binding is the point: ``X-Tenant-Id`` arrives in
    attacker-controlled bytes, so it cannot be authority on its own. A flat list of keys
    with the tenant taken from a header means any valid key can act as any tenant, which is
    not authentication — it is a doorbell.
    """

    model_config = ConfigDict(extra="forbid")

    #: The one tenant this credential speaks for.
    tenant_id: str
    #: A human label, for logs and for revoking the right key.
    name: str = ""


def _dev_credentials() -> dict[str, Credential]:
    return {"dev-key": Credential(tenant_id="acme", name="local development")}


class ServiceSettings(BaseModel):
    name: str = "agent-runs"
    environment: str = "dev"
    #: Every request is authenticated, in every environment.
    #:
    #: This used to read ``if auth_mode == "trusted_dev" and key not in api_keys``, which
    #: meant the startup check's own advice — use another mode outside dev — turned
    #: authentication *off*, and a typo in the mode name did the same thing silently. There
    #: is no mode switch now because there was never a second implementation to switch to.
    api_keys: dict[str, Credential] = Field(default_factory=_dev_credentials)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RUNS__", env_nested_delimiter="__", env_file=".env", extra="ignore"
    )

    service: ServiceSettings = ServiceSettings()
    database: DatabaseSettings = DatabaseSettings()
    observability: ObservabilitySettings = ObservabilitySettings()

    def check(self) -> None:
        """Refuse configurations that only look like they work."""
        if not self.service.api_keys:
            raise ValueError(
                "service.api_keys is empty: this service authenticates every request, so "
                "no caller could reach it"
            )
        if self.service.environment != "dev" and "dev-key" in self.service.api_keys:
            raise ValueError(
                "the development credential is still configured outside dev: "
                "issue real keys in service.api_keys"
            )
        if self.observability.otel_enabled and not self.observability.otel_endpoint:
            raise ValueError("observability.otel_enabled requires observability.otel_endpoint")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.check()
    return settings


def reset_settings_cache() -> None:
    get_settings.cache_clear()
