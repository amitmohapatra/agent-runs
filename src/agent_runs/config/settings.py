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
    #: Where uvicorn binds. In a container the port is chosen by whoever runs it, so it is
    #: configuration (RUNS__SERVICE__PORT), not a literal in the entry point.
    host: str = "0.0.0.0"
    port: int = 8090
    environment: str = "dev"
    #: Every request is authenticated, in every environment.
    #:
    #: This used to read ``if auth_mode == "trusted_dev" and key not in api_keys``, which
    #: meant the startup check's own advice — use another mode outside dev — turned
    #: authentication *off*, and a typo in the mode name did the same thing silently. There
    #: is no mode switch now because there was never a second implementation to switch to.
    api_keys: dict[str, Credential] = Field(default_factory=_dev_credentials)


class WebhookSettings(BaseModel):
    """Telling whoever started a run that it paused or finished.

    A UI should not have to poll to find out that the 3am job needs an approval. What this
    is *not* is a second source of truth: the run row stays authoritative, delivery is
    at-least-once, and a client that missed one reconciles by reading the run.
    """

    enabled: bool = True
    timeout_seconds: float = 10.0
    #: Attempts per notification, including the first. Each failure waits
    #: ``backoff_seconds * 2 ** attempt``.
    max_attempts: int = 4
    backoff_seconds: float = 1.0
    #: Signs the body as ``X-Run-Signature: sha256=<hex>`` so a receiver can tell a real
    #: notification from anything else that can reach its URL. Empty means unsigned, which
    #: is only defensible on a laptop — and the startup check refuses it anywhere else.
    signing_secret: str = ""
    #: Schemes a webhook may use. Plain http is allowed in dev and nowhere else: a
    #: notification carries a run's output.
    allowed_schemes: tuple[str, ...] = ("https", "http")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RUNS__", env_nested_delimiter="__", env_file=".env", extra="ignore"
    )

    service: ServiceSettings = ServiceSettings()
    database: DatabaseSettings = DatabaseSettings()
    observability: ObservabilitySettings = ObservabilitySettings()
    webhooks: WebhookSettings = WebhookSettings()

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
        if (
            self.service.environment != "dev"
            and self.webhooks.enabled
            and not self.webhooks.signing_secret
        ):
            raise ValueError(
                "webhooks.signing_secret is empty outside dev: an unsigned notification "
                "carries a run's output to whoever holds the URL, and a receiver has no "
                "way to tell it came from this service"
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
