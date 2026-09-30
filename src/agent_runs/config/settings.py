"""Deployment facts, and nothing else. One prefix, ``RUNS__``, with ``__`` between levels
(the Memory Service's shape). Every variable is documented in ``.env.example``; design
decisions are constants in ``constants.py``.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DEV = "dev"


class Credential(BaseModel):
    """What one API key is. The key *is* the caller: it names the tenant it speaks for (or
    none, for a platform key, which then names the tenant in ``X-Trellis-Tenant``), the
    principal recorded as ``created_by``, and the principals it may make runs execute as
    (``on_behalf_of``). ``"*"`` is the service grant a platform worker or an admin UI holds.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str | None
    principal: str
    may_act_as: tuple[str, ...] = ()

    @property
    def platform(self) -> bool:
        return self.tenant_id is None

    def may_act_for(self, principal: str) -> bool:
        return principal == self.principal or "*" in self.may_act_as or principal in self.may_act_as


def _dev_credentials() -> dict[str, Credential]:
    return {"dev-key": Credential(tenant_id="acme", principal="dev", may_act_as=("*",))}


class ServiceSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8090
    #: Anything but "dev" refuses the dev credential and plain-http webhook URLs.
    environment: str = DEV
    api_keys: dict[str, Credential] = Field(default_factory=_dev_credentials)

    @property
    def is_dev(self) -> bool:
        return self.environment == DEV


class DatabaseSettings(BaseModel):
    url: str = "postgresql+psycopg://memory:memory@localhost:5432/agent_runs"
    pool_size: int = 10


class ObservabilitySettings(BaseModel):
    log_level: str = "INFO"
    log_json: bool = True


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
            raise ValueError("service.api_keys is empty: no caller could reach this service")
        if self.service.is_dev:
            return
        if set(self.service.api_keys) & set(_dev_credentials()):
            raise ValueError(
                "the development credential is still configured outside dev: "
                "issue real keys in service.api_keys"
            )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.check()
    return settings


def reset_settings_cache() -> None:
    get_settings.cache_clear()
