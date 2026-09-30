"""Deployment facts, and nothing else. One prefix, ``RUNS__``, with ``__`` between levels
(the Memory Service's shape). Every variable is documented in ``.env.example``; design
decisions are constants in ``constants.py``.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

DEV = "dev"


class ServiceSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8090
    #: "dev" also accepts plain-http webhook URLs (a receiver on a laptop).
    environment: str = DEV

    @property
    def is_dev(self) -> bool:
        return self.environment == DEV


class MemorySettings(BaseModel):
    #: The Memory Service, the one key registry: every X-Api-Key is introspected there.
    url: str = "http://localhost:8080"


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
    memory: MemorySettings = MemorySettings()
    database: DatabaseSettings = DatabaseSettings()
    observability: ObservabilitySettings = ObservabilitySettings()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()
