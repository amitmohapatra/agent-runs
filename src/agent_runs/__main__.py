"""Console entry point: the API under uvicorn, ``RUNS__SERVICE__WORKERS`` processes (one per
CPU, 1 to 8, when unset). Each worker builds its own app from the settings (``factory``),
so the import string, not an app object, is what uvicorn gets."""

from __future__ import annotations

from typing import Final

import uvicorn

from agent_runs.config.settings import get_settings

APP_FACTORY: Final = "agent_runs.api.app:create_app"


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        APP_FACTORY,
        factory=True,
        host=settings.service.host,
        port=settings.service.port,
        workers=settings.service.worker_count,
        # SIGTERM: stop accepting, let requests in flight finish for this long, then close
        timeout_graceful_shutdown=settings.service.graceful_shutdown_seconds,
        log_config=None,  # the service configures its own structured logging
    )


if __name__ == "__main__":
    main()
