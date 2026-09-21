"""Console entry point."""

from __future__ import annotations

import uvicorn

from agent_runs.api.app import create_app
from agent_runs.config.settings import get_settings


def main() -> None:
    uvicorn.run(create_app(get_settings()), host="0.0.0.0", port=8090)


if __name__ == "__main__":
    main()
