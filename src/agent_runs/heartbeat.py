"""The ticker's liveness: a file touched after every tick, and the probe that reads it.

A ticker has no port to probe, and the failure that matters is a loop that hangs while the
process stays up: a heartbeat that stops advancing is the one signal that tells them apart.

Each ticker needs a file of its own, or a live ticker would vouch for a hung one sharing its
file. ``RUNS__TICKER__HEARTBEAT_FILE`` names it where a probe must find it (the container
sets it; one ticker per container). Unset, a ticker beats into a file named for its process
id in the temp directory, so tickers on one host never share one; the probe
(``python -m agent_runs.heartbeat``) needs the setting.
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

from agent_runs.config.constants import HEARTBEAT_MAX_AGE_SECONDS


def path_for(configured: Path | None) -> Path:
    """The configured file, else one of this process's own."""
    if configured is not None:
        return configured
    return Path(tempfile.gettempdir()) / f"agent-runs-ticker-{os.getpid()}.heartbeat"


def beat(path: Path) -> None:
    """Record that the loop came round. Raises ``OSError`` when the file cannot be written."""
    path.write_text(str(time.time()))


def alive(path: Path) -> bool:
    """Whether the loop came round within ``HEARTBEAT_MAX_AGE_SECONDS``."""
    try:
        last = float(path.read_text())
    except (OSError, ValueError):
        return False
    return time.time() - last <= HEARTBEAT_MAX_AGE_SECONDS


def main() -> int:
    from agent_runs.config.settings import get_settings  # noqa: PLC0415 - only the probe needs it

    configured = get_settings().ticker.heartbeat_file
    if configured is None:
        print("RUNS__TICKER__HEARTBEAT_FILE is not set: no heartbeat to probe")
        return 1
    return 0 if alive(configured) else 1


if __name__ == "__main__":
    raise SystemExit(main())
