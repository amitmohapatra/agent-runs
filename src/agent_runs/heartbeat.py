"""The ticker's liveness: a file touched after every tick, and the probe that reads it.

Its own light module (constants and the standard library only), so the container probe,
``python -m agent_runs.heartbeat``, answers in milliseconds instead of importing the service.
A ticker has no port to probe, and the failure that matters is a loop that hangs while the
process stays up: a heartbeat that stops advancing is the one signal that tells them apart.
"""

from __future__ import annotations

import time
from pathlib import Path

from agent_runs.config.constants import HEARTBEAT_MAX_AGE_SECONDS, HEARTBEAT_PATH

DEFAULT = Path(HEARTBEAT_PATH)


def beat(path: Path = DEFAULT) -> None:
    """Record that the loop came round. Raises ``OSError`` when the file cannot be written."""
    path.write_text(str(time.time()))


def alive(path: Path = DEFAULT) -> bool:
    """Whether the loop came round within ``HEARTBEAT_MAX_AGE_SECONDS``."""
    try:
        last = float(path.read_text())
    except (OSError, ValueError):
        return False
    return time.time() - last <= HEARTBEAT_MAX_AGE_SECONDS


if __name__ == "__main__":
    raise SystemExit(0 if alive() else 1)
