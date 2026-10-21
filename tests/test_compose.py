"""The compose stack comes back on its own: a crash, or the host or Docker restarting, needs
no one to run ``docker compose up`` again."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_every_long_running_service_restarts_unless_stopped_and_the_migration_never() -> None:
    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    assert services["migrate"]["restart"] == "no"
    for name in ("postgres", "api", "ticker"):
        assert services[name]["restart"] == "unless-stopped", name
    for name in ("api", "ticker"):
        assert services[name]["depends_on"]["migrate"] == {
            "condition": "service_completed_successfully"
        }, name
