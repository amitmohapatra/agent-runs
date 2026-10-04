"""Write the OpenAPI document to a file: ``docs/openapi.json`` by default, or the path given.

``python -m agent_runs.tools.export_openapi`` (``make openapi``) after any change to a route
or a model; CI exports it again and fails when the committed file differs, and so does
``tests/test_openapi.py``. Building the document needs no database: the app is created, not
started.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from agent_runs.api.app import create_app
from agent_runs.config.settings import Settings

DEFAULT_PATH = Path("docs/openapi.json")


def document() -> dict[str, Any]:
    return create_app(Settings()).openapi()


def render(schema: dict[str, Any]) -> str:
    """The committed form: sorted keys, two-space indent, a final newline, non-ASCII kept."""
    return json.dumps(schema, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main(argv: list[str]) -> int:
    out = Path(argv[1]) if len(argv) > 1 else DEFAULT_PATH
    schema = document()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(schema), encoding="utf-8")
    sys.stdout.write(f"wrote {out} ({len(schema.get('paths', {}))} paths)\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
