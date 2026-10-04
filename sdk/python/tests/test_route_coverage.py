"""Every operation in the committed OpenAPI document has its SDK call: the operation id
``<tag>.<function>`` names the method (``runs.*`` and ``ops.*`` on the client, the rest on its
resource), and the package source references the operation's path."""

from __future__ import annotations

import re

from conftest import ROOT, OpenAPI
from trellis.runs import RunsClient

PACKAGE = ROOT / "sdk" / "python" / "src" / "trellis" / "runs"
#: the tags whose operations are the client's own methods
ON_THE_CLIENT = frozenset({"runs", "ops"})


def test_every_operation_has_a_method_named_by_its_id(contract: OpenAPI) -> None:
    runs = RunsClient("http://runs.test")
    missing = []
    for item in contract.document["paths"].values():
        for op in item.values():
            tag, _, function = op["operationId"].partition(".")
            owner = runs if tag in ON_THE_CLIENT else getattr(runs, tag, None)
            if not callable(getattr(owner, function, None)):
                missing.append(op["operationId"])
    assert not missing, missing


def test_every_path_is_referenced_by_the_package(contract: OpenAPI) -> None:
    source = "\n".join(p.read_text() for p in sorted(PACKAGE.glob("*.py")))
    missing = []
    for path, item in contract.document["paths"].items():
        # "/v1/runs/{run_id}/pause" -> "/v1/runs/{" and "/pause"; "/health/live" as is
        pieces = [piece for piece in re.split(r"\{[^}]+\}", path) if piece]
        needles = [pieces[0] + ("{" if "{" in path else ""), *pieces[1:]]
        if not all(needle in source for needle in needles):
            missing.extend(f"{method.upper()} {path}" for method in item)
    assert not missing, missing
