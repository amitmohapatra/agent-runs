"""Fixtures and builders. The waits between retries and heartbeats are recorded, not slept:
the suite checks how long the client would wait without spending it. The records are built
from the contracts' models, so what the fakes answer is what agent-runs answers."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import httpx
import pytest
from jsonschema import Draft202012Validator
from trellis.contracts.runs import Interrupt, RunRecord, RunStatus, Schedule
from trellis.runs import _transport

ROOT: Final = Path(__file__).resolve().parents[3]
#: agent-runs' committed OpenAPI document: what the SDK is checked against.
OPENAPI: Final = ROOT / "docs" / "openapi.json"
URL: Final = "http://runs.test"
NOW: Final = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def slept(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[float]]:
    waits: list[float] = []

    async def record(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr(_transport, "_sleep", record)
    monkeypatch.delenv("RUNS_URL", raising=False)
    monkeypatch.delenv("TRELLIS_API_KEY", raising=False)
    yield waits


# --------------------------------------------------------------------------- builders
def record(run_id: str = "run_1", **over: Any) -> dict[str, Any]:
    """A run as agent-runs answers it (a paused one waits on :func:`interrupt`)."""
    fields: dict[str, Any] = {
        "run_id": run_id,
        "tenant_id": "acme",
        "agent_id": "triage",
        "status": RunStatus.RUNNING,
        "created_at": NOW,
        "updated_at": NOW,
        **over,
    }
    if fields["status"] in (RunStatus.PAUSED, "PAUSED"):
        fields.setdefault("awaiting", interrupt(run_id))
    return RunRecord(**fields).model_dump(mode="json")


def interrupt(run_id: str = "run_1", **over: Any) -> Interrupt:
    fields: dict[str, Any] = {
        "interrupt_id": "int_1",
        "tenant_id": "acme",
        "run_id": run_id,
        "question": "Create PO for 12 000 EUR?",
        "assignee": "role:procurement",
        "created_at": NOW,
        **over,
    }
    return Interrupt(**fields)


def summary(run_id: str = "run_1", **over: Any) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "agent_id": "triage",
        "status": "PAUSED",
        "awaiting": interrupt(run_id).model_dump(mode="json"),
        "assignee": "role:procurement",
        "deadline": None,
        "updated_at": NOW.isoformat(),
        **over,
    }


def lease(run_id: str = "run_1", worker_id: str = "w-1") -> dict[str, Any]:
    return {"run_id": run_id, "worker_id": worker_id, "expires_at": NOW.isoformat()}


def schedule(schedule_id: str = "sch_1", **over: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "schedule_id": schedule_id,
        "tenant_id": "acme",
        "agent_id": "briefing",
        "name": "morning briefing",
        "cadence": "daily",
        "on_behalf_of": "user_ada",
        "created_at": NOW,
        "updated_at": NOW,
        **over,
    }
    return Schedule(**fields).model_dump(mode="json")


def webhook(webhook_id: str = "wh_1", **over: Any) -> dict[str, Any]:
    return {
        "webhook_id": webhook_id,
        "url": "https://ui.example/h",
        "events": ["run.finished", "run.paused"],
        "created_by": "user_ada",
        "created_at": NOW.isoformat(),
        **over,
    }


def delivery(delivery_id: str = "dlv_1", *, dead: bool = False) -> dict[str, Any]:
    """An outbox delivery as agent-runs lists it: still owed, or given up on."""
    return {
        "delivery_id": delivery_id,
        "webhook_id": "wh_1",
        "event_id": "whd_1",
        "type": "run.finished",
        "run_id": "run_1",
        "state": "dead" if dead else "pending",
        "attempts": 7 if dead else 0,
        "last_error": "answered 503" if dead else None,
        "next_attempt_at": None if dead else NOW.isoformat(),
        "dead_at": NOW.isoformat() if dead else None,
        "created_at": NOW.isoformat(),
    }


def artifact(artifact_id: str = "art_1") -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "type": "blob",
        "uri": f"/v1/artifacts/{artifact_id}",
        "mime_type": "application/json",
        "checksum": "sha256:" + "0" * 64,
        "size_bytes": 2,
        "created_at": NOW.isoformat(),
        "metadata": {"run_id": "run_1"},
    }


def problem(status: int, code: str, detail: str = "refused", **over: Any) -> dict[str, Any]:
    return {
        "type": f"urn:trellis:problem:{code.lower().replace('_', '-')}",
        "title": code.replace("_", " ").capitalize(),
        "status": status,
        "detail": detail,
        "instance": "/v1/runs",
        "code": code,
        "retryable": False,
        "request_id": "req_1",
        "details": {},
        **over,
    }


# --------------------------------------------------------------------------- the contract
@dataclass
class OpenAPI:
    """The committed document as a checker of an exchange: the operation must exist, every
    query parameter must be one it documents, a JSON body must match its schema, and an
    answer's status must be documented and its JSON body match that status's schema. OpenAPI
    3.1 schemas are JSON Schema 2020-12, the document's own references resolving in it."""

    document: dict[str, Any]
    #: (method, compiled path template, template) for every operation
    operations: list[tuple[str, re.Pattern[str], str]] = field(default_factory=list)

    @classmethod
    def load(cls) -> OpenAPI:
        api = cls(json.loads(OPENAPI.read_text()))
        for template, item in api.document["paths"].items():
            pattern = re.compile("^" + re.sub(r"\{[^/]+\}", "[^/]+", template) + "$")
            api.operations.extend((method, pattern, template) for method in item)
        # a literal path wins over a templated one (/v1/runs/claim over /v1/runs/{run_id})
        api.operations.sort(key=lambda op: op[2].count("{"))
        return api

    def operation(self, method: str, path: str) -> dict[str, Any]:
        for verb, pattern, template in self.operations:
            if verb == method.lower() and pattern.match(path):
                return self.document["paths"][template][verb]
        raise AssertionError(f"{method} {path}: no such operation")

    def request(self, request: httpx.Request) -> list[str]:
        """What is wrong with a request the SDK sent (empty: nothing)."""
        where = f"{request.method} {request.url.path}"
        op = self.operation(request.method, request.url.path)
        known = {p["name"] for p in op.get("parameters", []) if p["in"] == "query"}
        wrong = [f"{where}: no query parameter {n!r}" for n in request.url.params if n not in known]
        content = (op.get("requestBody") or {}).get("content", {})
        if not request.content:
            return wrong
        if "*/*" in content:  # raw bytes: an artifact
            return wrong
        media = request.headers.get("content-type", "").split(";")[0]
        if media not in content:
            return [*wrong, f"{where}: takes no {media} body"]
        return wrong + self.check(where, content[media]["schema"], json.loads(request.content))

    def response(self, request: httpx.Request, response: httpx.Response) -> list[str]:
        """What is wrong with an answer to ``request`` (empty: nothing)."""
        where = f"{request.method} {request.url.path} -> {response.status_code}"
        op = self.operation(request.method, request.url.path)
        spec = op["responses"].get(str(response.status_code))
        if spec is None:
            return [f"{where}: status not documented ({sorted(op['responses'])})"]
        media = response.headers.get("content-type", "").split(";")[0]
        content = spec.get("content") or {}
        if not response.content or "*/*" in content:
            return []
        if media not in content:
            return [f"{where}: answers {media}, documented {sorted(content)}"]
        if media == "text/plain":
            return []
        return self.check(where, content[media]["schema"], response.json())

    def check(self, where: str, schema: dict[str, Any], instance: Any) -> list[str]:
        rooted = {**schema, "components": self.document["components"]}
        return [
            f"{where}: {'/'.join(map(str, e.absolute_path)) or '(body)'}: {e.message[:300]}"
            for e in Draft202012Validator(rooted).iter_errors(instance)
        ]

    def schema(self, name: str) -> dict[str, Any]:
        return self.document["components"]["schemas"][name]


@pytest.fixture(scope="session")
def contract() -> OpenAPI:
    return OpenAPI.load()
