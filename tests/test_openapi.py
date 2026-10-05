"""The OpenAPI document is part of the contract: committed (``docs/openapi.json``), equal to
what the code generates, and complete enough to build a client from without reading this
repository: ids, summaries, descriptions, the security scheme, every error as a problem,
an example for every body."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from agent_runs.tools import export_openapi

COMMITTED = Path(__file__).resolve().parents[1] / "docs" / "openapi.json"
METHODS = ("get", "post", "put", "patch", "delete")
EXAMPLE_RUN = "run_01J8ZQ4Y6V9W3X2K7M5N0P1R2S"


@pytest.fixture(scope="module")
def spec() -> dict[str, Any]:
    return export_openapi.document()


def operations(spec: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    return [
        (path, method, op)
        for path, methods in spec["paths"].items()
        for method, op in methods.items()
        if method in METHODS
    ]


def test_the_committed_document_is_the_one_the_code_generates(spec) -> None:
    assert COMMITTED.is_file(), "docs/openapi.json is missing: run `make openapi`"
    committed = COMMITTED.read_text(encoding="utf-8")
    assert committed == export_openapi.render(spec), (
        "docs/openapi.json is stale: run `make openapi` and commit it"
    )


def test_the_export_writes_the_committed_form(tmp_path, capsys) -> None:
    out = tmp_path / "nested" / "openapi.json"
    assert export_openapi.main(["export_openapi", str(out)]) == 0
    assert json.loads(out.read_text()) == export_openapi.document()
    assert "paths" in capsys.readouterr().out


def test_the_document_says_what_the_service_is(spec) -> None:
    info = spec["info"]
    assert (info["title"], info["version"]) == ("agent-runs", "0.3.0")
    assert info["description"].strip() and info["summary"]
    assert info["contact"]["url"] and info["license"]["name"]
    assert spec["servers"]
    assert {t["name"] for t in spec["tags"]} == {
        "runs",
        "artifacts",
        "schedules",
        "webhooks",
        "ops",
    }
    assert all(t["description"] for t in spec["tags"])


def test_operation_ids_are_tag_dot_function_and_unique(spec) -> None:
    ids = [op["operationId"] for _, _, op in operations(spec)]
    assert len(ids) == len(set(ids))
    for _, _, op in operations(spec):
        tag, _, name = op["operationId"].partition(".")
        assert tag == op["tags"][0] and name.isidentifier(), op["operationId"]
    assert "runs.list" in ids and "runs.start" in ids


def test_every_operation_and_parameter_is_described(spec) -> None:
    for path, method, op in operations(spec):
        where = f"{method.upper()} {path}"
        assert op.get("summary") and op.get("description"), where
        for parameter in op.get("parameters", []):
            assert parameter.get("description"), f"{where} {parameter['name']}"
        for status, response in op["responses"].items():
            assert response["description"] not in ("Successful Response", "Validation Error"), (
                f"{where} {status}"
            )


def test_a_create_and_its_repeat_are_told_apart(spec) -> None:
    for path, _method, op in operations(spec):
        responses = op["responses"]
        if "201" in responses and "200" in responses:
            assert responses["201"]["description"] != responses["200"]["description"], path
        if "201" in responses:
            assert "Location" in responses["201"]["headers"], path


def test_every_v1_operation_needs_the_api_key_and_names_the_tenant_header(spec) -> None:
    scheme = spec["components"]["securitySchemes"]["ApiKeyAuth"]
    assert (scheme["type"], scheme["in"], scheme["name"]) == ("apiKey", "header", "X-API-Key")
    for path, _method, op in operations(spec):
        if path.startswith("/v1/"):
            assert op["security"] == [{"ApiKeyAuth": []}], path
            tenant = [p for p in op["parameters"] if p["name"] == "X-Trellis-Tenant"]
            assert tenant and tenant[0]["in"] == "header", path
        else:
            assert "security" not in op, path


def test_every_error_response_is_a_problem(spec) -> None:
    assert "HTTPValidationError" not in spec["components"]["schemas"]
    assert "Problem" in spec["components"]["schemas"]
    for path, _method, op in operations(spec):
        statuses = set(op["responses"])
        if path.startswith("/v1/"):
            assert {"401", "403", "422", "429", "503"} <= statuses, path
            if "{" in path:
                assert "404" in statuses, path
            if "requestBody" in op:
                assert "413" in statuses, path
        for status, response in op["responses"].items():
            if int(status) >= 400:
                content = response["content"]["application/problem+json"]
                assert content["schema"] == {"$ref": "#/components/schemas/Problem"}
                assert content["example"]["instance"] == path
                assert content["example"]["status"] == int(status)
            if status in ("429", "503"):
                assert "Retry-After" in response["headers"], (path, status)


def test_the_conflicts_say_what_they_mean_where_they_happen(spec) -> None:
    finish = spec["paths"]["/v1/runs/{run_id}/finish"]["post"]["responses"]["409"]
    assert "LEASE_LOST" in finish["description"]
    resume = spec["paths"]["/v1/runs/{run_id}/resume"]["post"]["responses"]["409"]
    assert "CONFLICT" in resume["description"] and "LEASE_LOST" not in resume["description"]


def test_resume_says_who_may_answer(spec) -> None:
    resume = spec["paths"]["/v1/runs/{run_id}/resume"]["post"]
    assert "may not answer this run" in resume["responses"]["403"]["description"]
    assert "Who may answer" in resume["description"]


def test_listings_document_their_cursor_and_link(spec) -> None:
    for path in ("/v1/runs", "/v1/schedules", "/v1/webhooks", "/v1/runs/{run_id}/resolutions"):
        op = spec["paths"][path]["get"]
        names = {p["name"] for p in op["parameters"]}
        assert {"cursor", "limit"} <= names, path
        assert "Link" in op["responses"]["200"]["headers"], path


async def test_every_request_body_has_an_example_the_service_accepts(spec, client) -> None:
    """Each JSON example is sent to its route as written: at worst it may meet a state
    conflict or a missing record (its ids are illustrations), never a 422."""
    for path, method, op in operations(spec):
        body = op.get("requestBody")
        if body is None:
            continue
        media = next(iter(body["content"].values()))
        assert media.get("examples"), path
        for name, example in media["examples"].items():
            assert example.get("summary"), (path, name)
            if "application/json" not in body["content"]:
                continue
            # the run the examples name, so a check that the body is about this run passes
            url = path.replace("{run_id}", EXAMPLE_RUN).replace("{schedule_id}", "sch_example")
            response = await client.request(method.upper(), url, json=example["value"])
            assert response.status_code != 422, (path, name, response.text)


async def test_the_docs_are_served(client) -> None:
    for path in ("/docs", "/redoc"):
        assert (await client.get(path)).status_code == 200
    served = await client.get("/openapi.json")
    assert served.json()["info"]["title"] == "agent-runs"
