"""What a request may cost: the body cap (declared or counted as it streams), the run
payload cap, artifact uploads streamed rather than held, compression, conditional artifact
reads, the per-tenant rate limit, and the metrics that show all of it."""

from __future__ import annotations

import gzip
from collections.abc import AsyncIterator
from typing import Any

import pytest
from prometheus_client.parser import text_string_to_metric_families
from trellis.contracts.ids import now

from agent_runs.api import ratelimit
from agent_runs.api.ratelimit import TenantRateLimiter
from agent_runs.blob.filesystem import FilesystemBlobStore
from agent_runs.config.settings import RateLimitSettings, ServiceSettings
from agent_runs.observability import metrics
from tests.conftest import SETTINGS, client_of, serving, started


def _settings(**over: Any) -> Any:
    service = SETTINGS.service.model_copy(update=over.pop("service", {}))
    return SETTINGS.model_copy(update={"service": service, **over})


@pytest.fixture
async def small(migrated, memory, blobs) -> AsyncIterator[Any]:
    """The app with a 4 KiB body cap and a 2 KiB run payload cap."""
    settings = _settings(service={"max_body_bytes": 4096, "max_payload_bytes": 2048})
    async with serving(settings, memory, blobs) as application, client_of(application) as c:
        yield c


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


# ------------------------------------------------------------------ the body cap


async def test_a_declared_body_past_the_cap_is_refused_before_it_is_read(small) -> None:
    response = await small.post(
        "/v1/runs", content=b"{" + b" " * 5000 + b"}", headers={"content-type": "application/json"}
    )
    assert (response.status_code, response.json()["code"]) == (413, "PAYLOAD_TOO_LARGE")
    assert response.headers["content-type"] == "application/problem+json"
    assert (await small.get("/v1/runs")).json() == []


async def test_a_chunked_body_past_the_cap_is_counted_and_refused(small) -> None:
    body = _chunks(b'{"tenant_id": "acme", "agent_id": "triage", "input": "', b"x" * 5000, b'"}')
    response = await small.post(
        "/v1/runs", content=body, headers={"content-type": "application/json"}
    )
    assert "content-length" not in response.request.headers, "sent chunked"
    assert (response.status_code, response.json()["code"]) == (413, "PAYLOAD_TOO_LARGE")
    assert (await small.get("/v1/runs")).json() == []


async def test_a_chunked_body_within_the_cap_goes_through(small) -> None:
    body = _chunks(b'{"tenant_id": "acme", ', b'"agent_id": "triage"}')
    response = await small.post(
        "/v1/runs", content=body, headers={"content-type": "application/json"}
    )
    assert response.status_code == 201


async def test_a_run_input_or_output_past_its_cap_is_refused(small) -> None:
    big = {"text": "y" * 2100}
    refused = await small.post("/v1/runs", json=started(input=big))
    assert (refused.status_code, refused.json()["code"]) == (413, "PAYLOAD_TOO_LARGE")
    assert "input is" in refused.json()["detail"]
    rid = (await small.post("/v1/runs", json=started(input={"ok": 1}))).json()["run_id"]
    ended = await small.post(f"/v1/runs/{rid}/finish", json={"status": "SUCCESS", "output": big})
    assert (ended.status_code, ended.json()["code"]) == (413, "PAYLOAD_TOO_LARGE")
    assert (await small.get(f"/v1/runs/{rid}")).json()["status"] == "RUNNING"


async def test_an_artifact_is_not_held_to_the_json_cap(small) -> None:
    rid = (await small.post("/v1/runs", json=started())).json()["run_id"]
    response = await small.post(f"/v1/runs/{rid}/artifacts", content=b"z" * 10_000)
    assert response.status_code == 201


# ------------------------------------------------------------------ streamed uploads


async def test_an_upload_streams_to_the_store_chunk_by_chunk(client, blobs, monkeypatch) -> None:
    seen: list[int] = []
    original = FilesystemBlobStore.put_stream

    async def watched(self, key, chunks, *, content_type):  # type: ignore[no-untyped-def]
        async def counted() -> AsyncIterator[bytes]:
            async for chunk in chunks:
                seen.append(len(chunk))
                yield chunk

        return await original(self, key, counted(), content_type=content_type)

    monkeypatch.setattr(FilesystemBlobStore, "put_stream", watched)
    rid = (await client.post("/v1/runs", json=started())).json()["run_id"]
    response = await client.post(
        f"/v1/runs/{rid}/artifacts", content=_chunks(b"a" * 1000, b"", b"b" * 2000, b"c")
    )
    assert response.status_code == 201, response.text
    assert seen == [1000, 2000, 1], "chunks arrive as sent, empty ones dropped"
    assert (await client.get(response.headers["location"])).content == (
        b"a" * 1000 + b"b" * 2000 + b"c"
    )


async def test_a_stream_that_fails_leaves_nothing_in_the_store(tmp_path) -> None:
    store = FilesystemBlobStore(tmp_path / "blobs")

    async def broken() -> AsyncIterator[bytes]:
        yield b"partial"
        raise RuntimeError("the client went away")

    with pytest.raises(RuntimeError):
        await store.put_stream("artifacts/art_x", broken(), content_type="text/plain")
    assert [p.name for p in (tmp_path / "blobs").rglob("*") if p.is_file()] == []


async def test_a_streamed_upload_past_the_cap_stores_nothing(client, blobs, monkeypatch) -> None:
    monkeypatch.setattr("agent_runs.api.routers.artifacts.MAX_ARTIFACT_BYTES", 64)
    rid = (await client.post("/v1/runs", json=started())).json()["run_id"]
    response = await client.post(f"/v1/runs/{rid}/artifacts", content=_chunks(b"x" * 40, b"x" * 40))
    assert (response.status_code, response.json()["code"]) == (413, "PAYLOAD_TOO_LARGE")
    root = blobs._root
    assert not root.exists() or [p for p in root.rglob("*") if p.is_file()] == []


# ------------------------------------------------------------------ compression


async def test_large_answers_are_gzipped_for_a_client_that_accepts_it(client) -> None:
    for _ in range(12):
        await client.post("/v1/runs", json=started(agent_id="a-rather-long-agent-name"))
    raw = await client.get("/v1/runs", headers={"accept-encoding": "gzip"})
    assert raw.headers["content-encoding"] == "gzip"
    assert len(raw.json()) == 12  # httpx decodes it
    small = await client.get("/v1/runs/run_nope", headers={"accept-encoding": "gzip"})
    assert "content-encoding" not in small.headers, "under 1 KiB is sent as it is"
    plain = await client.get("/v1/runs", headers={"accept-encoding": "identity"})
    assert "content-encoding" not in plain.headers


async def test_artifact_bytes_are_never_recompressed(client) -> None:
    rid = (await client.post("/v1/runs", json=started())).json()["run_id"]
    body = b'{"rows": [' + b'{"a": 1},' * 500 + b"{}]}"
    ref = (
        await client.post(
            f"/v1/runs/{rid}/artifacts",
            content=body,
            headers={"content-type": "application/json"},
        )
    ).json()
    served = await client.get(ref["uri"], headers={"accept-encoding": "gzip"})
    assert "content-encoding" not in served.headers
    assert served.headers["content-length"] == str(len(body))
    assert gzip.compress(body) != served.content and served.content == body


# ------------------------------------------------------------------ conditional reads


async def test_an_artifact_the_client_has_is_a_304_and_cacheable(client) -> None:
    rid = (await client.post("/v1/runs", json=started())).json()["run_id"]
    ref = (await client.post(f"/v1/runs/{rid}/artifacts", content=b"hello")).json()
    first = await client.get(ref["uri"])
    etag = first.headers["etag"]
    assert etag == f'"{ref["checksum"]}"'
    assert first.headers["cache-control"] == "private, max-age=31536000, immutable"
    for header in (etag, f"W/{etag}", f'"sha256:other", {etag}', "*"):
        again = await client.get(ref["uri"], headers={"if-none-match": header})
        assert (again.status_code, again.content) == (304, b""), header
        assert again.headers["etag"] == etag
        assert again.headers["cache-control"] == first.headers["cache-control"]
    stale = await client.get(ref["uri"], headers={"if-none-match": '"sha256:other"'})
    assert (stale.status_code, stale.content) == (200, b"hello")


async def test_a_304_is_only_for_the_tenants_own_artifact(client, other_tenant) -> None:
    rid = (await client.post("/v1/runs", json=started())).json()["run_id"]
    ref = (await client.post(f"/v1/runs/{rid}/artifacts", content=b"hello")).json()
    theirs = await other_tenant.get(ref["uri"], headers={"if-none-match": "*"})
    assert theirs.status_code == 404


# ------------------------------------------------------------------ rate limiting


@pytest.fixture
async def limited(migrated, memory, blobs) -> AsyncIterator[Any]:
    settings = _settings(rate_limit=RateLimitSettings(per_minute=60, burst=2))
    async with serving(settings, memory, blobs) as application:
        yield application


async def test_a_tenant_past_its_budget_is_told_when_to_come_back(limited) -> None:
    async with client_of(limited) as acme, client_of(limited, "other-key") as globex:
        first = await acme.get("/v1/runs")
        assert (first.headers["x-ratelimit-limit"], first.headers["x-ratelimit-remaining"]) == (
            "60",
            "1",
        )
        await acme.get("/v1/runs")
        refused = await acme.get("/v1/runs")
        assert (refused.status_code, refused.json()["code"]) == (429, "RATE_LIMIT")
        assert refused.json()["retryable"] is True
        assert refused.headers["retry-after"] == "1"
        assert refused.headers["x-ratelimit-remaining"] == "0"
        assert (await globex.get("/v1/runs")).status_code == 200, "a bucket per tenant"
        assert (await acme.get("/health/live")).status_code == 200, "ops routes are not counted"


async def test_no_budget_headers_when_the_limit_is_off(migrated, memory, blobs) -> None:
    settings = _settings(rate_limit=RateLimitSettings(per_minute=0))
    async with serving(settings, memory, blobs) as application, client_of(application) as c:
        for _ in range(5):
            response = await c.get("/v1/runs")
            assert response.status_code == 200
            assert "x-ratelimit-limit" not in response.headers


def test_a_bucket_refills_at_the_rate_and_holds_at_most_the_burst(monkeypatch) -> None:
    clock = [0.0]
    limiter = TenantRateLimiter(RateLimitSettings(per_minute=60, burst=3), clock=lambda: clock[0])
    assert [limiter.take("acme").allowed for _ in range(4)] == [True, True, True, False]
    assert limiter.take("acme").retry_after == 1
    clock[0] += 1.5
    assert limiter.take("acme").allowed and not limiter.take("acme").allowed
    clock[0] += 3600
    decision = limiter.take("acme")
    assert (decision.allowed, decision.remaining) == (True, 2), "never more than the burst"

    monkeypatch.setattr(ratelimit, "MAX_BUCKETS", 2)
    for tenant in ("a", "b", "c"):
        limiter.take(tenant)
    assert list(limiter._buckets) == ["b", "c"], "least recently used goes first"


# ------------------------------------------------------------------ metrics


def _samples(text: str, name: str) -> dict[tuple[tuple[str, str], ...], float]:
    found: dict[tuple[tuple[str, str], ...], float] = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name == name:
                found[tuple(sorted(sample.labels.items()))] = sample.value
    return found


async def test_metrics_count_requests_by_route_template_and_status(client) -> None:
    await client.get("/v1/runs/run_nope")
    await client.post("/v1/runs", json=started(queue=True))
    await client.post("/v1/runs/claim", json={"worker_id": "w1", "agent_ids": ["triage"]})
    await client.post("/v1/runs/claim", json={"worker_id": "w1", "agent_ids": ["triage"]})
    response = await client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    text = response.text
    requests = _samples(text, "runs_http_requests_total")
    key = (("method", "GET"), ("route", "/v1/runs/{run_id}"), ("status", "404"))
    assert requests[key] >= 1
    claims = _samples(text, "runs_claims_total")
    assert claims[(("outcome", "claimed"),)] >= 1 and claims[(("outcome", "empty"),)] >= 1
    pool = _samples(text, "runs_db_pool_connections")
    assert pool[(("state", "size"),)] == SETTINGS.database.pool_size
    assert "runs_http_request_seconds_bucket" in text


async def test_metrics_count_an_unmatched_route_and_a_crash(app) -> None:
    from httpx import ASGITransport, AsyncClient

    async def boom() -> None:
        raise RuntimeError("x")

    app.add_api_route("/boom-metrics", boom)
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://runs") as anon:
        await anon.get("/nowhere-at-all")
        await anon.get("/boom-metrics")
        text = (await anon.get("/metrics")).text
    requests = _samples(text, "runs_http_requests_total")
    assert requests[(("method", "GET"), ("route", "unmatched"), ("status", "404"))] >= 1
    assert requests[(("method", "GET"), ("route", "/boom-metrics"), ("status", "500"))] >= 1


async def test_the_ticker_counts_its_passes_and_what_each_step_did(client, ticker) -> None:
    before = _samples(metrics.render().decode(), "runs_ticker_ticks_total")
    await ticker.tick(now=now())
    after = _samples(metrics.render().decode(), "runs_ticker_ticks_total")
    ok = (("outcome", "ok"),)
    assert after[ok] == before.get(ok, 0) + 1
    assert (("step", "fired"),) in _samples(metrics.render().decode(), "runs_ticker_swept_total")


async def test_the_ticker_serves_its_metrics_with_the_pool_read_at_scrape(app, monkeypatch) -> None:
    served: dict[str, Any] = {}
    monkeypatch.setattr(
        metrics, "start_http_server", lambda port, registry: served.update(port=port, r=registry)
    )
    metrics.serve(9464, app.state.engine)
    assert served["port"] == 9464
    names = {family.name for family in served["r"].collect()}
    assert {"runs_db_pool_connections", "runs_ticker_ticks"} <= names


def test_the_payload_cap_defaults_are_the_documented_ones() -> None:
    service = ServiceSettings()
    assert (service.max_body_bytes, service.max_payload_bytes) == (4 * 1024 * 1024, 1024 * 1024)
    limits = RateLimitSettings()
    assert (limits.per_minute, limits.burst) == (3000, 500)
