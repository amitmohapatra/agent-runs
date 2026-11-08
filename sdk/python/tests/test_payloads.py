"""The run payload format: every JSON value reads back as written, keys in ``jsonb`` order,
large payloads (de)compressed off the event loop, references by content under a tenant."""

from __future__ import annotations

from typing import Any

import pytest
from trellis.runs import payloads
from trellis.runs.payloads import (
    MEDIA_TYPE,
    OFFLOAD_BYTES,
    TENANT_SHARDS,
    adecode,
    aencode,
    decode,
    encode,
    jsonb_order,
    owned_by,
    ref_of,
    tenant_shard,
)

TENANT = "acme"
LARGE = {"rows": [{"id": i, "name": f"row {i}", "amount": i * 1.5} for i in range(20_000)]}
ODD = [None, "", {}, [], 0, -1.25, True, "ü€😀 \u2028 \x00", {"b": 1, "a": {"bb": 2, "a": 3}}]


@pytest.mark.parametrize("value", [*ODD, LARGE])
async def test_every_json_value_reads_back_as_written(value: Any) -> None:
    assert decode(encode(value)) == value
    assert await adecode(await aencode(value)) == value


def test_a_large_payload_is_past_the_offload_bound() -> None:
    assert len(encode(LARGE)) > OFFLOAD_BYTES // 10


def test_keys_go_shortest_first_then_by_their_bytes_as_tuples_become_lists() -> None:
    assert list(jsonb_order({"bb": 1, "c": 2, "a": 3, "é": 4})) == ["a", "c", "bb", "é"]
    assert jsonb_order(({"b": 1, "a": 2},)) == [{"a": 2, "b": 1}]


def test_what_json_cannot_hold_is_refused_and_garbage_is_not_a_payload() -> None:
    with pytest.raises(ValueError):
        encode({"x": float("nan")})
    with pytest.raises(ValueError):
        decode(b"not zstd")


def test_a_frame_declaring_more_than_the_bound_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    data = encode("more than eight bytes")
    monkeypatch.setattr(payloads, "MAX_DECODED_BYTES", 8)
    with pytest.raises(ValueError, match="at most 8 bytes"):
        decode(data)


def test_a_reference_names_its_content_under_its_tenant() -> None:
    data = encode({"a": 1})
    ref = ref_of(TENANT, data)
    assert ref == ref_of(TENANT, encode({"a": 1})), "the same content, the same key"
    shard = f"tenant-shard={tenant_shard(TENANT):03d}"
    assert ref.ref == f"{shard}/tenant={TENANT}/blobs/sha256={ref.sha256}.json.zst"
    assert ref.size == len(data) and 0 <= tenant_shard(TENANT) < TENANT_SHARDS
    assert ref.media_type == MEDIA_TYPE
    assert owned_by(ref, TENANT)
    assert not owned_by(ref, "globex"), "another tenant's reference is not its own"
    assert not owned_by(ref.model_copy(update={"ref": ref.ref.replace("blobs", "..")}), TENANT)
    assert not owned_by(ref.model_copy(update={"sha256": "ab"}), TENANT)
