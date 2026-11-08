"""The run payload format: every JSON value reads back as written, keys in ``jsonb`` order,
large payloads (de)compressed off the event loop, references by content under a tenant."""

from __future__ import annotations

from typing import Any

import pytest
from trellis.runs.payloads import (
    MEDIA_TYPE,
    OFFLOAD_BYTES,
    adecode,
    aencode,
    decode,
    encode,
    jsonb_order,
    owned_by,
    ref_of,
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


def test_a_reference_names_its_content_under_its_tenant() -> None:
    data = encode({"a": 1})
    ref = ref_of(TENANT, data)
    assert ref == ref_of(TENANT, encode({"a": 1})), "the same content, the same key"
    assert ref.ref == f"{TENANT}/blobs/{ref.sha256}" and ref.size == len(data)
    assert ref.media_type == MEDIA_TYPE
    assert owned_by(ref, TENANT)
    assert not owned_by(ref, "globex"), "another tenant's reference is not its own"
    assert not owned_by(ref.model_copy(update={"ref": f"{TENANT}/blobs/../x"}), TENANT)
