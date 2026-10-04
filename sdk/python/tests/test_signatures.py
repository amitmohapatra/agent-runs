"""The webhook signature: a fixed vector, the tolerance either side of it, tampering, and
every malformed header a receiver may be sent (all ``False``, none raising)."""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from pydantic import ValidationError
from trellis.contracts.runs import RunStatus
from trellis.runs import WebhookEvent, parse_delivery, sign, verify_signature
from trellis.runs import webhooks as hooks

SECRET = "whsec_test"
STAMP = 1_700_000_000
BODY = b'{"event_id":"whd_1","type":"run.finished"}'
#: HMAC-SHA256("whsec_test", "1700000000." + BODY), computed independently of ``sign``
DIGEST = hmac.new(b"whsec_test", b"1700000000." + BODY, hashlib.sha256).hexdigest()


def test_the_vector() -> None:
    assert sign(SECRET, STAMP, BODY) == f"t={STAMP},v1={DIGEST}"
    assert sign(SECRET, STAMP, b"") == (
        f"t={STAMP},v1={hmac.new(b'whsec_test', b'1700000000.', hashlib.sha256).hexdigest()}"
    )


def test_the_headers_are_the_ones_agent_runs_sends() -> None:
    assert (hooks.SIGNATURE_HEADER, hooks.EVENT_HEADER, hooks.DELIVERY_HEADER) == (
        "X-Trellis-Signature",
        "X-Trellis-Event",
        "X-Trellis-Delivery",
    )
    assert hooks.TOLERANCE_SECONDS == 300


def test_a_signature_verifies_within_the_tolerance_either_way() -> None:
    header = sign(SECRET, STAMP, BODY)
    assert verify_signature(SECRET, header, BODY, now=STAMP)
    assert verify_signature(SECRET, header, BODY, now=STAMP + 300)
    assert verify_signature(SECRET, header, BODY, now=STAMP - 300)
    assert not verify_signature(SECRET, header, BODY, now=STAMP + 301)
    assert not verify_signature(SECRET, header, BODY, now=STAMP - 301)
    assert verify_signature(SECRET, header, BODY, now=STAMP + 900, tolerance=900)


def test_the_clock_is_now_when_not_given(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("trellis.runs.webhooks.time.time", lambda: STAMP + 10.7)
    assert verify_signature(SECRET, sign(SECRET, STAMP, BODY), BODY)
    monkeypatch.setattr("trellis.runs.webhooks.time.time", lambda: STAMP + 3600.0)
    assert not verify_signature(SECRET, sign(SECRET, STAMP, BODY), BODY)


def test_spaces_around_the_parts_are_tolerated() -> None:
    assert verify_signature(SECRET, f" t={STAMP} , v1={DIGEST} ", BODY, now=STAMP)


@pytest.mark.parametrize(
    ("secret", "header", "body"),
    [
        ("whsec_other", f"t={STAMP},v1={DIGEST}", BODY),  # another secret
        (SECRET, f"t={STAMP},v1={DIGEST}", BODY + b" "),  # the body changed
        (SECRET, f"t={STAMP + 1},v1={DIGEST}", BODY),  # the timestamp changed
        (SECRET, f"t={STAMP},v1={DIGEST[:-1]}0", BODY),  # the digest changed
        (SECRET, f"t={STAMP},v1={DIGEST.upper()}", BODY),  # not the hex sent
    ],
)
def test_tampering_is_refused(secret: str, header: str, body: bytes) -> None:
    assert not verify_signature(secret, header, body, now=STAMP)


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "garbage",
        f"v1={DIGEST}",  # no timestamp
        f"t={STAMP}",  # no digest
        f"t={STAMP},v1=",  # an empty digest
        f"t=,v1={DIGEST}",
        f"t=-{STAMP},v1={DIGEST}",
        f"t=1.5,v1={DIGEST}",
        f"t=١٢٣,v1={DIGEST}",  # digits, but not ASCII ones
        f"t={STAMP},v0={DIGEST}",  # another scheme version
        f"t={STAMP},v1=ünïcode",  # compare_digest refuses non-ASCII str: still False
    ],
)
def test_a_malformed_header_is_false_and_never_raises(header: str | None) -> None:
    assert not verify_signature(SECRET, header, BODY, now=STAMP)


def test_a_delivery_parses_from_bytes_or_text() -> None:
    envelope = {
        "event_id": "whd_1",
        "type": "run.finished",
        "tenant_id": "acme",
        "workspace_id": None,
        "occurred_at": "2026-10-01T08:00:00+00:00",
        "data": {
            "run": {
                "run_id": "run_1",
                "agent_id": "triage",
                "status": "SUCCESS",
                "awaiting": None,
                "assignee": None,
                "deadline": None,
                "updated_at": "2026-10-01T08:00:00+00:00",
            }
        },
    }
    raw = json.dumps(envelope).encode()
    delivery = parse_delivery(raw)
    assert delivery.type is WebhookEvent.FINISHED and delivery.data.run.status is RunStatus.SUCCESS
    assert parse_delivery(raw.decode()) == delivery
    with pytest.raises(ValidationError):
        parse_delivery(b'{"event_id": "whd_1"}')
