"""Request examples for the OpenAPI document: one or more per request body, each a body this
service accepts as written (``tests/test_openapi.py`` sends every one)."""

from __future__ import annotations

from typing import Any, Final

_INTERRUPT: Final = {
    "interrupt_id": "int_01J8ZQ4Y6V9W3X2K7M5N0P1R2S",
    "tenant_id": "acme",
    "run_id": "run_01J8ZQ4Y6V9W3X2K7M5N0P1R2S",
    "reason": "APPROVAL",
    "question": "Create PO for 12 000 EUR?",
    "tool_call": {
        "tool": "create_purchase_order",
        "args": {"supplier": "ACME GmbH", "amount_eur": 12000},
        "task": "restock A-1",
    },
    "assignee": "role:procurement",
    "deadline": "2030-10-01T09:00:00Z",
    "escalate_to": "role:finance-leads",
}

START: Final[dict[str, Any]] = {
    "in_process": {
        "summary": "Record a run this process executes",
        "value": {
            "tenant_id": "acme",
            "agent_id": "triage",
            "run_id": "run_01J8ZQ4Y6V9W3X2K7M5N0P1R2S",
            "thread_id": "thr_42",
            "input": {"question": "Which invoices are overdue?"},
            "idempotency_key": "thr_42:turn-7:run:start",
        },
    },
    "queued": {
        "summary": "Queue a run for a worker",
        "value": {
            "tenant_id": "acme",
            "agent_id": "billing",
            "input": {"invoice_id": "inv_1001"},
            "queue": True,
        },
    },
}

CLAIM: Final[dict[str, Any]] = {
    "claim": {
        "summary": "Lease the oldest queued run of two agents",
        "value": {"worker_id": "w-1", "agent_ids": ["triage", "billing"], "lease_seconds": 60},
    }
}

HEARTBEAT: Final[dict[str, Any]] = {
    "extend": {
        "summary": "Extend the lease",
        "value": {"worker_id": "w-1", "lease_seconds": 60},
    },
    "progress": {
        "summary": "Extend the lease and save progress",
        "value": {
            "worker_id": "w-1",
            "lease_seconds": 60,
            "checkpoint": {"tools": {"call_1": {"output": "PO-17 created"}}},
        },
    },
}

PAUSE: Final[dict[str, Any]] = {
    "approval": {
        "summary": "Wait for procurement's approval, keeping the executor's checkpoint",
        "value": {
            "interrupt": _INTERRUPT,
            "checkpoint": {"asks": {}, "tools": {"call_1": {"output": "draft ready"}}},
        },
    }
}

RESUME: Final[dict[str, Any]] = {
    "approve": {
        "summary": "Approve",
        "value": {
            "interrupt_id": _INTERRUPT["interrupt_id"],
            "run_id": _INTERRUPT["run_id"],
            "decision": "APPROVE",
            "reviewer": "user:alice",
        },
    },
    "cancel": {
        "summary": "Cancel the run",
        "value": {
            "interrupt_id": _INTERRUPT["interrupt_id"],
            "run_id": _INTERRUPT["run_id"],
            "decision": "CANCEL",
            "reviewer": "user:alice",
        },
    },
}

FINISH: Final[dict[str, Any]] = {
    "success": {"summary": "Succeeded", "value": {"status": "SUCCESS", "output": {"po": "PO-17"}}},
    "error": {
        "summary": "Failed",
        "value": {
            "status": "ERROR",
            "error": {
                "code": "ToolFailed",
                "category": "TOOL",
                "message": "the ERP refused the order",
                "retryable": False,
            },
        },
    },
    "cancel": {"summary": "Cancel a queued or waiting run", "value": {"status": "CANCELLED"}},
}

SCHEDULE: Final[dict[str, Any]] = {
    "weekday_briefing": {
        "summary": "A weekday morning briefing, as Ada",
        "value": {
            "tenant_id": "acme",
            "agent_id": "briefing",
            "name": "morning briefing",
            "cadence": "0 8 * * 1-5",
            "timezone": "Europe/Berlin",
            "on_behalf_of": "user_ada",
            "input": {"topic": "inbox"},
        },
    }
}

SCHEDULE_UPDATE: Final[dict[str, Any]] = {
    "pause": {"summary": "Pause", "value": {"enabled": False}},
    "resume": {"summary": "Resume", "value": {"enabled": True}},
    "retime": {
        "summary": "Move it to 07:30",
        "value": {"cadence": "30 7 * * 1-5", "timezone": "Europe/Berlin"},
    },
}

FIRE: Final[dict[str, Any]] = {
    "now": {"summary": "Fire for the tick that is due, else now", "value": {}},
    "tick": {
        "summary": "Fire for one instant (idempotent)",
        "value": {"at": "2026-10-01T06:00:00Z"},
    },
}

WEBHOOK: Final[dict[str, Any]] = {
    "inbox": {
        "summary": "Hear about pauses and endings",
        "value": {
            "url": "https://ui.example/hooks/trellis",
            "events": ["run.paused", "run.finished"],
        },
    }
}
