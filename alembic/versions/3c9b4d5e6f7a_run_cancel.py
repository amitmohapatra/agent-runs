"""runs: cancelling a run, whatever its status

``cancel_reason`` and ``cancelled_by`` keep why a run was cancelled and the principal who
asked (``POST /v1/runs/{run_id}/cancel``). ``cancel_requested_at`` marks a running run a
worker holds that was asked to stop: its next heartbeat says so, and the ticker cancels it
when the lease in force runs out.

Revision ID: 3c9b4d5e6f7a
Revises: 2b8a3c4d5e6f
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "3c9b4d5e6f7a"
down_revision = "2b8a3c4d5e6f"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("cancel_requested_at", sa.DateTime(timezone=True)))
    op.add_column(_TABLE, sa.Column("cancel_reason", sa.String(length=1000)))
    op.add_column(_TABLE, sa.Column("cancelled_by", sa.String(length=256)))


def downgrade() -> None:
    op.drop_column(_TABLE, "cancelled_by")
    op.drop_column(_TABLE, "cancel_reason")
    op.drop_column(_TABLE, "cancel_requested_at")
