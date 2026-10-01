"""run resolutions: every HITL answer, append-only

``run_resolutions`` keeps one row per answered interrupt: the interrupt as it was asked, the
resolution, the reviewer, the attempt that paused, and when. ``agent_runs.last_resolution``
keeps only the latest; the history is the audit trail.

Revision ID: d3c8e9f0a1b2
Revises: c2b6d7e8f9a0
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "d3c8e9f0a1b2"
down_revision = "c2b6d7e8f9a0"
branch_labels = None
depends_on = None

_TIMESTAMP = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "run_resolutions",
        sa.Column("resolution_id", sa.String(length=64), primary_key=True),
        sa.Column(
            "run_id",
            sa.String(length=64),
            sa.ForeignKey("agent_runs.run_id"),
            nullable=False,
        ),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("interrupt_id", sa.String(length=64), nullable=False),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("reviewer", sa.String(length=256), nullable=True),
        sa.Column("interrupt", JSONB(), nullable=False),
        sa.Column("resolution", JSONB(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("resolved_at", _TIMESTAMP, nullable=False),
        sa.Column("recorded_at", _TIMESTAMP, nullable=False),
    )
    op.create_index(
        "ix_run_resolutions_run", "run_resolutions", ["tenant_id", "run_id", "recorded_at"]
    )


def downgrade() -> None:
    op.drop_table("run_resolutions")
