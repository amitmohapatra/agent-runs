"""run events: a run's event log, kept with the run, so any replica serves any run's events

A run's worker (or the process it runs in) appends its ``RunEvent``s; each takes the next
``position`` of the run's log (1, 2, ...), assigned under the run's row lock, so a reader
that has seen position n never misses an earlier one. ``(run_id, attempt, sequence)`` is
unique: a repeated append is stored once. The log goes with the run (``ON DELETE
CASCADE``).

Revision ID: 8c4d9e0f1a2b
Revises: 7b3c8d9e0f1a
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "8c4d9e0f1a2b"
down_revision = "7b3c8d9e0f1a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "run_events",
        sa.Column(
            "run_id",
            sa.String(length=64),
            sa.ForeignKey("agent_runs.run_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("position", sa.BigInteger(), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("event", postgresql.JSONB(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint("run_id", "attempt", "sequence", name="uq_run_events_sequence"),
    )


def downgrade() -> None:
    op.drop_table("run_events")
