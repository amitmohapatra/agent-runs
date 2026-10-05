"""runs: lease lapses counted apart from attempts, and the run deadline sweep

``agent_runs.lease_lapses`` counts the times a run's lease lapsed (its worker stopped
heartbeating); the ticker fails a run on its ``MAX_LEASE_LAPSES``-th. ``attempt`` keeps
counting executions, a person's answer included, so review rounds no longer use up the
budget meant for crashes. Existing runs start at no lapses.

``ix_runs_deadline`` serves the ticker's sweep of runs not yet ended past their own deadline
(``RunStart.deadline``), which it ends as ``TIMEOUT``.

Revision ID: f5e0a1b2c3d4
Revises: e4d9f0a1b2c3
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "f5e0a1b2c3d4"
down_revision = "e4d9f0a1b2c3"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"


def upgrade() -> None:
    op.add_column(
        _TABLE,
        sa.Column("lease_lapses", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )
    op.create_index(
        "ix_runs_deadline",
        _TABLE,
        ["deadline"],
        unique=False,
        postgresql_where=sa.text(
            "status IN ('QUEUED', 'RUNNING', 'PAUSED') AND deadline IS NOT NULL"
        ),
    )


def downgrade() -> None:
    op.drop_index("ix_runs_deadline", table_name=_TABLE)
    op.drop_column(_TABLE, "lease_lapses")
