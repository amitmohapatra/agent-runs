"""runs: who holds a run kept in its caller's process

``holder`` and ``held_until``: a run started ``RUNNING`` (never queued) that its caller holds
with heartbeats, as a worker keeps its lease; once the hold lapses the ticker ends the run
``ERROR`` (``holder_lost``), since only the process that held it could run it. ``ix_runs_held``
is that sweep's index. Both are null for every existing run: nothing holds it, as before.

Revision ID: b8d3e4f5a6c7
Revises: a7c1d2e3f4b5
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "b8d3e4f5a6c7"
down_revision = "a7c1d2e3f4b5"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("holder", sa.String(length=200)))
    op.add_column(_TABLE, sa.Column("held_until", sa.DateTime(timezone=True)))
    op.create_index(
        "ix_runs_held",
        _TABLE,
        ["held_until"],
        postgresql_where=sa.text("status = 'RUNNING' AND held_until IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_runs_held", table_name=_TABLE)
    op.drop_column(_TABLE, "held_until")
    op.drop_column(_TABLE, "holder")
