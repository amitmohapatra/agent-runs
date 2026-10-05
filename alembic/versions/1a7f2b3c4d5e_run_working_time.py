"""runs: a working-time limit, the time worked, and the agent version (contracts 0.5.1)

``timeout_seconds`` and ``agent_version`` are the start's (``RunStart``). ``worked_seconds``
adds up the run's RUNNING stretches as each ends; ``running_since`` is when the one going on
began, set exactly while the run is RUNNING (a run running now is taken to have started its
stretch at its last change). ``ix_runs_working`` serves the ticker's sweep of runs that
worked past their limit, which it ends as ``TIMEOUT``.

Revision ID: 1a7f2b3c4d5e
Revises: f5e0a1b2c3d4
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "1a7f2b3c4d5e"
down_revision = "f5e0a1b2c3d4"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("timeout_seconds", sa.Float()))
    op.add_column(
        _TABLE,
        sa.Column("worked_seconds", sa.Float(), server_default=sa.text("0"), nullable=False),
    )
    op.add_column(_TABLE, sa.Column("running_since", sa.DateTime(timezone=True)))
    op.add_column(_TABLE, sa.Column("agent_version", sa.String(length=128)))
    op.execute("UPDATE agent_runs SET running_since = updated_at WHERE status = 'RUNNING'")
    op.create_index(
        "ix_runs_working",
        _TABLE,
        ["running_since"],
        unique=False,
        postgresql_where=sa.text("status = 'RUNNING'"),
    )


def downgrade() -> None:
    op.drop_index("ix_runs_working", table_name=_TABLE)
    op.drop_column(_TABLE, "agent_version")
    op.drop_column(_TABLE, "running_since")
    op.drop_column(_TABLE, "worked_seconds")
    op.drop_column(_TABLE, "timeout_seconds")
