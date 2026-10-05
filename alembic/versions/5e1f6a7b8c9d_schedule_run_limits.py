"""schedules: the working-time limit and agent version of the runs they fire (contracts 0.6)

``timeout_seconds`` and ``agent_version`` are the schedule's (``ScheduleSpec``); each fire
copies them into the ``RunStart`` of the run it queues. Null on every existing schedule:
its runs stay as they were (no limit of their own, no version).

Revision ID: 5e1f6a7b8c9d
Revises: 4d0c5e6f7a8b
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "5e1f6a7b8c9d"
down_revision = "4d0c5e6f7a8b"
branch_labels = None
depends_on = None

_TABLE = "agent_schedules"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("timeout_seconds", sa.Float()))
    op.add_column(_TABLE, sa.Column("agent_version", sa.String(length=128)))


def downgrade() -> None:
    op.drop_column(_TABLE, "agent_version")
    op.drop_column(_TABLE, "timeout_seconds")
