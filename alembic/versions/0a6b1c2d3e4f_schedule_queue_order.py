"""schedules: the priority and concurrency key of the runs they fire (contracts 0.6.1)

``priority`` and ``concurrency_key`` are the schedule's (``ScheduleSpec``); each fire copies
them into the ``RunStart`` of the run it queues, as it does ``timeout_seconds`` and
``agent_version``. Every existing schedule gets priority 0 and no key: its runs are claimed
as they were.

Revision ID: 0a6b1c2d3e4f
Revises: 9d5e0f1a2b3c
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0a6b1c2d3e4f"
down_revision = "9d5e0f1a2b3c"
branch_labels = None
depends_on = None

_TABLE = "agent_schedules"


def upgrade() -> None:
    op.add_column(
        _TABLE, sa.Column("priority", sa.Integer(), server_default=sa.text("0"), nullable=False)
    )
    op.add_column(_TABLE, sa.Column("concurrency_key", sa.String(length=200)))


def downgrade() -> None:
    op.drop_column(_TABLE, "concurrency_key")
    op.drop_column(_TABLE, "priority")
