"""schedules: an end (contracts ``ScheduleSpec.end_at``)

``end_at`` is the instant from which a schedule fires no more. Null on every existing
schedule: none of them ends, and each fires exactly as before.

Revision ID: f6a7b8c9d0e1
Revises: d0f5a6b7c8e9
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "f6a7b8c9d0e1"
down_revision = "d0f5a6b7c8e9"
branch_labels = None
depends_on = None

_TABLE = "agent_schedules"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("end_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    op.drop_column(_TABLE, "end_at")
