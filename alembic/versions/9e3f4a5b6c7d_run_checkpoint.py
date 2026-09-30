"""runs: the executor's checkpoint

An opaque ``checkpoint`` (the executor's resume journal and the framework's own resume state)
written when a run pauses, returned with the run on every read and claim, and cleared when
the run finishes. The service bounds its size at the pause; nothing indexes or reads inside
it.

Revision ID: 9e3f4a5b6c7d
Revises: 8d2e3f4a5b6c
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "9e3f4a5b6c7d"
down_revision = "8d2e3f4a5b6c"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("checkpoint", postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column(_TABLE, "checkpoint")
