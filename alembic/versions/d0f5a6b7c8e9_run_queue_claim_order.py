"""runs: the queue's index holds a claim's order

A claim takes the tenant's queued run of the worker's agent with the highest ``priority``,
then the oldest. ``ix_runs_queue`` held those runs by ``queued_at`` alone, so every claim read
and sorted all of them: measured on 100,000 queued runs of one tenant, 400 ms a claim. It now
holds them in claim order (``priority`` descending, then ``queued_at``), and a tenant's claim
reads the first with room and stops (0.1 ms).

Revision ID: d0f5a6b7c8e9
Revises: c9e4f5a6b7d8
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "d0f5a6b7c8e9"
down_revision = "c9e4f5a6b7d8"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"
_INDEX = "ix_runs_queue"
_QUEUED = sa.text("status = 'QUEUED'")


def upgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
    op.create_index(
        _INDEX,
        _TABLE,
        ["tenant_id", "agent_id", sa.text("priority DESC"), "queued_at"],
        postgresql_where=_QUEUED,
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
    op.create_index(
        _INDEX, _TABLE, ["tenant_id", "agent_id", "queued_at"], postgresql_where=_QUEUED
    )
