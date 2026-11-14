"""runs: the line of a concurrency key holds its queued runs too

A run takes its place among the runs sharing its ``concurrency_key`` as it starts, queued or
running (``RunStore._placed``): just after the latest of them. ``ix_runs_concurrency`` served
the key's ``RUNNING`` runs (a claim's count); it now holds the ``QUEUED`` ones too, so placing a
run reads the key's line from the index. A claim's count, of the ``RUNNING`` ones, is served
by it as before.

Revision ID: c9e4f5a6b7d8
Revises: b8d3e4f5a6c7
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "c9e4f5a6b7d8"
down_revision = "b8d3e4f5a6c7"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"
_INDEX = "ix_runs_concurrency"
_COLUMNS = ["tenant_id", "concurrency_key"]


def upgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
    op.create_index(
        _INDEX,
        _TABLE,
        _COLUMNS,
        postgresql_where=sa.text("status IN ('RUNNING', 'QUEUED') AND concurrency_key IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
    op.create_index(
        _INDEX,
        _TABLE,
        _COLUMNS,
        postgresql_where=sa.text("status = 'RUNNING' AND concurrency_key IS NOT NULL"),
    )
