"""runs: retention of ended runs (``RUNS__RUNS__RETENTION_DAYS``)

``ix_runs_ended`` serves the ticker's sweep of runs that ended before the retention window,
which it deletes with their resolutions (their events go by ``ON DELETE CASCADE``). Nothing
is deleted unless the deployment sets a retention.

Revision ID: 9d5e0f1a2b3c
Revises: 8c4d9e0f1a2b
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "9d5e0f1a2b3c"
down_revision = "8c4d9e0f1a2b"
branch_labels = None
depends_on = None

_ENDED = "status IN ('SUCCESS', 'PARTIAL', 'ERROR', 'TIMEOUT', 'CANCELLED', 'REJECTED')"


def upgrade() -> None:
    op.create_index(
        "ix_runs_ended",
        "agent_runs",
        ["updated_at"],
        unique=False,
        postgresql_where=sa.text(_ENDED),
    )


def downgrade() -> None:
    op.drop_index("ix_runs_ended", table_name="agent_runs")
