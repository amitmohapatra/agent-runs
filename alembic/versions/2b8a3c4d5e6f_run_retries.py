"""runs: retries after a backoff

``available_at`` holds a queued run back from the claim until a backoff has passed: after a
lapsed lease (a short one, so a run that kills its worker is not handed straight to the
next), and after a retryable error its worker ended it with, which goes back on the queue
(``error_retries`` counts those, up to ``MAX_ERROR_RETRIES``). Null is available at once,
as every run queued before this revision is.

Revision ID: 2b8a3c4d5e6f
Revises: 1a7f2b3c4d5e
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "2b8a3c4d5e6f"
down_revision = "1a7f2b3c4d5e"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("available_at", sa.DateTime(timezone=True)))
    op.add_column(
        _TABLE,
        sa.Column("error_retries", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )


def downgrade() -> None:
    op.drop_column(_TABLE, "error_retries")
    op.drop_column(_TABLE, "available_at")
