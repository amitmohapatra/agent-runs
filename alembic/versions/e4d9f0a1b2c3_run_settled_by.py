"""run settled_by: who put a run in its paused or ended state

``agent_runs.settled_by`` is the ``worker_id`` whose pause or finish made the run's current
state (null for a caller without one: an in-process run, a person cancelling, the ticker).
A repeated pause or finish from the same caller, to the same state, answers the stored run
instead of a conflict: a worker that never saw the answer to its finish retries it.

Revision ID: e4d9f0a1b2c3
Revises: d3c8e9f0a1b2
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "e4d9f0a1b2c3"
down_revision = "d3c8e9f0a1b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("agent_runs", sa.Column("settled_by", sa.String(length=200), nullable=True))


def downgrade() -> None:
    op.drop_column("agent_runs", "settled_by")
