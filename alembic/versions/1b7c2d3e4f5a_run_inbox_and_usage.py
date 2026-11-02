"""runs: a run's inbox, and what it has used

``run_messages`` keeps each message sent to a run (``RunMessage``: an instruction or a pause)
at the next ``position`` of its inbox (1, 2, ...), counted on the run's row
(``agent_runs.inbox_position``) under its lock, so positions are never committed out of
order and the lease tells the worker the newest one without a read of the inbox.
``(run_id, message_id)`` is unique: a message sent again is kept once. The inbox goes with
the run (``ON DELETE CASCADE``).

``agent_runs.usage`` is what the run has used so far (``RunUsage``), saved with its
heartbeat, its pause and its ending. Existing runs have none.

Revision ID: 1b7c2d3e4f5a
Revises: 0a6b1c2d3e4f
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "1b7c2d3e4f5a"
down_revision = "0a6b1c2d3e4f"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("usage", postgresql.JSONB(), nullable=True))
    op.add_column(
        _TABLE,
        sa.Column("inbox_position", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )
    op.create_table(
        "run_messages",
        sa.Column(
            "run_id",
            sa.String(length=64),
            sa.ForeignKey("agent_runs.run_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("position", sa.BigInteger(), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("message_id", sa.String(length=200), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column("sender", sa.String(length=256), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint("run_id", "message_id", name="uq_run_messages_id"),
    )


def downgrade() -> None:
    op.drop_table("run_messages")
    op.drop_column(_TABLE, "inbox_position")
    op.drop_column(_TABLE, "usage")
