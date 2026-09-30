"""runs: the worker queue, leases, the assignee inbox and interrupt deadlines

A run is now the contracts' ``RunRecord``: ``workspace_id`` from ``RunStart``,
``last_resolution`` (the ``InterruptResolution`` the last resume carried, previously squeezed
into metadata as ``answer``), ``assignee`` and ``awaiting_deadline`` denormalised from the
``Interrupt`` a PAUSED run waits on, and ``queued_at``, ``lease_owner``, ``lease_expires_at``
for the queue a worker claims from.

The inbox index replaces ``ix_runs_tenant_status``, which is its prefix.

Revision ID: 7c1d2e3f4a5b
Revises: 1f6242bb21de
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "7c1d2e3f4a5b"
down_revision = "1f6242bb21de"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"
_TIMESTAMP = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("workspace_id", sa.String(length=128), nullable=True))
    op.add_column(_TABLE, sa.Column("last_resolution", postgresql.JSONB(), nullable=True))
    op.add_column(_TABLE, sa.Column("assignee", sa.String(length=256), nullable=True))
    op.add_column(_TABLE, sa.Column("awaiting_deadline", _TIMESTAMP, nullable=True))
    op.add_column(_TABLE, sa.Column("queued_at", _TIMESTAMP, nullable=True))
    op.add_column(_TABLE, sa.Column("lease_owner", sa.String(length=200), nullable=True))
    op.add_column(_TABLE, sa.Column("lease_expires_at", _TIMESTAMP, nullable=True))

    op.drop_index("ix_runs_tenant_status", table_name=_TABLE)
    op.create_index(
        "ix_runs_inbox", _TABLE, ["tenant_id", "status", "assignee", "created_at"], unique=False
    )
    op.create_index(
        "ix_runs_queue",
        _TABLE,
        ["tenant_id", "agent_id", "queued_at"],
        unique=False,
        postgresql_where=sa.text("status = 'QUEUED'"),
    )
    op.create_index(
        "ix_runs_lease",
        _TABLE,
        ["status", "lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("lease_expires_at IS NOT NULL"),
    )
    op.create_index(
        "ix_runs_escalation",
        _TABLE,
        ["awaiting_deadline"],
        unique=False,
        postgresql_where=sa.text("status = 'PAUSED' AND awaiting_deadline IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_runs_escalation", table_name=_TABLE)
    op.drop_index("ix_runs_lease", table_name=_TABLE)
    op.drop_index("ix_runs_queue", table_name=_TABLE)
    op.drop_index("ix_runs_inbox", table_name=_TABLE)
    op.create_index("ix_runs_tenant_status", _TABLE, ["tenant_id", "status"], unique=False)
    for column in (
        "lease_expires_at",
        "lease_owner",
        "queued_at",
        "awaiting_deadline",
        "assignee",
        "last_resolution",
        "workspace_id",
    ):
        op.drop_column(_TABLE, column)
