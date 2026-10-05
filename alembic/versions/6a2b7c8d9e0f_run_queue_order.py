"""runs: a queued run's priority and concurrency key (contracts 0.6)

``priority`` (``RunStart.priority``, 0 for every existing run) orders a tenant's claims,
higher first; ``concurrency_key`` (``RunStart.concurrency_key``) caps how many of a tenant's
runs sharing it may be RUNNING at once. ``ix_runs_concurrency`` serves that count, and
``ix_runs_leased`` the count of each tenant's runs held by workers, which a claim across
tenants shares the fleet by (and the per-tenant cap reads).

Revision ID: 6a2b7c8d9e0f
Revises: 5e1f6a7b8c9d
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "6a2b7c8d9e0f"
down_revision = "5e1f6a7b8c9d"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"


def upgrade() -> None:
    op.add_column(
        _TABLE, sa.Column("priority", sa.Integer(), server_default=sa.text("0"), nullable=False)
    )
    op.add_column(_TABLE, sa.Column("concurrency_key", sa.String(length=200)))
    op.create_index(
        "ix_runs_concurrency",
        _TABLE,
        ["tenant_id", "concurrency_key"],
        unique=False,
        postgresql_where=sa.text("status = 'RUNNING' AND concurrency_key IS NOT NULL"),
    )
    op.create_index(
        "ix_runs_leased",
        _TABLE,
        ["tenant_id", "agent_id"],
        unique=False,
        postgresql_where=sa.text("status = 'RUNNING' AND lease_owner IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_runs_leased", table_name=_TABLE)
    op.drop_index("ix_runs_concurrency", table_name=_TABLE)
    op.drop_column(_TABLE, "concurrency_key")
    op.drop_column(_TABLE, "priority")
