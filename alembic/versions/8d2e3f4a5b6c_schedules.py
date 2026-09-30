"""schedules: the table agent-schedules kept, now owned here

agent-schedules created this table with ``create_all`` at startup and patched a later
column in with ``ALTER TABLE ... IF NOT EXISTS``; it is a migration now, with the fields the
contracts' ``Schedule`` carries (``workspace_id``, ``webhook_url``). The due index is
partial and global: the ticker asks for every tenant's enabled, due schedules at once.

Revision ID: 8d2e3f4a5b6c
Revises: 7c1d2e3f4a5b
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "8d2e3f4a5b6c"
down_revision = "7c1d2e3f4a5b"
branch_labels = None
depends_on = None

_TABLE = "agent_schedules"
_TIMESTAMP = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("schedule_id", sa.String(length=64), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("cadence", sa.String(length=128), nullable=False),
        sa.Column("timezone", sa.String(length=64), nullable=False),
        sa.Column("input", postgresql.JSONB(), nullable=True),
        sa.Column("on_behalf_of", sa.String(length=128), nullable=False),
        sa.Column("workspace_id", sa.String(length=128), nullable=True),
        sa.Column("webhook_url", sa.String(length=2048), nullable=True),
        sa.Column("created_by", sa.String(length=128), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("next_fire_at", _TIMESTAMP, nullable=True),
        sa.Column("last_fired_at", _TIMESTAMP, nullable=True),
        sa.Column("last_run_id", sa.String(length=64), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("last_error", postgresql.JSONB(), nullable=True),
        sa.Column("retry_after", _TIMESTAMP, nullable=True),
        sa.Column("schedule_metadata", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", _TIMESTAMP, server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", _TIMESTAMP, server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("schedule_id"),
        sa.UniqueConstraint("tenant_id", "name", name="uq_schedules_tenant_name"),
    )
    op.create_index(
        "ix_schedules_tenant_created", _TABLE, ["tenant_id", "created_at"], unique=False
    )
    op.create_index(
        "ix_schedules_due",
        _TABLE,
        ["next_fire_at"],
        unique=False,
        postgresql_where=sa.text("enabled"),
    )


def downgrade() -> None:
    op.drop_index("ix_schedules_due", table_name=_TABLE)
    op.drop_index("ix_schedules_tenant_created", table_name=_TABLE)
    op.drop_table(_TABLE)
