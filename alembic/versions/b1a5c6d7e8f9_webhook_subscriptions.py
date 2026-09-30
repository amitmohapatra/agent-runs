"""webhooks: tenant subscriptions and a delivery outbox, instead of a URL per run

``webhooks`` holds a tenant's subscriptions (URL, events, the secret that signs them);
``webhook_deliveries`` is the outbox the ticker sends from, written in the transaction of
the run change that caused the event. The per-run and per-schedule ``webhook_url`` columns
go: a URL captured at start could only ever reach the one caller who started the run.

Revision ID: b1a5c6d7e8f9
Revises: a0f4b5c6d7e8
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "b1a5c6d7e8f9"
down_revision = "a0f4b5c6d7e8"
branch_labels = None
depends_on = None

_TIMESTAMP = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "webhooks",
        sa.Column("webhook_id", sa.String(length=64), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("url", sa.String(length=2048), nullable=False),
        sa.Column("events", postgresql.ARRAY(sa.String(length=32)), nullable=False),
        sa.Column("secret", sa.String(length=128), nullable=False),
        sa.Column("created_by", sa.String(length=128), nullable=False),
        sa.Column("created_at", _TIMESTAMP, server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_webhooks_tenant", "webhooks", ["tenant_id"])
    op.create_table(
        "webhook_deliveries",
        sa.Column("delivery_id", sa.String(length=64), primary_key=True),
        sa.Column(
            "webhook_id",
            sa.String(length=64),
            sa.ForeignKey("webhooks.webhook_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", _TIMESTAMP, nullable=False),
        sa.Column("created_at", _TIMESTAMP, server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_webhook_deliveries_due", "webhook_deliveries", ["next_attempt_at"])
    op.create_index("ix_webhook_deliveries_webhook", "webhook_deliveries", ["webhook_id"])
    op.drop_column("agent_runs", "webhook_url")
    op.drop_column("agent_schedules", "webhook_url")


def downgrade() -> None:
    op.add_column("agent_schedules", sa.Column("webhook_url", sa.String(length=2048)))
    op.add_column("agent_runs", sa.Column("webhook_url", sa.String(length=2048)))
    op.drop_table("webhook_deliveries")
    op.drop_table("webhooks")
