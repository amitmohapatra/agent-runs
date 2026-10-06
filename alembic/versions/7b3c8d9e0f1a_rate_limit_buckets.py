"""rate limits: one budget per tenant, shared by every replica

``rate_limit_buckets`` replaces the per-process token buckets: a row per tenant holds the
instant its budget would be full again (GCRA), which each request moves under the
database's clock (``api/ratelimit.py``). A missing row is a full budget, so the table starts
empty and losing it forgives everyone.

Revision ID: 7b3c8d9e0f1a
Revises: 6a2b7c8d9e0f
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "7b3c8d9e0f1a"
down_revision = "6a2b7c8d9e0f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "rate_limit_buckets",
        sa.Column("bucket", sa.String(length=256), primary_key=True),
        sa.Column("tat", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("rate_limit_buckets")
