"""webhooks: dead letters, and secret rotation with an overlap

A delivery that used its attempts, or was refused for good, is kept with ``dead_at`` (and
the ``last_error`` of its last attempt) instead of being deleted, so a tenant lists and
redelivers it; the ticker drops it after its retention (``ix_webhook_deliveries_dead``).
``ix_webhook_deliveries_due`` now covers only the deliveries still owed. A rotated secret
keeps the one it replaced in ``previous_secret`` until ``previous_secret_expires_at``, and
signs deliveries with both meanwhile.

Revision ID: 4d0c5e6f7a8b
Revises: 3c9b4d5e6f7a
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "4d0c5e6f7a8b"
down_revision = "3c9b4d5e6f7a"
branch_labels = None
depends_on = None

_DELIVERIES = "webhook_deliveries"
_TIMESTAMP = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.add_column("webhooks", sa.Column("previous_secret", sa.String(length=128)))
    op.add_column("webhooks", sa.Column("previous_secret_expires_at", _TIMESTAMP))
    op.add_column(_DELIVERIES, sa.Column("last_error", sa.Text()))
    op.add_column(_DELIVERIES, sa.Column("dead_at", _TIMESTAMP))
    op.drop_index("ix_webhook_deliveries_due", table_name=_DELIVERIES)
    op.create_index(
        "ix_webhook_deliveries_due",
        _DELIVERIES,
        ["next_attempt_at"],
        unique=False,
        postgresql_where=sa.text("dead_at IS NULL"),
    )
    op.create_index(
        "ix_webhook_deliveries_dead",
        _DELIVERIES,
        ["dead_at"],
        unique=False,
        postgresql_where=sa.text("dead_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.execute("DELETE FROM webhook_deliveries WHERE dead_at IS NOT NULL")
    op.drop_index("ix_webhook_deliveries_dead", table_name=_DELIVERIES)
    op.drop_index("ix_webhook_deliveries_due", table_name=_DELIVERIES)
    op.create_index("ix_webhook_deliveries_due", _DELIVERIES, ["next_attempt_at"], unique=False)
    op.drop_column(_DELIVERIES, "dead_at")
    op.drop_column(_DELIVERIES, "last_error")
    op.drop_column("webhooks", "previous_secret_expires_at")
    op.drop_column("webhooks", "previous_secret")
