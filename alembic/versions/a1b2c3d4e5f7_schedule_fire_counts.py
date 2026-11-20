"""schedules: the fires of a tenant's UTC day (``RUNS__SCHEDULES__MAX_FIRES_PER_DAY``)

One row per tenant and UTC day, holding how many runs its schedules fired that day. A fire
takes from it under the row's lock, so replicas firing at once never pass the limit. Kept only
while the limit is on (500 a day unless set); empty when it is 0 (off).

Revision ID: a1b2c3d4e5f7
Revises: f6a7b8c9d0e1
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "a1b2c3d4e5f7"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "schedule_fire_counts",
        sa.Column("tenant_id", sa.String(length=128), primary_key=True),
        sa.Column("day", sa.Date(), primary_key=True),
        sa.Column("fires", sa.Integer(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("schedule_fire_counts")
