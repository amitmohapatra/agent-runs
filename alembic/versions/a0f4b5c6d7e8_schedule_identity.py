"""schedules: identity is (tenant, agent, on_behalf_of, cadence, input hash), not the name

A create is an upsert on that identity, so a harness redeploying the same schedule gets the
one it made instead of a 409 to work around. ``input_sha256`` is the SHA-256 of the input
as canonical JSON (computed in Python here, exactly as the service computes it: PostgreSQL's
own JSON text is not the same canonical form). ``name`` becomes a label.

Revision ID: a0f4b5c6d7e8
Revises: 9e3f4a5b6c7d
"""

from __future__ import annotations

import json
from hashlib import sha256

import sqlalchemy as sa

from alembic import op

revision = "a0f4b5c6d7e8"
down_revision = "9e3f4a5b6c7d"
branch_labels = None
depends_on = None

_TABLE = "agent_schedules"
_IDENTITY = ["tenant_id", "agent_id", "on_behalf_of", "cadence", "input_sha256"]


def _digest(value: object) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(canonical.encode()).hexdigest()


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("input_sha256", sa.String(length=64), nullable=True))
    conn = op.get_bind()
    rows = conn.execute(sa.text(f"SELECT schedule_id, input FROM {_TABLE}")).all()
    for schedule_id, value in rows:
        conn.execute(
            sa.text(f"UPDATE {_TABLE} SET input_sha256 = :digest WHERE schedule_id = :sid"),
            {"digest": _digest(value), "sid": schedule_id},
        )
    op.alter_column(_TABLE, "input_sha256", nullable=False)
    op.drop_constraint("uq_schedules_tenant_name", _TABLE, type_="unique")
    op.create_unique_constraint("uq_schedules_identity", _TABLE, _IDENTITY)


def downgrade() -> None:
    op.drop_constraint("uq_schedules_identity", _TABLE, type_="unique")
    op.create_unique_constraint("uq_schedules_tenant_name", _TABLE, ["tenant_id", "name"])
    op.drop_column(_TABLE, "input_sha256")
