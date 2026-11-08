"""drop the inline payload columns: run payloads live in the blob store only

Contract step of the change-over. Run ``agent-runs-payloads clear`` before upgrading: the
upgrade refuses while any row still holds a payload inline (nothing is lost). The downgrade
adds the columns back, empty: the previous release reads the references.

Revision ID: b9e3f4a5c6d7
Revises: a7c1d2e3f4b5
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "b9e3f4a5c6d7"
down_revision = "a7c1d2e3f4b5"
branch_labels = None
depends_on = None

_RUNS = ("input", "output", "last_resolution", "checkpoint")
_RESOLUTIONS = ("interrupt", "resolution")


def _present(column: str) -> str:
    return f"coalesce(jsonb_typeof({column}), 'null') <> 'null'"


def upgrade() -> None:
    left = " OR ".join(_present(c) for c in _RUNS)
    op.execute(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM agent_runs WHERE "
        f"{left} OR awaiting ? 'payload') OR EXISTS (SELECT 1 FROM run_resolutions WHERE "
        f"{' OR '.join(_present(c) for c in _RESOLUTIONS)}) OR EXISTS (SELECT 1 FROM "
        "webhook_deliveries WHERE payload #> '{data,run,awaiting}' ? 'payload') THEN "
        "RAISE EXCEPTION 'payloads are still inline: run agent-runs-payloads clear first'; "
        "END IF; END $$"
    )
    for name in _RUNS:
        op.drop_column("agent_runs", name)
    for name in _RESOLUTIONS:
        op.drop_column("run_resolutions", name)
        op.alter_column("run_resolutions", f"{name}_ref", nullable=False)


def downgrade() -> None:
    for name in _RESOLUTIONS:
        op.alter_column("run_resolutions", f"{name}_ref", nullable=True)
        op.add_column("run_resolutions", sa.Column(name, postgresql.JSONB(), nullable=True))
    for name in _RUNS:
        op.add_column("agent_runs", sa.Column(name, postgresql.JSONB(), nullable=True))
