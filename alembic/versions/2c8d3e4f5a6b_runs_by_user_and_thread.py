"""runs: indexes for listing a user's runs and a thread's runs

``GET /v1/runs?user_id=`` (a person's tasks) and ``?thread_id=`` (a conversation's earlier
turns and its tasks, read at each turn) were served by ``ix_runs_tenant_created`` filtering
every newer run of the tenant, or by a scan of the table: at 100k runs, 22k rows filtered for
one user and a sequential scan for an old thread. Each now has an index leading with the
tenant and its filter, in the listing's order.

Revision ID: 2c8d3e4f5a6b
Revises: 0a6b1c2d3e4f
"""

from __future__ import annotations

from alembic import op

revision = "2c8d3e4f5a6b"
down_revision = "0a6b1c2d3e4f"
branch_labels = None
depends_on = None

_TABLE = "agent_runs"
_INDEXES = {
    "ix_runs_user": ["tenant_id", "user_id", "created_at"],
    "ix_runs_thread": ["tenant_id", "thread_id", "created_at"],
}


def upgrade() -> None:
    for name, columns in _INDEXES.items():
        op.create_index(name, _TABLE, columns, unique=False)


def downgrade() -> None:
    for name in _INDEXES:
        op.drop_index(name, table_name=_TABLE)
