"""run artifacts: large review payloads in blob storage, not in the checkpoint

``run_artifacts`` records each artifact a run uploaded (an ``ask`` table, a diff): its run
and tenant, the blob key its bytes are stored under, mime type, size and SHA-256. A run's
artifacts get ``expires_at`` when it ends; the ticker deletes them after it.

Revision ID: c2b6d7e8f9a0
Revises: b1a5c6d7e8f9
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "c2b6d7e8f9a0"
down_revision = "b1a5c6d7e8f9"
branch_labels = None
depends_on = None

_TIMESTAMP = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "run_artifacts",
        sa.Column("artifact_id", sa.String(length=64), primary_key=True),
        sa.Column(
            "run_id",
            sa.String(length=64),
            sa.ForeignKey("agent_runs.run_id"),
            nullable=False,
        ),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("blob_key", sa.String(length=512), nullable=False),
        sa.Column("mime", sa.String(length=255), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
        sa.Column("checksum", sa.String(length=80), nullable=False),
        sa.Column("created_at", _TIMESTAMP, server_default=sa.func.now(), nullable=False),
        sa.Column("expires_at", _TIMESTAMP, nullable=True),
        sa.UniqueConstraint("run_id", "checksum", name="uq_run_artifacts_content"),
    )
    op.create_index(
        "ix_run_artifacts_expiry",
        "run_artifacts",
        ["expires_at"],
        postgresql_where=sa.text("expires_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_table("run_artifacts")
