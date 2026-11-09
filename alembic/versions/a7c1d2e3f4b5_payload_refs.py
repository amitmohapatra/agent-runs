"""payload references: run payloads live in the blob store, the rows keep references

Every payload column is replaced by a ``*_ref`` (a ``PayloadRef``: key, size, SHA-256, media
type); there is no inline payload column.

- ``agent_runs``: ``input_ref``, ``output_ref``, ``awaiting_ref`` (the interrupt's
  ``payload``), ``last_resolution_ref``, ``checkpoint_ref`` (a whole checkpoint),
  ``checkpoint_segments`` (a segmented one), ``events_ref`` (an ended run's compacted log),
  in place of ``input``, ``output``, ``last_resolution`` and ``checkpoint``;
- ``run_resolutions``: ``interrupt_ref`` and ``resolution_ref``, in place of ``interrupt``
  and ``resolution``;
- ``webhook_deliveries``: ``payload_ref``, the run's interrupt payload the body carries;
- ``payload_blobs``: every payload object written, by key (what the orphan sweep reads,
  instead of listing the bucket), and ``run_payloads``: which runs refer to which objects, as what.

Revision ID: a7c1d2e3f4b5
Revises: 2c8d3e4f5a6b
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "a7c1d2e3f4b5"
down_revision = "2c8d3e4f5a6b"
branch_labels = None
depends_on = None

_RUN_REFS = (
    "input_ref",
    "output_ref",
    "awaiting_ref",
    "last_resolution_ref",
    "checkpoint_ref",
    "checkpoint_segments",
    "events_ref",
)
_INLINE = ("input", "output", "last_resolution", "checkpoint")
_ENDED = "status IN ('SUCCESS', 'PARTIAL', 'ERROR', 'TIMEOUT', 'CANCELLED', 'REJECTED')"


def upgrade() -> None:
    for name in _RUN_REFS:
        op.add_column("agent_runs", sa.Column(name, postgresql.JSONB(), nullable=True))
    # the ticker: ended runs whose event log is not compacted yet
    op.create_index(
        "ix_runs_events_uncompacted",
        "agent_runs",
        ["updated_at"],
        postgresql_where=sa.text(f"events_ref IS NULL AND {_ENDED}"),
    )
    for name in _INLINE:
        op.drop_column("agent_runs", name)
    for name in ("interrupt", "resolution"):
        op.drop_column("run_resolutions", name)
        op.add_column(
            "run_resolutions", sa.Column(f"{name}_ref", postgresql.JSONB(), nullable=False)
        )
    op.add_column("webhook_deliveries", sa.Column("payload_ref", postgresql.JSONB(), nullable=True))
    op.create_index(
        "ix_webhook_deliveries_payload_ref",
        "webhook_deliveries",
        [sa.text("(payload_ref ->> 'ref')")],
        postgresql_where=sa.text("payload_ref IS NOT NULL"),
    )
    op.create_table(
        "payload_blobs",
        sa.Column("key", sa.String(length=512), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
        sa.Column(
            "touched_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("ix_payload_blobs_touched", "payload_blobs", ["touched_at"])
    op.create_table(
        "run_payloads",
        sa.Column(
            "run_id",
            sa.String(length=64),
            sa.ForeignKey("agent_runs.run_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("key", sa.String(length=512), primary_key=True),
        # what the object is to the run (input, checkpoint, ...): a checkpoint's references
        # go when the run ends, its input's stay
        sa.Column("kind", sa.String(length=16), primary_key=True),
    )
    op.create_index("ix_run_payloads_key", "run_payloads", ["key"])


def downgrade() -> None:
    op.drop_index("ix_run_payloads_key", table_name="run_payloads")
    op.drop_table("run_payloads")
    op.drop_index("ix_payload_blobs_touched", table_name="payload_blobs")
    op.drop_table("payload_blobs")
    op.drop_index("ix_webhook_deliveries_payload_ref", table_name="webhook_deliveries")
    op.drop_column("webhook_deliveries", "payload_ref")
    for name in ("resolution", "interrupt"):
        op.drop_column("run_resolutions", f"{name}_ref")
        op.add_column("run_resolutions", sa.Column(name, postgresql.JSONB(), nullable=False))
    op.drop_index("ix_runs_events_uncompacted", table_name="agent_runs")
    for name in reversed(_RUN_REFS):
        op.drop_column("agent_runs", name)
    for name in _INLINE:
        op.add_column("agent_runs", sa.Column(name, postgresql.JSONB(), nullable=True))
