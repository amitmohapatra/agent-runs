"""initial runs table

The whole schema as of the first release, webhook_url included. The service used to call
``Base.metadata.create_all`` at startup, which creates a missing table and silently does
nothing about a missing *column* — so adding one broke every insert on an already-deployed
database with an UndefinedColumn error at runtime rather than a failure at deploy time.

Revision ID: 1f6242bb21de
Revises: 
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '1f6242bb21de'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('agent_runs',
    sa.Column('run_id', sa.String(length=64), nullable=False),
    sa.Column('tenant_id', sa.String(length=128), nullable=False),
    sa.Column('agent_id', sa.String(length=128), nullable=False),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('parent_run_id', sa.String(length=64), nullable=True),
    sa.Column('thread_id', sa.String(length=128), nullable=True),
    sa.Column('user_id', sa.String(length=128), nullable=True),
    sa.Column('on_behalf_of', sa.String(length=128), nullable=True),
    sa.Column('input', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('output', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('error', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('awaiting', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('attempt', sa.Integer(), nullable=False),
    sa.Column('deadline', sa.DateTime(timezone=True), nullable=True),
    sa.Column('idempotency_key', sa.String(length=255), nullable=True),
    sa.Column('webhook_url', sa.String(length=2048), nullable=True),
    sa.Column('run_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('run_id'),
    sa.UniqueConstraint('tenant_id', 'idempotency_key', name='uq_runs_tenant_idempotency')
    )
    op.create_index('ix_runs_parent', 'agent_runs', ['parent_run_id'], unique=False)
    op.create_index('ix_runs_tenant_created', 'agent_runs', ['tenant_id', 'created_at'], unique=False)
    op.create_index('ix_runs_tenant_status', 'agent_runs', ['tenant_id', 'status'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_runs_tenant_status', table_name='agent_runs')
    op.drop_index('ix_runs_tenant_created', table_name='agent_runs')
    op.drop_index('ix_runs_parent', table_name='agent_runs')
    op.drop_table('agent_runs')
