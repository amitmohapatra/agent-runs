"""The two tables this service owns. The schema itself is the Alembic migrations; these
mappings must agree with them (a test compares the two)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Index, Integer, String, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def _created() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class RunRow(Base):
    __tablename__ = "agent_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128))
    agent_id: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32))
    parent_run_id: Mapped[str | None] = mapped_column(String(64))
    thread_id: Mapped[str | None] = mapped_column(String(128))
    user_id: Mapped[str | None] = mapped_column(String(128))
    workspace_id: Mapped[str | None] = mapped_column(String(128))
    on_behalf_of: Mapped[str | None] = mapped_column(String(128))
    input: Mapped[Any] = mapped_column(JSONB, nullable=True)
    output: Mapped[Any] = mapped_column(JSONB, nullable=True)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    #: the Interrupt a PAUSED run waits on
    awaiting: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    #: the InterruptResolution the last resume carried
    last_resolution: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    #: the executor's opaque resume state, written at pause and cleared when the run finishes
    checkpoint: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    #: denormalised from ``awaiting`` for the inbox and the escalation sweep
    assignee: Mapped[str | None] = mapped_column(String(256))
    awaiting_deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    #: the run's own deadline (RunStart.deadline)
    deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    idempotency_key: Mapped[str | None] = mapped_column(String(255))
    webhook_url: Mapped[str | None] = mapped_column(String(2048))
    run_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    #: when it last entered the queue; set once a run is durable (queued at least once)
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _created()

    __table_args__ = (
        UniqueConstraint("tenant_id", "idempotency_key", name="uq_runs_tenant_idempotency"),
        Index("ix_runs_tenant_created", "tenant_id", "created_at"),
        Index("ix_runs_parent", "parent_run_id"),
        # the inbox: this tenant's PAUSED runs for an assignee, newest first
        Index("ix_runs_inbox", "tenant_id", "status", "assignee", "created_at"),
        # the claim: the oldest QUEUED run of the worker's agents
        Index(
            "ix_runs_queue",
            "tenant_id",
            "agent_id",
            "queued_at",
            postgresql_where=text("status = 'QUEUED'"),
        ),
        # the lease sweep: RUNNING runs whose lease lapsed
        Index(
            "ix_runs_lease",
            "status",
            "lease_expires_at",
            postgresql_where=text("lease_expires_at IS NOT NULL"),
        ),
        # the escalation sweep: PAUSED runs past the interrupt's deadline
        Index(
            "ix_runs_escalation",
            "awaiting_deadline",
            postgresql_where=text("status = 'PAUSED' AND awaiting_deadline IS NOT NULL"),
        ),
    )


class ScheduleRow(Base):
    __tablename__ = "agent_schedules"

    schedule_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128))
    agent_id: Mapped[str] = mapped_column(String(128))
    name: Mapped[str] = mapped_column(String(200))
    cadence: Mapped[str] = mapped_column(String(128))
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")
    input: Mapped[Any] = mapped_column(JSONB, nullable=True)
    #: SHA-256 of the input as canonical JSON: part of the schedule's identity
    input_sha256: Mapped[str] = mapped_column(String(64))
    #: NOT NULL: a schedule with no identity is a run nobody authorised
    on_behalf_of: Mapped[str] = mapped_column(String(128))
    workspace_id: Mapped[str | None] = mapped_column(String(128))
    webhook_url: Mapped[str | None] = mapped_column(String(2048))
    created_by: Mapped[str | None] = mapped_column(String(128))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    next_fire_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_run_id: Mapped[str | None] = mapped_column(String(64))
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    #: the backoff gate after a retryable failed fire; ``next_fire_at`` stays the tick owed
    retry_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    schedule_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _created()

    __table_args__ = (
        # the identity a create upserts on
        UniqueConstraint(
            "tenant_id",
            "agent_id",
            "on_behalf_of",
            "cadence",
            "input_sha256",
            name="uq_schedules_identity",
        ),
        Index("ix_schedules_tenant_created", "tenant_id", "created_at"),
        # the ticker: enabled schedules by next fire
        Index("ix_schedules_due", "next_fire_at", postgresql_where=text("enabled")),
    )
