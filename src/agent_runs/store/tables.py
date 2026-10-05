"""The tables this service owns. The schema itself is the Alembic migrations; these
mappings must agree with them (a test compares the two)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
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
    #: executions: the first, then one more for each resume that continues the run and each
    #: requeue after a lapsed lease
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    #: the times the run's lease lapsed (its worker stopped heartbeating), which alone decide
    #: when the ticker gives up on it (``MAX_LEASE_LAPSES``)
    lease_lapses: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    #: the times the run went back on the queue after its worker ended it ERROR with a
    #: retryable error (``MAX_ERROR_RETRIES``)
    error_retries: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    #: the run's own deadline (RunStart.deadline)
    deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    #: the most working time the run may take (RunStart.timeout_seconds)
    timeout_seconds: Mapped[float | None] = mapped_column(Float)
    #: the working time of the RUNNING stretches that ended; the one going on is counted from
    #: ``running_since``
    worked_seconds: Mapped[float] = mapped_column(Float, server_default=text("0"))
    #: when the run last became RUNNING; set exactly while it is
    running_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    #: the version of the agent's code that started the run (RunStart.agent_version)
    agent_version: Mapped[str | None] = mapped_column(String(128))
    #: claim order among the tenant's queued runs, higher first (RunStart.priority)
    priority: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    #: the tenant's runs sharing it run a few at a time (RunStart.concurrency_key)
    concurrency_key: Mapped[str | None] = mapped_column(String(200))
    idempotency_key: Mapped[str | None] = mapped_column(String(255))
    run_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    #: when it last entered the queue; set once a run is durable (queued at least once)
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    #: a QUEUED run is not claimed before this (a retry's backoff); null: at once
    available_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    #: the worker_id whose pause or finish made the current state (null: no worker), so a
    #: repeat of that call answers the stored run instead of a conflict
    settled_by: Mapped[str | None] = mapped_column(String(200))
    #: when someone asked to cancel the run while a worker held it; set only while RUNNING
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    #: why the run was cancelled (POST /v1/runs/{run_id}/cancel), and the principal who asked
    cancel_reason: Mapped[str | None] = mapped_column(String(1000))
    cancelled_by: Mapped[str | None] = mapped_column(String(256))
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
        # the deadline sweep: runs not yet ended past their own deadline
        Index(
            "ix_runs_deadline",
            "deadline",
            postgresql_where=text(
                "status IN ('QUEUED', 'RUNNING', 'PAUSED') AND deadline IS NOT NULL"
            ),
        ),
        # the working-time sweep: RUNNING runs, by how long they have been running
        Index(
            "ix_runs_working",
            "running_since",
            postgresql_where=text("status = 'RUNNING'"),
        ),
        # the claim: RUNNING runs sharing a concurrency key
        Index(
            "ix_runs_concurrency",
            "tenant_id",
            "concurrency_key",
            postgresql_where=text("status = 'RUNNING' AND concurrency_key IS NOT NULL"),
        ),
        # the claim: each tenant's runs held by workers (fair share, the per-tenant cap)
        Index(
            "ix_runs_leased",
            "tenant_id",
            "agent_id",
            postgresql_where=text("status = 'RUNNING' AND lease_owner IS NOT NULL"),
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
    created_by: Mapped[str | None] = mapped_column(String(128))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    #: copied into every fired run's RunStart (ScheduleSpec.timeout_seconds, agent_version)
    timeout_seconds: Mapped[float | None] = mapped_column(Float)
    agent_version: Mapped[str | None] = mapped_column(String(128))
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


class WebhookRow(Base):
    """A tenant's subscription: a URL, the events it wants, the secret that signs them."""

    __tablename__ = "webhooks"

    webhook_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128))
    url: Mapped[str] = mapped_column(String(2048))
    events: Mapped[list[str]] = mapped_column(ARRAY(String(32)))
    secret: Mapped[str] = mapped_column(String(128))
    #: the secret a rotation replaced, which also signs deliveries until it expires
    previous_secret: Mapped[str | None] = mapped_column(String(128))
    previous_secret_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_by: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = _created()

    __table_args__ = (Index("ix_webhooks_tenant", "tenant_id"),)


class WebhookDeliveryRow(Base):
    """The outbox: one event owed to one subscription, until it is accepted; one given up on
    stays, dead, to be listed and redelivered, until its retention ends."""

    __tablename__ = "webhook_deliveries"

    #: derived from the event and the subscription, so a repeated write is one row
    delivery_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    webhook_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("webhooks.webhook_id", ondelete="CASCADE")
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    attempts: Mapped[int] = mapped_column(Integer)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    #: why the last attempt failed (a status, an unreachable host, a refused address)
    last_error: Mapped[str | None] = mapped_column(Text)
    #: when it was given up on; null while it is still owed
    dead_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = _created()

    __table_args__ = (
        # the ticker: deliveries still owed, by when they are due
        Index(
            "ix_webhook_deliveries_due",
            "next_attempt_at",
            postgresql_where=text("dead_at IS NULL"),
        ),
        Index("ix_webhook_deliveries_webhook", "webhook_id"),
        # the ticker: dead deliveries, by when their retention ends
        Index(
            "ix_webhook_deliveries_dead",
            "dead_at",
            postgresql_where=text("dead_at IS NOT NULL"),
        ),
    )


class ResolutionRow(Base):
    """Every answer a run's interrupts got, append-only: who decided what about which question
    and when. ``agent_runs.last_resolution`` keeps only the latest, and HITL decisions are
    audit records. Written in the resume's own transaction, so a row exists exactly when the
    resume took effect."""

    __tablename__ = "run_resolutions"

    #: stable_id(run_id, interrupt_id): an interrupt is answered once
    resolution_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), ForeignKey("agent_runs.run_id"))
    tenant_id: Mapped[str] = mapped_column(String(128))
    interrupt_id: Mapped[str] = mapped_column(String(64))
    decision: Mapped[str] = mapped_column(String(16))
    reviewer: Mapped[str | None] = mapped_column(String(256))
    #: the Interrupt as it was asked (question, tool call) and the InterruptResolution
    interrupt: Mapped[dict[str, Any]] = mapped_column(JSONB)
    resolution: Mapped[dict[str, Any]] = mapped_column(JSONB)
    #: the attempt that paused on it
    attempt: Mapped[int] = mapped_column(Integer)
    resolved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("ix_run_resolutions_run", "tenant_id", "run_id", "recorded_at"),)


class RateLimitRow(Base):
    """A tenant's request budget (``api/ratelimit.py``), shared by every replica: the instant
    it would be full again."""

    __tablename__ = "rate_limit_buckets"

    #: the tenant (or ``key:<key_id>``, a platform key claiming across tenants)
    bucket: Mapped[str] = mapped_column(String(256), primary_key=True)
    tat: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ArtifactRow(Base):
    """A run artifact: what it is and whose; the bytes are in the blob store at ``blob_key``.
    ``expires_at`` is set when the run ends; the ticker deletes the artifact after it."""

    __tablename__ = "run_artifacts"

    artifact_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), ForeignKey("agent_runs.run_id"))
    tenant_id: Mapped[str] = mapped_column(String(128))
    blob_key: Mapped[str] = mapped_column(String(512))
    mime: Mapped[str] = mapped_column(String(255))
    size: Mapped[int] = mapped_column(BigInteger)
    #: ``sha256:<hex>`` of the bytes
    checksum: Mapped[str] = mapped_column(String(80))
    created_at: Mapped[datetime] = _created()
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        # one artifact per content per run: a retried upload is the same artifact
        UniqueConstraint("run_id", "checksum", name="uq_run_artifacts_content"),
        # the ticker: artifacts of ended runs, by when they go
        Index(
            "ix_run_artifacts_expiry",
            "expires_at",
            postgresql_where=text("expires_at IS NOT NULL"),
        ),
    )
