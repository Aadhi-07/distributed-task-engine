"""Initial schema for jobs, attempts, dependencies, and workers.

Revision ID: 001_initial
Revises: 
Create Date: 2026-11-24 12:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. jobs table
    op.create_table(
        "jobs",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("job_type", sa.String(length=128), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="4"),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("backoff_base_seconds", sa.Float(), nullable=False, server_default="1.0"),
        sa.Column("backoff_max_seconds", sa.Float(), nullable=False, server_default="60.0"),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False, server_default="300"),
        sa.Column("worker_id", sa.String(length=128), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_jobs_job_type", "jobs", ["job_type"])
    op.create_index("ix_jobs_status", "jobs", ["status"])
    op.create_index("ix_jobs_idempotency_key", "jobs", ["idempotency_key"], unique=True)
    op.create_index("ix_jobs_worker_id", "jobs", ["worker_id"])
    op.create_index("ix_jobs_created_at", "jobs", ["created_at"])
    op.create_index("ix_jobs_scheduled_at", "jobs", ["scheduled_at"])
    op.create_index("ix_jobs_status_priority", "jobs", ["status", "priority"])
    op.create_index("ix_jobs_created_at_desc", "jobs", [sa.text("created_at DESC")])

    # 2. job_attempts table
    op.create_table(
        "job_attempts",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("logs", sa.Text(), nullable=True),
        sa.Column("execution_duration_ms", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", "attempt_number", name="uq_job_attempt_number"),
    )
    op.create_index("ix_job_attempts_job_id", "job_attempts", ["job_id"])

    # 3. job_dependencies table
    op.create_table(
        "job_dependencies",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("depends_on_job_id", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["depends_on_job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", "depends_on_job_id", name="uq_job_dependency_pair"),
    )
    op.create_index("ix_job_dependencies_job_id", "job_dependencies", ["job_id"])
    op.create_index("ix_job_dependencies_depends_on_job_id", "job_dependencies", ["depends_on_job_id"])

    # 4. workers table
    op.create_table(
        "workers",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("hostname", sa.String(length=255), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("concurrency", sa.Integer(), nullable=False, server_default="4"),
        sa.Column("running_jobs_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cpu_percent", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("memory_percent", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_workers_status", "workers", ["status"])
    op.create_index("ix_workers_last_heartbeat_at", "workers", ["last_heartbeat_at"])
    op.create_index("ix_workers_status_last_heartbeat", "workers", ["status", "last_heartbeat_at"])


def downgrade() -> None:
    op.drop_table("workers")
    op.drop_table("job_dependencies")
    op.drop_table("job_attempts")
    op.drop_table("jobs")
