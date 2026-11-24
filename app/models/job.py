import enum
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from sqlalchemy import (
    String,
    Integer,
    Float,
    Text,
    DateTime,
    Enum,
    Index,
    ForeignKey,
    UniqueConstraint,
    JSON,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.core.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DEAD = "DEAD"


class AttemptStatus(str, enum.Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    job_type: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    payload: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status_enum", native_enum=False),
        default=JobStatus.PENDING,
        nullable=False,
        index=True,
    )
    priority: Mapped[int] = mapped_column(Integer, default=4, nullable=False)
    idempotency_key: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        unique=True,
        index=True,
    )

    # Retry and Execution Configuration
    max_retries: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    backoff_base_seconds: Mapped[float] = mapped_column(Float, default=1.0, nullable=False)
    backoff_max_seconds: Mapped[float] = mapped_column(Float, default=60.0, nullable=False)
    timeout_seconds: Mapped[int] = mapped_column(Integer, default=300, nullable=False)

    # Worker association
    worker_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)

    # Result & Errors
    result: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Timestamps (UTC)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        nullable=False,
        index=True,
    )
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    attempts: Mapped[List["JobAttempt"]] = relationship(
        "JobAttempt",
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="JobAttempt.attempt_number",
    )
    dependencies: Mapped[List["JobDependency"]] = relationship(
        "JobDependency",
        foreign_keys="JobDependency.job_id",
        back_populates="job",
        cascade="all, delete-orphan",
    )
    dependents: Mapped[List["JobDependency"]] = relationship(
        "JobDependency",
        foreign_keys="JobDependency.depends_on_job_id",
        back_populates="depends_on_job",
        cascade="all, delete-orphan",
    )

    @property
    def depends_on(self) -> List[uuid.UUID]:
        if not hasattr(self, "_sa_instance_state") or not self.dependencies:
            return []
        return [dep.depends_on_job_id for dep in self.dependencies]

    __table_args__ = (
        Index("ix_jobs_status_priority", "status", "priority"),
        Index("ix_jobs_created_at_desc", created_at.desc()),
    )


class JobAttempt(Base):
    __tablename__ = "job_attempts"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[AttemptStatus] = mapped_column(
        Enum(AttemptStatus, name="attempt_status_enum", native_enum=False),
        nullable=False,
        default=AttemptStatus.RUNNING,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        nullable=False,
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    logs: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    execution_duration_ms: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    job: Mapped["Job"] = relationship("Job", back_populates="attempts")

    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="uq_job_attempt_number"),
    )


class JobDependency(Base):
    __tablename__ = "job_dependencies"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    depends_on_job_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        nullable=False,
    )

    job: Mapped["Job"] = relationship(
        "Job",
        foreign_keys=[job_id],
        back_populates="dependencies",
    )
    depends_on_job: Mapped["Job"] = relationship(
        "Job",
        foreign_keys=[depends_on_job_id],
        back_populates="dependents",
    )

    __table_args__ = (
        UniqueConstraint("job_id", "depends_on_job_id", name="uq_job_dependency_pair"),
    )
