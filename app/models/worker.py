import enum
from datetime import datetime, timezone
from typing import Any, Dict
from sqlalchemy import (
    String,
    Integer,
    Float,
    DateTime,
    Enum,
    JSON,
    Index,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkerStatus(str, enum.Enum):
    STARTING = "STARTING"
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    DRAINING = "DRAINING"
    STOPPED = "STOPPED"
    DEAD = "DEAD"


class WorkerNode(Base):
    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[WorkerStatus] = mapped_column(
        Enum(WorkerStatus, name="worker_status_enum", native_enum=False),
        default=WorkerStatus.STARTING,
        nullable=False,
        index=True,
    )
    concurrency: Mapped[int] = mapped_column(Integer, default=4, nullable=False)
    running_jobs_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cpu_percent: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    memory_percent: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)

    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        nullable=False,
    )
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        nullable=False,
        index=True,
    )
    metadata_json: Mapped[Dict[str, Any]] = mapped_column(
        JSON,
        default=dict,
        nullable=False,
    )

    __table_args__ = (
        Index("ix_workers_status_last_heartbeat", "status", "last_heartbeat_at"),
    )
