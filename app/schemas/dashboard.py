import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class DashboardHubNode(BaseModel):
    id: str = "hub"
    label: str = "Central Scheduler"
    queue_depth: int
    delayed_depth: int
    dlq_depth: int


class DashboardWorkerNode(BaseModel):
    id: str
    hostname: str
    pid: int
    status: str
    is_alive: bool
    heartbeat_age_seconds: float
    concurrency: int
    running_jobs: int
    cpu_percent: float
    memory_percent: float
    load_score: float

    model_config = ConfigDict(from_attributes=True)


class DashboardJobAttempt(BaseModel):
    attempt_number: int
    worker_id: str
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    error: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class DashboardJobNode(BaseModel):
    id: str
    job_type: str
    status: str
    priority: int
    worker_id: Optional[str] = None
    created_at: datetime
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    retry_count: int
    max_retries: int
    timeout_seconds: int
    idempotency_key: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    depends_on: List[str] = Field(default_factory=list)
    attempts: List[DashboardJobAttempt] = Field(default_factory=list)
    is_blocked: bool = False

    model_config = ConfigDict(from_attributes=True)


class DashboardEdge(BaseModel):
    id: str
    source: str
    target: str
    edge_type: str  # "queue", "assignment", "dependency"
    status: Optional[str] = None  # "active", "satisfied", "waiting", "blocked_failed"
    priority: int = 4


class DashboardStats(BaseModel):
    queue_depth: int
    delayed_depth: int
    dlq_depth: int
    active_workers: int
    total_workers: int
    running_jobs: int
    total_jobs: int
    succeeded_jobs: int
    failed_jobs: int
    failure_rate_percent: float
    throughput_jobs_per_sec: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float


class DashboardEvent(BaseModel):
    id: str
    timestamp: datetime
    event_type: str  # "JOB_QUEUED", "JOB_STARTED", "JOB_SUCCEEDED", "JOB_FAILED", "JOB_RETRY", "WORKER_LOST", "JOB_DEAD"
    job_id: Optional[str] = None
    worker_id: Optional[str] = None
    message: str
    severity: str  # "info", "success", "warning", "error"


class DashboardSnapshotResponse(BaseModel):
    hub: DashboardHubNode
    workers: List[DashboardWorkerNode]
    jobs: List[DashboardJobNode]
    edges: List[DashboardEdge]
    stats: DashboardStats
    events: List[DashboardEvent]
    status_counts: Dict[str, int]
    total_rendered_jobs: int
    total_unrendered_jobs: int
    etag: str

    model_config = ConfigDict(from_attributes=True)
