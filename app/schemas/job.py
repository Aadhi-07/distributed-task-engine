import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field

from app.models.job import JobStatus, AttemptStatus


class JobCreate(BaseModel):
    job_type: str = Field(..., description="Registered task type (e.g., model_training, model_inference)", min_length=1, max_length=128)
    payload: Dict[str, Any] = Field(default_factory=dict, description="Input parameters and arguments for the task")
    priority: int = Field(default=4, ge=0, le=9, description="Scheduling priority (0=lowest, 9=highest)")
    timeout_seconds: int = Field(default=300, gt=0, le=86400, description="Maximum execution timeout in seconds")
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retry attempts upon failure")
    idempotency_key: Optional[str] = Field(default=None, max_length=255, description="Client-provided idempotency key")
    depends_on: List[uuid.UUID] = Field(default_factory=list, description="IDs of jobs this job depends on before running")


class JobAttemptResponse(BaseModel):
    id: uuid.UUID
    attempt_number: int
    worker_id: str
    status: AttemptStatus
    started_at: datetime
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    execution_duration_ms: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)


class JobResponse(BaseModel):
    id: uuid.UUID
    job_type: str
    payload: Dict[str, Any]
    status: JobStatus
    priority: int
    idempotency_key: Optional[str] = None
    max_retries: int
    retry_count: int
    timeout_seconds: int
    worker_id: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    depends_on: List[uuid.UUID] = Field(default_factory=list, description="IDs of direct parent jobs")
    attempts: List[JobAttemptResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class JobListResponse(BaseModel):
    items: List[JobResponse]
    total: int
    limit: int
    offset: int


class DAGNodeCreate(BaseModel):
    node_id: str = Field(..., description="Unique label for this node in the DAG batch (e.g. 'extract', 'train')")
    job_type: str = Field(..., description="Registered task type")
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=4, ge=0, le=9)
    timeout_seconds: int = Field(default=300, gt=0, le=86400)
    max_retries: int = Field(default=3, ge=0, le=10)
    depends_on: List[str] = Field(default_factory=list, description="List of parent node_ids within this batch")


class DAGSubmitRequest(BaseModel):
    nodes: List[DAGNodeCreate] = Field(..., min_length=1, description="Nodes of the DAG batch")


class DAGSubmitResponse(BaseModel):
    node_to_job_id: Dict[str, uuid.UUID]
    jobs: List[JobResponse]
