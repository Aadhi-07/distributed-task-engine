from datetime import datetime
from typing import Any, Dict, List
from pydantic import BaseModel, ConfigDict
from app.models.worker import WorkerStatus


class WorkerResponse(BaseModel):
    id: str
    hostname: str
    pid: int
    status: WorkerStatus
    concurrency: int
    running_jobs_count: int
    cpu_percent: float
    memory_percent: float
    registered_at: datetime
    last_heartbeat_at: datetime
    is_alive: bool
    metadata_json: Dict[str, Any]

    model_config = ConfigDict(from_attributes=True)


class WorkerListResponse(BaseModel):
    items: List[WorkerResponse]
    total: int
    active_workers: int
