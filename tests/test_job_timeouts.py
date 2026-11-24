import uuid
import pytest
from httpx import AsyncClient

from app.worker.main import WorkerProcess
from app.core.database import get_session_factory
from app.services.job_service import JobService


@pytest.mark.asyncio
async def test_job_execution_timeout(async_client: AsyncClient):
    """Verifies that a job exceeding timeout_seconds is interrupted and marked FAILED with TIMED_OUT attempt."""
    # Submit job that requests 2.0s sleep, but timeout_seconds is configured to 1s with max_retries=0
    res = await async_client.post("/jobs", json={
        "job_type": "sleep",
        "payload": {"seconds": 2.0},
        "timeout_seconds": 1,  # 1 second timeout
        "max_retries": 0,
        "priority": 8,
    })
    assert res.status_code == 201
    job_id = uuid.UUID(res.json()["id"])

    # Worker executes the job
    worker = WorkerProcess()
    processed = await worker.process_next_job()
    assert processed is False  # returns False on failure / timeout

    # Verify in PostgreSQL
    session_factory = get_session_factory()
    async with session_factory() as session:
        job = await JobService.get_job_by_id(session, job_id)
        assert job is not None
        assert job.status.value == "FAILED"
        assert "timeout" in job.error.lower()
        assert job.finished_at is not None

        # Verify attempt recorded as TIMED_OUT
        assert len(job.attempts) == 1
        attempt = job.attempts[0]
        assert attempt.status.value == "TIMED_OUT"
        assert "timeout" in attempt.error.lower()
        assert attempt.execution_duration_ms is not None
