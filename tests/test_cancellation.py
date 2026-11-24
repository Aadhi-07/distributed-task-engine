import uuid
import pytest
from httpx import AsyncClient

from app.worker.main import WorkerProcess
from app.core.database import get_session_factory
from app.services.job_service import JobService
from app.scheduler.priority_queue import PriorityQueue
from app.core.redis import get_redis


@pytest.mark.asyncio
async def test_cancel_queued_job(async_client: AsyncClient):
    """Submits a job, cancels it while queued, and verifies it transitions to CANCELLED and is not executed."""
    res = await async_client.post("/jobs", json={
        "job_type": "sleep",
        "payload": {"seconds": 0.1},
        "priority": 4,
    })
    assert res.status_code == 201
    job_id = uuid.UUID(res.json()["id"])

    # Cancel the queued job
    cancel_res = await async_client.post(f"/jobs/{job_id}/cancel")
    assert cancel_res.status_code == 200
    data = cancel_res.json()
    assert data["status"] == "CANCELLED"
    assert data["finished_at"] is not None

    # Worker attempts to process: should find nothing or skip
    worker = WorkerProcess()
    processed = await worker.process_next_job()
    assert processed is False

    # Verify status in PostgreSQL
    session_factory = get_session_factory()
    async with session_factory() as session:
        job = await JobService.get_job_by_id(session, job_id)
        assert job.status.value == "CANCELLED"
        assert len(job.attempts) == 0


@pytest.mark.asyncio
async def test_cancel_terminal_job_returns_409(async_client: AsyncClient):
    """Verifies that attempting to cancel an already finished job returns HTTP 409 Conflict."""
    # Submit and execute to completion
    res = await async_client.post("/jobs", json={
        "job_type": "sleep",
        "payload": {"seconds": 0.01},
        "priority": 9,
    })
    job_id = uuid.UUID(res.json()["id"])

    worker = WorkerProcess()
    await worker.process_next_job()

    # Attempt to cancel finished job
    cancel_res = await async_client.post(f"/jobs/{job_id}/cancel")
    assert cancel_res.status_code == 409
    assert "terminal status" in cancel_res.json()["detail"].lower()


@pytest.mark.asyncio
async def test_cancel_nonexistent_job_returns_404(async_client: AsyncClient):
    fake_id = uuid.uuid4()
    cancel_res = await async_client.post(f"/jobs/{fake_id}/cancel")
    assert cancel_res.status_code == 404
