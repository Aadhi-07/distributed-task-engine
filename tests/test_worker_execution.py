import uuid
import pytest
from httpx import AsyncClient
from app.worker.main import WorkerProcess
from app.core.database import get_session_factory
from app.services.job_service import JobService


@pytest.mark.asyncio
async def test_worker_executes_model_inference_job(async_client: AsyncClient):
    # 1. Submit model_inference job via API
    payload = {
        "job_type": "model_inference",
        "payload": {
            "model": "vit_base",
            "inputs": [{"id": 101, "features": [0.2, 0.4, 0.6]}],
        },
        "priority": 5,
    }
    submit_res = await async_client.post("/jobs", json=payload)
    assert submit_res.status_code == 201
    job_id = uuid.UUID(submit_res.json()["id"])

    # 2. Worker executes job from queue
    worker = WorkerProcess()
    processed = await worker.process_next_job(timeout=2)
    assert processed is True

    # 3. Verify in PostgreSQL
    session_factory = get_session_factory()
    async with session_factory() as session:
        job = await JobService.get_job_by_id(session, job_id)
        assert job is not None
        assert job.status.value == "SUCCEEDED"
        assert job.worker_id == worker.worker_id
        assert job.started_at is not None
        assert job.finished_at is not None
        assert job.result is not None
        assert job.result["model"] == "vit_base"
        assert len(job.result["predictions"]) == 1

        # Check attempts record
        assert len(job.attempts) == 1
        attempt = job.attempts[0]
        assert attempt.attempt_number == 1
        assert attempt.status.value == "SUCCEEDED"
        assert attempt.worker_id == worker.worker_id
        assert attempt.execution_duration_ms > 0


@pytest.mark.asyncio
async def test_worker_executes_failing_job(async_client: AsyncClient):
    # 1. Submit failing task
    payload = {
        "job_type": "failing_task",
        "payload": {"error_message": "Matrix multiplication dimension mismatch"},
    }
    submit_res = await async_client.post("/jobs", json=payload)
    assert submit_res.status_code == 201
    job_id = uuid.UUID(submit_res.json()["id"])

    # 2. Worker executes job
    worker = WorkerProcess()
    processed = await worker.process_next_job(timeout=2)
    assert processed is False  # execute_job returns False when task fails

    # 3. Verify failure status and error details in PostgreSQL
    session_factory = get_session_factory()
    async with session_factory() as session:
        job = await JobService.get_job_by_id(session, job_id)
        assert job is not None
        assert job.status.value == "FAILED"
        assert "Matrix multiplication dimension mismatch" in job.error
        assert job.finished_at is not None

        # Check attempt record captures failure
        assert len(job.attempts) == 1
        attempt = job.attempts[0]
        assert attempt.status.value == "FAILED"
        assert "Matrix multiplication dimension mismatch" in attempt.error
        assert attempt.execution_duration_ms is not None


@pytest.mark.asyncio
async def test_worker_executes_batch_scoring(async_client: AsyncClient):
    # 1. Submit batch scoring job
    payload = {
        "job_type": "batch_scoring",
        "payload": {"batch_size": 250, "partition_id": "part-042"},
    }
    submit_res = await async_client.post("/jobs", json=payload)
    assert submit_res.status_code == 201
    job_id = uuid.UUID(submit_res.json()["id"])

    # 2. Worker executes job
    worker = WorkerProcess()
    processed = await worker.process_next_job(timeout=2)
    assert processed is True

    # 3. Verify in PostgreSQL
    session_factory = get_session_factory()
    async with session_factory() as session:
        job = await JobService.get_job_by_id(session, job_id)
        assert job is not None
        assert job.status.value == "SUCCEEDED"
        assert job.result["records_scored"] == 250
        assert job.result["partition_id"] == "part-042"
