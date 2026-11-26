import json
import logging
import pytest
from httpx import AsyncClient

from app.core.logging import (
    StructuredJSONFormatter,
    set_request_id,
    set_job_id,
    set_worker_id,
    clear_context,
)
from app.core.database import get_session_factory
from app.core.redis import get_redis
from app.schemas.job import JobCreate
from app.services.job_service import JobService
from app.worker.main import WorkerProcess


def test_structured_json_formatter():
    """Verifies that StructuredJSONFormatter serializes log records into JSON with context variables."""
    clear_context()
    formatter = StructuredJSONFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Execution initialized",
        args=(),
        exc_info=None,
    )

    # Without context
    raw_json = formatter.format(record)
    data = json.loads(raw_json)
    assert data["name"] == "test_logger"
    assert data["level"] == "INFO"
    assert data["message"] == "Execution initialized"
    assert "timestamp" in data
    assert "request_id" not in data

    # With contextual tracking
    set_request_id("req-uuid-1234")
    set_job_id("job-uuid-5678")
    set_worker_id("worker-gpu-1")

    raw_json_ctx = formatter.format(record)
    data_ctx = json.loads(raw_json_ctx)
    assert data_ctx["request_id"] == "req-uuid-1234"
    assert data_ctx["job_id"] == "job-uuid-5678"
    assert data_ctx["worker_id"] == "worker-gpu-1"

    clear_context()


@pytest.mark.asyncio
async def test_metrics_endpoint_scrapes_valid_prometheus(async_client: AsyncClient):
    """Verifies that GET /metrics returns valid Prometheus metrics format."""
    res = await async_client.get("/metrics")
    assert res.status_code == 200
    assert "text/plain" in res.headers["content-type"]
    text = res.text

    assert "task_engine_queue_depth" in text
    assert "task_engine_active_workers_count" in text
    assert "task_engine_jobs_submitted_total" in text
    assert "task_engine_jobs_completed_total" in text
    assert "task_engine_job_execution_duration_seconds" in text
    assert "task_engine_job_scheduling_latency_seconds" in text


@pytest.mark.asyncio
async def test_metrics_increment_on_job_lifecycle(async_client: AsyncClient):
    """Verifies that submitting and running jobs increments counters and updates histograms."""
    session_factory = get_session_factory()
    redis = await get_redis()
    worker = WorkerProcess(worker_id="metrics-worker-test")

    # 1. Submit a job
    async with session_factory() as session:
        job_in = JobCreate(
            job_type="sleep",
            payload={"duration": 0.01},
            priority=8,
            max_retries=0,
        )
        job, _ = await JobService.submit_job(session, redis, job_in)

    # 2. Scrape metrics after submission
    res_after_submit = await async_client.get("/metrics")
    assert 'task_engine_jobs_submitted_total{job_type="sleep"}' in res_after_submit.text

    # 3. Worker executes job
    processed = await worker.process_next_job(timeout=2)
    assert processed is True

    # 4. Scrape metrics after execution
    res_after_run = await async_client.get("/metrics")
    metrics_text = res_after_run.text

    assert 'task_engine_jobs_completed_total{job_type="sleep",status="SUCCEEDED"}' in metrics_text
    assert 'task_engine_job_execution_duration_seconds_count{job_type="sleep"}' in metrics_text
    assert 'task_engine_job_scheduling_latency_seconds_count{job_type="sleep"}' in metrics_text


@pytest.mark.asyncio
async def test_request_id_header_and_correlation(async_client: AsyncClient):
    """Verifies that X-Request-ID is preserved or generated, and X-Response-Time-Ms is emitted."""
    custom_id = "test-custom-req-id-777"
    res = await async_client.get("/health", headers={"X-Request-ID": custom_id})
    assert res.status_code == 200
    assert res.headers["X-Request-ID"] == custom_id
    assert "X-Response-Time-Ms" in res.headers


@pytest.mark.asyncio
async def test_live_collector_computes_metrics_from_postgres_and_redis(async_client: AsyncClient):
    """Verifies that the API /metrics collector computes gauges, completed counters,
    retries, and execution latency from Postgres and Redis even when jobs are processed out-of-process.
    """
    import uuid
    from datetime import datetime, timezone
    from app.models.job import Job, JobAttempt, JobStatus, AttemptStatus

    session_factory = get_session_factory()
    redis = await get_redis()

    # Seed mock multi-process state directly into Postgres (simulating independent worker instances)
    unique_type = f"test_bg_{uuid.uuid4().hex[:6]}"
    async with session_factory() as session:
        # 1. Concurrently running job
        running_job = Job(
            id=uuid.uuid4(),
            job_type=unique_type,
            status=JobStatus.RUNNING,
            priority=5,
            payload={},
        )
        session.add(running_job)

        # 2. Succeeded job with recorded attempt execution duration
        succeeded_job = Job(
            id=uuid.uuid4(),
            job_type=unique_type,
            status=JobStatus.SUCCEEDED,
            priority=5,
            payload={},
        )
        session.add(succeeded_job)

        attempt = JobAttempt(
            id=uuid.uuid4(),
            job_id=succeeded_job.id,
            attempt_number=1,
            worker_id="remote-worker-xyz",
            status=AttemptStatus.SUCCEEDED,
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            execution_duration_ms=175.5,
        )
        session.add(attempt)

        # 3. Retried job
        retried_job = Job(
            id=uuid.uuid4(),
            job_type=unique_type,
            status=JobStatus.QUEUED,
            priority=3,
            payload={},
            retry_count=2,
        )
        session.add(retried_job)

        await session.commit()

    # Scrape /metrics endpoint
    res = await async_client.get("/metrics")
    assert res.status_code == 200
    metrics_text = res.text

    # Verify status gauge
    assert 'task_engine_jobs_by_status{status="RUNNING"}' in metrics_text
    assert 'task_engine_jobs_by_status{status="SUCCEEDED"}' in metrics_text

    # Verify running jobs gauge reflects the running job
    assert "task_engine_running_jobs" in metrics_text

    # Verify completed counter reflects the succeeded job from DB
    assert f'task_engine_jobs_completed_total{{job_type="{unique_type}",status="SUCCEEDED"}}' in metrics_text

    # Verify retried counter reflects retry_count from DB
    assert f'task_engine_jobs_retried_total{{job_type="{unique_type}"}}' in metrics_text

    # Verify execution latency histogram observed the attempt duration
    assert f'task_engine_job_execution_duration_seconds_count{{job_type="{unique_type}"}}' in metrics_text

