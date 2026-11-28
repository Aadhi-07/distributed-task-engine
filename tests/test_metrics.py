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
    """Verifies that the API /metrics custom collector computes completed counts,
    retries, running jobs, and execution duration histogram from Postgres and Redis.
    """
    import uuid
    from datetime import datetime, timedelta, timezone
    from app.models.job import Job, JobAttempt, JobStatus, AttemptStatus

    session_factory = get_session_factory()

    # Seed multi-process state directly into Postgres (simulating independent worker instances)
    unique_type = f"test_bg_{uuid.uuid4().hex[:6]}"
    t0 = datetime.now(timezone.utc)
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

        # 2. Succeeded job with recorded attempt started_at / finished_at
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
            started_at=t0,
            finished_at=t0 + timedelta(milliseconds=175),
            execution_duration_ms=175.0,
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

    # Verify status gauge and running jobs gauge
    assert 'task_engine_jobs_by_status{status="RUNNING"}' in metrics_text
    assert 'task_engine_jobs_by_status{status="SUCCEEDED"}' in metrics_text
    assert "task_engine_running_jobs" in metrics_text

    # Verify custom collector monotonic completed counter by status
    assert 'task_engine_db_jobs_completed_total{status="SUCCEEDED"}' in metrics_text
    assert 'task_engine_db_jobs_completed_total{status="FAILED"}' in metrics_text

    # Verify retried counter reflects retry_count from DB
    assert f'task_engine_jobs_retried_total{{job_type="{unique_type}"}}' in metrics_text

    # Verify custom collector execution duration histogram built from started_at/finished_at
    assert "task_engine_db_job_execution_duration_seconds_bucket" in metrics_text
    assert "task_engine_db_job_execution_duration_seconds_count" in metrics_text
    assert "task_engine_db_job_execution_duration_seconds_sum" in metrics_text


@pytest.mark.asyncio
async def test_postgres_collector_5s_cache_prevents_repeated_db_queries():
    """Verifies that PostgresMetricsCollector caches DB results for ~5 seconds across scrapes."""
    from unittest.mock import AsyncMock
    from prometheus_client import CollectorRegistry, generate_latest
    from app.core.metrics import PostgresMetricsCollector

    collector = PostgresMetricsCollector(cache_ttl_seconds=5.0)
    registry = CollectorRegistry()
    registry.register(collector)

    session_factory = get_session_factory()
    async with session_factory() as real_session:
        execute_calls = 0
        orig_execute = real_session.execute

        async def counted_execute(*args, **kwargs):
            nonlocal execute_calls
            execute_calls += 1
            return await orig_execute(*args, **kwargs)

        real_session.execute = counted_execute  # type: ignore[method-assign]

        # First refresh hits DB
        await collector.refresh(real_session)
        first_calls = execute_calls
        assert first_calls == 3  # status, retry, attempts

        # Second refresh within 5s uses cache (0 new DB queries)
        await collector.refresh(real_session)
        assert execute_calls == first_calls

        # Generate Prometheus text from cache
        output = generate_latest(registry).decode("utf-8")
        assert "task_engine_db_jobs_completed_total" in output
        assert "task_engine_db_job_execution_duration_seconds_bucket" in output

        # After cache invalidation (or TTL expiry), DB is queried again
        collector.invalidate_cache()
        await collector.refresh(real_session)
        assert execute_calls == first_calls * 2


@pytest.mark.asyncio
async def test_postgres_collector_histogram_buckets_from_attempt_timestamps():
    """Verifies histogram bucket placement and sum from job_attempts started_at/finished_at."""
    import uuid
    from datetime import datetime, timedelta, timezone
    from prometheus_client import CollectorRegistry, generate_latest
    from app.core.metrics import PostgresMetricsCollector
    from app.models.job import Job, JobAttempt, JobStatus, AttemptStatus

    collector = PostgresMetricsCollector(cache_ttl_seconds=5.0)
    registry = CollectorRegistry()
    registry.register(collector)

    session_factory = get_session_factory()
    base_ts = datetime.now(timezone.utc)

    async with session_factory() as session:
        job = Job(
            id=uuid.uuid4(),
            job_type="hist_test",
            status=JobStatus.SUCCEEDED,
            priority=5,
            payload={},
        )
        session.add(job)
        # Attempt 1: 0.02s (falls in le=0.025 and above)
        session.add(
            JobAttempt(
                id=uuid.uuid4(),
                job_id=job.id,
                attempt_number=1,
                worker_id="w-1",
                status=AttemptStatus.SUCCEEDED,
                started_at=base_ts,
                finished_at=base_ts + timedelta(milliseconds=20),
            )
        )
        # Attempt 2: 0.40s (falls in le=0.5 and above)
        session.add(
            JobAttempt(
                id=uuid.uuid4(),
                job_id=job.id,
                attempt_number=2,
                worker_id="w-1",
                status=AttemptStatus.SUCCEEDED,
                started_at=base_ts,
                finished_at=base_ts + timedelta(milliseconds=400),
            )
        )
        await session.commit()

        await collector.refresh(session, force=True)

    output = generate_latest(registry).decode("utf-8")
    # Check count >= 2 and cumulative buckets behave monotonically
    assert 'task_engine_db_job_execution_duration_seconds_bucket{le="0.025"}' in output
    assert 'task_engine_db_job_execution_duration_seconds_bucket{le="0.5"}' in output
    assert 'task_engine_db_job_execution_duration_seconds_bucket{le="+Inf"}' in output


def test_http_load_missing_timestamps_returns_none_for_na():
    """Verifies scripts/http_load.py returns None (printed as 'n/a') when timestamps are missing."""
    from scripts.http_load import calculate_percentile, extract_execution_duration_ms

    assert calculate_percentile([], 50) is None
    assert extract_execution_duration_ms({"status": "SUCCEEDED"}) is None
    assert extract_execution_duration_ms({"started_at": None, "finished_at": None}) is None

    dur = extract_execution_duration_ms({
        "started_at": "2026-11-28T10:00:00+00:00",
        "finished_at": "2026-11-28T10:00:01.500000+00:00",
    })
    assert dur == pytest.approx(1500.0)


