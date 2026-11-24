import asyncio
import uuid
import pytest
from httpx import AsyncClient

from app.scheduler.retry import calculate_backoff_delay
from app.worker.main import WorkerProcess
from app.core.database import get_session_factory
from app.services.job_service import JobService
from app.scheduler.priority_queue import PriorityQueue
from app.core.redis import get_redis


def test_calculate_backoff_delay():
    # Deterministic checks without jitter
    d0 = calculate_backoff_delay(0, base_seconds=1.0, max_seconds=10.0, jitter=False)
    d1 = calculate_backoff_delay(1, base_seconds=1.0, max_seconds=10.0, jitter=False)
    d2 = calculate_backoff_delay(2, base_seconds=1.0, max_seconds=10.0, jitter=False)
    d5 = calculate_backoff_delay(5, base_seconds=1.0, max_seconds=10.0, jitter=False)

    assert d0 == 1.0
    assert d1 == 2.0
    assert d2 == 4.0
    assert d5 == 10.0  # capped at max_seconds

    # With jitter: delay must be within [raw_delay/2, raw_delay]
    for count in range(4):
        raw = min(60.0, 1.0 * (2 ** count))
        jittered = calculate_backoff_delay(count, base_seconds=1.0, max_seconds=60.0, jitter=True)
        assert (raw / 2.0) <= jittered <= raw


@pytest.mark.asyncio
async def test_job_retries_with_delayed_queue_and_dlq(async_client: AsyncClient):
    """Verifies that a failing job:
    1. Retries up to max_retries with backoff.
    2. Moves through the delayed queue.
    3. Moves to Dead-Letter Queue (DEAD) after exhaustion.
    """
    res = await async_client.post("/jobs", json={
        "job_type": "failing_task",
        "payload": {"error_message": "Transient database network partition"},
        "max_retries": 2,
        "priority": 7,
    })
    assert res.status_code == 201
    job_id = uuid.UUID(res.json()["id"])

    worker = WorkerProcess()
    session_factory = get_session_factory()
    redis = await get_redis()
    pq = PriorityQueue(redis)

    # --- Attempt 1 ---
    processed_1 = await worker.process_next_job()
    assert processed_1 is False  # Task failed

    # Inspect DB after Attempt 1: retry_count=1, status=QUEUED, 1 attempt recorded
    async with session_factory() as session:
        job_1 = await JobService.get_job_by_id(session, job_id)
        assert job_1.status.value == "QUEUED"
        assert job_1.retry_count == 1
        assert len(job_1.attempts) == 1
        assert job_1.attempts[0].status.value == "FAILED"

    # Job is currently in delayed queue
    assert await pq.get_delayed_count() == 1
    assert await pq.get_queue_depth() == 0

    # Force job to be ready by updating score in delayed queue
    await redis.zadd("task_engine:queue:delayed", {f"{job_id}:7": 0.0})

    # Promote delayed job
    promoted = await pq.promote_delayed_jobs()
    assert promoted == 1
    assert await pq.get_queue_depth() == 1

    # --- Attempt 2 ---
    processed_2 = await worker.process_next_job()
    assert processed_2 is False

    # Inspect DB after Attempt 2: retry_count=2, status=QUEUED, 2 attempts
    async with session_factory() as session:
        job_2 = await JobService.get_job_by_id(session, job_id)
        assert job_2.status.value == "QUEUED"
        assert job_2.retry_count == 2
        assert len(job_2.attempts) == 2

    # Fast forward delayed queue for final attempt
    await redis.zadd("task_engine:queue:delayed", {f"{job_id}:7": 0.0})
    await pq.promote_delayed_jobs()

    # --- Attempt 3 (Exhaustion -> DLQ) ---
    processed_3 = await worker.process_next_job()
    assert processed_3 is False

    # Inspect DB after Exhaustion: status=DEAD, 3 attempts recorded
    async with session_factory() as session:
        job_final = await JobService.get_job_by_id(session, job_id)
        assert job_final.status.value == "DEAD"
        assert job_final.retry_count == 2
        assert len(job_final.attempts) == 3
        assert job_final.finished_at is not None

    # Verify present in DLQ
    assert await pq.get_dlq_count() >= 1
    assert await pq.get_queue_depth() == 0
    assert await pq.get_delayed_count() == 0
