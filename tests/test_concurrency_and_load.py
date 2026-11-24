import asyncio
import uuid
import pytest
from httpx import AsyncClient

from app.scheduler.load_balancer import LoadBalancer, compute_worker_load_score
from app.scheduler.rate_limiter import RateLimiter
from app.worker.main import WorkerProcess
from app.core.database import get_session_factory
from app.core.redis import get_redis
from app.services.job_service import JobService


def test_compute_worker_load_score():
    # Worker A: Idle node (0 jobs on concurrency 4, 10% CPU, 30% RAM)
    # Load = 0.5 * 0 + 0.3 * 10 + 0.2 * 30 = 0 + 3 + 6 = 9.0
    idle_score = compute_worker_load_score(cpu_percent=10.0, memory_percent=30.0, running_jobs=0, concurrency=4)
    assert idle_score == 9.0

    # Worker B: Saturated node (4 jobs on concurrency 4, 80% CPU, 70% RAM)
    # Load = 0.5 * 100 + 0.3 * 80 + 0.2 * 70 = 50 + 24 + 14 = 88.0
    busy_score = compute_worker_load_score(cpu_percent=80.0, memory_percent=70.0, running_jobs=4, concurrency=4)
    assert busy_score == 88.0

    assert idle_score < busy_score


@pytest.mark.asyncio
async def test_load_aware_assignment_prefers_least_loaded():
    """Verifies that the LoadBalancer identifies and directs jobs to the least-loaded worker."""
    redis = await get_redis()
    lb = LoadBalancer(redis)

    # Clean previous load scores
    await redis.delete("task_engine:workers:load")

    worker_idle_id = "worker-idle-node"
    worker_busy_id = "worker-busy-node"

    # Register Worker Idle (Load = 12.0)
    await lb.update_worker_load(
        worker_id=worker_idle_id,
        cpu_percent=10.0,
        memory_percent=35.0,
        running_jobs=0,
        concurrency=4,
    )

    # Register Worker Busy (Load = 85.0)
    await lb.update_worker_load(
        worker_id=worker_busy_id,
        cpu_percent=80.0,
        memory_percent=60.0,
        running_jobs=3,
        concurrency=4,
    )

    # Scheduler selects least loaded
    best_worker, score = await lb.get_least_loaded_worker()
    assert best_worker == worker_idle_id
    assert score < 20.0

    # Target assignment directly to Worker Idle
    test_job_id = str(uuid.uuid4())
    await lb.assign_to_worker(worker_idle_id, test_job_id)

    # Worker Idle checks private queue
    popped_job = await lb.pop_from_private_queue(worker_idle_id)
    assert popped_job == test_job_id


@pytest.mark.asyncio
async def test_global_concurrency_limit_per_job_type():
    """Tests that active concurrency limits per job type are enforced in Redis."""
    redis = await get_redis()
    rate_limiter = RateLimiter(redis)

    job_type = "custom_test_task"
    limit = 2

    # Clean keys
    await redis.delete(f"task_engine:concurrency:active:{job_type}")

    # Slot 1
    s1 = await rate_limiter.acquire_concurrency_slot(job_type, "job-1", custom_limit=limit)
    assert s1 is True
    assert await rate_limiter.get_active_concurrency(job_type) == 1

    # Slot 2
    s2 = await rate_limiter.acquire_concurrency_slot(job_type, "job-2", custom_limit=limit)
    assert s2 is True
    assert await rate_limiter.get_active_concurrency(job_type) == 2

    # Slot 3 (exceeds limit=2 -> rejected)
    s3 = await rate_limiter.acquire_concurrency_slot(job_type, "job-3", custom_limit=limit)
    assert s3 is False
    assert await rate_limiter.get_active_concurrency(job_type) == 2

    # Release slot 1 -> slot 3 can now acquire
    await rate_limiter.release_concurrency_slot(job_type, "job-1")
    assert await rate_limiter.get_active_concurrency(job_type) == 1

    s3_retry = await rate_limiter.acquire_concurrency_slot(job_type, "job-3", custom_limit=limit)
    assert s3_retry is True
    assert await rate_limiter.get_active_concurrency(job_type) == 2

    # Clean up
    await rate_limiter.release_concurrency_slot(job_type, "job-2")
    await rate_limiter.release_concurrency_slot(job_type, "job-3")


@pytest.mark.asyncio
async def test_sliding_window_rate_limiter():
    """Tests that the sliding window rate limiter prevents exceeding N executions per window."""
    redis = await get_redis()
    rate_limiter = RateLimiter(redis)

    job_type = "high_frequency_api"
    window = 1.0
    limit = 3

    await redis.delete(f"task_engine:ratelimit:window:{job_type}")

    # 3 calls within window must succeed
    assert await rate_limiter.check_rate_limit(job_type, limit=limit, window_seconds=window) is True
    assert await rate_limiter.check_rate_limit(job_type, limit=limit, window_seconds=window) is True
    assert await rate_limiter.check_rate_limit(job_type, limit=limit, window_seconds=window) is True

    # 4th call must be blocked
    assert await rate_limiter.check_rate_limit(job_type, limit=limit, window_seconds=window) is False

    # Wait for window to expire
    await asyncio.sleep(1.05)

    # Next call must succeed again
    assert await rate_limiter.check_rate_limit(job_type, limit=limit, window_seconds=window) is True
