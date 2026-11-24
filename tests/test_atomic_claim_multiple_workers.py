import asyncio
import uuid
import pytest
from httpx import AsyncClient

from app.worker.main import WorkerProcess
from app.core.database import get_session_factory
from app.services.job_service import JobService
from app.scheduler.priority_queue import PriorityQueue
from app.core.redis import get_redis


@pytest.mark.asyncio
async def test_multiple_workers_atomic_claim_no_duplicates(async_client: AsyncClient):
    """Simulates 3 distinct workers concurrently competing for 15 queued jobs.
    
    Verifies:
    1. Every job is executed exactly once (no duplicate execution).
    2. All jobs transition to SUCCEEDED.
    3. The in-flight processing hash is emptied cleanly.
    4. Multiple workers share execution.
    """
    total_jobs = 15
    job_ids = []

    # Submit 15 jobs with varying priorities
    for i in range(total_jobs):
        res = await async_client.post("/jobs", json={
            "job_type": "sleep",
            "payload": {"seconds": 0.02},
            "priority": i % 10,
        })
        assert res.status_code == 201
        job_ids.append(res.json()["id"])

    # Initialize 3 workers with unique IDs
    w1 = WorkerProcess(worker_id="worker-node-alpha", concurrency=2)
    w2 = WorkerProcess(worker_id="worker-node-beta", concurrency=2)
    w3 = WorkerProcess(worker_id="worker-node-gamma", concurrency=2)

    await w1.register()
    await w2.register()
    await w3.register()

    # Let workers process jobs concurrently until queue is empty
    async def worker_drain_loop(worker: WorkerProcess):
        jobs_handled = 0
        while True:
            did_work = await worker.process_next_job()
            if did_work:
                jobs_handled += 1
            else:
                break
        return jobs_handled

    results = await asyncio.gather(
        worker_drain_loop(w1),
        worker_drain_loop(w2),
        worker_drain_loop(w3),
    )

    # Total handled across all workers must equal total submitted
    assert sum(results) == total_jobs

    # Verify every job in Postgres: exactly 1 attempt, status SUCCEEDED
    session_factory = get_session_factory()
    workers_involved = set()
    async with session_factory() as session:
        for jid in job_ids:
            job = await JobService.get_job_by_id(session, uuid.UUID(jid))
            assert job is not None
            assert job.status.value == "SUCCEEDED"
            assert len(job.attempts) == 1
            assert job.attempts[0].status.value == "SUCCEEDED"
            workers_involved.add(job.worker_id)

    # Ensure multiple workers actively participated
    assert len(workers_involved) > 1

    # Verify Redis processing hash is empty
    redis = await get_redis()
    pq = PriorityQueue(redis)
    assert await pq.get_queue_depth() == 0
    assert await pq.get_processing_count() == 0
