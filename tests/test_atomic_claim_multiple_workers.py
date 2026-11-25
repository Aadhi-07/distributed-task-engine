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


@pytest.mark.asyncio
async def test_worker_concurrency_is_strictly_respected(async_client: AsyncClient):
    """Verifies that a worker strictly respects its concurrency limit and never
    claims or runs more jobs than its configured concurrency setting.
    """
    total_jobs = 6
    job_ids = []

    # Submit 6 jobs that sleep for 0.05s
    for i in range(total_jobs):
        res = await async_client.post("/jobs", json={
            "job_type": "sleep",
            "payload": {"seconds": 0.05},
            "priority": 5,
        })
        assert res.status_code == 201
        job_ids.append(res.json()["id"])

    # Worker with concurrency limit of 2
    worker = WorkerProcess(worker_id="strict-concurrency-worker", concurrency=2)
    await worker.register()

    max_running_observed = 0

    # Poller loop that tracks running count
    async def run_worker_task():
        nonlocal max_running_observed
        handled = 0
        while True:
            max_running_observed = max(max_running_observed, worker._running_jobs_count)
            # Ensure at no point running count exceeds concurrency
            assert worker._running_jobs_count <= worker.concurrency
            did_work = await worker.process_next_job()
            if did_work:
                handled += 1
            else:
                # If queues are empty and no jobs running, finish
                if worker._running_jobs_count == 0:
                    break
                await asyncio.sleep(0.01)
        return handled

    # Fire 5 concurrent polling loops competing on the same worker
    results = await asyncio.gather(
        run_worker_task(),
        run_worker_task(),
        run_worker_task(),
        run_worker_task(),
        run_worker_task(),
    )

    assert sum(results) == total_jobs
    assert max_running_observed <= 2

    # Verify all jobs succeeded in database
    session_factory = get_session_factory()
    async with session_factory() as session:
        for jid in job_ids:
            job = await JobService.get_job_by_id(session, uuid.UUID(jid))
            assert job is not None
            assert job.status.value == "SUCCEEDED"
            assert len(job.attempts) == 1

    # Verify processing hash is empty
    redis = await get_redis()
    pq = PriorityQueue(redis)
    assert await pq.get_processing_count() == 0


@pytest.mark.asyncio
async def test_load_balancer_prevents_worker_starvation_and_tracks_inflight():
    """Verifies that LoadBalancer distributes jobs across multiple workers,
    avoids starvation among tied workers, and properly registers in-flight jobs.
    """
    from app.scheduler.load_balancer import LoadBalancer
    redis = await get_redis()
    lb = LoadBalancer(redis)

    await redis.delete("task_engine:workers:load")

    # Register 3 workers with identical initial load (idle)
    w_ids = ["worker-node-1", "worker-node-2", "worker-node-3"]
    for wid in w_ids:
        await lb.update_worker_load(
            worker_id=wid,
            cpu_percent=10.0,
            memory_percent=20.0,
            running_jobs=0,
            concurrency=4,
        )

    # Successive calls to get_least_loaded_worker + assign_to_worker
    # should distribute work without starving any single worker
    assigned_counts = {wid: 0 for wid in w_ids}
    for i in range(9):
        selected_worker, score = await lb.get_least_loaded_worker()
        assert selected_worker in w_ids
        assigned_counts[selected_worker] += 1
        await lb.assign_to_worker(selected_worker, f"job-{i}")

    # Every worker must have received at least 1 job (no starvation)
    for wid in w_ids:
        assert assigned_counts[wid] > 0

    # Pop from private queue and verify atomic placement in task_engine:processing
    test_wid = w_ids[0]
    popped_job = await lb.pop_from_private_queue(test_wid)
    assert popped_job is not None

    in_flight = await redis.hget("task_engine:processing", popped_job)
    assert in_flight is not None
    assert in_flight.startswith(test_wid)

    # Clean up processing entry
    pq = PriorityQueue(redis)
    await pq.complete_processing(popped_job)
    assert await redis.hexists("task_engine:processing", popped_job) == 0
