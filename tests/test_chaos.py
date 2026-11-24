import asyncio
from datetime import datetime, timezone, timedelta
import uuid
import pytest
from sqlalchemy import select
from redis.asyncio import Redis, ConnectionError

from app.core.database import get_session_factory
from app.core.redis import get_redis
from app.models.job import Job, JobStatus, JobAttempt, AttemptStatus, utcnow
from app.models.worker import WorkerNode, WorkerStatus
from app.schemas.job import JobCreate
from app.services.job_service import JobService
from app.scheduler.priority_queue import PriorityQueue
from app.scheduler.heartbeat import HeartbeatManager
from app.worker.main import WorkerProcess


@pytest.mark.asyncio
async def test_chaos_kill_worker_mid_job_and_requeue():
    """Chaos Test: Abruptly kills a worker mid-execution and verifies recovery.
    
    Verifies that:
    1. Dead worker detection marks node DEAD.
    2. Stranded in-flight job is recovered with crash attempt recorded.
    3. Job is requeued into Redis priority queue (at-least-once guarantee).
    4. A new worker picks up the requeued job and completes it successfully.
    5. Zero job loss occurs.
    """
    session_factory = get_session_factory()
    redis = await get_redis()
    pq = PriorityQueue(redis)

    # 1. Submit a job with 2 retries
    async with session_factory() as session:
        job_in = JobCreate(
            job_type="sleep",
            payload={"duration": 5.0},
            priority=8,
            max_retries=2,
        )
        job, _ = await JobService.submit_job(session, redis, job_in)
        job_id = job.id

    # 2. Worker 1 registers and claims job
    worker_1_id = "crashed-worker-node-1"
    w1 = WorkerProcess(worker_id=worker_1_id)
    await w1.register()

    claimed_job_str = await pq.atomic_claim(worker_id=worker_1_id)
    assert claimed_job_str == str(job_id)

    # Worker 1 transitions job to RUNNING in database
    async with session_factory() as session:
        j = await JobService.get_job_by_id(session, job_id)
        j.status = JobStatus.RUNNING
        j.worker_id = worker_1_id
        j.started_at = utcnow()
        await session.commit()

    # 3. CHAOS EVENT: Worker abruptly dies mid-job (misses heartbeats for 60s)
    past_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=60)
    async with session_factory() as session:
        stmt = select(WorkerNode).where(WorkerNode.id == worker_1_id)
        node = (await session.execute(stmt)).scalar_one()
        node.last_heartbeat_at = past_heartbeat
        await session.commit()

    # Update Redis heartbeat score to 60s in the past
    await redis.zadd("task_engine:workers:heartbeats", {worker_1_id: (datetime.now(timezone.utc) - timedelta(seconds=60)).timestamp()})

    # Verify the job is currently stranded in RUNNING status
    async with session_factory() as session:
        stranded_job = await JobService.get_job_by_id(session, job_id)
        assert stranded_job.status == JobStatus.RUNNING

    # 4. Trigger Dead Worker Detection
    recovery_report = await HeartbeatManager.detect_and_requeue_dead_workers(
        redis=redis,
        session_factory=session_factory,
        dead_threshold_seconds=15.0,
    )

    assert worker_1_id in recovery_report["dead_workers"]
    assert str(job_id) in recovery_report["requeued_jobs"]

    # 5. Verify Postgres state: job is back to QUEUED, retry_count=1, crash attempt logged
    async with session_factory() as session:
        requeued_job = await JobService.get_job_by_id(session, job_id)
        assert requeued_job.status == JobStatus.QUEUED
        assert requeued_job.worker_id is None
        assert requeued_job.retry_count == 1
        assert len(requeued_job.attempts) == 1
        assert requeued_job.attempts[0].status == AttemptStatus.FAILED
        assert "died or missed heartbeats" in requeued_job.attempts[0].error

        # Worker 1 node is marked DEAD
        node_res = await session.execute(select(WorkerNode).where(WorkerNode.id == worker_1_id))
        dead_node = node_res.scalar_one()
        assert dead_node.status == WorkerStatus.DEAD

    # 6. Start Worker 2: it claims and executes the recovered job
    worker_2 = WorkerProcess(worker_id="survivor-worker-node-2")
    await worker_2.register()
    processed = await worker_2.process_next_job(timeout=3)
    assert processed is True

    # 7. Verify zero job loss: Job reached SUCCEEDED with 2 total attempts
    async with session_factory() as session:
        final_job = await JobService.get_job_by_id(session, job_id)
        assert final_job.status == JobStatus.SUCCEEDED
        assert len(final_job.attempts) == 2
        assert final_job.attempts[0].status == AttemptStatus.FAILED
        assert final_job.attempts[1].status == AttemptStatus.SUCCEEDED
        assert final_job.attempts[1].worker_id == "survivor-worker-node-2"


@pytest.mark.asyncio
async def test_transient_redis_disconnect_and_reconnect():
    """Chaos Test: Verifies system resilience during transient Redis connection errors."""
    session_factory = get_session_factory()
    redis = await get_redis()
    pq = PriorityQueue(redis)

    # 1. Normal submission
    async with session_factory() as session:
        job_in = JobCreate(
            job_type="sleep",
            payload={"duration": 0.01},
            priority=5,
            max_retries=1,
        )
        job, _ = await JobService.submit_job(session, redis, job_in)
        assert job.status == JobStatus.QUEUED

    # 2. Simulate worker encountering transient Redis error during claim
    class FaultyRedisWrapper:
        """Simulates intermittent network partitions."""
        def __init__(self, real_redis):
            self.real = real_redis
            self.fail = True

        def register_script(self, script):
            return self.real.register_script(script)

        async def zcard(self, *args, **kwargs):
            if self.fail:
                raise ConnectionError("Connection refused by synthetic network partition")
            return await self.real.zcard(*args, **kwargs)

    faulty_redis = FaultyRedisWrapper(redis)
    faulty_pq = PriorityQueue(faulty_redis)  # type: ignore

    with pytest.raises(ConnectionError):
        await faulty_pq.qsize()

    # 3. Connection self-heals
    faulty_redis.fail = False
    recovered_qsize = await faulty_pq.qsize()
    assert recovered_qsize == 1

    # 4. Worker executes the job without loss
    worker = WorkerProcess(worker_id="resilience-worker")
    processed = await worker.process_next_job(timeout=2)
    assert processed is True

    async with session_factory() as session:
        done_job = await JobService.get_job_by_id(session, job.id)
        assert done_job.status == JobStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_concurrent_dag_pipeline_integration():
    """Integration Test: Executes a multi-stage concurrent DAG with multiple workers."""
    session_factory = get_session_factory()
    redis = await get_redis()

    # Stage 1: Ingest (2 parallel jobs)
    async with session_factory() as session:
        j1_in = JobCreate(job_type="sleep", payload={"duration": 0.01}, priority=9)
        j1, _ = await JobService.submit_job(session, redis, j1_in)

        j2_in = JobCreate(job_type="sleep", payload={"duration": 0.01}, priority=9)
        j2, _ = await JobService.submit_job(session, redis, j2_in)

        # Stage 2: Transform (depends on both Stage 1 jobs)
        j3_in = JobCreate(
            job_type="sleep",
            payload={"duration": 0.01},
            priority=8,
            depends_on=[j1.id, j2.id],
        )
        j3, _ = await JobService.submit_job(session, redis, j3_in)

        # Stage 3: Train (depends on Transform)
        j4_in = JobCreate(
            job_type="model_training",
            payload={"epochs": 1, "learning_rate": 0.01},
            priority=7,
            depends_on=[j3.id],
        )
        j4, _ = await JobService.submit_job(session, redis, j4_in)

    # Launch two workers to process the workflow concurrently
    worker_a = WorkerProcess(worker_id="pipeline-worker-A")
    worker_b = WorkerProcess(worker_id="pipeline-worker-B")

    # Run jobs until all complete
    for _ in range(10):
        await worker_a.process_next_job(timeout=1)
        await worker_b.process_next_job(timeout=1)

    async with session_factory() as session:
        res1 = await JobService.get_job_by_id(session, j1.id)
        res2 = await JobService.get_job_by_id(session, j2.id)
        res3 = await JobService.get_job_by_id(session, j3.id)
        res4 = await JobService.get_job_by_id(session, j4.id)

        assert res1.status == JobStatus.SUCCEEDED
        assert res2.status == JobStatus.SUCCEEDED
        assert res3.status == JobStatus.SUCCEEDED
        assert res4.status == JobStatus.SUCCEEDED
        assert res4.result is not None
        assert "final_loss" in res4.result
