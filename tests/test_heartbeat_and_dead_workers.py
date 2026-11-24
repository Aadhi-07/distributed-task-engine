import asyncio
from datetime import datetime, timezone, timedelta
import uuid
import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.worker.main import WorkerProcess
from app.core.database import get_session_factory
from app.core.redis import get_redis
from app.models.job import Job, JobAttempt, JobStatus, AttemptStatus, utcnow
from app.models.worker import WorkerNode, WorkerStatus
from app.scheduler.heartbeat import HeartbeatManager
from app.scheduler.priority_queue import PriorityQueue
from app.services.job_service import JobService


@pytest.mark.asyncio
async def test_worker_registration_and_heartbeat_api(async_client: AsyncClient):
    """Verifies that an active worker emits heartbeats and appears as alive in GET /workers."""
    worker = WorkerProcess(worker_id="worker-test-telemetry-1", concurrency=4)
    await worker.register()
    await worker.send_heartbeat()

    res = await async_client.get("/workers")
    assert res.status_code == 200
    data = res.json()

    assert data["total"] >= 1
    assert data["active_workers"] >= 1

    matched = [w for w in data["items"] if w["id"] == "worker-test-telemetry-1"]
    assert len(matched) == 1
    w_data = matched[0]
    assert w_data["status"] == "HEALTHY"
    assert w_data["is_alive"] is True
    assert w_data["concurrency"] == 4
    assert w_data["cpu_percent"] >= 0.0
    assert w_data["memory_percent"] >= 0.0


@pytest.mark.asyncio
async def test_dead_worker_detection_and_job_requeue(async_client: AsyncClient):
    """Simulates a worker dying mid-job and verifies:
    1. Dead worker detection marks the node DEAD.
    2. In-flight job is recovered and requeued (at-least-once).
    3. Another worker picks it up and successfully completes it.
    """
    session_factory = get_session_factory()
    redis = await get_redis()
    pq = PriorityQueue(redis)

    # 1. Submit a job with max_retries=2
    res = await async_client.post("/jobs", json={
        "job_type": "sleep",
        "payload": {"seconds": 0.05},
        "max_retries": 2,
        "priority": 8,
    })
    job_id = uuid.UUID(res.json()["id"])

    # 2. Worker 1 claims job atomically from queue and transitions to RUNNING
    worker_1_id = "worker-crashed-node-x"
    w1 = WorkerProcess(worker_id=worker_1_id)
    await w1.register()

    claimed = await pq.atomic_claim(worker_id=worker_1_id)
    assert claimed == str(job_id)

    async with session_factory() as session:
        job = await JobService.get_job_by_id(session, job_id)
        job.status = JobStatus.RUNNING
        job.worker_id = worker_1_id
        job.started_at = utcnow()
        await session.commit()

    # 3. Simulate Worker 1 dying (last heartbeat 60 seconds ago)
    past_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=60)
    async with session_factory() as session:
        stmt = select(WorkerNode).where(WorkerNode.id == worker_1_id)
        node = (await session.execute(stmt)).scalar_one()
        node.last_heartbeat_at = past_heartbeat
        await session.commit()

    # 4. Run Dead Worker Detector
    recovery_report = await HeartbeatManager.detect_and_requeue_dead_workers(
        redis=redis,
        session_factory=session_factory,
        dead_threshold_seconds=15.0,
    )

    assert worker_1_id in recovery_report["dead_workers"]
    assert str(job_id) in recovery_report["requeued_jobs"]

    # 5. Verify Postgres state: job is back to QUEUED, retry_count=1, failure attempt logged
    async with session_factory() as session:
        recovered_job = await JobService.get_job_by_id(session, job_id)
        assert recovered_job.status.value == "QUEUED"
        assert recovered_job.worker_id is None
        assert recovered_job.retry_count == 1
        assert len(recovered_job.attempts) == 1
        assert recovered_job.attempts[0].status.value == "FAILED"
        assert "died or missed heartbeats" in recovered_job.attempts[0].error

        # Worker 1 node is marked DEAD
        node_res = await session.execute(select(WorkerNode).where(WorkerNode.id == worker_1_id))
        dead_node = node_res.scalar_one()
        assert dead_node.status.value == "DEAD"

    # 6. Worker 2 boots up and executes the requeued job
    worker_2 = WorkerProcess(worker_id="worker-healthy-node-y")
    await worker_2.register()
    executed = await worker_2.process_next_job()
    assert executed is True

    # 7. Final check: Job has SUCCEEDED with 2 attempts
    async with session_factory() as session:
        final_job = await JobService.get_job_by_id(session, job_id)
        assert final_job.status.value == "SUCCEEDED"
        assert final_job.worker_id == "worker-healthy-node-y"
        assert len(final_job.attempts) == 2
        assert final_job.attempts[1].status.value == "SUCCEEDED"


@pytest.mark.asyncio
async def test_dead_worker_requeue_exhaustion_moves_to_dlq():
    """Verifies that if a stranded job has already exhausted retries, it moves to DEAD/DLQ."""
    session_factory = get_session_factory()
    redis = await get_redis()
    pq = PriorityQueue(redis)

    worker_id = f"worker-crash-{uuid.uuid4().hex[:6]}"
    job_id = uuid.uuid4()

    # Create job with retry_count=3, max_retries=3 in RUNNING state
    async with session_factory() as session:
        w_node = WorkerNode(
            id=worker_id,
            hostname="host",
            pid=123,
            status=WorkerStatus.HEALTHY,
            concurrency=2,
            last_heartbeat_at=datetime.now(timezone.utc) - timedelta(seconds=60),
            registered_at=utcnow(),
        )
        session.add(w_node)

        job = Job(
            id=job_id,
            job_type="sleep",
            payload={"seconds": 0.1},
            status=JobStatus.RUNNING,
            worker_id=worker_id,
            retry_count=3,
            max_retries=3,
            priority=5,
            created_at=utcnow(),
        )
        session.add(job)
        await session.commit()

    # Run detection
    report = await HeartbeatManager.detect_and_requeue_dead_workers(
        redis=redis,
        session_factory=session_factory,
        dead_threshold_seconds=15.0,
    )

    assert str(job_id) in report["dlq_jobs"]

    async with session_factory() as session:
        res = await JobService.get_job_by_id(session, job_id)
        assert res.status.value == "DEAD"
        assert res.finished_at is not None
