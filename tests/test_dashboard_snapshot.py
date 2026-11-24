from datetime import datetime, timezone, timedelta
import uuid
import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.core.database import get_session_factory
from app.core.redis import get_redis
from app.models.job import Job, JobStatus
from app.models.worker import WorkerNode, WorkerStatus
from app.schemas.job import JobCreate
from app.services.job_service import JobService
from app.worker.main import WorkerProcess


@pytest.mark.asyncio
async def test_empty_dashboard_snapshot(async_client: AsyncClient):
    """Verifies that the dashboard snapshot endpoint returns a valid schema and structure."""
    res = await async_client.get("/api/v1/dashboard/snapshot")
    assert res.status_code == 200
    data = res.json()

    assert data["hub"]["id"] == "hub"
    assert isinstance(data["hub"]["queue_depth"], int)
    assert isinstance(data["workers"], list)
    assert isinstance(data["jobs"], list)
    assert isinstance(data["edges"], list)
    assert isinstance(data["events"], list)
    assert isinstance(data["status_counts"], dict)
    assert "QUEUED" in data["status_counts"]
    assert "RUNNING" in data["status_counts"]
    assert "total_jobs" in data["stats"]
    assert "active_workers" in data["stats"]
    assert "etag" in data
    assert res.headers.get("ETag") is not None


@pytest.mark.asyncio
async def test_dashboard_snapshot_with_workers_and_jobs(async_client: AsyncClient):
    """Verifies topology mapping for active workers, queued jobs, and queue edges."""
    session_factory = get_session_factory()
    redis = await get_redis()

    # 1. Register an active worker
    worker = WorkerProcess(worker_id="dash-worker-test-1", concurrency=4)
    await worker.register()
    await worker.send_heartbeat()

    # 2. Submit a queued job
    async with session_factory() as session:
        job_in = JobCreate(
            job_type="model_inference",
            payload={"features": [1.0, 2.0]},
            priority=7,
        )
        job, _ = await JobService.submit_job(session, redis, job_in)
        job_id = str(job.id)

    # 3. Query snapshot
    res = await async_client.get("/api/v1/dashboard/snapshot")
    assert res.status_code == 200
    data = res.json()

    assert data["stats"]["active_workers"] >= 1
    assert data["stats"]["queue_depth"] >= 1
    assert data["status_counts"]["QUEUED"] >= 1

    # Verify worker node
    workers = [w for w in data["workers"] if w["id"] == "dash-worker-test-1"]
    assert len(workers) == 1
    assert workers[0]["is_alive"] is True
    assert workers[0]["concurrency"] == 4

    # Verify job node
    jobs = [j for j in data["jobs"] if j["id"] == job_id]
    assert len(jobs) == 1
    assert jobs[0]["status"] == "QUEUED"
    assert jobs[0]["priority"] == 7

    # Verify queue edge (hub -> job)
    queue_edges = [e for e in data["edges"] if e["edge_type"] == "queue" and e["target"] == job_id]
    assert len(queue_edges) == 1
    assert queue_edges[0]["source"] == "hub"


@pytest.mark.asyncio
async def test_dashboard_snapshot_dag_with_failed_parent(async_client: AsyncClient):
    """Verifies that a DAG with a failed parent reflects blocked_failed status on dependency edges."""
    session_factory = get_session_factory()
    redis = await get_redis()
    worker = WorkerProcess(worker_id="dash-dag-worker-2")

    # 1. Submit parent configured to fail
    async with session_factory() as session:
        parent_in = JobCreate(
            job_type="failing_task",
            payload={"error_message": "Fault injection"},
            priority=6,
            max_retries=0,
        )
        p_job, _ = await JobService.submit_job(session, redis, parent_in)
        parent_id = str(p_job.id)

    # 2. Submit child depending on failing parent
    async with session_factory() as session:
        child_in = JobCreate(
            job_type="model_inference",
            payload={"features": [0.5]},
            priority=5,
            depends_on=[uuid.UUID(parent_id)],
        )
        c_job, _ = await JobService.submit_job(session, redis, child_in)
        child_id = str(c_job.id)

    # 3. Worker executes parent -> fails and cascades to child
    await worker.process_next_job(timeout=2)

    # 4. Query snapshot
    res = await async_client.get("/api/v1/dashboard/snapshot")
    assert res.status_code == 200
    data = res.json()

    # Find nodes
    p_node = next(j for j in data["jobs"] if j["id"] == parent_id)
    c_node = next(j for j in data["jobs"] if j["id"] == child_id)

    assert p_node["status"] == "FAILED"
    assert c_node["status"] == "FAILED"
    assert "Dependency failure" in c_node["error"]

    # Verify dependency edge has blocked_failed status
    dep_edge = next(
        e for e in data["edges"]
        if e["source"] == parent_id and e["target"] == child_id
    )
    assert dep_edge["edge_type"] == "dependency"
    assert dep_edge["status"] == "blocked_failed"


@pytest.mark.asyncio
async def test_dashboard_snapshot_dead_worker(async_client: AsyncClient):
    """Verifies that a dead/stale worker is accurately reported with is_alive=False."""
    session_factory = get_session_factory()

    dead_worker_id = "dash-dead-worker-node"
    worker = WorkerProcess(worker_id=dead_worker_id)
    await worker.register()

    # Artificially set last_heartbeat to 60s ago
    past = datetime.now(timezone.utc) - timedelta(seconds=60)
    async with session_factory() as session:
        stmt = select(WorkerNode).where(WorkerNode.id == dead_worker_id)
        node = (await session.execute(stmt)).scalar_one()
        node.last_heartbeat_at = past
        node.status = WorkerStatus.DEAD
        await session.commit()

    res = await async_client.get("/api/v1/dashboard/snapshot")
    assert res.status_code == 200
    data = res.json()

    w_node = next(w for w in data["workers"] if w["id"] == dead_worker_id)
    assert w_node["is_alive"] is False
    assert w_node["status"] == "DEAD"
    assert w_node["heartbeat_age_seconds"] >= 50.0


@pytest.mark.asyncio
async def test_dashboard_snapshot_etag_conditional_request(async_client: AsyncClient):
    """Verifies ETag generation and HTTP 304 Not Modified response."""
    # First request: 200 OK with ETag
    res1 = await async_client.get("/api/v1/dashboard/snapshot")
    assert res1.status_code == 200
    etag = res1.headers.get("ETag")
    assert etag is not None

    # Second request with If-None-Match: 304 Not Modified
    res2 = await async_client.get(
        "/api/v1/dashboard/snapshot",
        headers={"If-None-Match": etag},
    )
    assert res2.status_code == 304
    assert res2.headers.get("ETag") == etag
