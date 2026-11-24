import uuid
import pytest
from httpx import AsyncClient
from redis.asyncio import Redis

from app.core.database import get_session_factory
from app.core.redis import get_redis
from app.models.job import Job, JobStatus, JobDependency
from app.schemas.job import JobCreate
from app.services.job_service import JobService
from app.scheduler.priority_queue import PriorityQueue
from app.scheduler.dag import find_cycle_in_graph, CycleDetectedError, DAGManager
from app.worker.main import WorkerProcess


def test_cycle_detection_algorithms():
    """Tests the DFS cycle detector with various graph topologies."""
    # 1. Acyclic graph
    acyclic = {
        "A": ["B", "C"],
        "B": ["D"],
        "C": ["D"],
        "D": [],
    }
    assert find_cycle_in_graph(acyclic) is None

    # 2. Simple cycle: A -> B -> A
    cycle_2 = {
        "A": ["B"],
        "B": ["A"],
    }
    cycle = find_cycle_in_graph(cycle_2)
    assert cycle is not None
    assert cycle[0] == cycle[-1]

    # 3. 3-node cycle: A -> B -> C -> A
    cycle_3 = {
        "A": ["B"],
        "B": ["C"],
        "C": ["A"],
    }
    cycle = find_cycle_in_graph(cycle_3)
    assert cycle is not None
    assert set(cycle) == {"A", "B", "C"}

    # 4. Self loop
    self_loop = {"A": ["A"]}
    assert find_cycle_in_graph(self_loop) is not None


@pytest.mark.asyncio
async def test_dag_submission_cycle_rejection(async_client: AsyncClient):
    """Verifies that batch DAG submission rejects circular dependencies with HTTP 400."""
    dag_payload = {
        "nodes": [
            {"node_id": "step_a", "job_type": "model_inference", "payload": {}, "depends_on": ["step_c"]},
            {"node_id": "step_b", "job_type": "model_inference", "payload": {}, "depends_on": ["step_a"]},
            {"node_id": "step_c", "job_type": "model_inference", "payload": {}, "depends_on": ["step_b"]},
        ]
    }
    response = await async_client.post("/jobs/dag", json=dag_payload)
    assert response.status_code == 400
    assert "Circular dependency error" in response.json()["detail"]


@pytest.mark.asyncio
async def test_dag_dependent_job_execution_order():
    """Verifies that child job stays PENDING until parent completes SUCCEEDED."""
    session_factory = get_session_factory()
    redis = await get_redis()
    worker = WorkerProcess(worker_id="test-dag-worker-1")

    # 1. Submit Parent Job
    async with session_factory() as session:
        parent_in = JobCreate(
            job_type="model_inference",
            payload={"model": "test", "inputs": [{"features": [1.0]}]},
            priority=5,
            max_retries=0,
        )
        parent_job, _ = await JobService.submit_job(session, redis, parent_in)
        parent_id = parent_job.id
        assert parent_job.status == JobStatus.QUEUED

    # 2. Submit Child Job depending on Parent
    async with session_factory() as session:
        child_in = JobCreate(
            job_type="model_inference",
            payload={"model": "test", "inputs": [{"features": [2.0]}]},
            priority=5,
            max_retries=0,
            depends_on=[parent_id],
        )
        child_job, _ = await JobService.submit_job(session, redis, child_in)
        child_id = child_job.id
        assert child_job.status == JobStatus.PENDING

    # 3. Child should NOT be in Redis queue yet
    pq = PriorityQueue(redis)
    queue_len = await pq.qsize()
    assert queue_len == 1  # Only parent in queue

    # 4. Worker executes parent job
    processed = await worker.process_next_job(timeout=2)
    assert processed is True

    # 5. Verify parent is SUCCEEDED and child is promoted to QUEUED
    async with session_factory() as session:
        refreshed_parent = await JobService.get_job_by_id(session, parent_id)
        assert refreshed_parent.status == JobStatus.SUCCEEDED

        refreshed_child = await JobService.get_job_by_id(session, child_id)
        assert refreshed_child.status == JobStatus.QUEUED

    # 6. Child is now in Redis queue
    queue_len_after = await pq.qsize()
    assert queue_len_after == 1  # Child is now in queue

    # 7. Worker executes child job
    processed_child = await worker.process_next_job(timeout=2)
    assert processed_child is True

    # 8. Verify child reached SUCCEEDED
    async with session_factory() as session:
        final_child = await JobService.get_job_by_id(session, child_id)
        assert final_child.status == JobStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_dag_fail_fast_on_parent_failure():
    """Verifies that child fails immediately if parent job fails, without executing."""
    session_factory = get_session_factory()
    redis = await get_redis()
    worker = WorkerProcess(worker_id="test-dag-worker-2")

    # 1. Submit Parent configured to fail
    async with session_factory() as session:
        parent_in = JobCreate(
            job_type="failing_task",
            payload={"error_message": "Upstream data corrupted"},
            priority=5,
            max_retries=0,
        )
        parent_job, _ = await JobService.submit_job(session, redis, parent_in)
        parent_id = parent_job.id
        assert parent_job.status == JobStatus.QUEUED

    # 2. Submit Child depending on failing parent
    async with session_factory() as session:
        child_in = JobCreate(
            job_type="model_inference",
            payload={"features": [1.0]},
            priority=5,
            max_retries=0,
            depends_on=[parent_id],
        )
        child_job, _ = await JobService.submit_job(session, redis, child_in)
        child_id = child_job.id
        assert child_job.status == JobStatus.PENDING

    # 3. Worker executes parent -> fails
    processed = await worker.process_next_job(timeout=2)
    assert processed is False  # execute_job returns False when job encounters exception

    # 4. Check parent is FAILED and child failed fast
    async with session_factory() as session:
        refreshed_parent = await JobService.get_job_by_id(session, parent_id)
        assert refreshed_parent.status == JobStatus.FAILED

        refreshed_child = await JobService.get_job_by_id(session, child_id)
        assert refreshed_child.status == JobStatus.FAILED
        assert "Dependency failure" in refreshed_child.error
        assert str(parent_id) in refreshed_child.error


@pytest.mark.asyncio
async def test_dag_diamond_dependency_resolution():
    """Verifies diamond dependency DAG:
        A (Root)
       / \\
      B   C
       \\ /
        D (Sink)
    D must wait until BOTH B and C succeed before becoming QUEUED.
    """
    session_factory = get_session_factory()
    redis = await get_redis()
    worker = WorkerProcess(worker_id="test-dag-worker-3")

    async with session_factory() as session:
        # Step 1: Submit A
        a_in = JobCreate(job_type="sleep", payload={"duration": 0.01}, max_retries=0)
        job_a, _ = await JobService.submit_job(session, redis, a_in)
        a_id = job_a.id

        # Step 2: Submit B and C depending on A
        b_in = JobCreate(job_type="sleep", payload={"duration": 0.01}, max_retries=0, depends_on=[a_id])
        job_b, _ = await JobService.submit_job(session, redis, b_in)
        b_id = job_b.id

        c_in = JobCreate(job_type="sleep", payload={"duration": 0.01}, max_retries=0, depends_on=[a_id])
        job_c, _ = await JobService.submit_job(session, redis, c_in)
        c_id = job_c.id

        # Step 3: Submit D depending on B and C
        d_in = JobCreate(job_type="sleep", payload={"duration": 0.01}, max_retries=0, depends_on=[b_id, c_id])
        job_d, _ = await JobService.submit_job(session, redis, d_in)
        d_id = job_d.id

    async with session_factory() as session:
        assert (await JobService.get_job_by_id(session, a_id)).status == JobStatus.QUEUED
        assert (await JobService.get_job_by_id(session, b_id)).status == JobStatus.PENDING
        assert (await JobService.get_job_by_id(session, c_id)).status == JobStatus.PENDING
        assert (await JobService.get_job_by_id(session, d_id)).status == JobStatus.PENDING

    # Execute A
    await worker.process_next_job(timeout=2)
    async with session_factory() as session:
        assert (await JobService.get_job_by_id(session, a_id)).status == JobStatus.SUCCEEDED
        # B and C should now both be QUEUED, D should still be PENDING
        assert (await JobService.get_job_by_id(session, b_id)).status == JobStatus.QUEUED
        assert (await JobService.get_job_by_id(session, c_id)).status == JobStatus.QUEUED
        assert (await JobService.get_job_by_id(session, d_id)).status == JobStatus.PENDING

    # Execute B
    await worker.process_next_job(timeout=2)
    async with session_factory() as session:
        assert (await JobService.get_job_by_id(session, b_id)).status == JobStatus.SUCCEEDED
        # D must STILL be PENDING because C has not finished
        assert (await JobService.get_job_by_id(session, d_id)).status == JobStatus.PENDING

    # Execute C
    await worker.process_next_job(timeout=2)
    async with session_factory() as session:
        assert (await JobService.get_job_by_id(session, c_id)).status == JobStatus.SUCCEEDED
        # Now BOTH parents finished -> D should be QUEUED!
        assert (await JobService.get_job_by_id(session, d_id)).status == JobStatus.QUEUED

    # Execute D
    await worker.process_next_job(timeout=2)
    async with session_factory() as session:
        assert (await JobService.get_job_by_id(session, d_id)).status == JobStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_dag_batch_submission_api(async_client: AsyncClient):
    """Verifies POST /jobs/dag creates valid dependencies and enqueues root nodes."""
    batch_req = {
        "nodes": [
            {"node_id": "prep", "job_type": "sleep", "payload": {"duration": 0.01}, "priority": 7},
            {"node_id": "train", "job_type": "model_training", "payload": {"epochs": 1}, "priority": 5, "depends_on": ["prep"]},
            {"node_id": "eval", "job_type": "model_inference", "payload": {"features": [1.0]}, "priority": 4, "depends_on": ["train"]},
        ]
    }
    res = await async_client.post("/jobs/dag", json=batch_req)
    assert res.status_code == 201
    data = res.json()

    assert "prep" in data["node_to_job_id"]
    assert "train" in data["node_to_job_id"]
    assert "eval" in data["node_to_job_id"]

    jobs = {j["job_type"]: j for j in data["jobs"]}
    assert jobs["sleep"]["status"] == "QUEUED"
    assert jobs["model_training"]["status"] == "PENDING"
    assert jobs["model_inference"]["status"] == "PENDING"
