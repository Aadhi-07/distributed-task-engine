import asyncio
import uuid
import pytest
from httpx import AsyncClient

from app.scheduler.priority_queue import compute_priority_score
from app.worker.main import WorkerProcess
from app.core.database import get_session_factory
from app.services.job_service import JobService


def test_compute_priority_score_ordering():
    """Validates that scores order properly: higher priority = lower score, FIFO for ties."""
    t0 = 1000.0
    t1 = 1001.0

    score_prio_9 = compute_priority_score(9, t0)
    score_prio_8 = compute_priority_score(8, t0)
    score_prio_0 = compute_priority_score(0, t0)

    # Higher priority must have lower score for ZPOPMIN
    assert score_prio_9 < score_prio_8 < score_prio_0

    # Within same priority, earlier timestamp must have lower score (FIFO)
    score_prio_7_early = compute_priority_score(7, t0)
    score_prio_7_late = compute_priority_score(7, t1)
    assert score_prio_7_early < score_prio_7_late


@pytest.mark.asyncio
async def test_priority_based_execution_order(async_client: AsyncClient):
    """Submits jobs with priorities 2, 9, 5 and verifies worker executes 9 first, then 5, then 2."""
    # Submit Low Priority (2)
    res_low = await async_client.post("/jobs", json={
        "job_type": "sleep",
        "payload": {"seconds": 0.01},
        "priority": 2,
    })
    low_id = res_low.json()["id"]

    # Submit High Priority (9)
    res_high = await async_client.post("/jobs", json={
        "job_type": "sleep",
        "payload": {"seconds": 0.01},
        "priority": 9,
    })
    high_id = res_high.json()["id"]

    # Submit Medium Priority (5)
    res_med = await async_client.post("/jobs", json={
        "job_type": "sleep",
        "payload": {"seconds": 0.01},
        "priority": 5,
    })
    med_id = res_med.json()["id"]

    execution_order = []
    worker = WorkerProcess()

    # Process next 3 jobs
    for _ in range(3):
        session_factory = get_session_factory()
        processed = await worker.process_next_job()
        assert processed is True

    # Check finished_at order in Postgres
    async with session_factory() as session:
        job_high = await JobService.get_job_by_id(session, uuid.UUID(high_id))
        job_med = await JobService.get_job_by_id(session, uuid.UUID(med_id))
        job_low = await JobService.get_job_by_id(session, uuid.UUID(low_id))

        assert job_high.status.value == "SUCCEEDED"
        assert job_med.status.value == "SUCCEEDED"
        assert job_low.status.value == "SUCCEEDED"

        # High priority must start before Medium, and Medium before Low
        assert job_high.started_at <= job_med.started_at <= job_low.started_at


@pytest.mark.asyncio
async def test_fifo_ordering_within_same_priority(async_client: AsyncClient):
    """Submits two jobs of equal priority; the first submitted must be executed first."""
    res_first = await async_client.post("/jobs", json={
        "job_type": "sleep",
        "payload": {"seconds": 0.01},
        "priority": 6,
    })
    first_id = res_first.json()["id"]

    # Small delay to guarantee timestamp difference
    await asyncio.sleep(0.02)

    res_second = await async_client.post("/jobs", json={
        "job_type": "sleep",
        "payload": {"seconds": 0.01},
        "priority": 6,
    })
    second_id = res_second.json()["id"]

    worker = WorkerProcess()
    await worker.process_next_job()
    await worker.process_next_job()

    session_factory = get_session_factory()
    async with session_factory() as session:
        job_1 = await JobService.get_job_by_id(session, uuid.UUID(first_id))
        job_2 = await JobService.get_job_by_id(session, uuid.UUID(second_id))

        assert job_1.started_at <= job_2.started_at
