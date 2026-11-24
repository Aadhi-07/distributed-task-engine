import uuid
import pytest
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.models import (
    Base,
    Job,
    JobAttempt,
    JobDependency,
    JobStatus,
    AttemptStatus,
    WorkerNode,
    WorkerStatus,
)


@pytest.mark.asyncio
async def test_job_and_attempt_model_relationships():
    # In-memory SQLite async engine to verify SQLAlchemy 2.0 mapping and constraints
    test_engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        # Create a Job
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            job_type="model_inference",
            payload={"batch_size": 32, "model": "resnet50"},
            status=JobStatus.PENDING,
            priority=8,
            idempotency_key="batch-12345",
        )
        session.add(job)
        await session.commit()

        # Query job
        stmt = select(Job).where(Job.id == job_id)
        result = await session.execute(stmt)
        saved_job = result.scalar_one()

        assert saved_job.job_type == "model_inference"
        assert saved_job.priority == 8
        assert saved_job.status == JobStatus.PENDING
        assert saved_job.max_retries == 3
        assert saved_job.timeout_seconds == 300

        # Add JobAttempt
        attempt = JobAttempt(
            id=uuid.uuid4(),
            job_id=saved_job.id,
            attempt_number=1,
            worker_id="worker-node-1",
            status=AttemptStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )
        session.add(attempt)
        await session.commit()

        # Check relationship with eager loading for async session
        stmt_with_attempts = (
            select(Job)
            .where(Job.id == job_id)
            .options(selectinload(Job.attempts))
        )
        res = await session.execute(stmt_with_attempts)
        job_with_attempts = res.scalar_one()
        assert len(job_with_attempts.attempts) == 1
        assert job_with_attempts.attempts[0].worker_id == "worker-node-1"

    await test_engine.dispose()


@pytest.mark.asyncio
async def test_job_dependency_dag_mapping():
    test_engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        parent_job = Job(
            id=uuid.uuid4(),
            job_type="data_prep",
            payload={"source": "s3://dataset"},
            status=JobStatus.SUCCEEDED,
        )
        child_job = Job(
            id=uuid.uuid4(),
            job_type="model_training",
            payload={"epochs": 10},
            status=JobStatus.PENDING,
        )
        session.add_all([parent_job, child_job])
        await session.commit()

        dependency = JobDependency(
            id=uuid.uuid4(),
            job_id=child_job.id,
            depends_on_job_id=parent_job.id,
        )
        session.add(dependency)
        await session.commit()

        # Query child dependencies
        stmt = select(JobDependency).where(JobDependency.job_id == child_job.id)
        dep_result = await session.execute(stmt)
        dep = dep_result.scalar_one()
        assert dep.depends_on_job_id == parent_job.id

    await test_engine.dispose()


@pytest.mark.asyncio
async def test_worker_node_model():
    test_engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        worker = WorkerNode(
            id="worker-host-1-pid-101",
            hostname="host-1",
            pid=101,
            status=WorkerStatus.HEALTHY,
            concurrency=8,
            running_jobs_count=2,
            cpu_percent=24.5,
            memory_percent=45.2,
            metadata_json={"region": "us-east-1", "gpu": False},
        )
        session.add(worker)
        await session.commit()

        stmt = select(WorkerNode).where(WorkerNode.id == "worker-host-1-pid-101")
        res = await session.execute(stmt)
        saved_worker = res.scalar_one()

        assert saved_worker.hostname == "host-1"
        assert saved_worker.status == WorkerStatus.HEALTHY
        assert saved_worker.concurrency == 8
        assert saved_worker.cpu_percent == 24.5
        assert saved_worker.metadata_json["gpu"] is False

    await test_engine.dispose()
