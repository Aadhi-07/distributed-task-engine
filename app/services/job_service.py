import uuid
from typing import List, Optional, Tuple
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.models.job import Job, JobStatus, utcnow
from app.schemas.job import JobCreate
from app.core.logging import get_logger

logger = get_logger(__name__)

REDIS_QUEUE_KEY = "task_engine:queue:ready"


class JobService:
    """Manages job persistence, retrieval, and queueing lifecycle."""

    @staticmethod
    async def submit_job(
        session: AsyncSession,
        redis: Redis,
        job_in: JobCreate,
    ) -> Tuple[Job, bool]:
        """Submits a job. If idempotency_key is given and exists, returns existing job.
        
        Returns:
            (job, is_created) tuple.
        """
        if job_in.idempotency_key:
            stmt = (
                select(Job)
                .where(Job.idempotency_key == job_in.idempotency_key)
                .options(selectinload(Job.attempts))
            )
            res = await session.execute(stmt)
            existing_job = res.scalar_one_or_none()
            if existing_job:
                logger.info(
                    f"Idempotent submission hit: returning existing job [{existing_job.id}] "
                    f"for key '{job_in.idempotency_key}'"
                )
                return existing_job, False

        # Create new job
        job = Job(
            id=uuid.uuid4(),
            job_type=job_in.job_type,
            payload=job_in.payload,
            status=JobStatus.QUEUED,
            priority=job_in.priority,
            idempotency_key=job_in.idempotency_key,
            timeout_seconds=job_in.timeout_seconds,
            max_retries=job_in.max_retries,
            retry_count=0,
            created_at=utcnow(),
        )
        session.add(job)
        await session.commit()
        await session.refresh(job)

        # Enqueue job ID into Redis
        await redis.rpush(REDIS_QUEUE_KEY, str(job.id))
        logger.info(f"Job [{job.id}] (type: {job.job_type}, priority: {job.priority}) queued in Redis.")

        # Re-fetch with eager loaded attempts
        return await JobService.get_job_by_id(session, job.id), True

    @staticmethod
    async def get_job_by_id(session: AsyncSession, job_id: uuid.UUID) -> Optional[Job]:
        """Retrieves a job by its UUID, including its attempts."""
        stmt = (
            select(Job)
            .where(Job.id == job_id)
            .options(selectinload(Job.attempts))
        )
        res = await session.execute(stmt)
        return res.scalar_one_or_none()

    @staticmethod
    async def list_jobs(
        session: AsyncSession,
        status: Optional[JobStatus] = None,
        job_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[Job], int]:
        """Queries jobs with optional filtering, pagination, and total count."""
        base_query = select(Job)
        count_query = select(func.count(Job.id))

        if status:
            base_query = base_query.where(Job.status == status)
            count_query = count_query.where(Job.status == status)
        if job_type:
            base_query = base_query.where(Job.job_type == job_type)
            count_query = count_query.where(Job.job_type == job_type)

        # Total count
        total_res = await session.execute(count_query)
        total = total_res.scalar_one()

        # Paginated items
        items_query = (
            base_query
            .order_by(Job.created_at.desc())
            .offset(offset)
            .limit(limit)
            .options(selectinload(Job.attempts))
        )
        items_res = await session.execute(items_query)
        items = list(items_res.scalars().all())

        return items, total
