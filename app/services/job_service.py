import uuid
from typing import Dict, List, Optional, Tuple
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.models.job import Job, JobDependency, JobStatus, utcnow
from app.schemas.job import JobCreate, JobResponse, DAGSubmitRequest, DAGSubmitResponse
from app.scheduler.priority_queue import PriorityQueue
from app.scheduler.dag import DAGManager, CycleDetectedError
from app.core.logging import get_logger

logger = get_logger(__name__)


class JobService:
    """Manages job persistence, retrieval, DAG orchestration, and priority scheduling."""

    @staticmethod
    async def submit_job(
        session: AsyncSession,
        redis: Redis,
        job_in: JobCreate,
    ) -> Tuple[Job, bool]:
        """Submits a job. If idempotency_key is given and exists, returns existing job.
        
        Validates DAG dependencies:
        - If cycles or self-dependency detected: raises CycleDetectedError.
        - If any parent failed: child is created as FAILED immediately (fail-fast).
        - If all parents succeeded: child is created as QUEUED and enqueued to Redis.
        - If any parent pending/running: child is created as PENDING (waits for parents).
        
        Returns:
            (job, is_created) tuple.
        """
        if job_in.idempotency_key:
            stmt = (
                select(Job)
                .where(Job.idempotency_key == job_in.idempotency_key)
                .options(selectinload(Job.attempts), selectinload(Job.dependencies))
            )
            res = await session.execute(stmt)
            existing_job = res.scalar_one_or_none()
            if existing_job:
                logger.info(
                    f"Idempotent submission hit: returning existing job [{existing_job.id}] "
                    f"for key '{job_in.idempotency_key}'"
                )
                return existing_job, False

        job_id = uuid.uuid4()
        initial_status = JobStatus.QUEUED
        failure_error: Optional[str] = None

        # Validate dependencies if provided
        parents: List[Job] = []
        if job_in.depends_on:
            parents = await DAGManager.validate_submission_dependencies(
                session=session,
                candidate_job_id=job_id,
                parent_ids=job_in.depends_on,
            )

            # Check parent statuses
            terminal_failures = {JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.DEAD}
            failed_parent = next((p for p in parents if p.status in terminal_failures), None)

            if failed_parent:
                initial_status = JobStatus.FAILED
                failure_error = (
                    f"Dependency failure: parent job [{failed_parent.id}] "
                    f"terminated with status '{failed_parent.status}'"
                )
            elif all(p.status == JobStatus.SUCCEEDED for p in parents):
                initial_status = JobStatus.QUEUED
            else:
                initial_status = JobStatus.PENDING

        job = Job(
            id=job_id,
            job_type=job_in.job_type,
            payload=job_in.payload,
            status=initial_status,
            priority=job_in.priority,
            idempotency_key=job_in.idempotency_key,
            timeout_seconds=job_in.timeout_seconds,
            max_retries=job_in.max_retries,
            retry_count=0,
            created_at=utcnow(),
            error=failure_error,
            finished_at=utcnow() if initial_status == JobStatus.FAILED else None,
        )
        session.add(job)

        # Record DAG dependency edges
        for parent_id in job_in.depends_on:
            dep_edge = JobDependency(
                id=uuid.uuid4(),
                job_id=job.id,
                depends_on_job_id=parent_id,
            )
            session.add(dep_edge)

        await session.commit()
        await session.refresh(job)

        # Only enqueue to Redis if initial status is QUEUED
        if initial_status == JobStatus.QUEUED:
            pq = PriorityQueue(redis)
            score = await pq.enqueue(str(job.id), priority=job.priority)
            logger.info(
                f"Job [{job.id}] (type: {job.job_type}, priority: {job.priority}, score: {score}) "
                f"queued in Redis priority ZSET."
            )
        elif initial_status == JobStatus.PENDING:
            logger.info(
                f"Job [{job.id}] created in PENDING status awaiting {len(job_in.depends_on)} parent dependencies."
            )
        else:
            logger.warning(
                f"Job [{job.id}] created in FAILED status due to failed parent dependencies."
            )

        # Re-fetch with eager loaded attempts and dependencies
        return await JobService.get_job_by_id(session, job.id), True

    @staticmethod
    async def submit_dag(
        session: AsyncSession,
        redis: Redis,
        dag_req: DAGSubmitRequest,
    ) -> DAGSubmitResponse:
        """Submits an entire batch DAG atomically.
        
        Performs topological cycle detection before persistence.
        """
        node_ids = [n.node_id for n in dag_req.nodes]
        edges: Dict[str, List[str]] = {n.node_id: n.depends_on for n in dag_req.nodes}

        # 1. Topological cycle detection on batch
        DAGManager.validate_batch_dag(nodes=node_ids, edges=edges)

        # 2. Allocate UUIDs for each node in batch
        node_to_uuid: Dict[str, uuid.UUID] = {n.node_id: uuid.uuid4() for n in dag_req.nodes}
        created_jobs: List[Job] = []
        pq = PriorityQueue(redis)

        for node in dag_req.nodes:
            job_id = node_to_uuid[node.node_id]
            # If node has no dependencies, it can start immediately as QUEUED
            initial_status = JobStatus.QUEUED if not node.depends_on else JobStatus.PENDING

            job = Job(
                id=job_id,
                job_type=node.job_type,
                payload=node.payload,
                status=initial_status,
                priority=node.priority,
                timeout_seconds=node.timeout_seconds,
                max_retries=node.max_retries,
                retry_count=0,
                created_at=utcnow(),
            )
            session.add(job)
            created_jobs.append(job)

            # Add dependency edges
            for parent_label in node.depends_on:
                parent_uuid = node_to_uuid[parent_label]
                dep_edge = JobDependency(
                    id=uuid.uuid4(),
                    job_id=job_id,
                    depends_on_job_id=parent_uuid,
                )
                session.add(dep_edge)

        await session.commit()

        # Enqueue root jobs into Redis
        for job in created_jobs:
            if job.status == JobStatus.QUEUED:
                await pq.enqueue(str(job.id), priority=job.priority)

        # Return full job responses
        job_responses: List[JobResponse] = []
        for job in created_jobs:
            full_job = await JobService.get_job_by_id(session, job.id)
            if full_job:
                job_responses.append(JobResponse.model_validate(full_job))

        return DAGSubmitResponse(
            node_to_job_id=node_to_uuid,
            jobs=job_responses,
        )

    @staticmethod
    async def get_job_by_id(session: AsyncSession, job_id: uuid.UUID) -> Optional[Job]:
        """Retrieves a job by its UUID, including its attempts and dependencies."""
        stmt = (
            select(Job)
            .where(Job.id == job_id)
            .options(selectinload(Job.attempts), selectinload(Job.dependencies))
        )
        res = await session.execute(stmt)
        return res.scalar_one_or_none()

    @staticmethod
    async def cancel_job(
        session: AsyncSession,
        redis: Redis,
        job_id: uuid.UUID,
    ) -> Tuple[Optional[Job], str]:
        """Cancels a job if it has not reached a terminal state and cascades failure to children.
        
        Returns:
            (job, status_code_reason)
        """
        stmt = (
            select(Job)
            .where(Job.id == job_id)
            .with_for_update()
            .options(selectinload(Job.attempts), selectinload(Job.dependencies))
        )
        res = await session.execute(stmt)
        job = res.scalar_one_or_none()

        if not job:
            return None, "not_found"

        terminal_statuses = {
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.DEAD,
        }
        if job.status in terminal_statuses:
            return job, "terminal_state"

        # Transition status to CANCELLED
        job.status = JobStatus.CANCELLED
        job.finished_at = utcnow()
        await session.commit()

        # Signal cancellation in Redis and remove from active queues
        pq = PriorityQueue(redis)
        await pq.mark_cancellation(str(job.id))
        logger.info(f"Job [{job.id}] cancelled by client request.")

        # Cascade cancellation failure down the DAG
        await DAGManager.on_job_failed(session, job.id, JobStatus.CANCELLED)

        return job, "cancelled"

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

        total_res = await session.execute(count_query)
        total = total_res.scalar_one()

        items_query = (
            base_query
            .order_by(Job.created_at.desc())
            .offset(offset)
            .limit(limit)
            .options(selectinload(Job.attempts), selectinload(Job.dependencies))
        )
        items_res = await session.execute(items_query)
        items = list(items_res.scalars().all())

        return items, total
