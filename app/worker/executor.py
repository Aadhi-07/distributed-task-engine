import asyncio
import time
import traceback
import uuid
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.models.job import Job, JobAttempt, JobStatus, AttemptStatus, utcnow
from app.tasks.registry import execute_task
from app.scheduler.priority_queue import PriorityQueue
from app.core.logging import get_logger

logger = get_logger(__name__)


class JobExecutor:
    """Orchestrates atomic job execution, status transitions, attempts, and per-job timeouts."""

    @staticmethod
    async def execute_job(
        session: AsyncSession,
        redis: Redis,
        job_id: uuid.UUID,
        worker_id: str,
    ) -> bool:
        """Executes a single job with transactional state updates, enforcing per-job timeouts.
        
        Transitions:
        QUEUED -> RUNNING -> SUCCEEDED | FAILED (or TIMED_OUT attempt)
        """
        priority_queue = PriorityQueue(redis)

        try:
            # 1. Atomically lock and claim the job record
            stmt = (
                select(Job)
                .where(Job.id == job_id)
                .with_for_update()
            )
            res = await session.execute(stmt)
            job = res.scalar_one_or_none()

            if not job:
                logger.warning(f"Worker [{worker_id}] cannot execute job [{job_id}]: not found in database.")
                return False

            if job.status != JobStatus.QUEUED:
                logger.info(
                    f"Worker [{worker_id}] skipped job [{job_id}]: "
                    f"status is already '{job.status}' (expected QUEUED)."
                )
                return False

            # 2. Transition to RUNNING and record attempt
            job.status = JobStatus.RUNNING
            job.worker_id = worker_id
            job.started_at = utcnow()

            attempt = JobAttempt(
                id=uuid.uuid4(),
                job_id=job.id,
                attempt_number=job.retry_count + 1,
                worker_id=worker_id,
                status=AttemptStatus.RUNNING,
                started_at=utcnow(),
            )
            session.add(attempt)
            await session.commit()

            logger.info(f"Worker [{worker_id}] running job [{job.id}] (type: {job.job_type}, timeout: {job.timeout_seconds}s).")

            # 3. Execute the registered task wrapped in per-job timeout
            start_time = time.monotonic()
            timeout_limit = float(job.timeout_seconds) if job.timeout_seconds > 0 else 300.0

            try:
                raw_result = await asyncio.wait_for(
                    execute_task(job.job_type, job.payload),
                    timeout=timeout_limit,
                )
                duration_ms = round((time.monotonic() - start_time) * 1000, 2)
                task_result = raw_result if isinstance(raw_result, dict) else {"result": raw_result}

                # 4a. Transition to SUCCEEDED
                job.status = JobStatus.SUCCEEDED
                job.result = task_result
                job.finished_at = utcnow()

                attempt.status = AttemptStatus.SUCCEEDED
                attempt.finished_at = utcnow()
                attempt.execution_duration_ms = duration_ms

                await session.commit()
                logger.info(
                    f"Worker [{worker_id}] completed job [{job.id}] "
                    f"successfully in {duration_ms}ms."
                )
                return True

            except asyncio.TimeoutError:
                duration_ms = round((time.monotonic() - start_time) * 1000, 2)
                timeout_msg = f"Job exceeded execution timeout of {timeout_limit}s"

                # 4b. Transition to FAILED with TIMED_OUT attempt
                job.status = JobStatus.FAILED
                job.error = timeout_msg
                job.finished_at = utcnow()

                attempt.status = AttemptStatus.TIMED_OUT
                attempt.error = timeout_msg
                attempt.finished_at = utcnow()
                attempt.execution_duration_ms = duration_ms

                await session.commit()
                logger.warning(
                    f"Worker [{worker_id}] job [{job.id}] timed out after {duration_ms}ms."
                )
                return False

            except Exception as exc:
                duration_ms = round((time.monotonic() - start_time) * 1000, 2)
                error_trace = traceback.format_exc()

                # 4c. Transition to FAILED
                job.status = JobStatus.FAILED
                job.error = str(exc)
                job.finished_at = utcnow()

                attempt.status = AttemptStatus.FAILED
                attempt.error = str(exc)
                attempt.logs = f"{exc}\n{error_trace}"
                attempt.finished_at = utcnow()
                attempt.execution_duration_ms = duration_ms

                await session.commit()
                logger.warning(
                    f"Worker [{worker_id}] failed job [{job.id}] after {duration_ms}ms: {exc}"
                )
                return False

        finally:
            # Clean up in-flight processing entry from Redis
            try:
                await priority_queue.complete_processing(str(job_id))
            except Exception as exc:
                logger.warning(f"Failed to clear in-flight processing key for job [{job_id}]: {exc}")
