import time
import traceback
import uuid
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job, JobAttempt, JobStatus, AttemptStatus, utcnow
from app.tasks.registry import execute_task
from app.core.logging import get_logger

logger = get_logger(__name__)


class JobExecutor:
    """Orchestrates atomic job execution, status transitions, and attempt logging."""

    @staticmethod
    async def execute_job(
        session: AsyncSession,
        job_id: uuid.UUID,
        worker_id: str,
    ) -> bool:
        """Executes a single job with transactional state updates.
        
        Transitions:
        QUEUED -> RUNNING -> SUCCEEDED | FAILED
        """
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

        logger.info(f"Worker [{worker_id}] running job [{job.id}] (type: {job.job_type}).")

        # 3. Execute the registered task
        start_time = time.monotonic()
        task_error: Optional[str] = None
        task_result: Optional[dict] = None

        try:
            raw_result = await execute_task(job.job_type, job.payload)
            task_result = raw_result if isinstance(raw_result, dict) else {"result": raw_result}
            duration_ms = round((time.monotonic() - start_time) * 1000, 2)

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

        except Exception as exc:
            duration_ms = round((time.monotonic() - start_time) * 1000, 2)
            error_trace = traceback.format_exc()
            task_error = f"{exc}\n{error_trace}"

            # 4b. Transition to FAILED
            job.status = JobStatus.FAILED
            job.error = str(exc)
            job.finished_at = utcnow()

            attempt.status = AttemptStatus.FAILED
            attempt.error = str(exc)
            attempt.logs = task_error
            attempt.finished_at = utcnow()
            attempt.execution_duration_ms = duration_ms

            await session.commit()
            logger.warning(
                f"Worker [{worker_id}] failed job [{job.id}] after {duration_ms}ms: {exc}"
            )
            return False
