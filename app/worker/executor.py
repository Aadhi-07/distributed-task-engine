
import asyncio
from datetime import datetime, timezone, timedelta
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
from app.scheduler.retry import calculate_backoff_delay
from app.scheduler.rate_limiter import RateLimiter
from app.scheduler.dag import DAGManager
from app.core.metrics import (
    JOBS_COMPLETED,
    JOBS_RETRIED,
    JOB_EXECUTION_DURATION,
    JOB_SCHEDULING_LATENCY,
    RUNNING_JOBS,
)
from app.core.logging import (
    get_logger,
    set_worker_id,
    set_job_id,
    clear_context,
)

logger = get_logger(__name__)


class JobExecutor:
    """Orchestrates atomic job execution, timeouts, retries with exponential backoff, and DLQ."""

    @staticmethod
    async def execute_job(
        session: AsyncSession,
        redis: Redis,
        job_id: uuid.UUID,
        worker_id: str,
    ) -> bool:
        """Executes a single job with transactional state updates, enforcing timeouts, retries, and DLQ."""
        priority_queue = PriorityQueue(redis)
        rate_limiter = RateLimiter(redis)
        concurrency_slot_acquired = False
        running_metric_recorded = False

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

            # Check if cancelled before execution
            if job.status == JobStatus.CANCELLED or await priority_queue.is_cancelled(str(job_id)):
                job.status = JobStatus.CANCELLED
                job.finished_at = utcnow()
                await session.commit()
                await priority_queue.clear_cancellation(str(job_id))
                logger.info(f"Worker [{worker_id}] dropped cancelled job [{job_id}].")
                return False

            if job.status != JobStatus.QUEUED:
                logger.info(
                    f"Worker [{worker_id}] skipped job [{job_id}]: "
                    f"status is already '{job.status}' (expected QUEUED)."
                )
                return False

            # Check global active concurrency limit for this job type
            concurrency_slot_acquired = await rate_limiter.acquire_concurrency_slot(
                job_type=job.job_type,
                job_id=str(job.id),
            )
            if not concurrency_slot_acquired:
                logger.info(
                    f"Worker [{worker_id}] deferred job [{job.id}] (type: {job.job_type}): "
                    f"global concurrency quota saturated."
                )
                # Reschedule job back into delayed queue with short 0.5s backoff
                await priority_queue.enqueue_delayed(
                    job_id=str(job.id),
                    priority=job.priority,
                    delay_seconds=0.5,
                )
                return False

            # 2. Transition to RUNNING and record attempt
            job.status = JobStatus.RUNNING
            job.worker_id = worker_id
            job.started_at = utcnow()

            # Set context vars for structured logging & telemetry
            set_worker_id(worker_id)
            set_job_id(str(job.id))
            RUNNING_JOBS.inc()
            running_metric_recorded = True

            # Track scheduling latency (from creation to execution start)
            sched_sec = (datetime.now(timezone.utc) - job.created_at).total_seconds()
            JOB_SCHEDULING_LATENCY.labels(job_type=job.job_type).observe(max(0.0, sched_sec))

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

            logger.info(
                f"Worker [{worker_id}] running job [{job.id}] "
                f"(type: {job.job_type}, attempt: {attempt.attempt_number}/{job.max_retries + 1})."
            )

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

                # Check if cancelled during task execution
                if await priority_queue.is_cancelled(str(job_id)):
                    job.status = JobStatus.CANCELLED
                    job.finished_at = utcnow()
                    attempt.status = AttemptStatus.FAILED
                    attempt.error = "Cancelled during execution"
                    attempt.finished_at = utcnow()
                    attempt.execution_duration_ms = duration_ms
                    await session.commit()
                    await priority_queue.clear_cancellation(str(job_id))
                    try:
                        await DAGManager.on_job_failed(session, job.id, JobStatus.CANCELLED)
                    except Exception as exc:
                        logger.error(f"Error cascading DAG cancellation for job [{job.id}]: {exc}")
                    return False

                # 4a. Success: Transition to SUCCEEDED
                job.status = JobStatus.SUCCEEDED
                job.result = task_result
                job.finished_at = utcnow()
                job.scheduled_at = None

                attempt.status = AttemptStatus.SUCCEEDED
                attempt.finished_at = utcnow()
                attempt.execution_duration_ms = duration_ms

                # Record Prometheus completion metrics
                duration_sec = duration_ms / 1000.0
                JOB_EXECUTION_DURATION.labels(job_type=job.job_type).observe(duration_sec)
                JOBS_COMPLETED.labels(job_type=job.job_type, status="SUCCEEDED").inc()

                await session.commit()
                logger.info(
                    f"Worker [{worker_id}] completed job [{job.id}] "
                    f"successfully in {duration_ms}ms."
                )

                # Trigger DAG resolution for dependent downstream jobs
                try:
                    await DAGManager.on_job_succeeded(session, redis, job.id)
                except Exception as exc:
                    logger.error(f"Error resolving DAG dependencies for job [{job.id}]: {exc}")

                return True

            except asyncio.TimeoutError:
                duration_ms = round((time.monotonic() - start_time) * 1000, 2)
                error_msg = f"Job exceeded execution timeout of {timeout_limit}s"

                attempt.status = AttemptStatus.TIMED_OUT
                attempt.error = error_msg
                attempt.finished_at = utcnow()
                attempt.execution_duration_ms = duration_ms

                return await JobExecutor._handle_failure(
                    session=session,
                    priority_queue=priority_queue,
                    job=job,
                    attempt=attempt,
                    error_msg=error_msg,
                    worker_id=worker_id,
                )

            except Exception as exc:
                duration_ms = round((time.monotonic() - start_time) * 1000, 2)
                error_trace = traceback.format_exc()
                error_msg = str(exc)

                attempt.status = AttemptStatus.FAILED
                attempt.error = error_msg
                attempt.logs = f"{exc}\n{error_trace}"
                attempt.finished_at = utcnow()
                attempt.execution_duration_ms = duration_ms

                return await JobExecutor._handle_failure(
                    session=session,
                    priority_queue=priority_queue,
                    job=job,
                    attempt=attempt,
                    error_msg=error_msg,
                    worker_id=worker_id,
                )

        finally:
            if running_metric_recorded:
                RUNNING_JOBS.dec()
            clear_context()

            if concurrency_slot_acquired and job:
                try:
                    await rate_limiter.release_concurrency_slot(job.job_type, str(job.id))
                except Exception as exc:
                    logger.warning(f"Failed to release concurrency slot for job [{job_id}]: {exc}")

            # Clean up in-flight processing entry from Redis
            try:
                await priority_queue.complete_processing(str(job_id))
            except Exception as exc:
                logger.warning(f"Failed to clear in-flight processing key for job [{job_id}]: {exc}")

    @staticmethod
    async def _handle_failure(
        session: AsyncSession,
        priority_queue: PriorityQueue,
        job: Job,
        attempt: JobAttempt,
        error_msg: str,
        worker_id: str,
    ) -> bool:
        """Evaluates retry eligibility or transitions to Dead Letter Queue (DEAD)."""
        job.error = error_msg

        # Check if job was cancelled
        if await priority_queue.is_cancelled(str(job.id)):
            job.status = JobStatus.CANCELLED
            job.finished_at = utcnow()
            await session.commit()
            await priority_queue.clear_cancellation(str(job.id))
            return False

        # Record failure metric
        duration_sec = (attempt.execution_duration_ms or 0.0) / 1000.0
        JOB_EXECUTION_DURATION.labels(job_type=job.job_type).observe(duration_sec)
        JOBS_COMPLETED.labels(job_type=job.job_type, status=attempt.status.value).inc()

        if job.retry_count < job.max_retries:
            # Eligible for retry: schedule with exponential backoff + jitter
            delay = calculate_backoff_delay(
                retry_count=job.retry_count,
                base_seconds=job.backoff_base_seconds,
                max_seconds=job.backoff_max_seconds,
                jitter=True,
            )
            job.retry_count += 1
            job.status = JobStatus.QUEUED
            job.scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
            JOBS_RETRIED.labels(job_type=job.job_type).inc()
            await session.commit()

            await priority_queue.enqueue_delayed(
                job_id=str(job.id),
                priority=job.priority,
                delay_seconds=delay,
            )
            logger.info(
                f"Worker [{worker_id}] scheduled retry #{job.retry_count}/{job.max_retries} "
                f"for job [{job.id}] in {delay}s."
            )
            return False
        else:
            # Exhausted retries or max_retries == 0
            if job.max_retries > 0:
                job.status = JobStatus.DEAD
                await priority_queue.send_to_dlq(str(job.id))
                logger.warning(
                    f"Worker [{worker_id}] job [{job.id}] exhausted all {job.max_retries} "
                    f"retries -> moved to Dead-Letter Queue (DEAD)."
                )
            else:
                job.status = JobStatus.FAILED
                logger.info(f"Worker [{worker_id}] job [{job.id}] failed (0 retries configured).")

            job.finished_at = utcnow()
            await session.commit()

            # Cascade DAG failure to downstream dependent jobs
            try:
                await DAGManager.on_job_failed(session, job.id, job.status)
            except Exception as exc:
                logger.error(f"Error cascading DAG failure for job [{job.id}]: {exc}")

            return False
