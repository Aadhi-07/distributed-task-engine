import asyncio
import json
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.worker import WorkerNode, WorkerStatus, utcnow
from app.models.job import Job, JobAttempt, JobStatus, AttemptStatus
from app.scheduler.priority_queue import PriorityQueue, REDIS_PROCESSING_HASH_KEY
from app.core.metrics import JOBS_RETRIED
from app.core.logging import get_logger

logger = get_logger(__name__)

REDIS_WORKERS_HEARTBEAT_ZSET = "task_engine:workers:heartbeats"
REDIS_WORKER_META_PREFIX = "task_engine:worker:meta:"


class HeartbeatManager:
    """Manages worker heartbeats, failure detection, and automatic job recovery."""

    @staticmethod
    async def record_heartbeat(
        redis: Redis,
        session: AsyncSession,
        worker_id: str,
        cpu_percent: float,
        memory_percent: float,
        running_jobs_count: int,
    ) -> None:
        """Publishes heartbeat to both Redis and PostgreSQL."""
        now_ts = time.time()
        now_dt = utcnow()

        # 1. Update Redis
        await redis.zadd(REDIS_WORKERS_HEARTBEAT_ZSET, {worker_id: now_ts})
        meta = {
            "worker_id": worker_id,
            "cpu_percent": cpu_percent,
            "memory_percent": memory_percent,
            "running_jobs_count": running_jobs_count,
            "timestamp": now_ts,
        }
        await redis.set(f"{REDIS_WORKER_META_PREFIX}{worker_id}", json.dumps(meta), ex=60)

        # 2. Update PostgreSQL
        stmt = select(WorkerNode).where(WorkerNode.id == worker_id)
        res = await session.execute(stmt)
        node = res.scalar_one_or_none()
        if node:
            node.last_heartbeat_at = now_dt
            node.cpu_percent = cpu_percent
            node.memory_percent = memory_percent
            node.running_jobs_count = running_jobs_count
            if node.status in (WorkerStatus.STARTING, WorkerStatus.UNHEALTHY):
                node.status = WorkerStatus.HEALTHY
            await session.commit()

    @staticmethod
    async def detect_and_requeue_dead_workers(
        redis: Redis,
        session_factory: async_sessionmaker[AsyncSession],
        dead_threshold_seconds: float = 15.0,
    ) -> Dict[str, Any]:
        """Detects workers whose heartbeats have expired and recovers their in-flight jobs.
        
        Guarantees at-least-once execution by safely requeueing stranded jobs.
        """
        now_ts = time.time()
        cutoff_ts = now_ts - dead_threshold_seconds
        cutoff_dt = datetime.now(timezone.utc) - timedelta(seconds=dead_threshold_seconds)

        dead_worker_ids: List[str] = []
        requeued_jobs: List[str] = []
        dlq_jobs: List[str] = []

        pq = PriorityQueue(redis)

        async with session_factory() as session:
            # 1. Find dead workers in DB
            stmt = select(WorkerNode).where(
                WorkerNode.status == WorkerStatus.HEALTHY,
                WorkerNode.last_heartbeat_at < cutoff_dt,
            )
            res = await session.execute(stmt)
            dead_nodes = list(res.scalars().all())

            for node in dead_nodes:
                node.status = WorkerStatus.DEAD
                dead_worker_ids.append(node.id)
                logger.warning(
                    f"Detected dead worker [{node.id}]: last heartbeat at {node.last_heartbeat_at} "
                    f"(>{dead_threshold_seconds}s ago). Marking DEAD."
                )

                # Remove from Redis active heartbeats
                await redis.zrem(REDIS_WORKERS_HEARTBEAT_ZSET, node.id)
                await redis.delete(f"{REDIS_WORKER_META_PREFIX}{node.id}")

                # Drain any unstarted jobs stranded in the dead worker's private queue
                private_queue_key = f"task_engine:worker:queue:{node.id}"
                while True:
                    orphaned = await redis.lpop(private_queue_key)
                    if not orphaned:
                        break
                    orphaned_id = orphaned.decode() if isinstance(orphaned, bytes) else str(orphaned)
                    job_stmt = select(Job).where(Job.id == uuid.UUID(orphaned_id))
                    job_res = await session.execute(job_stmt)
                    queued_job = job_res.scalar_one_or_none()
                    prio = queued_job.priority if queued_job else 5
                    await pq.enqueue(orphaned_id, priority=prio)
                    requeued_jobs.append(orphaned_id)
                    logger.warning(f"Requeued unstarted private-queue job [{orphaned_id}] from dead worker [{node.id}].")

                # 2. Find any RUNNING jobs assigned to this dead worker
                job_stmt = select(Job).where(
                    Job.worker_id == node.id,
                    Job.status == JobStatus.RUNNING,
                ).with_for_update()
                job_res = await session.execute(job_stmt)
                stranded_jobs = list(job_res.scalars().all())

                for job in stranded_jobs:
                    # Record a failure attempt for the worker crash
                    attempt = JobAttempt(
                        id=uuid.uuid4(),
                        job_id=job.id,
                        attempt_number=job.retry_count + 1,
                        worker_id=node.id,
                        status=AttemptStatus.FAILED,
                        started_at=job.started_at or utcnow(),
                        finished_at=utcnow(),
                        error=f"Worker node [{node.id}] died or missed heartbeats during execution",
                    )
                    session.add(attempt)

                    # 3. Check retry eligibility
                    if job.retry_count < job.max_retries:
                        job.retry_count += 1
                        job.status = JobStatus.QUEUED
                        job.worker_id = None
                        job.started_at = None
                        JOBS_RETRIED.labels(job_type=job.job_type).inc()
                        await pq.enqueue(str(job.id), priority=job.priority)
                        requeued_jobs.append(str(job.id))
                        logger.warning(
                            f"Requeued stranded job [{job.id}] (retry {job.retry_count}/{job.max_retries}) "
                            f"from dead worker [{node.id}]."
                        )
                    else:
                        job.status = JobStatus.DEAD
                        job.finished_at = utcnow()
                        await pq.send_to_dlq(str(job.id))
                        dlq_jobs.append(str(job.id))
                        logger.error(
                            f"Stranded job [{job.id}] exhausted all retries -> moved to DLQ (DEAD)."
                        )

                    # Remove from in-flight Redis processing hash
                    await pq.complete_processing(str(job.id))

            await session.commit()

        return {
            "dead_workers": dead_worker_ids,
            "requeued_jobs": requeued_jobs,
            "dlq_jobs": dlq_jobs,
        }
