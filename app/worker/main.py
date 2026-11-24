import asyncio
import os
import signal
import socket
import sys
import uuid
import psutil
from typing import Optional, Set
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.core.config import get_settings
from app.core.logging import setup_logging, get_logger
from app.core.database import (
    get_session_factory,
    check_database_health,
    close_database_connection,
)
from app.core.redis import get_redis, check_redis_health, close_redis
from app.models.worker import WorkerNode, WorkerStatus, utcnow
from app.scheduler.priority_queue import PriorityQueue
from app.worker.executor import JobExecutor
import app.tasks  # registers workloads

setup_logging()
logger = get_logger("task_engine.worker")


class WorkerProcess:
    """Core worker daemon managing registration, heartbeats, concurrency, and priority queue consumption."""

    def __init__(self, worker_id: Optional[str] = None, concurrency: Optional[int] = None) -> None:
        self.settings = get_settings()
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.worker_id = worker_id or f"worker-{self.hostname}-{self.pid}-{uuid.uuid4().hex[:6]}"
        self.concurrency = concurrency or self.settings.WORKER_CONCURRENCY
        self.is_running = False
        self._shutdown_event = asyncio.Event()
        self._active_tasks: Set[asyncio.Task] = set()
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._running_jobs_count = 0

    async def register(self) -> None:
        """Registers this worker node in PostgreSQL."""
        session_factory = get_session_factory()
        async with session_factory() as session:
            stmt = select(WorkerNode).where(WorkerNode.id == self.worker_id)
            res = await session.execute(stmt)
            existing = res.scalar_one_or_none()

            now = utcnow()
            if existing:
                existing.status = WorkerStatus.HEALTHY
                existing.last_heartbeat_at = now
                existing.concurrency = self.concurrency
            else:
                worker_node = WorkerNode(
                    id=self.worker_id,
                    hostname=self.hostname,
                    pid=self.pid,
                    status=WorkerStatus.HEALTHY,
                    concurrency=self.concurrency,
                    running_jobs_count=0,
                    cpu_percent=psutil.cpu_percent(interval=None),
                    memory_percent=psutil.virtual_memory().percent,
                    registered_at=now,
                    last_heartbeat_at=now,
                    metadata_json={"concurrency": self.concurrency},
                )
                session.add(worker_node)
            await session.commit()
            logger.info(f"Worker [{self.worker_id}] registered in database as HEALTHY (concurrency={self.concurrency}).")

    async def initialize(self) -> None:
        logger.info(
            f"Initializing worker [{self.worker_id}] with max concurrency {self.concurrency}"
        )

        db_check = await check_database_health()
        redis_check = await check_redis_health()

        if db_check["status"] != "healthy":
            logger.warning(f"Database not yet healthy: {db_check.get('error')}")
        else:
            logger.info("Database connection established.")

        if redis_check["status"] != "healthy":
            logger.warning(f"Redis not yet healthy: {redis_check.get('error')}")
        else:
            logger.info("Redis connection established.")

        await self.register()

    async def process_next_job(self, timeout: int = 1) -> bool:
        """Atomically pulls and executes one job using Redis Lua script.
        
        Promotes any ready delayed retry jobs before claiming.
        Returns True if a job was claimed and executed, False if queue was empty.
        """
        redis = await get_redis()
        pq = PriorityQueue(redis)

        # Check and promote any due delayed retry jobs
        await pq.promote_delayed_jobs()

        claimed_id_str = await pq.atomic_claim(worker_id=self.worker_id)
        if not claimed_id_str:
            return False

        job_id = uuid.UUID(claimed_id_str)
        session_factory = get_session_factory()
        async with session_factory() as session:
            self._running_jobs_count += 1
            try:
                return await JobExecutor.execute_job(
                    session=session,
                    redis=redis,
                    job_id=job_id,
                    worker_id=self.worker_id,
                )
            finally:
                self._running_jobs_count -= 1

    async def _worker_loop(self) -> None:
        """Continuous polling loop respecting concurrency semaphore."""
        while not self._shutdown_event.is_set():
            await self._semaphore.acquire()

            # Define execution wrapper that releases semaphore upon completion
            async def _run():
                try:
                    processed = await self.process_next_job()
                    if not processed:
                        await asyncio.sleep(0.05)
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logger.error(f"Worker [{self.worker_id}] loop error: {exc}")
                finally:
                    self._semaphore.release()

            task = asyncio.create_task(_run())
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)

    async def start(self) -> None:
        await self.initialize()
        self.is_running = True
        logger.info(f"Worker [{self.worker_id}] running and listening for priority jobs...")

        try:
            await self._worker_loop()
        except asyncio.CancelledError:
            logger.info(f"Worker [{self.worker_id}] received cancellation signal.")
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        logger.info(f"Shutting down worker [{self.worker_id}]...")
        self.is_running = False

        # Wait for in-flight tasks to complete
        if self._active_tasks:
            logger.info(f"Waiting for {len(self._active_tasks)} active job task(s) to finish...")
            await asyncio.gather(*self._active_tasks, return_exceptions=True)

        # Update status in DB
        try:
            session_factory = get_session_factory()
            async with session_factory() as session:
                stmt = select(WorkerNode).where(WorkerNode.id == self.worker_id)
                res = await session.execute(stmt)
                node = res.scalar_one_or_none()
                if node:
                    node.status = WorkerStatus.STOPPED
                    node.last_heartbeat_at = utcnow()
                    await session.commit()
        except Exception as exc:
            logger.warning(f"Could not update worker stopped status in DB: {exc}")

        await close_database_connection()
        await close_redis()
        logger.info(f"Worker [{self.worker_id}] stopped cleanly.")

    def stop(self) -> None:
        self._shutdown_event.set()


async def run_worker() -> None:
    worker = WorkerProcess()
    loop = asyncio.get_running_loop()

    # Graceful signal handling
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.stop)
        except (NotImplementedError, RuntimeError):
            # Windows signal handling fallback
            signal.signal(sig, lambda *_: worker.stop())

    await worker.start()


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
