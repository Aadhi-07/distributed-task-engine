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
from app.scheduler.heartbeat import HeartbeatManager
from app.scheduler.load_balancer import LoadBalancer
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

        # Unique worker identity:
        # Priority: explicit arg > WORKER_ID env > RENDER_INSTANCE_ID (Render) > hostname-pid-random
        render_instance_id = os.environ.get("RENDER_INSTANCE_ID")
        env_worker_id = os.environ.get("WORKER_ID")
        if worker_id:
            self.worker_id = worker_id
        elif env_worker_id:
            self.worker_id = env_worker_id
        elif render_instance_id:
            self.worker_id = f"worker-{render_instance_id}-{uuid.uuid4().hex[:6]}"
        else:
            self.worker_id = f"worker-{self.hostname}-{self.pid}-{uuid.uuid4().hex[:6]}"

        # Concurrency per worker: explicit arg > WORKER_CONCURRENCY env > settings (default 4)
        env_concurrency = os.environ.get("WORKER_CONCURRENCY")
        if concurrency is not None:
            self.concurrency = concurrency
        elif env_concurrency is not None:
            try:
                self.concurrency = max(1, int(env_concurrency))
            except ValueError:
                self.concurrency = self.settings.WORKER_CONCURRENCY
        else:
            self.concurrency = self.settings.WORKER_CONCURRENCY

        self.is_running = False
        self._shutdown_event = asyncio.Event()
        self._active_tasks: Set[asyncio.Task] = set()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._claim_lock = asyncio.Lock()
        self._running_jobs_count = 0

    async def register(self) -> None:
        """Registers this worker node in PostgreSQL and LoadBalancer."""
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

        # Register in Redis LoadBalancer
        try:
            redis = await get_redis()
            lb = LoadBalancer(redis)
            await lb.update_worker_load(
                worker_id=self.worker_id,
                cpu_percent=round(psutil.cpu_percent(interval=None), 1),
                memory_percent=round(psutil.virtual_memory().percent, 1),
                running_jobs=self._running_jobs_count,
                concurrency=self.concurrency,
            )
        except Exception as exc:
            logger.warning(f"Worker [{self.worker_id}] failed to register load in LoadBalancer: {exc}")

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

    async def send_heartbeat(self) -> None:
        """Sends a single heartbeat update and refreshes load telemetry."""
        redis = await get_redis()
        session_factory = get_session_factory()
        cpu_val = round(psutil.cpu_percent(interval=None), 1)
        mem_val = round(psutil.virtual_memory().percent, 1)

        async with session_factory() as session:
            await HeartbeatManager.record_heartbeat(
                redis=redis,
                session=session,
                worker_id=self.worker_id,
                cpu_percent=cpu_val,
                memory_percent=mem_val,
                running_jobs_count=self._running_jobs_count,
            )

        # Update composite load in Redis
        lb = LoadBalancer(redis)
        await lb.update_worker_load(
            worker_id=self.worker_id,
            cpu_percent=cpu_val,
            memory_percent=mem_val,
            running_jobs=self._running_jobs_count,
            concurrency=self.concurrency,
        )

    async def _heartbeat_loop(self) -> None:
        """Periodic background heartbeat emitter."""
        while not self._shutdown_event.is_set():
            try:
                await self.send_heartbeat()
            except Exception as exc:
                logger.warning(f"Worker [{self.worker_id}] heartbeat failed: {exc}")
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.settings.WORKER_HEARTBEAT_INTERVAL,
                )
            except asyncio.TimeoutError:
                pass

    async def process_next_job(self, timeout: int = 1) -> bool:
        """Pulls and executes one job. Checks private queue first, then priority queue.
        
        Returns True if a job was claimed and executed, False if queues were empty or worker saturated.
        """
        async with self._claim_lock:
            # Strictly enforce concurrency: do not claim when already at full capacity
            if self._running_jobs_count >= self.concurrency:
                return False

            redis = await get_redis()
            lb = LoadBalancer(redis)
            pq = PriorityQueue(redis)

            # 1. Check targeted private queue first (for load-aware assignments)
            claimed_id_str = await lb.pop_from_private_queue(self.worker_id)

            # 2. If no targeted job, check and promote delayed jobs, then claim from priority queue
            if not claimed_id_str:
                await pq.promote_delayed_jobs()
                claimed_id_str = await pq.atomic_claim(worker_id=self.worker_id)

            if not claimed_id_str:
                return False

            self._running_jobs_count += 1

        # Reflect active load in Redis LoadBalancer
        try:
            await lb.update_worker_load(
                worker_id=self.worker_id,
                cpu_percent=round(psutil.cpu_percent(interval=None), 1),
                memory_percent=round(psutil.virtual_memory().percent, 1),
                running_jobs=self._running_jobs_count,
                concurrency=self.concurrency,
            )
        except Exception:
            pass

        job_id = uuid.UUID(claimed_id_str)
        session_factory = get_session_factory()
        async with session_factory() as session:
            try:
                return await JobExecutor.execute_job(
                    session=session,
                    redis=redis,
                    job_id=job_id,
                    worker_id=self.worker_id,
                )
            finally:
                async with self._claim_lock:
                    self._running_jobs_count -= 1
                try:
                    await lb.update_worker_load(
                        worker_id=self.worker_id,
                        cpu_percent=round(psutil.cpu_percent(interval=None), 1),
                        memory_percent=round(psutil.virtual_memory().percent, 1),
                        running_jobs=self._running_jobs_count,
                        concurrency=self.concurrency,
                    )
                except Exception:
                    pass

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
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
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

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Wait for in-flight tasks to complete (drain with clean 25s timeout)
        if self._active_tasks:
            logger.info(f"Draining {len(self._active_tasks)} active in-flight job task(s)...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._active_tasks, return_exceptions=True),
                    timeout=25.0,
                )
                logger.info(f"All active tasks drained cleanly for worker [{self.worker_id}].")
            except asyncio.TimeoutError:
                logger.warning(
                    f"Worker [{self.worker_id}] drain timed out after 25s. "
                    f"Cancelling {len(self._active_tasks)} remaining task(s)..."
                )
                for task in self._active_tasks:
                    task.cancel()
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

        # Remove from load balancer registry
        try:
            redis = await get_redis()
            lb = LoadBalancer(redis)
            await lb.remove_worker(self.worker_id)
        except Exception:
            pass

        await close_database_connection()
        await close_redis()
        logger.info(f"Worker [{self.worker_id}] stopped cleanly.")

    def stop(self) -> None:
        self._shutdown_event.set()


async def run_worker() -> None:
    worker = WorkerProcess()
    loop = asyncio.get_running_loop()

    def _handle_signal():
        logger.info(f"Received termination signal. Draining worker [{worker.worker_id}]...")
        worker.stop()

    # Graceful signal handling for SIGINT and SIGTERM
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, RuntimeError):
            # Windows signal handling fallback
            signal.signal(sig, lambda *_: _handle_signal())

    await worker.start()


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
