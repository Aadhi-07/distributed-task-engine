import asyncio
import os
import signal
import socket
import sys
import uuid
from typing import Optional

from app.core.config import get_settings
from app.core.logging import setup_logging, get_logger
from app.core.database import check_database_health, close_database_connection
from app.core.redis import check_redis_health, close_redis

setup_logging()
logger = get_logger("task_engine.worker")


class WorkerProcess:
    """Core daemon managing worker lifecycle, heartbeat, and execution loop."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.worker_id = f"worker-{self.hostname}-{self.pid}-{uuid.uuid4().hex[:6]}"
        self.is_running = False
        self._shutdown_event = asyncio.Event()

    async def initialize(self) -> None:
        logger.info(
            f"Initializing worker [{self.worker_id}] with max concurrency {self.settings.WORKER_CONCURRENCY}"
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

    async def start(self) -> None:
        await self.initialize()
        self.is_running = True
        logger.info(f"Worker [{self.worker_id}] running and ready to process jobs.")

        try:
            while not self._shutdown_event.is_set():
                # In Phase 1, worker runs the daemon heartbeat loop
                await asyncio.sleep(self.settings.WORKER_HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            logger.info(f"Worker [{self.worker_id}] received cancellation signal.")
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        logger.info(f"Shutting down worker [{self.worker_id}]...")
        self.is_running = False
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
            # Windows signal handling workaround
            signal.signal(sig, lambda *_: worker.stop())

    await worker.start()


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
