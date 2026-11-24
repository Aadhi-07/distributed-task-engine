from datetime import datetime, timezone, timedelta
from typing import List, Tuple
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.models.worker import WorkerNode, WorkerStatus
from app.schemas.worker import WorkerResponse, WorkerListResponse
from app.core.config import get_settings


class WorkerService:
    """Manages worker node queries and lifecycle inspection."""

    @staticmethod
    async def list_workers(
        session: AsyncSession,
        redis: Redis,
    ) -> WorkerListResponse:
        settings = get_settings()
        threshold_seconds = float(settings.WORKER_DEAD_THRESHOLD)
        cutoff_dt = datetime.now(timezone.utc) - timedelta(seconds=threshold_seconds)

        stmt = select(WorkerNode).order_by(WorkerNode.registered_at.asc())
        res = await session.execute(stmt)
        nodes = list(res.scalars().all())

        items: List[WorkerResponse] = []
        active_count = 0

        for node in nodes:
            # A node is considered alive if not explicitly STOPPED/DEAD and heartbeat is recent
            is_alive = (
                node.status == WorkerStatus.HEALTHY
                and node.last_heartbeat_at >= cutoff_dt
            )
            if is_alive:
                active_count += 1

            items.append(
                WorkerResponse(
                    id=node.id,
                    hostname=node.hostname,
                    pid=node.pid,
                    status=node.status,
                    concurrency=node.concurrency,
                    running_jobs_count=node.running_jobs_count,
                    cpu_percent=node.cpu_percent,
                    memory_percent=node.memory_percent,
                    registered_at=node.registered_at,
                    last_heartbeat_at=node.last_heartbeat_at,
                    is_alive=is_alive,
                    metadata_json=node.metadata_json or {},
                )
            )

        return WorkerListResponse(
            items=items,
            total=len(items),
            active_workers=active_count,
        )
