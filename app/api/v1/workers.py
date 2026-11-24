from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.core.database import get_db_session
from app.core.redis import get_redis
from app.schemas.worker import WorkerListResponse
from app.services.worker_service import WorkerService

router = APIRouter(prefix="/workers", tags=["Workers"])


@router.get(
    "",
    response_model=WorkerListResponse,
    summary="List all registered worker nodes and health status",
)
async def list_workers(
    session: AsyncSession = Depends(get_db_session),
    redis: Redis = Depends(get_redis),
) -> WorkerListResponse:
    """Returns cluster workers, CPU/memory telemetry, active job count, and heartbeat status."""
    return await WorkerService.list_workers(session=session, redis=redis)
