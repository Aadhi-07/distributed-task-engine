from fastapi import APIRouter, Depends, Response
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from prometheus_client import CONTENT_TYPE_LATEST

from app.core.redis import get_redis
from app.core.database import get_db_session
from app.core.metrics import collect_system_metrics

router = APIRouter(tags=["Metrics"])


@router.get(
    "/metrics",
    summary="Expose Prometheus formatted metrics",
    response_class=Response,
)
async def metrics_endpoint(
    redis: Redis = Depends(get_redis),
    session: AsyncSession = Depends(get_db_session),
) -> Response:
    """Returns queue depths, throughput, execution latencies, and worker telemetry in Prometheus text format."""
    metrics_data = await collect_system_metrics(redis=redis, session=session)
    return Response(content=metrics_data, media_type=CONTENT_TYPE_LATEST)
