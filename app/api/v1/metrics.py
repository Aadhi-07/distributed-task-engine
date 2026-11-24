from fastapi import APIRouter, Depends, Response
from redis.asyncio import Redis
from prometheus_client import CONTENT_TYPE_LATEST

from app.core.redis import get_redis
from app.core.metrics import collect_system_metrics

router = APIRouter(tags=["Metrics"])


@router.get(
    "/metrics",
    summary="Expose Prometheus formatted metrics",
    response_class=Response,
)
async def metrics_endpoint(redis: Redis = Depends(get_redis)) -> Response:
    """Returns queue depths, throughput, execution latencies, and worker telemetry in Prometheus text format."""
    metrics_data = await collect_system_metrics(redis)
    return Response(content=metrics_data, media_type=CONTENT_TYPE_LATEST)
