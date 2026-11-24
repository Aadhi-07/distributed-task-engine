import time
from typing import Optional, Dict, Any
import redis.asyncio as aioredis
from redis.asyncio import Redis

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_redis_client: Optional[Redis] = None


async def get_redis() -> Redis:
    global _redis_client
    if _redis_client is None:
        settings = get_settings()
        _redis_client = aioredis.from_url(
            settings.redis_connection_url,
            decode_responses=True,
            encoding="utf-8",
        )
    return _redis_client


async def close_redis() -> None:
    global _redis_client
    if _redis_client is not None:
        await _redis_client.close()
        _redis_client = None
        logger.info("Redis connection closed.")


async def check_redis_health() -> Dict[str, Any]:
    """Health check for Redis connectivity and ping latency."""
    start_time = time.monotonic()
    try:
        client = await get_redis()
        pong = await client.ping()
        latency_ms = round((time.monotonic() - start_time) * 1000, 2)
        if pong:
            return {
                "status": "healthy",
                "latency_ms": latency_ms,
            }
        return {
            "status": "unhealthy",
            "error": "Unexpected PING response",
            "latency_ms": latency_ms,
        }
    except Exception as exc:
        latency_ms = round((time.monotonic() - start_time) * 1000, 2)
        logger.warning(f"Redis health check failed: {exc}")
        return {
            "status": "unhealthy",
            "error": str(exc),
            "latency_ms": latency_ms,
        }
