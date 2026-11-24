import os
import psutil
from typing import Dict, Any

from app.core.database import check_database_health
from app.core.redis import check_redis_health
from app.core.config import get_settings


async def get_system_health() -> Dict[str, Any]:
    """Aggregates database, redis, and system resource metrics."""
    settings = get_settings()

    db_health = await check_database_health()
    redis_health = await check_redis_health()

    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()

    is_ready = (db_health["status"] == "healthy") and (redis_health["status"] == "healthy")

    return {
        "status": "healthy" if is_ready else "degraded",
        "ready": is_ready,
        "environment": settings.ENV,
        "version": settings.VERSION,
        "components": {
            "database": db_health,
            "redis": redis_health,
        },
        "system": {
            "process_rss_mb": round(memory_info.rss / (1024 * 1024), 2),
            "cpu_percent": psutil.cpu_percent(interval=None),
            "system_memory_percent": psutil.virtual_memory().percent,
        },
    }
