from fastapi import APIRouter, status, Response
from typing import Dict, Any

from app.services.health import get_system_health

router = APIRouter(tags=["Health"])


@router.get("/health", summary="Basic health probe")
async def health_check() -> Dict[str, str]:
    """Lightweight check to verify API process responsiveness."""
    return {"status": "ok"}


@router.get("/health/live", summary="Kubernetes / container liveness probe")
async def liveness_probe() -> Dict[str, str]:
    """Liveness probe: verifies process is alive."""
    return {"status": "alive"}


@router.get("/health/ready", summary="Readiness probe for DB and Redis dependencies")
async def readiness_probe(response: Response) -> Dict[str, Any]:
    """Readiness probe: validates database and Redis dependencies are reachable."""
    health_data = await get_system_health()
    if not health_data["ready"]:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return health_data
