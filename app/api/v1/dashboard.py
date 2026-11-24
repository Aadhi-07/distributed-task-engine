from typing import Optional
from fastapi import APIRouter, Depends, Header, Response, status
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.core.database import get_db_session
from app.core.redis import get_redis
from app.schemas.dashboard import DashboardSnapshotResponse
from app.services.dashboard_service import DashboardService

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


@router.get(
    "/snapshot",
    response_model=Optional[DashboardSnapshotResponse],
    summary="Get visual dashboard topology snapshot",
    responses={
        200: {"description": "Full graph topology and metrics snapshot"},
        304: {"description": "Not Modified - client cache is fresh"},
    },
)
async def get_dashboard_snapshot(
    response: Response,
    if_none_match: Optional[str] = Header(None, alias="If-None-Match"),
    session: AsyncSession = Depends(get_db_session),
    redis: Redis = Depends(get_redis),
):
    """Returns the visual topology (hub, workers, jobs, edges), performance stats, and event feed.
    
    Supports HTTP ETag and conditional polling: returns 304 Not Modified when graph state is unchanged.
    """
    snapshot, etag, is_not_modified = await DashboardService.get_snapshot(
        session=session,
        redis=redis,
        if_none_match=if_none_match,
    )

    response.headers["ETag"] = f'"{etag}"'
    response.headers["Cache-Control"] = "no-cache"

    if is_not_modified:
        response.status_code = status.HTTP_304_NOT_MODIFIED
        return None

    return snapshot
