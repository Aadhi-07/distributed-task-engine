import uuid
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.core.database import get_db_session
from app.core.redis import get_redis
from app.models.job import JobStatus
from app.schemas.job import JobCreate, JobResponse, JobListResponse
from app.services.job_service import JobService

router = APIRouter(prefix="/jobs", tags=["Jobs"])


@router.post(
    "",
    response_model=JobResponse,
    summary="Submit a new job",
    status_code=status.HTTP_201_CREATED,
)
async def submit_job(
    job_in: JobCreate,
    response: Response,
    session: AsyncSession = Depends(get_db_session),
    redis: Redis = Depends(get_redis),
) -> JobResponse:
    """Submit a computational workload.
    
    If idempotency_key is provided and an existing job with that key exists,
    returns the existing job with HTTP 200 OK. Otherwise, creates the job,
    persists it in PostgreSQL as QUEUED, enqueues to Redis, and returns HTTP 201 Created.
    """
    job, is_created = await JobService.submit_job(session=session, redis=redis, job_in=job_in)
    if not is_created:
        response.status_code = status.HTTP_200_OK
    return JobResponse.model_validate(job)


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    summary="Get job by ID",
)
async def get_job(
    job_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
) -> JobResponse:
    """Retrieves full details, execution status, attempts, and results for a job."""
    job = await JobService.get_job_by_id(session=session, job_id=job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with id '{job_id}' not found",
        )
    return JobResponse.model_validate(job)


@router.get(
    "",
    response_model=JobListResponse,
    summary="List jobs with filters and pagination",
)
async def list_jobs(
    status: Optional[JobStatus] = Query(None, description="Filter by job status"),
    job_type: Optional[str] = Query(None, description="Filter by task job type"),
    limit: int = Query(50, ge=1, le=100, description="Page limit"),
    offset: int = Query(0, ge=0, description="Page offset"),
    session: AsyncSession = Depends(get_db_session),
) -> JobListResponse:
    """Returns paginated jobs matching optional status and job_type criteria."""
    items, total = await JobService.list_jobs(
        session=session,
        status=status,
        job_type=job_type,
        limit=limit,
        offset=offset,
    )
    return JobListResponse(
        items=[JobResponse.model_validate(item) for item in items],
        total=total,
        limit=limit,
        offset=offset,
    )
