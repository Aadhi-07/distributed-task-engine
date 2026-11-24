from fastapi import APIRouter
from app.api.v1.health import router as health_router
from app.api.v1.jobs import router as jobs_router
from app.api.v1.workers import router as workers_router

v1_router = APIRouter(prefix="/v1")
v1_router.include_router(health_router)
v1_router.include_router(jobs_router)
v1_router.include_router(workers_router)

__all__ = ["v1_router", "jobs_router", "health_router", "workers_router"]
