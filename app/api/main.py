import uuid
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.config import get_settings
from app.core.logging import setup_logging, get_logger
from app.core.database import get_engine, close_database_connection
from app.core.redis import get_redis, close_redis
from app.api.v1 import v1_router
from app.api.v1.health import router as root_health_router

setup_logging()
logger = get_logger("task_engine.api")


class RequestCorrelationMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        request.state.request_id = request_id
        start_time = time.monotonic()

        response = await call_next(request)

        duration_ms = round((time.monotonic() - start_time) * 1000, 2)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time-Ms"] = str(duration_ms)
        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger.info(
        f"Starting {settings.PROJECT_NAME} v{settings.VERSION} "
        f"[env={settings.ENV}, debug={settings.DEBUG}]"
    )

    # Initialize and pre-warm DB and Redis connection pools
    try:
        engine = get_engine()
        redis = await get_redis()
        logger.info("Database and Redis connection pools successfully initialized.")
    except Exception as exc:
        logger.error(f"Failed to initialize infrastructure clients on startup: {exc}")

    yield

    logger.info("Initiating graceful shutdown...")
    await close_database_connection()
    await close_redis()
    logger.info("Shutdown complete.")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.PROJECT_NAME,
        version=settings.VERSION,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    app.add_middleware(RequestCorrelationMiddleware)

    # Register endpoints: accessible at root (/jobs, /workers, /health) and /api/v1 prefix
    app.include_router(root_health_router)
    from app.api.v1.jobs import router as jobs_router
    from app.api.v1.workers import router as workers_router
    app.include_router(jobs_router)
    app.include_router(workers_router)
    app.include_router(v1_router, prefix="/api")

    return app


app = create_app()
