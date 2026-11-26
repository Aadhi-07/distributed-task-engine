import os

# Ensure local test suite targets local PostgreSQL and Redis if Render internal hostnames are in .env
if "dpg-" in os.environ.get("DATABASE_URL", ""):
    os.environ["DATABASE_URL"] = "postgresql+asyncpg://taskuser:taskpass@127.0.0.1:5432/taskdb"
if "red-" in os.environ.get("REDIS_URL", ""):
    os.environ["REDIS_URL"] = "redis://127.0.0.1:6379/0"

# Also check if Pydantic might load .env before os.environ was set
from app.core.config import get_settings
settings = get_settings()
if "dpg-" in (settings.DATABASE_URL or ""):
    os.environ["DATABASE_URL"] = "postgresql+asyncpg://taskuser:taskpass@127.0.0.1:5432/taskdb"
    get_settings.cache_clear()
if "red-" in (settings.REDIS_URL or ""):
    os.environ["REDIS_URL"] = "redis://127.0.0.1:6379/0"
    get_settings.cache_clear()

import pytest_asyncio
from typing import AsyncGenerator
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.core.database import close_database_connection
from app.core.redis import get_redis, close_redis


@pytest_asyncio.fixture(autouse=True)
async def cleanup_connections():
    """Ensure database and redis connection pools are cleanly closed and queues cleared."""
    try:
        redis = await get_redis()
        await redis.flushdb()
    except Exception:
        pass

    yield

    await close_database_connection()
    await close_redis()


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
