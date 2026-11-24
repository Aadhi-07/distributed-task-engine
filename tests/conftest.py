import pytest_asyncio
from typing import AsyncGenerator
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.core.database import close_database_connection
from app.core.redis import close_redis


@pytest_asyncio.fixture(autouse=True)
async def cleanup_connections():
    """Ensure database and redis connection pools are cleanly closed between tests."""
    yield
    await close_database_connection()
    await close_redis()


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
