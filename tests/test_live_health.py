import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.services.health import get_system_health


@pytest.mark.asyncio
async def test_live_system_health():
    """Validates real live PostgreSQL and Redis connections using .env configuration."""
    health = await get_system_health()
    assert health["status"] == "healthy", f"System health is degraded: {health}"
    assert health["ready"] is True
    assert health["components"]["database"]["status"] == "healthy"
    assert health["components"]["redis"]["status"] == "healthy"
    assert health["components"]["database"]["latency_ms"] >= 0
    assert health["components"]["redis"]["latency_ms"] >= 0


@pytest.mark.asyncio
async def test_live_readiness_probe_endpoint():
    """Tests the /health/ready endpoint against the live PostgreSQL and Redis."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert data["ready"] is True
        assert data["components"]["database"]["status"] == "healthy"
        assert data["components"]["redis"]["status"] == "healthy"
