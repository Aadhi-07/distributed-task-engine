import pytest
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch


@pytest.mark.asyncio
async def test_health_check_endpoint(async_client: AsyncClient):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert "X-Request-ID" in response.headers
    assert "X-Response-Time-Ms" in response.headers


@pytest.mark.asyncio
async def test_liveness_probe_endpoint(async_client: AsyncClient):
    response = await async_client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


@pytest.mark.asyncio
async def test_readiness_probe_healthy(async_client: AsyncClient):
    with patch("app.services.health.check_database_health", new_callable=AsyncMock) as mock_db, \
         patch("app.services.health.check_redis_health", new_callable=AsyncMock) as mock_redis:
        mock_db.return_value = {"status": "healthy", "latency_ms": 1.25}
        mock_redis.return_value = {"status": "healthy", "latency_ms": 0.45}

        response = await async_client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["ready"] is True
        assert data["components"]["database"]["status"] == "healthy"
        assert data["components"]["redis"]["status"] == "healthy"
        assert "system" in data
        assert "process_rss_mb" in data["system"]


@pytest.mark.asyncio
async def test_readiness_probe_database_down(async_client: AsyncClient):
    with patch("app.services.health.check_database_health", new_callable=AsyncMock) as mock_db, \
         patch("app.services.health.check_redis_health", new_callable=AsyncMock) as mock_redis:
        mock_db.return_value = {"status": "unhealthy", "error": "Connection refused", "latency_ms": 50.0}
        mock_redis.return_value = {"status": "healthy", "latency_ms": 0.5}

        response = await async_client.get("/health/ready")
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "degraded"
        assert data["ready"] is False
        assert data["components"]["database"]["status"] == "unhealthy"
        assert data["components"]["redis"]["status"] == "healthy"


@pytest.mark.asyncio
async def test_readiness_probe_redis_down(async_client: AsyncClient):
    with patch("app.services.health.check_database_health", new_callable=AsyncMock) as mock_db, \
         patch("app.services.health.check_redis_health", new_callable=AsyncMock) as mock_redis:
        mock_db.return_value = {"status": "healthy", "latency_ms": 1.0}
        mock_redis.return_value = {"status": "unhealthy", "error": "Redis unreachable", "latency_ms": 20.0}

        response = await async_client.get("/health/ready")
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "degraded"
        assert data["ready"] is False


@pytest.mark.asyncio
async def test_correlation_id_passthrough(async_client: AsyncClient):
    custom_id = "test-correlation-uuid-12345"
    response = await async_client.get("/health", headers={"X-Request-ID": custom_id})
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == custom_id
