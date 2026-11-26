import os
import pytest
from httpx import AsyncClient

from app.core.config import (
    Settings,
    get_settings,
    normalize_async_database_url,
    normalize_sync_database_url,
)
from app.worker.main import WorkerProcess


def test_database_url_normalization():
    """Verifies that postgresql://, postgres://, and postgresql+asyncpg:// are correctly normalized."""
    # Render provides postgresql://
    render_url = "postgresql://user:pass@dpg-abc123xyz:5432/taskdb_render"
    assert (
        normalize_async_database_url(render_url)
        == "postgresql+asyncpg://user:pass@dpg-abc123xyz:5432/taskdb_render"
    )

    # Legacy postgres:// format
    legacy_url = "postgres://user:pass@dpg-abc123xyz:5432/taskdb_render"
    assert (
        normalize_async_database_url(legacy_url)
        == "postgresql+asyncpg://user:pass@dpg-abc123xyz:5432/taskdb_render"
    )

    # Already asyncpg format
    asyncpg_url = "postgresql+asyncpg://user:pass@localhost:5432/taskdb"
    assert normalize_async_database_url(asyncpg_url) == asyncpg_url

    # Sync normalization
    assert (
        normalize_sync_database_url("postgresql+asyncpg://user:pass@host:5432/db")
        == "postgresql://user:pass@host:5432/db"
    )
    assert (
        normalize_sync_database_url("postgres://user:pass@host:5432/db")
        == "postgresql://user:pass@host:5432/db"
    )

    # Settings property integration
    s = Settings(DATABASE_URL=render_url)
    assert s.async_database_url == "postgresql+asyncpg://user:pass@dpg-abc123xyz:5432/taskdb_render"
    assert s.sync_database_url == "postgresql://user:pass@dpg-abc123xyz:5432/taskdb_render"


def test_database_url_sslmode_conversion():
    """Verifies that sslmode query parameters are converted for asyncpg and syncpg."""
    external_render_url = "postgresql://user:pass@dpg-abc123xyz.oregon-postgres.render.com:5432/taskdb?sslmode=require"
    async_url = normalize_async_database_url(external_render_url)
    assert "ssl=require" in async_url
    assert "sslmode=" not in async_url

    sync_url = normalize_sync_database_url(async_url)
    assert "sslmode=require" in sync_url


def test_render_database_and_redis_env_aliases():
    """Verifies that INTERNAL_DATABASE_URL, POSTGRES_URL, and INTERNAL_REDIS_URL are recognized."""
    # Test INTERNAL_DATABASE_URL
    s1 = Settings(_env_file=None, INTERNAL_DATABASE_URL="postgres://user:pass@dpg-internal:5432/db1")
    assert s1.async_database_url == "postgresql+asyncpg://user:pass@dpg-internal:5432/db1"

    # Test POSTGRES_URL
    s2 = Settings(_env_file=None, POSTGRES_URL="postgresql://user:pass@dpg-internal:5432/db2")
    assert s2.async_database_url == "postgresql+asyncpg://user:pass@dpg-internal:5432/db2"

    # Test INTERNAL_REDIS_URL
    s3 = Settings(_env_file=None, INTERNAL_REDIS_URL="redis://red-internal:6379")
    assert s3.redis_connection_url == "redis://red-internal:6379"


def test_render_missing_database_url_error(monkeypatch: pytest.MonkeyPatch):
    """Verifies that running on Render without DATABASE_URL raises a clear descriptive error."""
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("INTERNAL_DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_URL", raising=False)

    s = Settings(_env_file=None)
    with pytest.raises(ValueError, match="DATABASE_URL is not set on Render"):
        _ = s.async_database_url

    with pytest.raises(ValueError, match="DATABASE_URL is not set on Render"):
        _ = s.sync_database_url


def test_render_missing_redis_url_error(monkeypatch: pytest.MonkeyPatch):
    """Verifies that running on Render without REDIS_URL raises a clear descriptive error."""
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("INTERNAL_REDIS_URL", raising=False)

    s = Settings(_env_file=None)
    with pytest.raises(ValueError, match="REDIS_URL is not set on Render"):
        _ = s.redis_connection_url


def test_app_server_entrypoint():
    """Verifies that app.server exists and exposes the FastAPI app and create_app factory."""
    import app.server as server_module
    assert hasattr(server_module, "app")
    assert hasattr(server_module, "create_app")
    assert server_module.app is not None


def test_worker_instance_id_and_concurrency(monkeypatch: pytest.MonkeyPatch):
    """Verifies worker ID uniqueness using RENDER_INSTANCE_ID and env-configured concurrency."""
    # 1. Using RENDER_INSTANCE_ID (Render multi-instance deployments)
    monkeypatch.setenv("RENDER_INSTANCE_ID", "srv-render-inst-42")
    monkeypatch.delenv("WORKER_ID", raising=False)
    monkeypatch.setenv("WORKER_CONCURRENCY", "8")

    w1 = WorkerProcess()
    assert "worker-srv-render-inst-42-" in w1.worker_id
    assert w1.concurrency == 8

    # Another instance gets a unique random suffix
    w2 = WorkerProcess()
    assert "worker-srv-render-inst-42-" in w2.worker_id
    assert w1.worker_id != w2.worker_id

    # 2. Explicit WORKER_ID override
    monkeypatch.setenv("WORKER_ID", "fixed-worker-alpha")
    w3 = WorkerProcess()
    assert w3.worker_id == "fixed-worker-alpha"

    # 3. Invalid concurrency falls back to default
    monkeypatch.setenv("WORKER_CONCURRENCY", "invalid")
    w4 = WorkerProcess()
    assert w4.concurrency == get_settings().WORKER_CONCURRENCY


@pytest.mark.asyncio
async def test_api_key_auth_disabled_by_default(async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch):
    """Verifies that when API_KEY is unset, all endpoints work without authentication."""
    settings = get_settings()
    monkeypatch.setattr(settings, "API_KEY", None)

    # Health probes work
    r_live = await async_client.get("/health/live")
    assert r_live.status_code == 200

    r_ready = await async_client.get("/health/ready")
    assert r_ready.status_code in (200, 503)

    # Metrics and docs work
    r_metrics = await async_client.get("/metrics")
    assert r_metrics.status_code == 200

    r_docs = await async_client.get("/docs")
    assert r_docs.status_code == 200

    # Jobs and workers work without any key
    r_jobs = await async_client.get("/api/v1/jobs")
    assert r_jobs.status_code == 200

    r_workers = await async_client.get("/api/v1/workers")
    assert r_workers.status_code == 200


@pytest.mark.asyncio
async def test_api_key_auth_enabled_protects_endpoints(async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch):
    """Verifies that when API_KEY is set, protected routes require X-API-Key and exempt routes do not."""
    secret = "test-render-api-key-xyz987"
    settings = get_settings()
    monkeypatch.setattr(settings, "API_KEY", secret)

    # 1. Exempt endpoints MUST NOT require auth
    r_live = await async_client.get("/health/live")
    assert r_live.status_code == 200

    r_metrics = await async_client.get("/metrics")
    assert r_metrics.status_code == 200

    r_docs = await async_client.get("/docs")
    assert r_docs.status_code == 200

    r_openapi = await async_client.get("/openapi.json")
    assert r_openapi.status_code == 200

    # 2. Protected endpoints without auth header -> 401 Unauthorized
    r_no_auth = await async_client.get("/api/v1/jobs")
    assert r_no_auth.status_code == 401
    assert r_no_auth.json() == {"detail": "Invalid or missing API key"}

    r_no_auth_root = await async_client.get("/jobs")
    assert r_no_auth_root.status_code == 401

    r_no_auth_workers = await async_client.get("/api/v1/workers")
    assert r_no_auth_workers.status_code == 401

    # 3. Protected endpoints with wrong key -> 401
    r_bad_auth = await async_client.get("/api/v1/jobs", headers={"X-API-Key": "wrong-key"})
    assert r_bad_auth.status_code == 401

    # 4. Protected endpoints with valid X-API-Key -> 200 OK
    r_auth_jobs = await async_client.get("/api/v1/jobs", headers={"X-API-Key": secret})
    assert r_auth_jobs.status_code == 200

    r_auth_workers = await async_client.get("/api/v1/workers", headers={"X-API-Key": secret})
    assert r_auth_workers.status_code == 200
