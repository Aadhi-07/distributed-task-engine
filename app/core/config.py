import os
from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


def normalize_async_database_url(url: Optional[str]) -> Optional[str]:
    """Normalizes Postgres connection URLs to asyncpg dialect (postgresql+asyncpg://).
    
    Handles standard Render database URLs (which begin with postgresql:// or postgres://)
    as well as existing asyncpg schemes and sslmode parameter translation for asyncpg.
    """
    if not url:
        return url
    if url.startswith("postgres://"):
        url = "postgresql+asyncpg://" + url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        url = "postgresql+asyncpg://" + url[len("postgresql://"):]

    # asyncpg uses 'ssl' keyword parameter instead of psycopg2's 'sslmode'
    if "sslmode=" in url:
        url = url.replace("sslmode=require", "ssl=require")
        url = url.replace("sslmode=prefer", "ssl=prefer")
        url = url.replace("sslmode=verify-ca", "ssl=verify-ca")
        url = url.replace("sslmode=verify-full", "ssl=verify-full")
        url = url.replace("sslmode=disable", "ssl=disable")
    return url


def normalize_sync_database_url(url: Optional[str]) -> Optional[str]:
    """Normalizes Postgres connection URLs to synchronous driver dialect (postgresql://)."""
    if not url:
        return url
    if url.startswith("postgresql+asyncpg://"):
        url = "postgresql://" + url[len("postgresql+asyncpg://"):]
    elif url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if "ssl=require" in url and "sslmode=" not in url:
        url = url.replace("ssl=require", "sslmode=require")
    return url


def normalize_redis_url(url: Optional[str]) -> Optional[str]:
    """Normalizes Redis URLs, mapping localhost to 127.0.0.1 to avoid Windows IPv6 resolution latency."""
    if not url:
        return url
    if "://localhost" in url:
        return url.replace("://localhost", "://127.0.0.1", 1)
    elif "://@localhost" in url:
        return url.replace("://@localhost", "://@127.0.0.1", 1)
    return url


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    PROJECT_NAME: str = "Distributed Task Execution Engine"
    VERSION: str = "0.1.0"
    ENV: str = "development"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 10000
    PORT: Optional[int] = None
    API_KEY: Optional[str] = None

    # PostgreSQL Persistence
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "task_engine"
    DATABASE_URL: Optional[str] = None
    INTERNAL_DATABASE_URL: Optional[str] = None
    POSTGRES_URL: Optional[str] = None

    # Redis Queue & Coordination
    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    REDIS_URL: Optional[str] = None
    INTERNAL_REDIS_URL: Optional[str] = None

    # Worker Defaults
    WORKER_CONCURRENCY: int = 4
    WORKER_HEARTBEAT_INTERVAL: int = 5
    WORKER_DEAD_THRESHOLD: int = 15
    DEFAULT_JOB_TIMEOUT: int = 300

    @property
    def server_port(self) -> int:
        """Effective API port binding, preferring $PORT (Render) over API_PORT."""
        return self.PORT or self.API_PORT

    @property
    def effective_database_url(self) -> Optional[str]:
        """Returns the configured database URL across standard environment variable aliases."""
        return self.DATABASE_URL or self.INTERNAL_DATABASE_URL or self.POSTGRES_URL

    @property
    def effective_redis_url(self) -> Optional[str]:
        """Returns the configured Redis URL across standard environment variable aliases."""
        return self.REDIS_URL or self.INTERNAL_REDIS_URL

    @property
    def async_database_url(self) -> str:
        """Returns the async SQLAlchemy database URL (using asyncpg)."""
        db_url = self.effective_database_url
        if db_url:
            return normalize_async_database_url(db_url)

        # If running on Render without a database connection URL, raise an explicit actionable error
        is_render = os.environ.get("RENDER") == "true" or bool(os.environ.get("RENDER_SERVICE_ID"))
        if is_render:
            raise ValueError(
                "DATABASE_URL is not set on Render! "
                "Your service is running on Render, but no PostgreSQL database connection URL was configured. "
                "To resolve: "
                "1. In your Render Dashboard, open your PostgreSQL instance (or create one under 'New + -> PostgreSQL'). "
                "2. Under 'Connections', copy the 'Internal Database URL'. "
                "3. In your Web Service settings -> 'Environment', add an environment variable named 'DATABASE_URL' and paste the URL. "
                "4. Save changes to redeploy."
            )

        host = "127.0.0.1" if self.POSTGRES_HOST == "localhost" else self.POSTGRES_HOST
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{host}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def sync_database_url(self) -> str:
        """Returns the sync SQLAlchemy database URL (used by Alembic migrations)."""
        db_url = self.effective_database_url
        if db_url:
            return normalize_sync_database_url(db_url)

        is_render = os.environ.get("RENDER") == "true" or bool(os.environ.get("RENDER_SERVICE_ID"))
        if is_render:
            raise ValueError(
                "DATABASE_URL is not set on Render! "
                "Your service is running on Render, but no PostgreSQL database connection URL was configured. "
                "To resolve: "
                "1. In your Render Dashboard, open your PostgreSQL instance (or create one under 'New + -> PostgreSQL'). "
                "2. Under 'Connections', copy the 'Internal Database URL'. "
                "3. In your Web Service settings -> 'Environment', add an environment variable named 'DATABASE_URL' and paste the URL. "
                "4. Save changes to redeploy."
            )

        host = "127.0.0.1" if self.POSTGRES_HOST == "localhost" else self.POSTGRES_HOST
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{host}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def redis_connection_url(self) -> str:
        """Returns standard Redis connection string."""
        redis_url = self.effective_redis_url
        if redis_url:
            return normalize_redis_url(redis_url)

        is_render = os.environ.get("RENDER") == "true" or bool(os.environ.get("RENDER_SERVICE_ID"))
        if is_render:
            raise ValueError(
                "REDIS_URL is not set on Render! "
                "Your service is running on Render, but no Redis connection URL was configured. "
                "To resolve: "
                "1. In your Render Dashboard, open your Redis / Key-Value service. "
                "2. Copy the 'Internal Connection String'. "
                "3. In your Web Service settings -> 'Environment', add an environment variable named 'REDIS_URL' and paste the string. "
                "4. Save changes to redeploy."
            )

        host = "127.0.0.1" if self.REDIS_HOST == "localhost" else self.REDIS_HOST
        auth_part = f":{self.REDIS_PASSWORD}@" if self.REDIS_PASSWORD else ""
        return f"redis://{auth_part}{host}:{self.REDIS_PORT}/{self.REDIS_DB}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
