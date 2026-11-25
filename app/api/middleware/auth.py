"""API key authentication middleware for protecting public endpoints on Render."""
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.core.config import get_settings


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    """Enforces API key authentication when API_KEY is set in configuration.
    
    Exempt endpoints:
      - /health* (liveness and readiness probes)
      - /metrics* (Prometheus scraping)
      - /docs*, /redoc*, /openapi.json (API documentation)
      - /dashboard* (Internal visual topology dashboard)
    
    Protected endpoints:
      - /api/v1/jobs*, /jobs*
      - /api/v1/workers*, /workers*
    """

    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        api_key = settings.API_KEY

        # If API_KEY is unset or empty, auth is disabled (backward-compatible)
        if not api_key:
            return await call_next(request)

        path = request.url.path

        # Check exemptions first
        is_exempt = (
            path == "/health"
            or path.startswith("/health/")
            or path == "/api/v1/health"
            or path.startswith("/api/v1/health/")
            or path == "/metrics"
            or path.startswith("/metrics/")
            or path == "/api/v1/metrics"
            or path.startswith("/api/v1/metrics/")
            or path in ("/docs", "/redoc", "/openapi.json")
            or path.startswith("/docs/")
            or path.startswith("/redoc/")
            or path == "/dashboard"
            or path.startswith("/dashboard/")
            or path.startswith("/api/v1/dashboard")
        )

        if is_exempt:
            return await call_next(request)

        # Check protected paths
        is_protected = (
            path == "/jobs"
            or path.startswith("/jobs/")
            or path == "/api/v1/jobs"
            or path.startswith("/api/v1/jobs/")
            or path == "/workers"
            or path.startswith("/workers/")
            or path == "/api/v1/workers"
            or path.startswith("/api/v1/workers/")
            or path == "/api/jobs"
            or path.startswith("/api/jobs/")
            or path == "/api/workers"
            or path.startswith("/api/workers/")
        )

        if is_protected:
            client_key = request.headers.get("X-API-Key")
            if not client_key or client_key != api_key:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Invalid or missing API key"},
                )

        return await call_next(request)
