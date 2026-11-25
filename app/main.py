"""FastAPI application entrypoint for uvicorn app.main:app."""
from app.api.main import app, create_app

__all__ = ["app", "create_app"]

if __name__ == "__main__":
    import uvicorn
    from app.core.config import get_settings

    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.server_port,
        reload=settings.DEBUG,
    )
