"""FastAPI application entrypoint for Render and production deployments.

Allows running directly via:
    python -m app.server
    uvicorn app.server:app
"""
import os
import sys
from pathlib import Path

# Ensure project root is in sys.path regardless of execution environment
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import uvicorn

from app.api.main import app, create_app
from app.core.config import get_settings

__all__ = ["app", "create_app"]

if __name__ == "__main__":
    settings = get_settings()
    port = int(os.environ.get("PORT", settings.server_port))
    uvicorn.run(
        "app.server:app",
        host="0.0.0.0",
        port=port,
        reload=False,
    )
