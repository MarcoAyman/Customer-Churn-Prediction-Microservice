"""
scripts/run_api.py
══════════════════════════════════════════════════════════════════════════════
CLI entry point for the ChurnGuard FastAPI application.

USAGE:
  Development (auto-reload on file changes):
    python scripts/run_api.py

  Production (Render runs this via the Dockerfile CMD):
    uvicorn src.api.main:app --host 0.0.0.0 --port $PORT

WHAT THIS SCRIPT DOES:
  - Adds project root to sys.path so all module imports resolve
  - Calls uvicorn.run() with environment-appropriate settings
  - Development: hot-reload enabled, port 8000
  - Production:  reload disabled, port from $PORT env var (Render sets this)
══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import uvicorn
from src.api.config import get_settings

if __name__ == "__main__":
    settings = get_settings()

    # Render sets the PORT environment variable automatically.
    # Locally, default to 8000.
    port = int(os.environ.get("PORT", 8000))

    print(f"Starting ChurnGuard API on port {port}...")
    print(f"Environment: {settings.environment}")
    print(f"Docs: http://localhost:{port}/docs")

    uvicorn.run(
        # The app object — 'src.api.main:app' means:
        #   module path = src.api.main
        #   variable    = app (the FastAPI instance)
        app="src.api.main:app",

        host="0.0.0.0",   # listen on all interfaces (required for Render)
        port=port,

        # Hot-reload in development only.
        # Reload watches for file changes and restarts the server automatically.
        # NEVER enable reload in production — it consumes extra memory.
        reload=settings.is_development,

        # Log level — INFO in production, DEBUG locally if needed
        log_level="info",
    )
