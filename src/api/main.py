"""
src/api/main.py
══════════════════════════════════════════════════════════════════════════════
FASTAPI APPLICATION FACTORY

This file creates the FastAPI app, configures it, and mounts all routers.
It is the entry point for the entire API.

STARTUP SEQUENCE:
  1. Uvicorn starts and imports this module
  2. FastAPI app is created with metadata
  3. CORS middleware is added (allows frontend origins)
  4. Global exception handlers are registered
  5. All routers are mounted at /api/v1
  6. @app.on_event("startup") runs:
     - Tests database connection
     - Logs configuration summary
  7. App is ready to accept requests

HOW TO RUN:
  Development:
    python scripts/run_api.py
    OR
    uvicorn src.api.main:app --reload --port 8000

  Production (Render):
    uvicorn src.api.main:app --host 0.0.0.0 --port $PORT
══════════════════════════════════════════════════════════════════════════════
"""

import logging
import sys
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Add project root to sys.path so all imports resolve correctly
# when running from any directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.connection import DatabaseConnection
from src.api.config import get_settings
from src.api.models.responses import ErrorResponse, HealthResponse

# Import all routers
from src.api.routes.admin     import router as admin_router
from src.api.routes.customers import router as customers_router
from src.api.routes.events    import router as events_router

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# Configure logging before anything else so startup messages are captured
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI APP CREATION
# ─────────────────────────────────────────────────────────────────────────────

settings = get_settings()

app = FastAPI(
    title="ChurnGuard API",
    description="""
    Internal API for the ChurnGuard customer churn prediction system.

    **Three responsibilities:**
    - **Customer registration**: accepts form submissions from the entry form,
      inserts into Supabase, creates initial feature snapshot
    - **Dashboard data**: serves the operations dashboard with risk summaries,
      churn trends, at-risk customers, and drift reports
    - **SSE event stream**: pushes real-time events to the dashboard live feed

    **Authentication**: Admin endpoints require `X-Admin-Key` header.
    """,
    version="1.0.0",
    # OpenAPI docs available at /docs (Swagger UI) and /redoc
    docs_url="/docs",
    redoc_url="/redoc",
)


# ─────────────────────────────────────────────────────────────────────────────
# CORS MIDDLEWARE
#
# WHY CORS?
#   The frontend (Vercel, localhost:3000) and the API (Render, localhost:8000)
#   are on different origins. Browsers block cross-origin requests by default.
#   CORS middleware tells the browser: "these origins are allowed".
#
# CRITICAL: allow_credentials=False is intentional.
#   We use header-based auth (X-Admin-Key), not cookies.
#   Credentials mode is not needed and introduces security complexity.
# ─────────────────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,   # from .env: CORS_ORIGINS=http://...
    allow_credentials=False,   # no cookies — header auth only
    allow_methods=["GET", "POST", "PUT", "OPTIONS"],   # OPTIONS required for preflight
    allow_headers=["Content-Type", "X-Admin-Key"],     # X-Admin-Key must be explicitly listed
)

logger.info(f"CORS configured for origins: {settings.cors_origins_list}")


# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL EXCEPTION HANDLERS
#
# These catch exceptions that escape route handlers and return clean JSON
# instead of exposing raw Python tracebacks to the client.
# ─────────────────────────────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Catch-all handler for any unhandled exception.
    Logs the full traceback server-side, returns a clean JSON error to client.
    """
    logger.error(
        f"Unhandled exception on {request.method} {request.url.path}: {exc}",
        exc_info=True,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            message="An internal server error occurred. "
                     "The error has been logged."
        ).model_dump(),
    )


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    """
    Handle ValueError — typically raised by service layer for validation issues.
    Returns 422 Unprocessable Entity.
    """
    logger.warning(f"ValueError on {request.url.path}: {exc}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=ErrorResponse(message=str(exc)).model_dump(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# MOUNT ROUTERS
#
# All routes are prefixed with /api/v1 for versioning.
# Full paths:
#   /api/v1/customers/register
#   /api/v1/admin/overview
#   /api/v1/admin/events   (SSE)
# ─────────────────────────────────────────────────────────────────────────────

app.include_router(customers_router, prefix=settings.api_v1_prefix)
app.include_router(admin_router,     prefix=settings.api_v1_prefix)
app.include_router(events_router,    prefix=settings.api_v1_prefix)

logger.info(f"Routers mounted at prefix: {settings.api_v1_prefix}")


# ─────────────────────────────────────────────────────────────────────────────
# HEALTH CHECK ENDPOINT
# No auth required — used by Render health checks and GitHub Actions warm-up ping
# ─────────────────────────────────────────────────────────────────────────────

@app.get(
    "/api/v1/health",
    response_model=HealthResponse,
    tags=["System"],
    summary="Health check — no auth required",
    description="Used by Render health checks and GitHub Actions warm-up ping.",
)
def health_check() -> HealthResponse:
    """
    Simple health check endpoint.
    Tests DB connectivity and returns current status.

    GitHub Actions warm-up script calls this with 3 retries before
    triggering the batch scoring job — ensures Render has woken up.
    """
    logger.info("GET /api/v1/health")

    # Quick DB connectivity check
    db_ok = False
    try:
        db = DatabaseConnection()
        db.connect()
        db_ok = db.health_check()
        db.disconnect()
    except Exception as e:
        logger.warning(f"  DB health check failed: {e}")

    return HealthResponse(
        status="healthy" if db_ok else "degraded",
        environment=settings.environment,
        version="1.0.0",
        db_connected=db_ok,
    )


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP EVENT
# Runs once when Uvicorn starts the application
# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")#Connecting to Supabase involves sending a signal across the internet. That takes time (milliseconds). By making this async, FastAPI can handle the "bookkeeping" of starting the rest of the app while the database signal is traveling through the cables.

async def startup_event() -> None:
    """
    Application startup tasks.
    Runs once when Uvicorn initialises the app.
    """
    logger.info("=" * 55)
    logger.info("  CHURNGUARD API — STARTING UP")
    logger.info("=" * 55)
    logger.info(f"  Environment:  {settings.environment}")
    logger.info(f"  API prefix:   {settings.api_v1_prefix}")
    logger.info(f"  CORS origins: {settings.cors_origins_list}")
    logger.info(f"  Admin key:    {'SET' if settings.admin_api_key != 'dev-admin-key-change-in-production' else 'USING DEFAULT (change in production!)'}")

    # Test database connectivity at startup
    logger.info("  Testing database connection...")
    try:
        db = DatabaseConnection()
        db.connect()
        healthy = db.health_check()
        db.disconnect()
        if healthy:
            logger.info("  ✓ Database connection verified")
        else:
            logger.warning("  ⚠ Database health check returned unexpected result")
    except Exception as e:
        logger.error(
            f"  ✗ Database connection FAILED at startup: {e}\n"
            "    The API will start but DB-dependent endpoints will fail.\n"
            "    Check DATABASE_URL in your .env file."
        )

    logger.info("=" * 55)
    logger.info("  API ready. Docs available at /docs")
    logger.info("=" * 55)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """
    Application shutdown tasks.
    Runs when Uvicorn receives a shutdown signal.
    """
    logger.info("ChurnGuard API shutting down...")
