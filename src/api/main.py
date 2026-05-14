"""
src/api/main.py
══════════════════════════════════════════════════════════════════════════════
FASTAPI APPLICATION FACTORY

STARTUP SEQUENCE (updated Stage 3.4):
  1. Uvicorn starts and imports this module
  2. FastAPI app is created with metadata
  3. CORS middleware is added
  4. Global exception handlers registered
  5. All routers mounted at /api/v1
  6. @app.on_event("startup") runs:
     a. Tests database connection
     b. Queries Supabase for production model version
     c. Downloads artifacts from HF Hub → loads into app.state.model_service
     d. Runs smoke test on loaded model
  7. App is ready to accept requests

HOW TO RUN:
  Development:  python scripts/run_api.py
  Production:   uvicorn src.api.main:app --host 0.0.0.0 --port $PORT
══════════════════════════════════════════════════════════════════════════════
"""

import logging
import sys
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.connection import DatabaseConnection
from src.api.config import get_settings
from src.api.models.responses import ErrorResponse, HealthResponse

from src.api.routes.admin       import router as admin_router
from src.api.routes.customers   import router as customers_router
from src.api.routes.events      import router as events_router
from src.api.routes.predictions import router as predictions_router   # ← NEW

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

settings = get_settings()

app = FastAPI(
    title="ChurnGuard API",
    description="""
    Internal API for the ChurnGuard customer churn prediction system.

    **Four responsibilities:**
    - **Customer registration**: accepts form submissions, inserts into Supabase
    - **Predictions**: POST /predict/single — scores a customer with SHAP reasons
    - **Dashboard data**: serves the operations dashboard
    - **SSE event stream**: pushes real-time events to the dashboard live feed

    **Authentication**: Admin endpoints require `X-Admin-Key` header.
    """,
    version="1.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS ─────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "OPTIONS"],
    allow_headers=["Content-Type", "X-Admin-Key"],
)
logger.info(f"CORS configured for origins: {settings.cors_origins_list}")


# ── EXCEPTION HANDLERS ────────────────────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error(
        f"Unhandled exception on {request.method} {request.url.path}: {exc}",
        exc_info=True,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            message="An internal server error occurred. The error has been logged."
        ).model_dump(),
    )


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    logger.warning(f"ValueError on {request.url.path}: {exc}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=ErrorResponse(message=str(exc)).model_dump(),
    )


# ── ROUTERS ───────────────────────────────────────────────────────────────────
app.include_router(customers_router,   prefix=settings.api_v1_prefix)
app.include_router(admin_router,       prefix=settings.api_v1_prefix)
app.include_router(events_router,      prefix=settings.api_v1_prefix)
app.include_router(predictions_router, prefix=settings.api_v1_prefix)   # ← NEW

logger.info(f"Routers mounted at prefix: {settings.api_v1_prefix}")


# ── HEALTH CHECK ─────────────────────────────────────────────────────────────
@app.get(
    "/api/v1/health",
    response_model=HealthResponse,
    tags=["System"],
    summary="Health check — no auth required",
)
def health_check() -> HealthResponse:
    """Simple health check. Tests DB connectivity."""
    logger.info("GET /api/v1/health")
    db_ok = False
    try:
        db = DatabaseConnection()
        db.connect()
        db_ok = db.health_check()
        db.disconnect()
    except Exception as e:
        logger.warning(f"  DB health check failed: {e}")

    # Include model status in health response
    ms = getattr(app.state, "model_service", None)
    model_ready = ms is not None and ms.is_loaded

    return HealthResponse(
        status="healthy" if (db_ok and model_ready) else "degraded",
        environment=settings.environment,
        version="1.1.0",
        db_connected=db_ok,
    )


# ── STARTUP EVENT ─────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event() -> None:
    """
    Application startup tasks — runs once when Uvicorn initialises.

    Stage 3.4 additions:
        b. Query Supabase for production model artifact_path
        c. Download artifacts from HF Hub → load into app.state.model_service
    """
    logger.info("=" * 60)
    logger.info("  CHURNGUARD API — STARTING UP  (v1.1.0)")
    logger.info("=" * 60)
    logger.info(f"  Environment:  {settings.environment}")
    logger.info(f"  API prefix:   {settings.api_v1_prefix}")
    logger.info(f"  CORS origins: {settings.cors_origins_list}")

    # ── a. Test database connection ───────────────────────────────────────────
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
        logger.error(f"  ✗ Database connection FAILED at startup: {e}")

    # ── b. Load production model from HF Hub ─────────────────────────────────
    logger.info("  Loading production model from HF Hub...")
    try:
        # Query Supabase for the production model's artifact_path
        # This is the source of truth — whoever has status='production' gets loaded
        db = DatabaseConnection()
        db.connect()
        rows = db.execute_query(
            "SELECT version, artifact_path FROM model_versions "
            "WHERE status = 'production' ORDER BY created_at DESC LIMIT 1;"
        )
        db.disconnect()

        if not rows:
            logger.error(
                "  ✗ No production model found in model_versions table.\n"
                "    Run scripts/run_training.py first."
            )
            app.state.model_service = None
            return

        version       = rows[0]["version"]
        artifact_path = rows[0]["artifact_path"]
        logger.info(
            f"  Production model: version={version}"
            f"  artifact_path={artifact_path}"
        )

        # Download and load all artifacts
        from src.api.services.model_service import ModelLoader
        loader = ModelLoader()
        loader.load(artifact_path=artifact_path)

        # Store in app.state — shared across all requests
        # Access in routes via: request.app.state.model_service
        app.state.model_service = loader
        logger.info(f"  ✓ Model v{version} loaded and ready for inference")

    except Exception as e:
        logger.error(
            f"  ✗ Model loading FAILED at startup: {e}\n"
            "    Prediction endpoints will return 503 until the model is loaded.\n"
            "    Check HF Hub connectivity and HUGGINGFACE_TOKEN in .env",
            exc_info=True,
        )
        app.state.model_service = None

    logger.info("=" * 60)
    logger.info("  API ready. Docs available at /docs")
    logger.info("=" * 60)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    logger.info("ChurnGuard API shutting down...")
