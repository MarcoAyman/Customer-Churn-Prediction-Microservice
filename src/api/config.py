"""
src/api/config.py
══════════════════════════════════════════════════════════════════════════════
API CONFIGURATION — single source of truth for all settings.

WHY THIS FILE EXISTS:
  Every configurable value reads from the environment here.
  No other file in the API ever calls os.environ directly.
  If a setting changes — its name, its default, its type —
  you change it here. Nothing else breaks.

USAGE IN OTHER FILES:
  from src.api.config import settings
  print(settings.database_url)
  print(settings.cors_origins)

HOW IT WORKS (Pydantic BaseSettings):
  Pydantic reads each field from:
    1. Environment variable (exact name, case-insensitive)
    2. .env file (loaded automatically if env_file is set)
    3. Default value if neither is set
  It also validates types — DATABASE_URL must be a string,
  POOL_MAX_CONNECTIONS must be an int. Wrong types raise on startup.
══════════════════════════════════════════════════════════════════════════════
"""

import logging
from functools import lru_cache   # cache the settings object so it is only
                                   # created once per process (not per request)
from pathlib import Path
from typing import List

from pydantic import field_validator   # for custom validation logic
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS CLASS
# ─────────────────────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    """
    All application settings, read from environment variables or .env file.

    Pydantic validates every field on instantiation.
    The app will refuse to start if required fields are missing or wrong type.
    This is intentional — silent misconfiguration causes worse bugs than a
    loud startup failure.
    """

    # ── Database ─────────────────────────────────────────────────────────────
    # Full Supabase PostgreSQL connection URL.
    # Must use port 6543 (pgBouncer pooler), not 5432 (direct).
    # Set in .env: DATABASE_URL=postgresql://...
    database_url: str

    # Connection pool limits — see DatabaseConnection class for explanation
    pool_min_connections: int = 1
    pool_max_connections: int = 5
    query_timeout_seconds: int = 30

    # ── API Security ──────────────────────────────────────────────────────────
    # Secret key used to protect admin-only endpoints.
    # The dashboard and GitHub Actions batch workflow must send this in the
    # X-Admin-Key header. Set in .env and in Render's environment variables.
    admin_api_key: str = "dev-admin-key-change-in-production"

    # ── CORS Cross-Origin Resource Sharing─────────────────────────────────────────────────────────────────
    # Origins allowed to make cross-origin requests to this API.
    # In production: your Vercel URLs.
    # In development: localhost:3000 (dashboard) and localhost:3001 (entry form).
    # Stored as a comma-separated string in the env var:
    #   CORS_ORIGINS=https://churnguard.vercel.app,https://churnguard-entry.vercel.app
    cors_origins: str = "http://localhost:3000,http://localhost:3001"

    # ── SSE (Server-Sent Events) ──────────────────────────────────────────────
    # How often to send a keepalive ping to prevent Render from closing
    # idle SSE connections (Render closes connections idle for 55+ seconds)
    sse_ping_interval_seconds: int = 30

    # Maximum number of undelivered SSE events to buffer in memory.
    # Events beyond this limit drop the oldest.
    sse_max_queue_size: int = 100

    # ── ML Model ─────────────────────────────────────────────────────────────
    # HuggingFace Hub repository where model artifacts are stored.
    # The batch scoring service downloads model.pkl and preprocessor.pkl from here.
    # Set to empty string until the model is trained and registered.
    huggingface_repo: str = ""

    # HuggingFace API token for downloading from private repos.
    # Set in Render environment variables. Never commit to git.
    hf_token: str = ""

    # ── Application ───────────────────────────────────────────────────────────
    # Environment name — controls log level, debug mode, etc.
    # Values: 'development' | 'production' | 'test'
    environment: str = "development"

    # API version prefix — all routes are mounted under this path
    api_v1_prefix: str = "/api/v1"

    # ── Pydantic Settings config ──────────────────────────────────────────────
    model_config = SettingsConfigDict(
        # Read from .env file at the project root
        env_file=str(Path(__file__).parent.parent.parent / ".env"),
        # Allow extra env vars to exist without raising an error
        extra="ignore",
        # Case-insensitive env var matching
        case_sensitive=False,
    )

    # ── Derived properties (computed from raw settings) ───────────────────────

    @property
    def cors_origins_list(self) -> List[str]:
        """
        Parse the comma-separated CORS_ORIGINS string into a Python list.
        Example: "http://localhost:3000,https://app.vercel.app"
              → ["http://localhost:3000", "https://app.vercel.app"]
        """
        return [origin.strip() for origin in self.cors_origins.split(",")
                if origin.strip()]

    @property
    def is_production(self) -> bool:
        """True when running on Render in production."""
        return self.environment.lower() == "production"

    @property
    def is_development(self) -> bool:
        """True when running locally."""
        return self.environment.lower() == "development"

    # ── Validators ────────────────────────────────────────────────────────────

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        """
        Validate DATABASE_URL format and warn if wrong port is used.
        The app will still start with port 5432, but logs a loud warning.
        """
        if not v.startswith("postgresql://") and not v.startswith("postgres://"):
            raise ValueError(
                "DATABASE_URL must start with 'postgresql://' or 'postgres://'. "
                "Check your .env file."
            )
        if ":5432" in v:
            logger.warning(
                "DATABASE_URL uses port 5432 (direct connection). "
                "Use port 6543 (pgBouncer pooler) for Render deployment. "
                "Direct connections will exhaust Supabase's connection limit."
            )
        return v

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Ensure environment is one of the recognised values."""
        allowed = {"development", "production", "test"}
        if v.lower() not in allowed:
            raise ValueError(f"environment must be one of {allowed}, got '{v}'")
        return v.lower()


# ─────────────────────────────────────────────────────────────────────────────
# SINGLETON — get_settings()
#
# @lru_cache means this function runs only ONCE per process.
# Every subsequent call returns the cached Settings object.
# This is important because Settings reads from the environment and validates
# on every instantiation — we do not want that overhead on every request.
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: mask password in DB URL for safe logging
# ─────────────────────────────────────────────────────────────────────────────

def _mask_db_url(url: str) -> str:
    """Replace the password in a DB URL with **** for log safety."""
    try:
        if "@" in url:
            creds, host = url.rsplit("@", 1)
            scheme, rest = creds.split("://", 1)
            parts = rest.split(":")
            parts[1:] = ["****"]
            return f"{scheme}://{':'.join(parts)}@{host}"
    except Exception:
        pass
    return "postgresql://****:****@****"
    
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the singleton Settings instance.
    Cached after first call — safe to call from anywhere.

    Usage:
        from src.api.config import get_settings
        settings = get_settings()
        print(settings.database_url)
    """
    logger.info("Loading application settings...")
    s = Settings()
    logger.info(f"  Environment: {s.environment}")
    logger.info(f"  API prefix:  {s.api_v1_prefix}")
    logger.info(f"  CORS origins: {s.cors_origins_list}")
    logger.info(f"  DB URL: {_mask_db_url(s.database_url)}")
    return s


# Convenience alias — import this directly in most files
settings = get_settings()


