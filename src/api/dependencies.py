"""
src/api/dependencies.py
══════════════════════════════════════════════════════════════════════════════
FASTAPI DEPENDENCIES — shared objects injected into route handlers.

WHY DEPENDENCIES?
  FastAPI's dependency injection system lets you declare shared resources
  (database connection, admin auth check) once here, and inject them into
  any route that needs them with a single line:

    @router.get("/example")
    async def my_route(db: DatabaseConnection = Depends(get_db)):
        rows = db.execute_query("SELECT 1")

  This means:
    - Routes never manage connection lifecycle (open/close)
    - Admin protection is one line, not repeated in every protected route
    - Tests can swap real DB for a mock by overriding the dependency

DEPENDENCIES DEFINED HERE:
  get_db()         — yields a connected DatabaseConnection, closes on request end
  verify_admin()   — checks X-Admin-Key header, raises 403 if wrong
══════════════════════════════════════════════════════════════════════════════
"""

import logging
from typing import Generator

from fastapi import Depends, Header, HTTPException, status

# Our reusable connection class from the database module
from database.connection import DatabaseConnection
from src.api.config import get_settings

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE DEPENDENCY
# ─────────────────────────────────────────────────────────────────────────────

def get_db() -> Generator[DatabaseConnection, None, None]:
    """
    FastAPI dependency that provides a connected DatabaseConnection.

    HOW IT WORKS:
      FastAPI calls this function before executing the route handler.
      The 'yield' hands the connection to the route.
      Everything after 'yield' (the finally block) runs after the
      route handler returns its response — guaranteed cleanup.

    WHY yield AND NOT return?
      'return' would give the route the connection but FastAPI would have
      no way to clean it up afterwards.
      'yield' turns this into a context manager — FastAPI handles the
      lifecycle automatically.

    USAGE IN A ROUTE:
      @router.get("/example")
      def my_route(db: DatabaseConnection = Depends(get_db)):
          return db.execute_query("SELECT COUNT(*) FROM customers")

    Yields:
        DatabaseConnection: connected, ready to execute queries
    """
    # Create a new connection instance for this request
    db = DatabaseConnection()

    try:
        # Connect to Supabase — opens the connection pool
        db.connect()
        logger.debug("DB connection opened for request")

        # Hand the connected db to the route handler
        yield db

    except ConnectionError as e:
        # If connection fails, return a 503 (service unavailable)
        # so the client knows the database is the problem
        logger.error(f"Database connection failed during request: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection unavailable. Please try again shortly.",
        )

    finally:
        # This runs after every request, whether it succeeded or failed.
        # Closes the connection pool and releases all connections.
        db.disconnect()
        logger.debug("DB connection closed after request")


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN AUTHENTICATION DEPENDENCY
# ─────────────────────────────────────────────────────────────────────────────

def verify_admin(
    x_admin_key: str = Header(
        default=None,
        # The header name in HTTP is X-Admin-Key
        # FastAPI converts underscores to hyphens automatically
        alias="X-Admin-Key",
        description="Admin API key. Required for all admin and batch endpoints.",
    )
) -> str:
    """
    FastAPI dependency that validates the X-Admin-Key header.

    HOW IT WORKS:
      FastAPI reads the X-Admin-Key header from the incoming request.
      We compare it to the ADMIN_API_KEY from settings.
      If it matches, the request proceeds.
      If it does not match (or is missing), we raise HTTP 403 Forbidden.

    WHY A HEADER AND NOT A QUERY PARAMETER?
      Headers are not visible in browser history or server access logs.
      Query parameters (?key=abc) appear in URLs and get logged everywhere.
      For API keys, headers are the correct approach.

    USAGE IN A ROUTE:
      @router.post("/batch")
      def run_batch(_: str = Depends(verify_admin)):
          # only reachable if X-Admin-Key is correct
          ...

    Args:
        x_admin_key: value of the X-Admin-Key header (None if not present)

    Returns:
        str: the validated admin key (rarely used by the caller)

    Raises:
        HTTPException 403: if the key is missing or incorrect
    """
    cfg = get_settings()

    if x_admin_key is None:
        logger.warning("Admin endpoint accessed without X-Admin-Key header")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="X-Admin-Key header is required for this endpoint.",
        )

    # Use a constant-time comparison to prevent timing attacks.
    # A naive == comparison can leak information about the key length
    # through response time differences. secrets.compare_digest is safe.
    import secrets
    if not secrets.compare_digest(x_admin_key, cfg.admin_api_key):
        logger.warning(f"Admin endpoint accessed with invalid key: {x_admin_key[:4]}****")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid admin key.",
        )

    logger.debug("Admin authentication passed")
    return x_admin_key
