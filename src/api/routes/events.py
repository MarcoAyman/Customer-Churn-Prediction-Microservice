"""
src/api/routes/events.py
══════════════════════════════════════════════════════════════════════════════
EVENTS ROUTER — Server-Sent Events streaming endpoint.

FIX APPLIED:
  The browser's EventSource API cannot send custom HTTP headers.
  This is a browser spec limitation — not a code bug.

  Solution: accept the admin key as a URL query parameter for SSE only.
  The header-based auth (verify_admin dependency) is used everywhere else.

  SSE clients connect as:
    GET /api/v1/admin/events?admin_key=YOUR_ADMIN_KEY
══════════════════════════════════════════════════════════════════════════════
"""

import json
import logging
import secrets

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from src.api.config import get_settings
from src.api.services.sse_service import sse_service

logger = logging.getLogger(__name__)

# NOTE: We do NOT add Depends(verify_admin) to this router
# because EventSource cannot send the X-Admin-Key header.
# Authentication is handled manually inside the route via query param.
router = APIRouter(
    prefix="/admin",
    tags=["SSE Events"],
)

settings = get_settings()


# ─────────────────────────────────────────────────────────────────────────────
# GET /admin/events — SSE streaming endpoint
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/events",
    summary="SSE event stream for the live dashboard feed",
    response_class=StreamingResponse,
)
async def stream_sse_events(
    # Admin key passed as a query parameter because EventSource
    # cannot send custom HTTP headers — browser spec limitation.
    # Example: GET /api/v1/admin/events?admin_key=abc123
    admin_key: str = Query(
        default=None,
        alias="admin_key",
        description="Admin API key. Required. Passed as query param because EventSource cannot send headers.",
    )
) -> StreamingResponse:
    """
    Open a persistent SSE connection and stream live events.

    Authentication via ?admin_key= query parameter.
    EventSource (browser) cannot send X-Admin-Key header — this is a browser
    spec limitation, not a bug. Query param is the standard workaround.
    """

    # ── Manual admin key validation ──────────────────────────────────────────
    # We replicate verify_admin() logic here because we cannot use
    # Depends(verify_admin) — FastAPI would try to read the Header,
    # which EventSource never sends.

    if admin_key is None:
        logger.warning("SSE connection attempted without admin_key query param")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="admin_key query parameter is required. "
                   "Usage: GET /api/v1/admin/events?admin_key=YOUR_KEY",
        )

    # Constant-time comparison — prevents timing attacks
    if not secrets.compare_digest(admin_key, settings.admin_api_key):
        logger.warning(
            f"SSE connection attempted with invalid admin_key: "
            f"{admin_key[:4]}****"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid admin_key.",
        )

    logger.info("GET /admin/events — SSE connection authenticated and opened")

    # ── Event generator ───────────────────────────────────────────────────────
    async def event_generator():
        """
        Yields SSE-formatted text chunks.
        SSE wire format: "data: <JSON>\n\n"
        Double newline is the event terminator that triggers onmessage in browser.
        """
        event_count = 0
        try:
            async for event in sse_service.listen():
                event_count += 1
                event_type = event.get("event_type", "unknown")

                # Format as SSE text — the browser EventSource reads this
                sse_text = f"data: {json.dumps(event)}\n\n"

                if event_type != "ping":
                    logger.info(
                        f"  SSE → client: type='{event_type}' "
                        f"(#{event_count} on this connection)"
                    )

                yield sse_text

        except Exception as e:
            logger.info(
                f"  SSE connection closed: {type(e).__name__} "
                f"(streamed {event_count} events)"
            )
        finally:
            logger.info(f"  SSE connection ended. Events streamed: {event_count}")

    return StreamingResponse(
        content=event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",       # never cache SSE responses
            "Connection":        "keep-alive",     # keep HTTP connection open
            "X-Accel-Buffering": "no",             # disable Render/nginx buffering
        },
    )
