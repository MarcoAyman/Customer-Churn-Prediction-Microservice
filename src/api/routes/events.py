"""
src/api/routes/events.py
══════════════════════════════════════════════════════════════════════════════
EVENTS ROUTER — Server-Sent Events streaming endpoint.

ENDPOINT:
  GET /api/v1/admin/events
    → Opens a persistent SSE connection
    → Streams events from the SSEService queue as they arrive
    → Sends a keepalive ping every 30s to prevent Render from closing the connection
    → Used by: src/operational_dashboard (the EventFeed component via useSSE hook)

HOW SSE WORKS (technical):
  1. Browser calls GET /api/v1/admin/events
  2. FastAPI keeps the HTTP response open (does not close it)
  3. FastAPI yields chunks of text in the format:
       data: {"event_type": "new_customer", ...}\n\n
  4. Browser's EventSource fires onmessage for each \n\n-terminated chunk
  5. Connection stays open until the browser closes it or the server restarts

WHY StreamingResponse AND NOT WebSocket?
  SSE is one-directional: server → client.
  The dashboard only needs to RECEIVE events, never send them.
  SSE is simpler than WebSocket, works through HTTP/1.1,
  and auto-reconnects natively in the browser.
  WebSocket would add complexity for no benefit here.
══════════════════════════════════════════════════════════════════════════════
"""

import json
import logging

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from src.api.dependencies import verify_admin
from src.api.services.sse_service import sse_service

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/admin",
    tags=["SSE Events"],
    dependencies=[Depends(verify_admin)],
)


# ─────────────────────────────────────────────────────────────────────────────
# GET /admin/events — the SSE streaming endpoint
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/events",
    summary="SSE event stream for the live dashboard feed",
    description="""
    Opens a Server-Sent Events (SSE) connection.
    The client receives real-time events as they occur:
    - `new_customer`: a customer registered via the entry form
    - `batch_completed`: a batch scoring run finished
    - `high_churn_alert`: a customer crossed the HIGH risk threshold
    - `drift_alert`: feature drift PSI exceeded 0.20
    - `model_promoted`: a new model version went to production
    - `ping`: keepalive (every 30s, prevents Render connection timeout)

    **Used by**: `useSSE.js` hook in the operational dashboard.
    """,
    # Tell OpenAPI this endpoint returns a stream, not JSON
    response_class=StreamingResponse,
)
async def stream_sse_events() -> StreamingResponse:
    """
    Open a persistent SSE connection and stream events.

    Returns a StreamingResponse with Content-Type: text/event-stream.
    The async generator runs indefinitely until the client disconnects.
    """
    logger.info("GET /admin/events — SSE connection opened")

    async def event_generator():
        """
        Async generator that formats events as SSE-spec text chunks.

        SSE format (per spec):
          data: <JSON string>\n\n

        The double newline (\n\n) is the event terminator.
        The browser's EventSource fires onmessage when it receives \n\n.

        Each event is a JSON-serialised dict with keys:
          id, event_type, payload, created_at
        """
        # Count events for this connection (for logging)
        event_count = 0

        try:
            # Listen to the SSEService queue — yields events as they arrive
            # The queue blocks (without CPU) when empty, yields pings every 25s
            async for event in sse_service.listen():

                event_count += 1
                event_type = event.get("event_type", "unknown")

                # Format as SSE text:
                #   data: {"event_type": "new_customer", ...}
                #   (blank line)
                sse_text = f"data: {json.dumps(event)}\n\n"

                if event_type != "ping":
                    # Log real events (not pings — too noisy)
                    logger.info(
                        f"  SSE → client: type='{event_type}' "
                        f"(event #{event_count} on this connection)"
                    )
                else:
                    logger.debug(f"  SSE → ping (event #{event_count})")

                # Yield the formatted text chunk to the StreamingResponse
                yield sse_text

        except Exception as e:
            # If the generator throws (e.g. client disconnects abruptly),
            # log the disconnection reason and stop cleanly
            logger.info(
                f"  SSE connection closed: {type(e).__name__}: {e} "
                f"(streamed {event_count} events)"
            )

        finally:
            logger.info(
                f"  SSE connection closed. Total events streamed: {event_count}"
            )

    # Return a StreamingResponse with the correct Content-Type.
    # Content-Type: text/event-stream is required for the browser's
    # EventSource to recognise this as an SSE stream.
    return StreamingResponse(
        content=event_generator(),
        media_type="text/event-stream",
        headers={
            # Disable caching — SSE must always be live
            "Cache-Control": "no-cache",
            # Keep the connection open — do not close after response
            "Connection": "keep-alive",
            # Required for some proxy configurations (Render, nginx)
            # to pass through streaming data without buffering
            "X-Accel-Buffering": "no",
        },
    )
