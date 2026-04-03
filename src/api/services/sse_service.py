"""
src/api/services/sse_service.py
══════════════════════════════════════════════════════════════════════════════
SSE SERVICE — manages the in-memory event queue for the live event feed.

HOW SSE WORKS IN THIS SYSTEM:
  1. Something happens (customer registers, batch completes, drift fires)
  2. The relevant service calls sse_service.publish(event_type, payload)
  3. SSEService stores the event in an asyncio Queue
  4. The SSE route handler (routes/events.py) reads from the queue
     and streams events to the connected admin browser
  5. The browser's EventSource receives the event and the dashboard updates

WHY AN IN-MEMORY QUEUE AND NOT JUST THE DATABASE sse_events TABLE?
  The database sse_events table is the persistent backup — it survives
  Render sleep/wake cycles.
  The in-memory queue is for latency — events from the queue reach the
  browser in milliseconds. Database polling adds 2-5 second delay.

  Both work together:
    publish() → writes to DB (persistent) + puts in queue (fast)
    SSE stream → reads from queue first, falls back to DB on reconnect

WHY asyncio.Queue?
  FastAPI is async. The SSE stream uses 'async for' to iterate events.
  asyncio.Queue is the correct async-safe data structure for this pattern.
  It blocks without consuming CPU when the queue is empty.
══════════════════════════════════════════════════════════════════════════════
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

# Database table for persistent event storage
TABLE_SSE_EVENTS = "sse_events"


# ─────────────────────────────────────────────────────────────────────────────
# SSE SERVICE CLASS
# ─────────────────────────────────────────────────────────────────────────────

class SSEService:
    """
    Manages the in-memory event queue for Server-Sent Events.

    ONE INSTANCE PER APPLICATION PROCESS.
    Created once in main.py and shared across all requests.
    This is the singleton pattern — one queue, many listeners.

    FLOW:
      Any service calls:      sse_service.publish("new_customer", {...})
      SSE route reads with:   async for event in sse_service.listen():
    """

    def __init__(self, max_queue_size: int = 100):
        """
        Initialise the SSE service.

        Args:
            max_queue_size: maximum events to buffer before dropping oldest
        """
        # asyncio.Queue: async-safe FIFO queue for event objects
        # maxsize=0 means unlimited — we manage size manually below
        self._queue: asyncio.Queue = asyncio.Queue()

        # Maximum events to keep in the queue
        self._max_size = max_queue_size

        # Count of total events published (for monitoring/logging)
        self._published_count: int = 0

        logger.info(f"SSEService initialised (max_queue_size={max_queue_size})")

    def publish(
        self,
        event_type: str,
        payload: dict,
        db: Optional[DatabaseConnection] = None,
    ) -> None:
        """
        Publish a new event to the SSE stream.

        Called by any service when something happens:
          - customer_service.py: "new_customer"
          - batch scoring:       "batch_completed"
          - drift detector:      "drift_alert"

        Steps:
          1. Build the event dict with a unique ID and timestamp
          2. Put it in the in-memory queue (immediately visible to listeners)
          3. If db is provided, write to sse_events table (persistent backup)

        Args:
            event_type: one of the sse_event_type_enum values from schema.sql
            payload:    dict of event-specific data (varies by event type)
            db:         optional DatabaseConnection for persistent storage
        """
        # Build the event object
        event = {
            "id":         str(uuid4()),    # unique ID for deduplication
            "event_type": event_type,
            "payload":    payload,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # If queue is full, drop the oldest event to make room
        # This prevents unbounded memory growth if no one is listening
        if self._queue.qsize() >= self._max_size:
            try:
                dropped = self._queue.get_nowait()   # remove oldest
                logger.warning(
                    f"SSE queue full ({self._max_size}). "
                    f"Dropped oldest event: {dropped.get('event_type')}"
                )
            except asyncio.QueueEmpty:
                pass   # queue emptied between check and get — fine

        # Put the new event in the queue — non-blocking (put_nowait)
        # put_nowait raises QueueFull if the queue is at maxsize,
        # but we just trimmed it above so this should not happen
        try:
            self._queue.put_nowait(event)
            self._published_count += 1
            logger.info(
                f"  SSE event published: type='{event_type}' "
                f"id={event['id'][:8]}... "
                f"(queue size: {self._queue.qsize()})"
            )
        except asyncio.QueueFull:
            logger.error(f"SSE queue full — event dropped: {event_type}")

        # Persist to database if a connection was provided
        # Database write is non-critical — SSE works without it,
        # but persistence allows reconnecting clients to catch up
        if db is not None:
            self._persist_event(db, event)

    def _persist_event(
        self,
        db: DatabaseConnection,
        event: dict,
    ) -> None:
        """
        Write the event to the sse_events table for persistence.
        Called internally by publish() when a db connection is available.

        WHY NON-CRITICAL?
          If this DB write fails, the in-memory event was already published.
          The SSE stream still works. We log the error but do not raise.

        Args:
            db:    connected DatabaseConnection
            event: the event dict to persist
        """
        try:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {TABLE_SSE_EVENTS}
                            (event_type, payload, delivered, created_at)
                        VALUES (%s, %s, %s, NOW());
                        """,
                        (
                            event["event_type"],
                            json.dumps(event["payload"]),   # JSONB needs string
                            False,   # delivered = False — not yet sent to browser
                        )
                    )
                conn.commit()
            logger.debug(f"  SSE event persisted to DB: {event['event_type']}")
        except Exception as e:
            # Non-critical — log but do not raise
            logger.error(f"  Failed to persist SSE event to DB: {e}")

    async def listen(self):
        """
        Async generator that yields events from the queue.

        The SSE route handler uses this as:
          async for event in sse_service.listen():
              yield f"data: {json.dumps(event)}\n\n"

        This generator runs forever (until the client disconnects).
        It yields events as they arrive and blocks (without consuming CPU)
        when the queue is empty.

        Yields:
            dict: event objects from the queue
        """
        logger.info("  SSE listener started — waiting for events...")

        while True:
            try:
                # Wait for an event — blocks until one is available
                # asyncio.wait_for adds a timeout so the loop can also
                # send keepalive pings at regular intervals
                event = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=25.0   # 25s timeout — yields None for a ping
                )
                logger.debug(f"  SSE listener yielding: {event.get('event_type')}")
                yield event

            except asyncio.TimeoutError:
                # No event arrived in 25 seconds — yield a ping to keep
                # the connection alive (Render closes idle SSE at 55s)
                yield {
                    "id":         str(uuid4()),
                    "event_type": "ping",
                    "payload":    {},
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }

            except asyncio.CancelledError:
                # Client disconnected — stop the generator cleanly
                logger.info("  SSE listener cancelled (client disconnected)")
                return

    def get_status(self) -> dict:
        """
        Return diagnostic info about the SSE service.
        Exposed at GET /api/v1/admin/sse-status for monitoring.
        """
        return {
            "queue_size":      self._queue.qsize(),
            "max_queue_size":  self._max_size,
            "published_total": self._published_count,
        }


# ─────────────────────────────────────────────────────────────────────────────
# SINGLETON INSTANCE
# Created once and imported wherever needed.
# ─────────────────────────────────────────────────────────────────────────────

# This is the single SSEService instance for the whole application.
# Import it in any service or route that needs to publish events:
#   from src.api.services.sse_service import sse_service
sse_service = SSEService(max_queue_size=100)
