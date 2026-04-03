/*
  src/hooks/useSSE.js
  ─────────────────────────────────────────────────────────────────────────────
  CUSTOM HOOK: useSSE
  Manages a Server-Sent Events (SSE) connection to the FastAPI backend.

  WHY A CUSTOM HOOK?
    The SSE connection logic (open, listen, reconnect, close) is the same
    regardless of which component uses it. Extracting it into a hook means:
      - The EventFeed component just calls useSSE('/api/v1/admin/events')
      - The HealthBar component calls useSSE() for the live status dot
      - Neither component knows anything about EventSource or reconnection

  HOW SSE WORKS:
    1. Browser opens a GET request to /api/v1/admin/events
    2. FastAPI keeps the connection open and pushes events as text
    3. EventSource fires onmessage for each event
    4. If the connection drops, EventSource auto-reconnects (built-in)

  WHEN BACKEND IS READY:
    Change MOCK_MODE to false. The hook will connect to the real FastAPI endpoint.
    The component code does not change at all.
  ─────────────────────────────────────────────────────────────────────────────
*/

import { useState, useEffect, useRef, useCallback } from 'react'
import { MOCK_INITIAL_EVENTS } from '../data/mockData'

/* ── CONFIGURATION ───────────────────────────────────────────────────────── */

/* Set to true during development when FastAPI is not running yet.
   When false, the hook connects to the real SSE endpoint. */
const MOCK_MODE = false

/* FastAPI SSE endpoint — will be used when MOCK_MODE = false */

const BASE_URL = import.meta.env.VITE_API_URL || ''
const SSE_ENDPOINT = `${BASE_URL}/api/v1/admin/events`

/* Maximum events to keep in the feed before removing oldest ones */
const MAX_EVENTS = 50

/* How long to wait before attempting to reconnect after a connection error */
const RECONNECT_DELAY_MS = 3000


/* ── HOOK ────────────────────────────────────────────────────────────────── */

/*
  useSSE()
  ─────────────────────────────────────────────────────────────────────────────
  Returns:
    events       — array of event objects, newest first (for display in feed)
    isConnected  — boolean — true when SSE connection is open
    status       — 'connecting' | 'connected' | 'error' | 'mock'
    clearEvents  — function to empty the event list
  ─────────────────────────────────────────────────────────────────────────────
*/
export function useSSE() {
  /* events: array of event objects shown in the live feed */
  const [events, setEvents] = useState(MOCK_INITIAL_EVENTS)

  /* connection status for the health bar indicator */
  const [status, setStatus] = useState(MOCK_MODE ? 'mock' : 'connecting')

  /* ref to the EventSource instance — ref so it persists across renders
     without triggering re-renders when it changes */
  const eventSourceRef = useRef(null)

  /* Adds a new event to the front of the array.
     Trims the array to MAX_EVENTS to prevent memory growth. */
  const addEvent = useCallback((newEvent) => {
    setEvents(prev => {
      /* Insert at front (newest first), trim to max length */
      const updated = [newEvent, ...prev]
      return updated.slice(0, MAX_EVENTS)
    })
  }, [])

  /* Empties the event list — used by a "clear feed" button */
  const clearEvents = useCallback(() => {
    setEvents([])
  }, [])


  useEffect(() => {
    /* ── MOCK MODE ── */
    if (MOCK_MODE) {
      /* Simulate incoming SSE events every 8 seconds to show the feed is live */
      const simulatedEvents = [
        { event_type: 'ping',           payload: {},                                    created_at: new Date().toISOString() },
        { event_type: 'new_customer',   payload: { customer_id: '#50999', city_tier: 2 }, created_at: new Date().toISOString() },
        { event_type: 'high_churn_alert', payload: { customer_id: '#50500', score: 1.0, top_reason: 'Complaint raised' }, created_at: new Date().toISOString() },
        { event_type: 'ping',           payload: {},                                    created_at: new Date().toISOString() },
      ]
      let simulatedIndex = 0
      const interval = setInterval(() => {
        const evt = simulatedEvents[simulatedIndex % simulatedEvents.length]
        addEvent({ ...evt, id: `mock-${Date.now()}`, created_at: new Date().toISOString() })
        simulatedIndex++
      }, 8000)

      /* Cleanup: stop simulation when component unmounts */
      return () => clearInterval(interval)
    }


    /* ── REAL SSE MODE (when MOCK_MODE = false) ── */
    function connect() {
      setStatus('connecting')

      /* EventSource: browser's built-in SSE client.
         Opens a GET connection to the endpoint and auto-reconnects. */
      const es = new EventSource(SSE_ENDPOINT, {
        withCredentials: false  /* set to true if your API requires cookies */
      })
      eventSourceRef.current = es

      /* onopen: called when the SSE connection is first established */
      es.onopen = () => {
        setStatus('connected')
        console.log('[SSE] Connected to', SSE_ENDPOINT)
      }

      /* onmessage: called for every 'data: ...' line the server sends.
         FastAPI sends JSON-serialised event objects. */
      es.onmessage = (event) => {
        try {
          /* Parse the JSON string sent by FastAPI */
          const parsed = JSON.parse(event.data)
          addEvent(parsed)
        } catch (err) {
          console.warn('[SSE] Failed to parse event:', event.data, err)
        }
      }

      /* onerror: called when the connection drops or the server sends an error.
         EventSource will try to reconnect automatically, but we set our own
         delay via a manual close + setTimeout to avoid hammering the server. */
      es.onerror = (err) => {
        setStatus('error')
        console.error('[SSE] Connection error:', err)

        /* Close the broken connection so EventSource stops auto-retrying */
        es.close()
        eventSourceRef.current = null

        /* Reconnect after RECONNECT_DELAY_MS milliseconds */
        setTimeout(connect, RECONNECT_DELAY_MS)
      }
    }

    connect()

    /* Cleanup: close the EventSource when the component unmounts.
       Without this, the connection stays open forever even after navigating away. */
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close()
        eventSourceRef.current = null
        console.log('[SSE] Connection closed (component unmounted)')
      }
    }
  }, [addEvent]) /* addEvent is stable (useCallback), so this only runs once */


  return {
    events,
    isConnected: status === 'connected' || status === 'mock',
    status,
    clearEvents,
  }
}
