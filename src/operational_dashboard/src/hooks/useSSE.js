/*
  src/hooks/useSSE.js
  ─────────────────────────────────────────────────────────────────────────────
  CUSTOM HOOK: useSSE
  Manages a Server-Sent Events (SSE) connection to the FastAPI backend.

  FIX APPLIED:
    The browser's EventSource API cannot send custom HTTP headers.
    This is a browser spec limitation — not a bug in our code.

    BEFORE (broken):
      const es = new EventSource(SSE_ENDPOINT)
      // X-Admin-Key header never reaches the server → 403

    AFTER (fixed):
      const es = new EventSource(`${SSE_ENDPOINT}?admin_key=${ADMIN_KEY}`)
      // Admin key travels in the URL query param → FastAPI accepts it

  The backend event.py route was updated to accept ?admin_key= for this reason.
  ─────────────────────────────────────────────────────────────────────────────
*/

import { useState, useEffect, useRef, useCallback } from 'react'
import { MOCK_INITIAL_EVENTS } from '../data/mockData'

// ── CONFIGURATION ─────────────────────────────────────────────────────────────

// Set to false when FastAPI is running and reachable
const MOCK_MODE = false

// Base API URL from Vite environment variable
// Falls back to empty string for same-origin requests in development
const BASE_URL = import.meta.env.VITE_API_URL || ''

// Admin API key from Vite environment variable
// Sent as query param because EventSource cannot send custom headers
const ADMIN_KEY = import.meta.env.VITE_ADMIN_KEY || ''

// SSE endpoint — admin_key appended as query param (browser spec requirement)
const SSE_ENDPOINT = `${BASE_URL}/api/v1/admin/events?admin_key=${ADMIN_KEY}`

// Maximum events to keep in the feed before dropping oldest
const MAX_EVENTS = 50

// Seconds to wait before reconnecting after a connection error
const RECONNECT_DELAY_MS = 3000


// ── HOOK ──────────────────────────────────────────────────────────────────────

export function useSSE() {
  // events: array of event objects, newest first
  const [events, setEvents] = useState(MOCK_MODE ? MOCK_INITIAL_EVENTS : [])

  // SSE connection status
  const [status, setStatus] = useState(MOCK_MODE ? 'mock' : 'connecting')

  // Ref to the EventSource instance — persists across renders without re-render
  const eventSourceRef = useRef(null)

  // Adds a new event to the front of the array and trims to MAX_EVENTS
  const addEvent = useCallback((newEvent) => {
    setEvents(prev => {
      const updated = [newEvent, ...prev]
      return updated.slice(0, MAX_EVENTS)
    })
  }, [])

  // Empties the event list — for the "clear" button
  const clearEvents = useCallback(() => {
    setEvents([])
  }, [])


  useEffect(() => {

    // ── MOCK MODE ─────────────────────────────────────────────────────────────
    if (MOCK_MODE) {
      // Simulate incoming SSE events every 8 seconds during development
      const simulatedEvents = [
        { event_type: 'ping',           payload: {},                                         created_at: new Date().toISOString() },
        { event_type: 'new_customer',   payload: { customer_id: '#50999', city_tier: 2 },    created_at: new Date().toISOString() },
        { event_type: 'high_churn_alert', payload: { customer_id: '#50500', score: 1.0, top_reason: 'Complaint raised' }, created_at: new Date().toISOString() },
        { event_type: 'ping',           payload: {},                                         created_at: new Date().toISOString() },
      ]
      let simulatedIndex = 0
      const interval = setInterval(() => {
        const evt = simulatedEvents[simulatedIndex % simulatedEvents.length]
        addEvent({ ...evt, id: `mock-${Date.now()}`, created_at: new Date().toISOString() })
        simulatedIndex++
      }, 8000)

      return () => clearInterval(interval)
    }


    // ── REAL SSE MODE ─────────────────────────────────────────────────────────

    // Validate that ADMIN_KEY is set before attempting connection
    // If empty, the server will return 403 and we reconnect forever pointlessly
    if (!ADMIN_KEY) {
      console.error(
        '[SSE] VITE_ADMIN_KEY is not set. ' +
        'Add it to your Vercel environment variables and redeploy.'
      )
      setStatus('error')
      return
    }

    function connect() {
      setStatus('connecting')

      // EventSource: browser's built-in SSE client.
      // We append ?admin_key= because EventSource cannot send custom headers.
      // The URL is built at the top of this file.
      console.log(`[SSE] Connecting to: ${BASE_URL}/api/v1/admin/events?admin_key=****`)
      const es = new EventSource(SSE_ENDPOINT)
      eventSourceRef.current = es

      // onopen: SSE connection established successfully
      es.onopen = () => {
        setStatus('connected')
        console.log('[SSE] Connected successfully')
      }

      // onmessage: called for every "data: ...\n\n" chunk from FastAPI
      es.onmessage = (event) => {
        try {
          const parsed = JSON.parse(event.data)
          addEvent(parsed)
        } catch (err) {
          console.warn('[SSE] Failed to parse event:', event.data, err)
        }
      }

      // onerror: connection dropped or server returned error
      es.onerror = (err) => {
        setStatus('error')
        console.error('[SSE] Connection error. Will retry in', RECONNECT_DELAY_MS, 'ms')
        es.close()
        eventSourceRef.current = null
        // Wait before reconnecting — avoids hammering a sleeping Render server
        setTimeout(connect, RECONNECT_DELAY_MS)
      }
    }

    connect()

    // Cleanup: close connection when component unmounts
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close()
        eventSourceRef.current = null
        console.log('[SSE] Connection closed (component unmounted)')
      }
    }
  }, [addEvent])


  return {
    events,
    isConnected: status === 'connected' || status === 'mock',
    status,
    clearEvents,
  }
}
