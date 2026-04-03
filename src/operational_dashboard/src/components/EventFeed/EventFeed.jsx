/*
  src/components/EventFeed/EventFeed.jsx
  ─────────────────────────────────────────────────────────────────────────────
  ZONE 3 — LIVE SSE EVENT FEED

  The right-column feed that shows real-time events pushed from FastAPI.
  Events slide in from the top as they arrive.

  EVENT TYPES (from sse_event_type_enum in schema.sql):
    new_customer      — a customer registered through the frontend
    high_churn_alert  — a customer crossed the HIGH risk threshold
    batch_completed   — a batch scoring run finished
    drift_alert       — feature drift PSI exceeded threshold
    model_promoted    — a new model version went to production
    ping              — keepalive (shown subtly, not prominently)

  DATA SOURCE: useSSE() hook → EventSource → FastAPI /api/v1/admin/events
  ─────────────────────────────────────────────────────────────────────────────
*/

import './EventFeed.css'

/* ── ICON MAP — one symbol per event type ────────────────────────────────── */
/* Simple text symbols keep the bundle small — no icon library needed */
const EVENT_ICONS = {
  new_customer:     '◆',
  high_churn_alert: '▲',
  batch_completed:  '■',
  drift_alert:      '◈',
  model_promoted:   '★',
  ping:             '·',
}

/* ── HELPER: format ISO timestamp → human-readable relative time ─────────── */
function timeAgo(isoString) {
  if (!isoString) return ''
  const diff = Math.floor((Date.now() - new Date(isoString).getTime()) / 1000)
  if (diff < 5)     return 'just now'
  if (diff < 60)    return `${diff}s ago`
  if (diff < 3600)  return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

/* ── HELPER: build a human-readable body string from event payload ────────── */
/* Each event type has a different payload shape — we handle each one. */
function buildEventBody(eventType, payload) {
  if (!payload) return ''

  switch (eventType) {

    case 'new_customer':
      /* payload: { customer_id, city_tier, payment, device } */
      return [
        payload.customer_id && `ID ${payload.customer_id}`,
        payload.city_tier   && `City Tier ${payload.city_tier}`,
        payload.payment     && payload.payment,
        payload.device      && payload.device,
      ].filter(Boolean).join(' · ')

    case 'high_churn_alert':
      /* payload: { customer_id, score, risk_tier, top_reason } */
      return [
        payload.customer_id && `${payload.customer_id}`,
        payload.score !== undefined && `Score: ${Number(payload.score).toFixed(2)}`,
        payload.top_reason  && `Reason: ${payload.top_reason}`,
      ].filter(Boolean).join('\n')

    case 'batch_completed':
      /* payload: { customers_scored, high_risk_count, duration_seconds, drift_alert } */
      return [
        payload.customers_scored !== undefined && `${Number(payload.customers_scored).toLocaleString()} scored`,
        payload.high_risk_count  !== undefined && `${payload.high_risk_count} HIGH risk`,
        payload.duration_seconds !== undefined && `${payload.duration_seconds}s`,
        payload.drift_alert ? '⚠ drift alert' : 'no drift',
      ].filter(Boolean).join(' · ')

    case 'drift_alert':
      /* payload: { feature, psi, batch_run_id } */
      return [
        payload.feature && `Feature: ${payload.feature}`,
        payload.psi !== undefined && `PSI: ${Number(payload.psi).toFixed(3)}`,
        'Retrain recommended',
      ].filter(Boolean).join('\n')

    case 'model_promoted':
      /* payload: { version, auc_roc } */
      return [
        payload.version && `Version: ${payload.version}`,
        payload.auc_roc !== undefined && `AUC-ROC: ${Number(payload.auc_roc).toFixed(3)}`,
      ].filter(Boolean).join(' · ')

    case 'ping':
      /* ping has no meaningful body — show a minimal label */
      return 'Connection keepalive'

    default:
      /* Unknown event type — show raw payload as JSON */
      return JSON.stringify(payload).slice(0, 120)
  }
}


/* ── SINGLE EVENT CARD ───────────────────────────────────────────────────── */
/*
  Renders one event in the feed.

  Props:
    event — { id, event_type, payload, created_at }
*/
function EventCard({ event }) {
  const { event_type, payload, created_at } = event

  /* Skip rendering ping events if they have no meaningful content —
     they only exist to prevent Render from closing idle SSE connections */
  const isPing = event_type === 'ping'

  /* Build the body text from the payload */
  const bodyText = buildEventBody(event_type, payload)

  return (
    <div className={`event-card event-card--${event_type} ${isPing ? 'event-card--ping' : ''}`}>

      {/* Event type label with icon */}
      <div className="event-card__type">
        <span className="event-card__icon" aria-hidden="true">
          {EVENT_ICONS[event_type] ?? '·'}
        </span>
        {/* Replace underscores with spaces for readability */}
        <span>{event_type.replace(/_/g, ' ')}</span>
      </div>

      {/* Event body — multi-line text separated by \n */}
      {bodyText && (
        <div className="event-card__body">
          {bodyText.split('\n').map((line, i) => (
            <span key={i}>{line}</span>
          ))}
        </div>
      )}

      {/* Timestamp */}
      <div className="event-card__time">
        {timeAgo(created_at)}
      </div>

    </div>
  )
}


/* ── MAIN EXPORT ─────────────────────────────────────────────────────────── */
/*
  Props:
    events      — array from useSSE() hook, newest first
    sseStatus   — 'connected' | 'connecting' | 'error' | 'mock'
    onClear     — function to empty the event list
*/
export default function EventFeed({ events, sseStatus, onClear }) {

  /* Map status to a readable connection label */
  const statusLabel = {
    connected:  '● LIVE',
    mock:       '● LIVE',
    connecting: '○ CONNECTING',
    error:      '✕ ERROR',
  }[sseStatus] ?? '○ —'

  return (
    <div className="event-feed">

      {/* Feed header — always visible at top */}
      <div className="event-feed__header">
        <div className="event-feed__header-left">
          <h3 className="event-feed__title">Live Event Feed</h3>
          {/* SSE connection status badge */}
          <span className={`event-feed__status event-feed__status--${sseStatus}`}>
            {statusLabel}
          </span>
        </div>

        {/* Clear button — empties the feed */}
        {events.length > 0 && (
          <button className="event-feed__clear" onClick={onClear}>
            clear
          </button>
        )}
      </div>

      {/* Scrollable event list */}
      <div className="event-feed__list">
        {events.length === 0 ? (
          /* Empty state — shown when no events have arrived yet */
          <div className="event-feed__empty">
            <span className="event-feed__empty-icon">◌</span>
            <span>Awaiting events...</span>
            <span className="event-feed__empty-sub">
              Events appear here in real-time via SSE
            </span>
          </div>
        ) : (
          /* Render each event card — newest first (events array is already sorted) */
          events.map((event) => (
            <EventCard key={event.id} event={event} />
          ))
        )}
      </div>

    </div>
  )
}
