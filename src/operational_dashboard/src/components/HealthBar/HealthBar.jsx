/*
  src/components/HealthBar/HealthBar.jsx
  ─────────────────────────────────────────────────────────────────────────────
  ZONE 1 — SYSTEM HEALTH BAR

  A sticky strip pinned to the top of the viewport.
  Answers in one glance: is the system healthy right now?

  WHAT IT SHOWS:
    - Live pulse dot (green = healthy, amber = drift alert, red = batch failed)
    - Last batch run: when it ran, how long it took, model version used
    - Total customers in the database
    - Drift status (none / warning / critical)
    - SSE connection status (● LIVE or ○ RECONNECTING)
    - A real-time clock (ticks every second)

  PROPS:
    lastBatch   — object from useLastBatch() hook
    kpiSummary  — object from useKPISummary() hook
    sseStatus   — string: 'mock' | 'connected' | 'connecting' | 'error'
  ─────────────────────────────────────────────────────────────────────────────
*/

import { useState, useEffect } from 'react'
import './HealthBar.css'

/* ── HELPER: format a UTC ISO timestamp into a human-readable relative time */
function timeAgo(isoString) {
  if (!isoString) return '—'
  const diff = Math.floor((Date.now() - new Date(isoString).getTime()) / 1000)
  if (diff < 60)              return `${diff}s ago`
  if (diff < 3600)            return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400)           return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

/* ── HELPER: determine overall system health status from batch + drift data */
function getSystemStatus(lastBatch) {
  if (!lastBatch)                         return 'loading'
  if (lastBatch.status === 'failed')      return 'error'
  if (lastBatch.drift_alert_fired)        return 'warning'
  return 'healthy'
}


export default function HealthBar({ lastBatch, kpiSummary, sseStatus }) {
  /* Live clock — updates every second using setInterval */
  const [clock, setClock] = useState('')

  useEffect(() => {
    /* Set initial time immediately on mount */
    setClock(new Date().toTimeString().slice(0, 8))

    /* Update clock every 1000ms */
    const interval = setInterval(() => {
      setClock(new Date().toTimeString().slice(0, 8))
    }, 1000)

    /* Cleanup: stop the interval when HealthBar unmounts */
    return () => clearInterval(interval)
  }, []) /* empty deps — run once on mount */

  /* Determine the overall status colour class */
  const systemStatus = getSystemStatus(lastBatch)

  /* Map system status to CSS class names */
  const statusClassMap = {
    healthy: 'status--healthy',
    warning: 'status--warning',
    error:   'status--error',
    loading: 'status--loading',
  }

  /* Human-readable system status label */
  const statusLabelMap = {
    healthy: 'operational',
    warning: 'drift detected',
    error:   'batch failed',
    loading: 'loading...',
  }

  return (
    <div className={`health-bar ${statusClassMap[systemStatus]}`}>

      {/* LEFT SECTION — system status indicator */}
      <div className="health-bar__left">

        {/* Animated pulse dot — colour reflects system status */}
        <span className={`health-bar__dot health-bar__dot--${systemStatus}`} />

        {/* System label */}
        <span className="health-bar__item">
          <span className="health-bar__label">system</span>
          <span className="health-bar__value">{statusLabelMap[systemStatus]}</span>
        </span>

        {/* Divider */}
        <span className="health-bar__divider" />

        {/* Model version currently serving predictions */}
        <span className="health-bar__item">
          <span className="health-bar__label">model</span>
          <span className="health-bar__value">
            {lastBatch?.model_version ?? '—'}
          </span>
        </span>

        <span className="health-bar__divider" />

        {/* Last batch run timing */}
        <span className="health-bar__item">
          <span className="health-bar__label">last batch</span>
          <span className="health-bar__value">
            {lastBatch
              ? `${timeAgo(lastBatch.completed_at)} · ${lastBatch.duration_seconds}s`
              : '—'
            }
          </span>
        </span>

        <span className="health-bar__divider" />

        {/* Total customers in database */}
        <span className="health-bar__item">
          <span className="health-bar__label">customers</span>
          <span className="health-bar__value">
            {kpiSummary
              ? kpiSummary.total_customers.toLocaleString()
              : '—'
            }
          </span>
        </span>

        <span className="health-bar__divider" />

        {/* Drift status */}
        <span className="health-bar__item">
          <span className="health-bar__label">drift</span>
          <span className={`health-bar__value ${lastBatch?.drift_alert_fired ? 'health-bar__value--warn' : 'health-bar__value--ok'}`}>
            {lastBatch?.drift_alert_fired ? 'alert fired' : 'none detected'}
          </span>
        </span>
      </div>

      {/* RIGHT SECTION — SSE status + clock */}
      <div className="health-bar__right">

        {/* SSE connection indicator */}
        <span className="health-bar__item">
          <span className={`health-bar__sse-dot health-bar__sse-dot--${sseStatus}`} />
          <span className="health-bar__label">
            {sseStatus === 'connected' || sseStatus === 'mock' ? 'LIVE' : 'RECONNECTING'}
          </span>
        </span>

        <span className="health-bar__divider" />

        {/* Real-time clock */}
        <span className="health-bar__clock">{clock}</span>
      </div>

    </div>
  )
}
