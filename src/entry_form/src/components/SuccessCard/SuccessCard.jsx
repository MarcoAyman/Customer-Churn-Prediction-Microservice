/*
  src/components/SuccessCard/SuccessCard.jsx
  ─────────────────────────────────────────────────────────────────────────────
  COMPONENT: SuccessCard

  Shown below the submit button after a customer is successfully registered.
  Displays the confirmation data returned by the API:
    - Customer UUID (the database primary key)
    - registered_at timestamp
    - Initial customer_features values (tenure=0, orders=0)
    - "Scoreable in 30 days" badge

  WHY SHOW FEATURE VALUES HERE?
    The operator entering data needs to know what was created in the database.
    Showing tenure=0.0 and order_count=0 confirms that the customer_features
    row was inserted correctly with the expected starting state.
    It also explains why the customer shows ONBOARDING on the dashboard.

  PROPS:
    data     — { customer_id, registered_at, days_until_scoreable }
    onReset  — function() to manually reset before the 5s auto-reset
  ─────────────────────────────────────────────────────────────────────────────
*/

import './SuccessCard.css'
import { INITIAL_FEATURES } from '../../data/formConfig'

/* ── HELPER: format ISO timestamp to readable local time ─────────────────── */
function formatTimestamp(isoString) {
  if (!isoString) return '—'
  const d = new Date(isoString)
  /* Format: "Mar 22, 2026 · 14:35:09 UTC" */
  return d.toUTCString().replace('GMT', 'UTC')
}

/* ── HELPER: format feature value for display ────────────────────────────── */
/* null → shown as '—' instead of 'null' */
function fmtFeature(val) {
  if (val === null || val === undefined) return '—'
  if (typeof val === 'boolean') return val ? 'true' : 'false'
  return String(val)
}


export default function SuccessCard({ data, onReset }) {
  if (!data) return null   /* render nothing if no success data yet */

  return (
    /* Outer card — teal-tinted glass surface */
    <div className="success-card">

      {/* Top highlight line — same glass edge treatment as panels */}
      <div className="success-card__highlight" />

      {/* Header row — icon + title + reset button */}
      <div className="success-card__header">
        <div className="success-card__header-left">
          {/* Green success indicator dot */}
          <span className="success-card__dot" />
          <span className="success-card__title">Customer registered</span>
        </div>

        {/* Manual reset — lets operator immediately enter another customer
            without waiting for the 5s auto-reset */}
        <button
          className="success-card__reset-btn"
          onClick={onReset}
          aria-label="Register another customer"
        >
          + new
        </button>
      </div>

      {/* UUID — the most important piece of data to show */}
      <div className="success-card__uuid-block">
        <span className="success-card__section-label">customer id</span>
        <code className="success-card__uuid">{data.customer_id}</code>
      </div>

      {/* Divider */}
      <div className="success-card__divider" />

      {/* Registration timestamp */}
      <div className="success-card__row">
        <span className="success-card__row-label">registered_at</span>
        <span className="success-card__row-value">
          {formatTimestamp(data.registered_at)}
        </span>
      </div>

      {/* Divider */}
      <div className="success-card__divider" />

      {/* Initial feature values — shows what was inserted into customer_features */}
      <div className="success-card__features-header">
        <span className="success-card__section-label">customer_features (initial state)</span>
      </div>

      <div className="success-card__features-grid">
        {/* Render each starting feature value from the config */}
        {Object.entries(INITIAL_FEATURES).map(([key, val]) => (
          <div key={key} className="success-card__feature-item">
            <span className="success-card__feature-key">{key}</span>
            <span className="success-card__feature-val">{fmtFeature(val)}</span>
          </div>
        ))}
      </div>

      {/* Divider */}
      <div className="success-card__divider" />

      {/* Footer — scoreable badge + auto-reset notice */}
      <div className="success-card__footer">
        {/* Indigo badge — same style as the HTML prototype */}
        <span className="success-card__badge">
          scoreable in {data.days_until_scoreable ?? 30} days
        </span>
        <span className="success-card__reset-notice">
          form resets in 5s
        </span>
      </div>

    </div>
  )
}
