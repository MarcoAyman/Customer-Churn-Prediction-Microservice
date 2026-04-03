/*
  src/components/KPICards/KPICards.jsx
  ─────────────────────────────────────────────────────────────────────────────
  ZONE 2 — KPI CARDS

  Four metric cards showing the most important numbers at a glance.
  Polled every 60 seconds via React Query.
  Invalidated immediately when a 'batch_completed' SSE event arrives.

  CARDS:
    1. Total Customers     — total rows in customers table
    2. High Risk           — count + % with danger colour
    3. Retained / Low Risk — count + % with success colour
    4. Avg Churn Score     — mean churn_probability from predictions

  PROPS:
    data    — object from useKPISummary() hook
    isLoading — boolean
  ─────────────────────────────────────────────────────────────────────────────
*/

import './KPICards.css'

/* ── SUBCOMPONENT: SingleCard ────────────────────────────────────────────── */
/*
  A single KPI card. Extracted to keep the parent component clean.

  Props:
    label    — string: the card title (e.g. 'Total Customers')
    value    — string|number: the big number displayed
    subtext  — string: smaller context line below the number
    variant  — 'default' | 'danger' | 'success' | 'warning' | 'info'
    loading  — boolean: show skeleton shimmer when true
*/
function SingleCard({ label, value, subtext, variant = 'default', loading }) {
  return (
    <div className={`kpi-card kpi-card--${variant}`}>
      {/* Card title label */}
      <p className="kpi-card__label">{label}</p>

      {loading ? (
        /* Skeleton shimmer while data loads — prevents layout shift */
        <div className="kpi-card__skeleton" />
      ) : (
        /* The main metric number */
        <p className="kpi-card__value">{value}</p>
      )}

      {/* Small context line below the number */}
      {!loading && subtext && (
        <p className="kpi-card__subtext">{subtext}</p>
      )}
    </div>
  )
}


/* ── MAIN EXPORT ─────────────────────────────────────────────────────────── */
export default function KPICards({ data, isLoading }) {

  /* Format the last_scored_at timestamp into a readable label.
     e.g. '2026-03-15T23:47:57Z' → 'scored 1h ago' */
  const scoredLabel = data?.last_scored_at
    ? `scored ${timeAgo(data.last_scored_at)}`
    : 'not yet scored'

  return (
    <div className="kpi-grid">

      {/* Card 1 — Total Customers */}
      <SingleCard
        label="Total Customers"
        value={data ? data.total_customers.toLocaleString() : '—'}
        subtext={scoredLabel}
        variant="default"
        loading={isLoading}
      />

      {/* Card 2 — High Risk */}
      <SingleCard
        label="High Risk"
        value={data ? data.high_risk_count.toLocaleString() : '—'}
        /* Show the percentage of total customers who are HIGH risk */
        subtext={data ? `${data.high_risk_pct.toFixed(1)}% of total` : ''}
        variant="danger"
        loading={isLoading}
      />

      {/* Card 3 — Retained */}
      <SingleCard
        label="Retained"
        value={data ? data.low_risk_count.toLocaleString() : '—'}
        subtext={data ? `${(100 - data.high_risk_pct).toFixed(1)}% low risk` : ''}
        variant="success"
        loading={isLoading}
      />

      {/* Card 4 — Average Churn Score */}
      <SingleCard
        label="Avg Churn Score"
        /* high_risk_pct / 100 gives the approximate average churn probability
           when using kaggle_baseline binary labels. A real trained model will
           produce a continuous value here. */
        value={data ? (data.high_risk_pct / 100).toFixed(3) : '—'}
        subtext="baseline · no live model yet"
        variant="warning"
        loading={isLoading}
      />

    </div>
  )
}

/* ── HELPER: relative time formatter ─────────────────────────────────────── */
function timeAgo(isoString) {
  if (!isoString) return '—'
  const diff = Math.floor((Date.now() - new Date(isoString).getTime()) / 1000)
  if (diff < 60)    return `${diff}s ago`
  if (diff < 3600)  return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}
