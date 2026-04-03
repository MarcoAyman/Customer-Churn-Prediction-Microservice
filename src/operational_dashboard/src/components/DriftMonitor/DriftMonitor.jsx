/*
  src/components/DriftMonitor/DriftMonitor.jsx
  ─────────────────────────────────────────────────────────────────────────────
  ZONE 3 — FEATURE DRIFT MONITOR

  Shows PSI (Population Stability Index) per feature.
  Each row: feature name | PSI value | status badge | distribution shift bar.

  PSI thresholds (from drift_reports table):
    < 0.10  → 'none'     → green
    0.10–0.20 → 'warning' → amber
    > 0.20  → 'critical' → red

  While no batch has run yet, shows reference mean and 'pending' status.

  DATA SOURCE: useDriftMonitor() → drift_reports joined with batch_runs
  ─────────────────────────────────────────────────────────────────────────────
*/

import './DriftMonitor.css'

/* ── PSI STATUS BADGE ────────────────────────────────────────────────────── */
function PSIBadge({ level }) {
  const classMap = {
    none:     'psi-badge--none',
    warning:  'psi-badge--warning',
    critical: 'psi-badge--critical',
    pending:  'psi-badge--pending',
  }
  return (
    <span className={`psi-badge ${classMap[level] ?? 'psi-badge--pending'}`}>
      {level}
    </span>
  )
}

/* ── PSI BAR — visual gauge from 0 to 0.30+ ─────────────────────────────── */
function PSIBar({ psiValue }) {
  if (psiValue === null) {
    /* No data yet — show empty pending bar */
    return <div className="psi-bar psi-bar--pending" />
  }

  /* Clamp the fill width: PSI of 0.30 fills the bar fully.
     0.30 is chosen as the maximum visual scale because PSI rarely exceeds 0.30
     in practice, and values above 0.30 are all "critically drifted." */
  const fillPct = Math.min((psiValue / 0.30) * 100, 100)

  /* Colour based on PSI value — mirrors the threshold logic */
  const colour =
    psiValue >= 0.20 ? 'var(--color-drift-critical)' :
    psiValue >= 0.10 ? 'var(--color-drift-warning)'  :
                       'var(--color-drift-none)'

  return (
    <div className="psi-bar">
      <div
        className="psi-bar__fill"
        style={{ width: `${fillPct}%`, background: colour }}
      />
    </div>
  )
}


/* ── MAIN EXPORT ─────────────────────────────────────────────────────────── */
export default function DriftMonitor({ data, isLoading }) {

  return (
    <div className="drift__panel">

      {/* Panel header */}
      <div className="drift__header">
        <h3 className="drift__title">Feature Drift Monitor — PSI</h3>
        <span className="drift__badge">awaiting first live batch</span>
      </div>

      {/* PSI legend */}
      <div className="drift__legend">
        <span className="drift__legend-item drift__legend-item--none">none &lt;0.10</span>
        <span className="drift__legend-item drift__legend-item--warning">warning 0.10–0.20</span>
        <span className="drift__legend-item drift__legend-item--critical">critical &gt;0.20</span>
      </div>

      {/* Feature grid — headers */}
      <div className="drift__grid">
        <div className="drift__col-header">Feature</div>
        <div className="drift__col-header">Ref. Mean</div>
        <div className="drift__col-header">PSI</div>
        <div className="drift__col-header">Status</div>
        <div className="drift__col-header">Distribution shift</div>

        {/* Feature rows */}
        {isLoading ? (
          /* Skeleton rows */
          Array.from({ length: 5 }).map((_, i) => (
            <>
              <div key={`sk-${i}`} className="drift__skeleton-cell" />
              <div key={`sk2-${i}`} className="drift__skeleton-cell" />
              <div key={`sk3-${i}`} className="drift__skeleton-cell" />
              <div key={`sk4-${i}`} className="drift__skeleton-cell" />
              <div key={`sk5-${i}`} className="drift__skeleton-cell" />
            </>
          ))
        ) : (
          (data ?? []).map((feature) => (
            <>
              {/* Feature name */}
              <div key={`${feature.feature_name}-name`} className="drift__cell drift__cell--name">
                {feature.feature_name}
              </div>

              {/* Reference mean from training data */}
              <div key={`${feature.feature_name}-ref`} className="drift__cell drift__cell--mono">
                {feature.reference_mean !== null
                  ? feature.reference_mean.toFixed(2)
                  : '—'
                }
              </div>

              {/* PSI value */}
              <div key={`${feature.feature_name}-psi`} className="drift__cell drift__cell--mono">
                {feature.psi_value !== null
                  ? feature.psi_value.toFixed(3)
                  : '—'
                }
              </div>

              {/* Status badge */}
              <div key={`${feature.feature_name}-status`} className="drift__cell">
                <PSIBadge level={feature.drift_level} />
              </div>

              {/* Visual PSI bar */}
              <div key={`${feature.feature_name}-bar`} className="drift__cell">
                <PSIBar psiValue={feature.psi_value} />
              </div>
            </>
          ))
        )}
      </div>

    </div>
  )
}
