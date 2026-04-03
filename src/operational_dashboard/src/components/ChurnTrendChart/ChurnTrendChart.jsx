/*
  src/components/ChurnTrendChart/ChurnTrendChart.jsx
  ─────────────────────────────────────────────────────────────────────────────
  ZONE 3 — CHURN TREND LINE CHART

  Shows % of customers scored as HIGH risk per batch cycle over time.
  Drift alert batches are marked with a red dot on the line.

  DATA SOURCE: useChurnTrend() → v_churn_trend view
  ─────────────────────────────────────────────────────────────────────────────
*/

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceDot,
} from 'recharts'
import './ChurnTrendChart.css'

/* ── FORMAT batch_date for the X axis ───────────────────────────────────── */
/* e.g. '2026-03-15' → 'Mar 15' */
function formatDate(dateStr) {
  if (!dateStr) return ''
  const d = new Date(dateStr)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

/* ── CUSTOM TOOLTIP ──────────────────────────────────────────────────────── */
function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  const item = payload[0].payload

  return (
    <div className="trend-chart__tooltip">
      <p className="trend-chart__tooltip-date">{formatDate(item.batch_date)}</p>
      <p className="trend-chart__tooltip-value">
        {item.high_risk_pct.toFixed(1)}% HIGH risk
      </p>
      <p className="trend-chart__tooltip-scored">
        {item.customers_scored.toLocaleString()} scored
      </p>
      {/* Show drift warning if this batch fired a drift alert */}
      {item.drift_alert_fired && (
        <p className="trend-chart__tooltip-drift">⚠ Drift alert fired</p>
      )}
    </div>
  )
}


/* ── MAIN EXPORT ─────────────────────────────────────────────────────────── */
export default function ChurnTrendChart({ data, isLoading }) {

  /* Format data for Recharts — add a short date label for the X axis */
  const chartData = (data ?? []).map(d => ({
    ...d,
    date_label: formatDate(d.batch_date),
  }))

  /* Find batches where a drift alert fired — these get a ReferenceDot marker */
  const driftBatches = chartData.filter(d => d.drift_alert_fired)

  return (
    <div className="trend-chart__panel">

      {/* Panel header */}
      <div className="trend-chart__header">
        <h3 className="trend-chart__title">Churn Trend</h3>
        <span className="trend-chart__badge">last {chartData.length} batches</span>
      </div>

      {/* Chart */}
      <div className="trend-chart__body">
        {isLoading ? (
          <div className="trend-chart__skeleton" />
        ) : (
          <ResponsiveContainer width="100%" height={180}>
            <LineChart
              data={chartData}
              margin={{ top: 8, right: 12, left: 0, bottom: 0 }}
            >
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="rgba(255,255,255,0.05)"
              />

              <XAxis
                dataKey="date_label"
                tick={{ fill: '#4a5568', fontSize: 10, fontFamily: 'Inter' }}
                tickLine={false}
                axisLine={false}
              />

              <YAxis
                tick={{ fill: '#4a5568', fontSize: 10, fontFamily: 'JetBrains Mono' }}
                tickLine={false}
                axisLine={false}
                /* Keep Y axis range stable — add padding above/below data */
                domain={['auto', 'auto']}
                /* Show % suffix on Y axis ticks */
                tickFormatter={v => `${v}%`}
                width={36}
              />

              <Tooltip content={<CustomTooltip />} />

              {/* Main line — HIGH risk % over time */}
              <Line
                type="monotone"
                dataKey="high_risk_pct"
                stroke="#ef4444"
                strokeWidth={2}
                dot={{ fill: '#ef4444', r: 3, strokeWidth: 0 }}
                activeDot={{ r: 5, fill: '#ef4444', stroke: 'var(--bg-surface)', strokeWidth: 2 }}
              />

              {/* Drift alert markers — amber dot on batches where drift fired */}
              {driftBatches.map((batch) => (
                <ReferenceDot
                  key={batch.batch_date}
                  x={batch.date_label}
                  y={batch.high_risk_pct}
                  r={6}
                  fill="#f59e0b"
                  stroke="var(--bg-surface)"
                  strokeWidth={2}
                  label={{ value: '⚠', position: 'top', fontSize: 10 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Footnote */}
      <p className="trend-chart__footnote">
        % HIGH risk per batch cycle · amber dots = drift alert
      </p>

    </div>
  )
}
