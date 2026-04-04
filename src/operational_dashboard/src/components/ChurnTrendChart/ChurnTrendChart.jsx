/*
  src/components/ChurnTrendChart/ChurnTrendChart.jsx
  ─────────────────────────────────────────────────────────────────────────────
  ZONE 3 — CHURN TREND LINE CHART

  FIX APPLIED — null safety in CustomTooltip:
    BEFORE: {item.high_risk_pct.toFixed(1)}%
            → TypeError if high_risk_pct is null (e.g. batch with 0 customers)
            → This was the most likely cause of the React tree crash

    AFTER:  {(item.high_risk_pct ?? 0).toFixed(1)}%
            → Safe: null/undefined becomes 0, no crash

  Real data from v_churn_trend:
    batch_date:        "2026-03-15" (date → ISO string via _make_json_safe)
    high_risk_pct:     16.8         (Decimal → float via _make_json_safe)
    customers_scored:  5630         (integer)
    drift_alert_fired: false        (boolean)
    duration_seconds:  7            (integer, may be null for seed run)

  Kaggle seed run note:
    The seed run appears in v_churn_trend as one data point at the right
    of the chart: 16.8% HIGH risk, 5630 scored. This is the baseline.
    Future real model batch runs will add more points.
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

// ── FORMAT batch_date for X axis ─────────────────────────────────────────────
// "2026-03-15" → "Mar 15"
function formatDate(dateStr) {
  if (!dateStr) return ''
  try {
    const d = new Date(dateStr)
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  } catch {
    return String(dateStr)
  }
}

// ── CUSTOM TOOLTIP ─────────────────────────────────────────────────────────────
function CustomTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const item = payload[0].payload

  return (
    <div className="trend-chart__tooltip">
      <p className="trend-chart__tooltip-date">
        {formatDate(item.batch_date)}
      </p>
      <p className="trend-chart__tooltip-value">
        {/* FIX: guard against null with ?? 0 */}
        {(item.high_risk_pct ?? 0).toFixed(1)}% HIGH risk
      </p>
      <p className="trend-chart__tooltip-scored">
        {(item.customers_scored ?? 0).toLocaleString()} scored
      </p>
      {item.drift_alert_fired && (
        <p className="trend-chart__tooltip-drift">⚠ Drift alert fired</p>
      )}
    </div>
  )
}

// ── MAIN COMPONENT ─────────────────────────────────────────────────────────────
export default function ChurnTrendChart({ data, isLoading }) {
  // Add a formatted date label for the X axis — guard against null batch_date
  const chartData = (data ?? []).map(d => ({
    ...d,
    date_label:    formatDate(d.batch_date),
    // Guard: high_risk_pct may be null for batch runs with 0 scored customers
    high_risk_pct: d.high_risk_pct ?? 0,
  }))

  // Batches where a drift alert fired get a special marker on the line
  const driftBatches = chartData.filter(d => d.drift_alert_fired)

  const isDark     = true   // dashboard is always dark
  const gridColour = 'rgba(255,255,255,0.05)'
  const tickColour = '#4a5568'

  return (
    <div className="trend-chart__panel">
      <div className="trend-chart__header">
        <h3 className="trend-chart__title">Churn Trend</h3>
        <span className="trend-chart__badge">last {chartData.length} batches</span>
      </div>

      <div className="trend-chart__body">
        {isLoading ? (
          <div className="trend-chart__skeleton" />
        ) : chartData.length === 0 ? (
          // Empty state — no completed batch runs yet
          <div style={{
            display:        'flex',
            alignItems:     'center',
            justifyContent: 'center',
            height:         '180px',
            color:          'var(--text-tertiary)',
            fontSize:       '12px',
          }}>
            Awaiting first batch run
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={180}>
            <LineChart
              data={chartData}
              margin={{ top: 8, right: 12, left: 0, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke={gridColour} />

              <XAxis
                dataKey="date_label"
                tick={{ fill: tickColour, fontSize: 10, fontFamily: 'Inter' }}
                tickLine={false}
                axisLine={false}
              />

              <YAxis
                tick={{ fill: tickColour, fontSize: 10, fontFamily: 'JetBrains Mono' }}
                tickLine={false}
                axisLine={false}
                domain={['auto', 'auto']}
                tickFormatter={v => `${v}%`}
                width={36}
              />

              <Tooltip content={<CustomTooltip />} />

              <Line
                type="monotone"
                dataKey="high_risk_pct"
                stroke="#ef4444"
                strokeWidth={2}
                dot={{ fill: '#ef4444', r: 3, strokeWidth: 0 }}
                activeDot={{ r: 5, fill: '#ef4444', stroke: 'var(--bg-surface)', strokeWidth: 2 }}
              />

              {/* Amber dot markers on batches where drift alert fired */}
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

      <p className="trend-chart__footnote">
        % HIGH risk per batch cycle · amber dots = drift alert
      </p>
    </div>
  )
}
