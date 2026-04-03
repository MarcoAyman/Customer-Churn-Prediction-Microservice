/*
  src/components/RiskDistributionChart/RiskDistributionChart.jsx
  ─────────────────────────────────────────────────────────────────────────────
  ZONE 3 — RISK DISTRIBUTION CHART

  A horizontal bar chart showing customer count per risk tier.
  Horizontal bars are used because the tier labels (HIGH, MEDIUM, LOW, ONBOARDING)
  are long — they fit naturally on the Y axis.

  DATA SOURCE: useRiskDistribution() → v_current_risk_summary view
  LIBRARY: Recharts (BarChart with layout='vertical')
  ─────────────────────────────────────────────────────────────────────────────
*/

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import './RiskDistributionChart.css'

/* ── CUSTOM TOOLTIP ──────────────────────────────────────────────────────── */
/* Recharts calls this with { active, payload } when user hovers a bar */
function CustomTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const { tier, count } = payload[0].payload

  return (
    <div className="risk-chart__tooltip">
      <span className="risk-chart__tooltip-tier">{tier}</span>
      <span className="risk-chart__tooltip-count">
        {count.toLocaleString()} customers
      </span>
    </div>
  )
}

/* ── TIER COLOUR MAP ─────────────────────────────────────────────────────── */
/* Map each risk tier to its semantic colour.
   Recharts uses hex colours, not CSS variables. */
const TIER_COLOURS = {
  HIGH:       '#ef4444',
  MEDIUM:     '#f59e0b',
  LOW:        '#10b981',
  ONBOARDING: '#3b82f6',
}


/* ── MAIN EXPORT ─────────────────────────────────────────────────────────── */
export default function RiskDistributionChart({ data, isLoading }) {

  /* Filter out tiers with zero customers so the chart doesn't show
     empty bars for MEDIUM and ONBOARDING (which are 0 in kaggle_baseline) */
  const chartData = (data ?? []).filter(d => d.count > 0)

  return (
    <div className="risk-chart__panel">

      {/* Panel header */}
      <div className="risk-chart__header">
        <h3 className="risk-chart__title">Risk Distribution</h3>
        <span className="risk-chart__badge">v_current_risk_summary</span>
      </div>

      {/* Chart area */}
      <div className="risk-chart__body">
        {isLoading ? (
          /* Loading state — three skeleton bars */
          <div className="risk-chart__skeleton">
            {[60, 90, 40].map((w, i) => (
              <div key={i} className="risk-chart__skeleton-bar" style={{ width: `${w}%` }} />
            ))}
          </div>
        ) : (
          /* ResponsiveContainer: makes the chart fill its parent div */
          <ResponsiveContainer width="100%" height={180}>
            <BarChart
              data={chartData}
              layout="vertical"    /* horizontal bars need layout='vertical' */
              margin={{ top: 0, right: 12, left: 0, bottom: 0 }}
            >
              {/* Subtle horizontal grid lines */}
              <CartesianGrid
                strokeDasharray="3 3"
                horizontal={false}    /* only vertical grid lines for horizontal bars */
                stroke="rgba(255,255,255,0.05)"
              />

              {/* X axis — the count scale */}
              <XAxis
                type="number"
                tick={{ fill: '#4a5568', fontSize: 10, fontFamily: 'JetBrains Mono' }}
                tickLine={false}
                axisLine={false}
                /* Format large numbers: 4682 → 4.7k */
                tickFormatter={v => v >= 1000 ? `${(v / 1000).toFixed(1)}k` : v}
              />

              {/* Y axis — the tier labels */}
              <YAxis
                type="category"
                dataKey="tier"
                tick={{ fill: '#8892aa', fontSize: 11, fontFamily: 'Inter' }}
                tickLine={false}
                axisLine={false}
                width={80}
              />

              <Tooltip
                content={<CustomTooltip />}
                cursor={{ fill: 'rgba(255,255,255,0.03)' }}
              />

              {/* The bars — each gets its tier colour via Cell */}
              <Bar dataKey="count" radius={[0, 3, 3, 0]} maxBarSize={24}>
                {chartData.map((entry) => (
                  <Cell
                    key={entry.tier}
                    fill={TIER_COLOURS[entry.tier] ?? '#4a5568'}
                    /* Slightly transparent fill — full opacity feels too heavy */
                    fillOpacity={0.85}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Legend — manual HTML instead of Recharts default (gives more control) */}
      <div className="risk-chart__legend">
        {Object.entries(TIER_COLOURS).map(([tier, colour]) => (
          <span key={tier} className="risk-chart__legend-item">
            <span
              className="risk-chart__legend-dot"
              style={{ background: colour }}
            />
            {tier}
          </span>
        ))}
      </div>

    </div>
  )
}
