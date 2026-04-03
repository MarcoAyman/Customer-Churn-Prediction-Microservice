/*
  src/components/AtRiskTable/AtRiskTable.jsx
  ─────────────────────────────────────────────────────────────────────────────
  ZONE 3 — TOP AT-RISK CUSTOMER TABLE

  Shows the top 20 customers by churn probability.
  Columns: Risk badge, Score bar, Customer ID, Tenure, Last Order,
           Satisfaction stars, Complaint flag, Top SHAP reason.

  DATA SOURCE: useTopAtRisk() → v_top_at_risk view
  ─────────────────────────────────────────────────────────────────────────────
*/

import { useState } from 'react'
import './AtRiskTable.css'

/* ── RISK BADGE ──────────────────────────────────────────────────────────── */
function RiskBadge({ tier }) {
  const classMap = {
    HIGH:       'badge--high',
    MEDIUM:     'badge--medium',
    LOW:        'badge--low',
    ONBOARDING: 'badge--onboard',
  }
  return (
    <span className={`risk-badge ${classMap[tier] ?? 'badge--low'}`}>
      {tier}
    </span>
  )
}

/* ── SCORE BAR — visual representation of churn probability ─────────────── */
function ScoreBar({ probability }) {
  const pct = Math.round(probability * 100)
  /* Colour the fill based on the score value */
  const colour =
    pct >= 70 ? 'var(--color-high)'   :
    pct >= 45 ? 'var(--color-medium)' :
                'var(--color-low)'

  return (
    <div className="score-bar">
      <div
        className="score-bar__fill"
        style={{ width: `${pct}%`, background: colour }}
      />
      <span className="score-bar__label">{probability.toFixed(2)}</span>
    </div>
  )
}

/* ── SATISFACTION STARS ──────────────────────────────────────────────────── */
/* Shows 1–5 stars filled/empty based on satisfaction_score */
function SatisfactionStars({ score }) {
  return (
    <span className="sat-stars">
      {[1, 2, 3, 4, 5].map(i => (
        <span
          key={i}
          className={`sat-stars__star ${i <= score ? 'sat-stars__star--filled' : ''}`}
        >
          ★
        </span>
      ))}
    </span>
  )
}


/* ── MAIN EXPORT ─────────────────────────────────────────────────────────── */
export default function AtRiskTable({ data, isLoading }) {
  /* Track which row is expanded (click to see more detail) */
  const [expandedId, setExpandedId] = useState(null)

  /* Toggle row expansion */
  const toggleExpand = (id) => {
    setExpandedId(prev => prev === id ? null : id)
  }

  return (
    <div className="at-risk__panel">

      {/* Panel header */}
      <div className="at-risk__header">
        <h3 className="at-risk__title">Top At-Risk Customers</h3>
        <span className="at-risk__badge">v_top_at_risk · top 20</span>
      </div>

      {/* Table */}
      <div className="at-risk__scroll">
        <table className="at-risk__table">
          <thead>
            <tr>
              <th>Tier</th>
              <th>Score</th>
              <th>Customer</th>
              <th>Tenure</th>
              <th>Last Order</th>
              <th>Sat.</th>
              <th>Complaint</th>
              <th>Top Reason</th>
            </tr>
          </thead>

          <tbody>
            {isLoading ? (
              /* Skeleton rows while data loads */
              Array.from({ length: 5 }).map((_, i) => (
                <tr key={i} className="at-risk__skeleton-row">
                  {Array.from({ length: 8 }).map((_, j) => (
                    <td key={j}><div className="at-risk__skeleton-cell" /></td>
                  ))}
                </tr>
              ))
            ) : (
              (data ?? []).map((customer) => (
                <>
                  {/* Main row */}
                  <tr
                    key={customer.customer_id}
                    className={`at-risk__row ${expandedId === customer.customer_id ? 'at-risk__row--expanded' : ''}`}
                    onClick={() => toggleExpand(customer.customer_id)}
                  >
                    <td>
                      <RiskBadge tier={customer.risk_tier} />
                    </td>
                    <td>
                      <ScoreBar probability={customer.churn_probability} />
                    </td>
                    <td className="at-risk__id">
                      {customer.display_id}
                    </td>
                    <td className="at-risk__mono">
                      {customer.tenure_months}mo
                    </td>
                    <td className="at-risk__mono">
                      {customer.day_since_last_order}d ago
                    </td>
                    <td>
                      <SatisfactionStars score={customer.satisfaction_score} />
                    </td>
                    <td>
                      {/* Complaint flag — red dot if complaint raised */}
                      <span className={`complaint-flag ${customer.complain ? 'complaint-flag--yes' : 'complaint-flag--no'}`}>
                        {customer.complain ? '● yes' : '○ no'}
                      </span>
                    </td>
                    <td className="at-risk__reason">
                      {customer.top_reason}
                    </td>
                  </tr>

                  {/* Expanded detail row — shown on click */}
                  {expandedId === customer.customer_id && (
                    <tr key={`${customer.customer_id}-detail`} className="at-risk__detail-row">
                      <td colSpan={8}>
                        <div className="at-risk__detail">
                          <span className="at-risk__detail-item">
                            <span className="at-risk__detail-label">Gender</span>
                            <span>{customer.gender}</span>
                          </span>
                          <span className="at-risk__detail-item">
                            <span className="at-risk__detail-label">City Tier</span>
                            <span>{customer.city_tier}</span>
                          </span>
                          <span className="at-risk__detail-item">
                            <span className="at-risk__detail-label">Churn Prob.</span>
                            <span className="at-risk__mono">{customer.churn_probability.toFixed(4)}</span>
                          </span>
                          <span className="at-risk__detail-item">
                            <span className="at-risk__detail-label">SHAP Reason</span>
                            <span>{customer.top_reason}</span>
                          </span>
                        </div>
                      </td>
                    </tr>
                  )}
                </>
              ))
            )}
          </tbody>
        </table>
      </div>

    </div>
  )
}
