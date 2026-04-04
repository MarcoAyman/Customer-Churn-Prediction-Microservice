/*
  src/components/AtRiskTable/AtRiskTable.jsx
  ─────────────────────────────────────────────────────────────────────────────
  ZONE 3 — TOP AT-RISK CUSTOMER TABLE

  FIXES APPLIED:
    1. React.Fragment key fix:
       BEFORE: <> inside .map() with key on child <tr> — React reconciliation
               could fail to track rows correctly on data updates.
       AFTER:  <React.Fragment key={customer.customer_id}> on the Fragment itself.

    2. Null safety on all fields:
       Real API data can have null values where mock data had numbers.
       E.g. day_since_last_order=null for new customers, satisfaction_score=null.
       Added '?? 0' / '?? —' guards everywhere a value is used in arithmetic
       or method calls.

    3. churn_probability guard:
       Real API returns Decimal converted to float. Guard against NaN/null.
  ─────────────────────────────────────────────────────────────────────────────
*/

import { Fragment, useState } from 'react'
import './AtRiskTable.css'

// ── RISK BADGE ─────────────────────────────────────────────────────────────────
function RiskBadge({ tier }) {
  const classMap = {
    HIGH:       'badge--high',
    MEDIUM:     'badge--medium',
    LOW:        'badge--low',
    ONBOARDING: 'badge--onboard',
  }
  return (
    <span className={`risk-badge ${classMap[tier] ?? 'badge--low'}`}>
      {tier ?? '—'}
    </span>
  )
}

// ── SCORE BAR ──────────────────────────────────────────────────────────────────
function ScoreBar({ probability }) {
  // Guard: probability may be null (new customers) or NaN
  const safeProb = typeof probability === 'number' && !isNaN(probability)
    ? probability
    : 0

  const pct = Math.round(safeProb * 100)
  const colour =
    pct >= 70 ? 'var(--color-high)'   :
    pct >= 45 ? 'var(--color-medium)' :
                'var(--color-low)'

  return (
    <div className="score-bar">
      <div>
        <div
          className="score-bar__fill"
          style={{ width: `${pct}%`, background: colour }}
        />
      </div>
      <span className="score-bar__label">{safeProb.toFixed(2)}</span>
    </div>
  )
}

// ── SATISFACTION STARS ─────────────────────────────────────────────────────────
function SatisfactionStars({ score }) {
  // Guard: score may be null — show no filled stars
  const safeScore = typeof score === 'number' ? score : 0
  return (
    <span className="sat-stars">
      {[1, 2, 3, 4, 5].map(i => (
        <span
          key={i}
          className={`sat-stars__star ${i <= safeScore ? 'sat-stars__star--filled' : ''}`}
        >
          ★
        </span>
      ))}
    </span>
  )
}

// ── MAIN COMPONENT ─────────────────────────────────────────────────────────────
export default function AtRiskTable({ data, isLoading }) {
  const [expandedId, setExpandedId] = useState(null)

  const toggleExpand = (id) => {
    setExpandedId(prev => prev === id ? null : id)
  }

  return (
    <div className="at-risk__panel">
      <div className="at-risk__header">
        <h3 className="at-risk__title">Top At-Risk Customers</h3>
        <span className="at-risk__badge">v_top_at_risk · top 20</span>
      </div>

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
              // Skeleton rows while data is loading
              Array.from({ length: 5 }).map((_, i) => (
                <tr key={i} className="at-risk__skeleton-row">
                  {Array.from({ length: 8 }).map((_, j) => (
                    <td key={j}><div className="at-risk__skeleton-cell" /></td>
                  ))}
                </tr>
              ))
            ) : !data || data.length === 0 ? (
              // Empty state — no at-risk customers
              <tr>
                <td colSpan={8} style={{
                  textAlign: 'center',
                  padding: '24px',
                  color: 'var(--text-tertiary)',
                  fontSize: '12px',
                }}>
                  No HIGH or MEDIUM risk customers found
                </td>
              </tr>
            ) : (
              // ── FIX: use React.Fragment with key on the Fragment, not on child tr ──
              // Before: <> ... <tr key={...}> — key on inner element, not outer Fragment
              // After:  <Fragment key={...}> ... <tr> — correct React reconciliation
              (data ?? []).map((customer) => (
                <Fragment key={customer.customer_id}>

                  {/* Main data row — click to expand detail */}
                  <tr
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
                      {/* display_id is "#e5fdab1d" (first 8 chars of UUID) */}
                      {customer.display_id ?? `#${String(customer.customer_id ?? '').slice(0, 8)}`}
                    </td>
                    <td className="at-risk__mono">
                      {/* tenure_months may be 0.0 for new customers */}
                      {customer.tenure_months != null ? `${customer.tenure_months}mo` : '—'}
                    </td>
                    <td className="at-risk__mono">
                      {/* day_since_last_order is null for customers with no orders */}
                      {customer.day_since_last_order != null
                        ? `${customer.day_since_last_order}d ago`
                        : '—'
                      }
                    </td>
                    <td>
                      {/* satisfaction_score is null until customer rates the service */}
                      <SatisfactionStars score={customer.satisfaction_score} />
                    </td>
                    <td>
                      <span className={`complaint-flag ${customer.complain ? 'complaint-flag--yes' : 'complaint-flag--no'}`}>
                        {customer.complain ? '● yes' : '○ no'}
                      </span>
                    </td>
                    <td className="at-risk__reason">
                      {/* top_reason: "—" for kaggle seed (no SHAP), feature name for live model */}
                      {customer.top_reason ?? '—'}
                    </td>
                  </tr>

                  {/* Expanded detail row — shown when this row is clicked */}
                  {expandedId === customer.customer_id && (
                    <tr className="at-risk__detail-row">
                      <td colSpan={8}>
                        <div className="at-risk__detail">
                          <span className="at-risk__detail-item">
                            <span className="at-risk__detail-label">Gender</span>
                            <span>{customer.gender ?? '—'}</span>
                          </span>
                          <span className="at-risk__detail-item">
                            <span className="at-risk__detail-label">City Tier</span>
                            <span>{customer.city_tier ?? '—'}</span>
                          </span>
                          <span className="at-risk__detail-item">
                            <span className="at-risk__detail-label">Order Count</span>
                            <span className="at-risk__mono">
                              {customer.order_count ?? 0}
                            </span>
                          </span>
                          <span className="at-risk__detail-item">
                            <span className="at-risk__detail-label">Churn Prob.</span>
                            <span className="at-risk__mono">
                              {typeof customer.churn_probability === 'number'
                                ? customer.churn_probability.toFixed(4)
                                : '—'
                              }
                            </span>
                          </span>
                        </div>
                      </td>
                    </tr>
                  )}

                </Fragment>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
