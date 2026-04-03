/*
  src/data/mockData.js
  ─────────────────────────────────────────────────────────────────────────────
  MOCK DATA — mirrors the real Supabase data shape exactly.

  WHY THIS FILE EXISTS:
    The backend (FastAPI) is not built yet. Every component that will
    eventually fetch from /api/v1/... reads from here during development.

    When the backend is ready, you replace each hook's mock return value
    with a real fetch() call. The component JSX never changes — only the
    data source changes.

  DATA SOURCE:
    All numbers here come from the actual Supabase seed:
      5,630 customers, 948 HIGH risk, 4,682 LOW risk, kaggle_baseline model.
    Trend data and drift data are illustrative placeholders.
  ─────────────────────────────────────────────────────────────────────────────
*/

/* ── KPI SUMMARY (mirrors v_current_risk_summary view) ─────────────────── */
export const MOCK_KPI_SUMMARY = {
  total_customers:    5630,
  high_risk_count:    948,
  medium_risk_count:  0,      /* 0 until a real trained model runs — kaggle_baseline only produces HIGH/LOW */
  low_risk_count:     4682,
  onboarding_count:   0,
  high_risk_pct:      16.8,
  last_scored_at:     '2026-03-15T23:47:57Z',
}

/* ── RISK DISTRIBUTION (for the horizontal bar chart) ──────────────────── */
export const MOCK_RISK_DISTRIBUTION = [
  { tier: 'HIGH',       count: 948,  color: '#ef4444' },
  { tier: 'MEDIUM',     count: 0,    color: '#f59e0b' },
  { tier: 'LOW',        count: 4682, color: '#10b981' },
  { tier: 'ONBOARDING', count: 0,    color: '#3b82f6' },
]

/* ── CHURN TREND (mirrors v_churn_trend view — last 10 batch cycles) ────── */
export const MOCK_CHURN_TREND = [
  { batch_date: '2025-10-15', high_risk_pct: 19.2, drift_alert_fired: false, customers_scored: 180 },
  { batch_date: '2025-11-04', high_risk_pct: 17.8, drift_alert_fired: false, customers_scored: 195 },
  { batch_date: '2025-11-24', high_risk_pct: 18.5, drift_alert_fired: true,  customers_scored: 202 }, /* drift event */
  { batch_date: '2025-12-14', high_risk_pct: 16.1, drift_alert_fired: false, customers_scored: 210 },
  { batch_date: '2026-01-03', high_risk_pct: 17.4, drift_alert_fired: false, customers_scored: 218 },
  { batch_date: '2026-01-23', high_risk_pct: 15.9, drift_alert_fired: false, customers_scored: 225 },
  { batch_date: '2026-02-12', high_risk_pct: 17.0, drift_alert_fired: false, customers_scored: 229 },
  { batch_date: '2026-03-04', high_risk_pct: 16.5, drift_alert_fired: false, customers_scored: 234 },
  { batch_date: '2026-03-15', high_risk_pct: 16.8, drift_alert_fired: false, customers_scored: 5630 }, /* seed run */
]

/* ── TOP AT-RISK CUSTOMERS (mirrors v_top_at_risk view) ─────────────────── */
export const MOCK_TOP_AT_RISK = [
  { customer_id: 'e5fdab1d', display_id: '#50001', churn_probability: 1.00, risk_tier: 'HIGH', tenure_months: 4,  day_since_last_order: 5,  satisfaction_score: 2, complain: true,  top_reason: 'Complaint raised',    gender: 'Female', city_tier: 3 },
  { customer_id: 'a1b2c3d4', display_id: '#50089', churn_probability: 1.00, risk_tier: 'HIGH', tenure_months: 2,  day_since_last_order: 12, satisfaction_score: 1, complain: true,  top_reason: 'Low satisfaction',    gender: 'Male',   city_tier: 2 },
  { customer_id: 'b3c4d5e6', display_id: '#50134', churn_probability: 1.00, risk_tier: 'HIGH', tenure_months: 7,  day_since_last_order: 3,  satisfaction_score: 3, complain: true,  top_reason: 'Short tenure',        gender: 'Female', city_tier: 1 },
  { customer_id: 'c5d6e7f8', display_id: '#50201', churn_probability: 1.00, risk_tier: 'HIGH', tenure_months: 1,  day_since_last_order: 20, satisfaction_score: 2, complain: false, top_reason: 'DaySinceLastOrder',   gender: 'Male',   city_tier: 3 },
  { customer_id: 'd7e8f9a0', display_id: '#50388', churn_probability: 1.00, risk_tier: 'HIGH', tenure_months: 3,  day_since_last_order: 8,  satisfaction_score: 1, complain: true,  top_reason: 'Complaint + low sat', gender: 'Female', city_tier: 2 },
  { customer_id: 'e9f0a1b2', display_id: '#50512', churn_probability: 1.00, risk_tier: 'HIGH', tenure_months: 5,  day_since_last_order: 15, satisfaction_score: 2, complain: false, top_reason: 'Low order frequency', gender: 'Male',   city_tier: 1 },
  { customer_id: 'f1a2b3c4', display_id: '#50677', churn_probability: 1.00, risk_tier: 'HIGH', tenure_months: 9,  day_since_last_order: 31, satisfaction_score: 3, complain: false, top_reason: 'DaySinceLastOrder',   gender: 'Female', city_tier: 2 },
  { customer_id: 'a2b3c4d5', display_id: '#50721', churn_probability: 1.00, risk_tier: 'HIGH', tenure_months: 2,  day_since_last_order: 2,  satisfaction_score: 1, complain: true,  top_reason: 'New + complaint',     gender: 'Male',   city_tier: 3 },
  { customer_id: 'b4c5d6e7', display_id: '#50844', churn_probability: 1.00, risk_tier: 'HIGH', tenure_months: 6,  day_since_last_order: 9,  satisfaction_score: 2, complain: true,  top_reason: 'Complaint raised',    gender: 'Female', city_tier: 1 },
  { customer_id: 'c6d7e8f9', display_id: '#50912', churn_probability: 1.00, risk_tier: 'HIGH', tenure_months: 11, day_since_last_order: 22, satisfaction_score: 1, complain: false, top_reason: 'Silent dissatisfied', gender: 'Male',   city_tier: 2 },
]

/* ── FEATURE DRIFT (mirrors drift_reports joined with batch_runs) ────────── */
export const MOCK_DRIFT_FEATURES = [
  { feature_name: 'DaySinceLastOrder',          psi_value: null, drift_level: 'pending', reference_mean: 4.54,  current_mean: null },
  { feature_name: 'Tenure',                     psi_value: null, drift_level: 'pending', reference_mean: 10.19, current_mean: null },
  { feature_name: 'OrderCount',                 psi_value: null, drift_level: 'pending', reference_mean: 3.01,  current_mean: null },
  { feature_name: 'SatisfactionScore',          psi_value: null, drift_level: 'pending', reference_mean: 3.07,  current_mean: null },
  { feature_name: 'CouponUsed',                 psi_value: null, drift_level: 'pending', reference_mean: 1.75,  current_mean: null },
  { feature_name: 'CashbackAmount',             psi_value: null, drift_level: 'pending', reference_mean: 177.2, current_mean: null },
  { feature_name: 'HourSpendOnApp',             psi_value: null, drift_level: 'pending', reference_mean: 2.93,  current_mean: null },
  { feature_name: 'WarehouseToHome',            psi_value: null, drift_level: 'pending', reference_mean: 15.64, current_mean: null },
  { feature_name: 'OrderAmountHikeFromlastYear',psi_value: null, drift_level: 'pending', reference_mean: 15.71, current_mean: null },
  { feature_name: 'Complain',                   psi_value: null, drift_level: 'pending', reference_mean: 0.28,  current_mean: null },
]

/* ── BATCH RUN INFO (mirrors batch_runs table) ───────────────────────────── */
export const MOCK_LAST_BATCH = {
  id:                 'seed-run-001',
  model_version:      'kaggle_baseline',
  triggered_by:       'manual',
  status:             'completed',
  started_at:         '2026-03-15T23:47:50Z',
  completed_at:       '2026-03-15T23:47:57Z',
  duration_seconds:   7,
  customers_scored:   5630,
  high_risk_count:    948,
  medium_risk_count:  0,
  low_risk_count:     4682,
  drift_checked:      false,
  drift_alert_fired:  false,
}

/* ── INITIAL SSE EVENTS (simulated — will come from FastAPI in production) ── */
export const MOCK_INITIAL_EVENTS = [
  {
    id:         'evt-001',
    event_type: 'batch_completed',
    payload: {
      customers_scored:  5630,
      high_risk_count:   948,
      duration_seconds:  7,
      drift_alert:       false,
    },
    created_at: '2026-03-15T23:47:57Z',
  },
  {
    id:         'evt-002',
    event_type: 'high_churn_alert',
    payload: {
      customer_id:  '#50001',
      score:        1.00,
      risk_tier:    'HIGH',
      top_reason:   'Complaint raised',
    },
    created_at: '2026-03-15T23:47:55Z',
  },
  {
    id:         'evt-003',
    event_type: 'new_customer',
    payload: {
      customer_id: '#50001',
      city_tier:   3,
      payment:     'Debit Card',
      device:      'Mobile Phone',
    },
    created_at: '2026-03-15T23:47:51Z',
  },
]
