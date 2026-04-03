/*
  src/hooks/useDashboardData.js
  ─────────────────────────────────────────────────────────────────────────────
  CUSTOM HOOKS: React Query hooks for all polled dashboard data.

  WHY REACT QUERY?
    The dashboard needs fresh data every 60 seconds. Without React Query you
    would write: useEffect + setInterval + fetch + useState + error handling
    for EVERY data source. That is 50+ lines per query.

    React Query gives you: caching, deduplication, background refetching,
    loading/error states — all in one hook call.

  MOCK MODE:
    MOCK_MODE = true returns mock data immediately.
    MOCK_MODE = false fetches from the real FastAPI endpoints.
    Switch it when the backend is ready. Components never change.

  POLLING INTERVALS:
    KPI summary:   60 seconds — fast enough for dashboards, light on DB
    Top at-risk:   60 seconds — same
    Churn trend:   120 seconds — rarely changes between batch cycles
    Drift reports: 120 seconds — only changes after a batch run
  ─────────────────────────────────────────────────────────────────────────────
*/

import { useQuery } from '@tanstack/react-query'
import {
  MOCK_KPI_SUMMARY,
  MOCK_RISK_DISTRIBUTION,
  MOCK_CHURN_TREND,
  MOCK_TOP_AT_RISK,
  MOCK_DRIFT_FEATURES,
  MOCK_LAST_BATCH,
} from '../data/mockData'

/* Change to false when FastAPI is running */
const MOCK_MODE = false

/* ── HELPER: create a fake async function for mock mode ─────────────────── */
/* Returns a Promise that resolves with `data` after a brief delay
   (simulates network latency so loading states work during development) */
const mockFetch = (data, delayMs = 300) =>
  new Promise(resolve => setTimeout(() => resolve(data), delayMs))

/* ── HELPER: real API fetcher ────────────────────────────────────────────── */
/* Wraps fetch() and throws on non-2xx responses so React Query catches them */
const BASE_URL = import.meta.env.VITE_API_URL || ''
const ADMIN_KEY = import.meta.env.VITE_ADMIN_KEY || ''

const apiFetch = async (endpoint) => {
  const res = await fetch(`${BASE_URL}${endpoint}`, {
    headers: { 'X-Admin-Key': ADMIN_KEY }
  })
  if (!res.ok) throw new Error(`API error ${res.status}: ${endpoint}`)
  return res.json()
}


/* ── HOOK: useKPISummary ─────────────────────────────────────────────────── */
/*
  Fetches the KPI summary numbers for the four top cards.
  Source: v_current_risk_summary view via GET /api/v1/admin/overview
*/
export function useKPISummary() {
  return useQuery({
    /* queryKey uniquely identifies this query in the React Query cache.
       When a 'batch_completed' SSE event arrives, we call
       queryClient.invalidateQueries(['kpi-summary']) to force a refetch. */
    queryKey: ['kpi-summary'],

    queryFn: MOCK_MODE
      ? () => mockFetch(MOCK_KPI_SUMMARY)
      : () => apiFetch('/api/v1/admin/overview'),

    /* Refetch every 60 seconds in the background */
    refetchInterval: 60_000,

    /* Keep showing stale data while refetching — no loading flash */
    staleTime: 50_000,
  })
}


/* ── HOOK: useRiskDistribution ───────────────────────────────────────────── */
/*
  Fetches risk tier counts for the horizontal bar chart.
  Source: v_current_risk_summary view
*/
export function useRiskDistribution() {
  return useQuery({
    queryKey: ['risk-distribution'],
    queryFn:  MOCK_MODE
      ? () => mockFetch(MOCK_RISK_DISTRIBUTION)
      : () => apiFetch('/api/v1/admin/risk-distribution'),
    refetchInterval: 60_000,
    staleTime:       50_000,
  })
}


/* ── HOOK: useChurnTrend ─────────────────────────────────────────────────── */
/*
  Fetches churn rate per batch cycle for the line chart.
  Source: v_churn_trend view via GET /api/v1/admin/churn-trend
*/
export function useChurnTrend() {
  return useQuery({
    queryKey: ['churn-trend'],
    queryFn:  MOCK_MODE
      ? () => mockFetch(MOCK_CHURN_TREND)
      : () => apiFetch('/api/v1/admin/churn-trend'),
    /* Trend data changes only after a batch run — 2 min polling is enough */
    refetchInterval: 120_000,
    staleTime:       110_000,
  })
}


/* ── HOOK: useTopAtRisk ──────────────────────────────────────────────────── */
/*
  Fetches the top 20 at-risk customers for the table.
  Source: v_top_at_risk view via GET /api/v1/admin/at-risk
*/
export function useTopAtRisk() {
  return useQuery({
    queryKey: ['top-at-risk'],
    queryFn:  MOCK_MODE
      ? () => mockFetch(MOCK_TOP_AT_RISK)
      : () => apiFetch('/api/v1/admin/at-risk'),
    refetchInterval: 60_000,
    staleTime:       50_000,
  })
}


/* ── HOOK: useDriftMonitor ───────────────────────────────────────────────── */
/*
  Fetches PSI drift values per feature for the drift monitor table.
  Source: drift_reports joined with batch_runs via GET /api/v1/admin/drift
*/
export function useDriftMonitor() {
  return useQuery({
    queryKey: ['drift-monitor'],
    queryFn:  MOCK_MODE
      ? () => mockFetch(MOCK_DRIFT_FEATURES)
      : () => apiFetch('/api/v1/admin/drift'),
    refetchInterval: 120_000,
    staleTime:       110_000,
  })
}


/* ── HOOK: useLastBatch ──────────────────────────────────────────────────── */
/*
  Fetches the most recent batch run for the health bar.
  Source: batch_runs table via GET /api/v1/admin/last-batch
*/
export function useLastBatch() {
  return useQuery({
    queryKey: ['last-batch'],
    queryFn:  MOCK_MODE
      ? () => mockFetch(MOCK_LAST_BATCH)
      : () => apiFetch('/api/v1/admin/last-batch'),
    refetchInterval: 60_000,
    staleTime:       50_000,
  })
}
