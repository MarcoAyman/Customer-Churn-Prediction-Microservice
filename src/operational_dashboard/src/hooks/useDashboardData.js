/*
  src/hooks/useDashboardData.js
  ─────────────────────────────────────────────────────────────────────────────
  React Query hooks for all polled dashboard data.

  All admin endpoints require X-Admin-Key header.
  Regular fetch() supports custom headers — only EventSource cannot.
  So these polling hooks use the header approach (correct for fetch).
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

// ── CONFIGURATION ─────────────────────────────────────────────────────────────

// Set to false when FastAPI is running and reachable
const MOCK_MODE = false

// Read environment variables set in Vercel dashboard
const BASE_URL  = import.meta.env.VITE_API_URL  || ''
const ADMIN_KEY = import.meta.env.VITE_ADMIN_KEY || ''

// Log config on load so you can see in browser console what was picked up
if (!MOCK_MODE) {
  console.log('[Dashboard] API URL:', BASE_URL || '(not set — check VITE_API_URL)')
  console.log('[Dashboard] Admin key set:', ADMIN_KEY ? 'YES' : 'NO — check VITE_ADMIN_KEY')
}


// ── HELPERS ───────────────────────────────────────────────────────────────────

// Simulates network delay for mock mode so loading states are visible
const mockFetch = (data, delayMs = 300) =>
  new Promise(resolve => setTimeout(() => resolve(data), delayMs))

// Real API fetcher — sends X-Admin-Key header with every admin request
// fetch() supports custom headers (unlike EventSource)
const apiFetch = async (endpoint) => {
  const url = `${BASE_URL}${endpoint}`

  const res = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      'X-Admin-Key':  ADMIN_KEY,   // sent as header — fetch supports this
    },
  })

  if (!res.ok) {
    const errorBody = await res.json().catch(() => ({}))
    throw new Error(
      `API error ${res.status} on ${endpoint}: ` +
      (errorBody.message || errorBody.detail || res.statusText)
    )
  }

  const body = await res.json()
  // All admin endpoints return { success, data, message }
  // Return just the data field for React Query
  return body.data ?? body
}


// ── HOOKS ─────────────────────────────────────────────────────────────────────

export function useKPISummary() {
  return useQuery({
    queryKey: ['kpi-summary'],
    queryFn:  MOCK_MODE
      ? () => mockFetch(MOCK_KPI_SUMMARY)
      : () => apiFetch('/api/v1/admin/overview'),
    refetchInterval: 60_000,
    staleTime:       50_000,
  })
}

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

export function useChurnTrend() {
  return useQuery({
    queryKey: ['churn-trend'],
    queryFn:  MOCK_MODE
      ? () => mockFetch(MOCK_CHURN_TREND)
      : () => apiFetch('/api/v1/admin/churn-trend'),
    refetchInterval: 120_000,
    staleTime:       110_000,
  })
}

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
