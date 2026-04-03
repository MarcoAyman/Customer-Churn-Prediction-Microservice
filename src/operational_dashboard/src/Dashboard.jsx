/*
  src/Dashboard.jsx
  ─────────────────────────────────────────────────────────────────────────────
  DASHBOARD ORCHESTRATOR

  This is the root component that:
    1. Calls all data hooks (React Query + SSE)
    2. Assembles all six zone components in the correct layout
    3. Passes data down as props — no component fetches its own data

  WHY ALL HOOKS AT THE TOP LEVEL?
    Centralising data fetching here means:
    - Each query runs once, not per-component
    - When a 'batch_completed' SSE event arrives, we call
      queryClient.invalidateQueries() once here and ALL charts refresh
    - Components are pure presentational — they receive data as props

  LAYOUT:
    ┌──────────────────────────────────────────────────────────┐
    │  HealthBar (sticky, full width)                          │
    ├──────────────────────────────────────────────────────────┤
    │  KPICards (4 cards, full width)                          │
    ├─────────────────────────────────┬────────────────────────┤
    │  RiskDistributionChart          │  EventFeed (SSE)       │
    │  ChurnTrendChart                │  (right col, 2 rows    │
    │  AtRiskTable                    │   spanning)            │
    │  DriftMonitor                   │                        │
    └─────────────────────────────────┴────────────────────────┘
  ─────────────────────────────────────────────────────────────────────────────
*/

import { useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'

/* ── Zone components ─────────────────────────────────────────────────────── */
import HealthBar              from './components/HealthBar/HealthBar'
import KPICards               from './components/KPICards/KPICards'
import RiskDistributionChart  from './components/RiskDistributionChart/RiskDistributionChart'
import ChurnTrendChart        from './components/ChurnTrendChart/ChurnTrendChart'
import AtRiskTable            from './components/AtRiskTable/AtRiskTable'
import DriftMonitor           from './components/DriftMonitor/DriftMonitor'
import EventFeed              from './components/EventFeed/EventFeed'

/* ── Data hooks ──────────────────────────────────────────────────────────── */
import { useSSE }            from './hooks/useSSE'
import {
  useKPISummary,
  useRiskDistribution,
  useChurnTrend,
  useTopAtRisk,
  useDriftMonitor,
  useLastBatch,
}                            from './hooks/useDashboardData'

/* ── Dashboard CSS ───────────────────────────────────────────────────────── */
import './Dashboard.css'


export default function Dashboard() {

  /* ── React Query client — used to invalidate queries on SSE events ─────── */
  const queryClient = useQueryClient()

  /* ── SSE hook — provides live events and connection status ─────────────── */
  const { events, status: sseStatus, clearEvents } = useSSE()

  /* ── REST data hooks — each polls on its own interval ───────────────────── */
  const { data: kpiData,          isLoading: kpiLoading }    = useKPISummary()
  const { data: riskDistData,     isLoading: riskLoading }   = useRiskDistribution()
  const { data: trendData,        isLoading: trendLoading }  = useChurnTrend()
  const { data: atRiskData,       isLoading: atRiskLoading } = useTopAtRisk()
  const { data: driftData,        isLoading: driftLoading }  = useDriftMonitor()
  const { data: lastBatchData }                               = useLastBatch()

  /* ── SSE event handler — refresh queries when key events arrive ──────────
     When a 'batch_completed' event arrives, we know the database has fresh
     prediction and batch data. We invalidate all queries so they refetch
     immediately instead of waiting for their next polling interval.
     This is the "reactive" part of the dashboard — SSE triggers REST refresh. */
  useEffect(() => {
    if (events.length === 0) return

    /* Look at the most recent event */
    const latest = events[0]

    if (latest.event_type === 'batch_completed') {
      /* Batch run finished — refresh every data source */
      queryClient.invalidateQueries({ queryKey: ['kpi-summary'] })
      queryClient.invalidateQueries({ queryKey: ['risk-distribution'] })
      queryClient.invalidateQueries({ queryKey: ['churn-trend'] })
      queryClient.invalidateQueries({ queryKey: ['top-at-risk'] })
      queryClient.invalidateQueries({ queryKey: ['drift-monitor'] })
      queryClient.invalidateQueries({ queryKey: ['last-batch'] })
    }

    if (latest.event_type === 'drift_alert') {
      /* Drift alert — refresh drift and batch data specifically */
      queryClient.invalidateQueries({ queryKey: ['drift-monitor'] })
      queryClient.invalidateQueries({ queryKey: ['last-batch'] })
    }

    if (latest.event_type === 'model_promoted') {
      /* Model changed — refresh everything */
      queryClient.invalidateQueries()
    }

  /* We only want this to re-run when events[0] changes (a new event arrived).
     Depending on events.length would cause unnecessary re-runs. */
  }, [events[0]?.id, queryClient])


  return (
    <div className="dashboard">

      {/* ── ZONE 1: System Health Bar ─────────────────────────────────────── */}
      <HealthBar
        lastBatch={lastBatchData}
        kpiSummary={kpiData}
        sseStatus={sseStatus}
      />

      {/* ── ZONE 2: KPI Cards ─────────────────────────────────────────────── */}
      <KPICards
        data={kpiData}
        isLoading={kpiLoading}
      />

      {/* ── ZONE 3: Main Content + SSE Feed ───────────────────────────────── */}
      <div className="dashboard__main">

        {/* LEFT COLUMN — charts, tables */}
        <div className="dashboard__left">

          {/* Chart row — Risk Distribution + Churn Trend side by side */}
          <div className="dashboard__chart-row">
            <RiskDistributionChart
              data={riskDistData}
              isLoading={riskLoading}
            />
            <ChurnTrendChart
              data={trendData}
              isLoading={trendLoading}
            />
          </div>

          {/* At-Risk Customer Table */}
          <AtRiskTable
            data={atRiskData}
            isLoading={atRiskLoading}
          />

          {/* Feature Drift Monitor */}
          <DriftMonitor
            data={driftData}
            isLoading={driftLoading}
          />

        </div>

        {/* RIGHT COLUMN — Live SSE Event Feed */}
        <div className="dashboard__right">
          <EventFeed
            events={events}
            sseStatus={sseStatus}
            onClear={clearEvents}
          />
        </div>

      </div>

    </div>
  )
}
