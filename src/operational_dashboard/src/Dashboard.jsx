/*
  src/Dashboard.jsx
  ─────────────────────────────────────────────────────────────────────────────
  DASHBOARD ORCHESTRATOR

  FIX APPLIED — ErrorBoundary:
    The main content is now wrapped in <ErrorBoundary>.
    If any child component crashes (TypeError, null dereference, etc.),
    the ErrorBoundary catches it and renders an error message instead of
    unmounting the entire tree → black screen.

  TIMING CONSTANTS:
    React Query polling:
      KPI / at-risk / last-batch:    60 seconds (in useDashboardData.js)
      churn-trend / drift:           120 seconds (in useDashboardData.js)
    SSE keepalive ping:              25 seconds (in sse_service.py)
    SSE reconnect delay on error:    3 seconds  (in useSSE.js)
  ─────────────────────────────────────────────────────────────────────────────
*/

import { useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'

import ErrorBoundary from './ErrorBoundary'

// Zone components
import HealthBar             from './components/HealthBar/HealthBar'
import KPICards              from './components/KPICards/KPICards'
import RiskDistributionChart from './components/RiskDistributionChart/RiskDistributionChart'
import ChurnTrendChart       from './components/ChurnTrendChart/ChurnTrendChart'
import AtRiskTable           from './components/AtRiskTable/AtRiskTable'
import DriftMonitor          from './components/DriftMonitor/DriftMonitor'
import EventFeed             from './components/EventFeed/EventFeed'

// Data hooks
import { useSSE } from './hooks/useSSE'
import {
  useKPISummary,
  useRiskDistribution,
  useChurnTrend,
  useTopAtRisk,
  useDriftMonitor,
  useLastBatch,
} from './hooks/useDashboardData'

import './Dashboard.css'


export default function Dashboard() {
  const queryClient = useQueryClient()

  // SSE — live event stream (always open)
  const { events, status: sseStatus, clearEvents } = useSSE()

  // REST — polled data (React Query)
  const { data: kpiData,      isLoading: kpiLoading }    = useKPISummary()
  const { data: riskDistData, isLoading: riskLoading }   = useRiskDistribution()
  const { data: trendData,    isLoading: trendLoading }  = useChurnTrend()
  const { data: atRiskData,   isLoading: atRiskLoading } = useTopAtRisk()
  const { data: driftData,    isLoading: driftLoading }  = useDriftMonitor()
  const { data: lastBatchData }                           = useLastBatch()

  // When SSE delivers a batch_completed event, immediately invalidate all
  // React Query caches so charts refresh without waiting for the next poll.
  useEffect(() => {
    if (!events.length) return
    const latest = events[0]

    if (latest.event_type === 'batch_completed') {
      // Batch finished — all data is stale, refetch everything now
      queryClient.invalidateQueries({ queryKey: ['kpi-summary'] })
      queryClient.invalidateQueries({ queryKey: ['risk-distribution'] })
      queryClient.invalidateQueries({ queryKey: ['churn-trend'] })
      queryClient.invalidateQueries({ queryKey: ['top-at-risk'] })
      queryClient.invalidateQueries({ queryKey: ['drift-monitor'] })
      queryClient.invalidateQueries({ queryKey: ['last-batch'] })
    }

    if (latest.event_type === 'drift_alert') {
      queryClient.invalidateQueries({ queryKey: ['drift-monitor'] })
      queryClient.invalidateQueries({ queryKey: ['last-batch'] })
    }

    if (latest.event_type === 'model_promoted') {
      queryClient.invalidateQueries()  // everything changed
    }
  }, [events[0]?.id, queryClient])


  return (
    <div className="dashboard">

      {/* Zone 1: Health Bar — outside ErrorBoundary so it stays visible even on crash */}
      <HealthBar
        lastBatch={lastBatchData}
        kpiSummary={kpiData}
        sseStatus={sseStatus}
      />

      {/* ErrorBoundary wraps all main content.
          Any crash in Zones 2–3 shows the error message instead of black screen. */}
      <ErrorBoundary>

        {/* Zone 2: KPI Cards */}
        <KPICards
          data={kpiData}
          isLoading={kpiLoading}
        />

        {/* Zone 3: Main Content + SSE Feed */}
        <div className="dashboard__main">

          {/* Left column — charts and tables */}
          <div className="dashboard__left">
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

            <AtRiskTable
              data={atRiskData}
              isLoading={atRiskLoading}
            />

            <DriftMonitor
              data={driftData}
              isLoading={driftLoading}
            />
          </div>

          {/* Right column — live SSE event feed */}
          <div className="dashboard__right">
            <EventFeed
              events={events}
              sseStatus={sseStatus}
              onClear={clearEvents}
            />
          </div>

        </div>

      </ErrorBoundary>

    </div>
  )
}
