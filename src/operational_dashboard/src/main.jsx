/*
  src/main.jsx
  ─────────────────────────────────────────────────────────────────────────────
  REACT ENTRY POINT

  This file does three things only:
    1. Import global CSS (design tokens, reset)
    2. Wrap the app in QueryClientProvider (required by React Query)
    3. Mount Dashboard into the #root div in index.html

  WHY QueryClientProvider HERE?
    React Query's useQuery hooks require a QueryClient context above them
    in the component tree. Wrapping at the root means every component in
    the entire app has access to the query cache — no need to pass it down.

  STALE TIME DEFAULTS:
    staleTime: 0 means React Query considers cached data stale immediately
    after fetching. Individual hooks override this with their own staleTime.
    We set a global default of 30s so queries don't refetch unnecessarily
    when components re-mount.
  ─────────────────────────────────────────────────────────────────────────────
*/

import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

/* Global CSS — design tokens, reset, base styles */
import './index.css'

/* Root component */
import Dashboard from './Dashboard'

/* ── Create the React Query client ──────────────────────────────────────── */
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      /* How long cached data is considered fresh before React Query
         refetches it in the background.
         Individual hooks set their own staleTime — this is the fallback. */
      staleTime: 30_000,       /* 30 seconds */

      /* How many times to retry a failed query before giving up */
      retry: 2,

      /* How long to wait between retries — exponential backoff:
         attempt 1: 1000ms, attempt 2: 2000ms */
      retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 5000),

      /* Do not refetch when the user switches back to this browser tab.
         Dashboards benefit from stable data while being read.
         Remove this line if you want aggressive freshness on tab focus. */
      refetchOnWindowFocus: false,
    },
  },
})


/* ── Mount React into the DOM ────────────────────────────────────────────── */
ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    {/*
      StrictMode: in development, React renders components twice to detect
      side-effects. This is intentional and harmless — it does not happen
      in production builds (npm run build).
    */}
    <QueryClientProvider client={queryClient}>
      <Dashboard />
    </QueryClientProvider>
  </React.StrictMode>
)
