/*
  src/ErrorBoundary.jsx
  ─────────────────────────────────────────────────────────────────────────────
  REACT ERROR BOUNDARY

  WHY THIS EXISTS:
    Without an ErrorBoundary, any unhandled JavaScript error thrown inside
    a React component during render causes the entire component tree to
    unmount. React 18 replaces everything with nothing → black screen.

    An ErrorBoundary catches these errors, logs them to the console with
    the full stack trace, and renders a visible error message instead.
    The rest of the app stays mounted.

  HOW IT WORKS:
    ErrorBoundary must be a CLASS component — React's getDerivedStateFromError
    and componentDidCatch lifecycle methods are not available as hooks.

    getDerivedStateFromError(error):
      Called during rendering when a child throws.
      Must be pure — no side effects. Just updates state to trigger fallback UI.

    componentDidCatch(error, info):
      Called after rendering. Used for side effects — logging.
      info.componentStack shows which component threw.

  PLACEMENT:
    Wrap the entire Dashboard so any crash in any zone shows the error message
    instead of a black screen. The HealthBar is outside the boundary so it
    stays visible even when the main content crashes.
  ─────────────────────────────────────────────────────────────────────────────
*/

import { Component } from 'react'

export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props)
    // hasError: true → render the fallback UI
    // error: the caught Error object (for display)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error) {
    // Called synchronously during the render that threw.
    // Returns new state — triggers the next render with fallback UI.
    return { hasError: true, error }
  }

  componentDidCatch(error, info) {
    // Called after the boundary has rendered the fallback UI.
    // Use for logging — this is the full crash report.
    console.error(
      '[ErrorBoundary] React component crashed:',
      '\nError:', error.message,
      '\nStack:', error.stack,
      '\nComponent stack:', info.componentStack
    )
  }

  render() {
    if (this.state.hasError) {
      // Fallback UI — visible instead of black screen
      return (
        <div style={{
          // Use inline styles so this renders even if CSS files are broken
          margin:       '20px',
          padding:      '20px 24px',
          background:   'rgba(239, 68, 68, 0.08)',
          border:       '1px solid rgba(239, 68, 68, 0.35)',
          borderRadius: '12px',
          fontFamily:   'JetBrains Mono, monospace',
        }}>
          {/* Error header */}
          <p style={{ color: '#ef4444', fontSize: '12px', fontWeight: 500, margin: '0 0 8px' }}>
            ▲ DASHBOARD RENDER ERROR
          </p>

          {/* Error message — the actual JavaScript error */}
          <p style={{ color: 'rgba(255,255,255,0.85)', fontSize: '12px', margin: '0 0 12px' }}>
            {this.state.error?.message ?? 'Unknown error'}
          </p>

          {/* Instructions */}
          <p style={{ color: 'rgba(255,255,255,0.45)', fontSize: '11px', margin: '0 0 12px' }}>
            Open browser DevTools → Console for the full stack trace.
            This error is also logged to Render's dashboard.
          </p>

          {/* Retry button — resets the error boundary state */}
          <button
            onClick={() => this.setState({ hasError: false, error: null })}
            style={{
              padding:      '6px 14px',
              background:   'rgba(239, 68, 68, 0.15)',
              border:       '1px solid rgba(239, 68, 68, 0.35)',
              borderRadius: '8px',
              color:        '#ef4444',
              fontSize:     '11px',
              cursor:       'pointer',
              fontFamily:   'inherit',
            }}
          >
            Retry
          </button>
        </div>
      )
    }

    // No error — render children normally
    return this.props.children
  }
}
