/*
  src/main.jsx
  ─────────────────────────────────────────────────────────────────────────────
  REACT ENTRY POINT

  Minimal — just imports global CSS and mounts the root component.
  No providers needed here because this form has no React Query polling
  or global state. All state lives inside useCustomerForm.js.
  ─────────────────────────────────────────────────────────────────────────────
*/

import React from 'react'
import ReactDOM from 'react-dom/client'

/* Global CSS — Tailwind directives + CSS variables + body reset */
import './index.css'

/* Root component — the entire form lives here */
import CustomerEntryForm from './CustomerEntryForm'

/* Mount into the #root div defined in index.html */
ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <CustomerEntryForm />
  </React.StrictMode>
)
