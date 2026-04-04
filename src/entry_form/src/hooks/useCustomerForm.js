/*
  src/hooks/useCustomerForm.js
  ─────────────────────────────────────────────────────────────────────────────
  CUSTOM HOOK: useCustomerForm

  FIX APPLIED — Response unwrapping:
    The FastAPI backend wraps every response in:
      { "success": true, "data": {...}, "message": "..." }

    BEFORE (broken):
      responseData = await response.json()
      // responseData = { success, data, message }
      // SuccessCard tried: responseData.customer_id → undefined

    AFTER (fixed):
      const body = await response.json()
      responseData = body.data   // unwrap the envelope
      // SuccessCard now gets: { customer_id, registered_at, ... }

  TIMING CONSTANTS (visible to the caller):
    MOCK_DELAY_MS = 900ms   — simulated network delay in mock mode
    Auto-reset after 5000ms — form clears 5s after successful registration
  ─────────────────────────────────────────────────────────────────────────────
*/

import { useState, useCallback } from 'react'
import { FORM_FIELDS, INITIAL_FORM_STATE } from '../data/formConfig'

// ── CONFIGURATION ─────────────────────────────────────────────────────────────

// Set to false: always use real FastAPI
const MOCK_MODE = false

// Simulated network delay in mock mode — makes loading spinner visible
const MOCK_DELAY_MS = 900

// How long the SuccessCard stays visible before the form auto-resets (ms)
const SUCCESS_AUTO_RESET_MS = 5000

// ── UUID GENERATOR (mock only) ─────────────────────────────────────────────────
function generateMockUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0
    const v = c === 'x' ? r : (r & 0x3) | 0x8
    return v.toString(16)
  })
}

// ── FORM VALIDATION ────────────────────────────────────────────────────────────
function validateForm(formData) {
  const errors = {}
  FORM_FIELDS.forEach((field) => {
    if (!field.required) return
    if (!formData[field.id] || formData[field.id].trim() === '') {
      errors[field.id] = `${field.label} is required`
    }
  })
  return errors
}

// ── MAIN HOOK ──────────────────────────────────────────────────────────────────
export function useCustomerForm() {
  const [formData,     setFormData]     = useState(INITIAL_FORM_STATE)
  const [errors,       setErrors]       = useState({})
  const [submitStatus, setSubmitStatus] = useState('idle')
  // 'idle' | 'loading' | 'success' | 'error'

  const [successData,   setSuccessData]   = useState(null)
  // Populated from body.data after successful registration:
  // { customer_id, registered_at, days_until_scoreable, status, initial_features }

  const [errorMessage, setErrorMessage] = useState('')

  // ── handleChange ─────────────────────────────────────────────────────────────
  const handleChange = useCallback((fieldId, value) => {
    setFormData(prev => ({ ...prev, [fieldId]: value }))
    // Clear this field's error as user corrects it
    setErrors(prev => {
      if (!prev[fieldId]) return prev
      const next = { ...prev }
      delete next[fieldId]
      return next
    })
  }, [])

  // ── handleSubmit ──────────────────────────────────────────────────────────────
  const handleSubmit = useCallback(async () => {

    // Step 1: Client-side validation
    const validationErrors = validateForm(formData)
    if (Object.keys(validationErrors).length > 0) {
      setErrors(validationErrors)
      return
    }

    setErrors({})
    setErrorMessage('')
    setSubmitStatus('loading')

    try {
      let responseData

      if (MOCK_MODE) {
        // ── MOCK path ────────────────────────────────────────────────────────
        await new Promise(resolve => setTimeout(resolve, MOCK_DELAY_MS))
        responseData = {
          customer_id:          generateMockUUID(),
          registered_at:        new Date().toISOString(),
          days_until_scoreable: 30,
          status:               'created',
          initial_features:     {},
        }

      } else {
        // ── REAL API path ─────────────────────────────────────────────────────
        const BASE_URL = import.meta.env.VITE_API_URL || ''

        // city_tier is stored as SMALLINT in the DB — convert string → int
        const payload = {
          ...formData,
          city_tier: formData.city_tier ? parseInt(formData.city_tier, 10) : null,
        }

        const response = await fetch(`${BASE_URL}/api/v1/customers/register`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify(payload),
        })

        if (!response.ok) {
          // FastAPI returns { detail: "..." } for error responses
          const errorBody = await response.json().catch(() => ({}))
          throw new Error(
            errorBody.detail ??
            errorBody.message ??
            `Registration failed (HTTP ${response.status})`
          )
        }

        // ── FIX: unwrap the APIResponse envelope ─────────────────────────────
        // FastAPI returns: { "success": true, "data": { ... }, "message": "..." }
        // We need body.data, NOT the wrapper itself.
        // Before this fix, SuccessCard received the wrapper and
        // data.customer_id was undefined.
        const body = await response.json()
        responseData = body.data ?? body
        // body.data ?? body: if body.data exists (normal case), use it.
        // If somehow the endpoint returns data directly (defensive fallback), use body.
      }

      // Step 3: Success — store data and show SuccessCard
      setSuccessData(responseData)
      setSubmitStatus('success')

      // Auto-reset after SUCCESS_AUTO_RESET_MS so operator can enter next customer
      setTimeout(() => {
        resetForm()
      }, SUCCESS_AUTO_RESET_MS)

    } catch (err) {
      setErrorMessage(err.message ?? 'Registration failed. Please try again.')
      setSubmitStatus('error')
    }
  }, [formData])

  // ── resetForm ─────────────────────────────────────────────────────────────────
  const resetForm = useCallback(() => {
    setFormData(INITIAL_FORM_STATE)
    setErrors({})
    setSubmitStatus('idle')
    setSuccessData(null)
    setErrorMessage('')
  }, [])

  return {
    formData,
    errors,
    submitStatus,
    successData,
    errorMessage,
    handleChange,
    handleSubmit,
    resetForm,
  }
}
