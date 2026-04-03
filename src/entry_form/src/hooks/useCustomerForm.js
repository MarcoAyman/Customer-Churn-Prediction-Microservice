/*
  src/hooks/useCustomerForm.js
  ─────────────────────────────────────────────────────────────────────────────
  CUSTOM HOOK: useCustomerForm

  Manages all form state and submission logic.
  The CustomerEntryForm component calls this hook and receives back
  everything it needs — no form logic lives in the component itself.

  WHY A CUSTOM HOOK?
    The form has: field state, validation state, submit status, success data,
    error state, and a reset function. That is six pieces of state.
    If all of this lived inside CustomerEntryForm.jsx, the component would
    be 200+ lines of mixed logic and JSX.
    The hook handles all logic. The component handles only rendering.

  RETURNS:
    formData       — current value of every field
    errors         — validation error messages per field
    submitStatus   — 'idle' | 'loading' | 'success' | 'error'
    successData    — { customerId, registeredAt } shown in SuccessCard
    errorMessage   — server-level error string (shown below submit button)
    handleChange   — function(fieldId, value) called by inputs on change
    handleSubmit   — function() called when the submit button is clicked
    resetForm      — function() resets everything back to initial state
  ─────────────────────────────────────────────────────────────────────────────
*/

import { useState, useCallback } from 'react'
import { FORM_FIELDS, INITIAL_FORM_STATE } from '../data/formConfig'

/* ── MOCK MODE ───────────────────────────────────────────────────────────── */
/* Set to false when FastAPI is running and /api/v1/customers/register exists */
const MOCK_MODE = true

/* Simulated network delay in mock mode — makes loading state visible */
const MOCK_DELAY_MS = 900

/* ── UUID GENERATOR (mock only) ──────────────────────────────────────────── */
/* In real mode, PostgreSQL generates the UUID and the API returns it.
   In mock mode, we generate a UUID in JavaScript to simulate the response. */
function generateMockUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0
    const v = c === 'x' ? r : (r & 0x3) | 0x8
    return v.toString(16)
  })
}


/* ── VALIDATION ──────────────────────────────────────────────────────────── */
/*
  Validates the form data against the FORM_FIELDS schema.
  Returns a dict of { fieldId: errorMessage } for every failing field.
  Returns an empty object if all required fields are filled.
*/
function validateForm(formData) {
  const errors = {}

  FORM_FIELDS.forEach((field) => {
    /* Skip validation for optional fields */
    if (!field.required) return

    /* A required field is invalid if its value is empty or whitespace-only */
    if (!formData[field.id] || formData[field.id].trim() === '') {
      errors[field.id] = `${field.label} is required`
    }
  })

  return errors
}


/* ── MAIN HOOK ───────────────────────────────────────────────────────────── */

export function useCustomerForm() {

  /* Current value of every form field — initialised from config */
  const [formData, setFormData] = useState(INITIAL_FORM_STATE)

  /* Validation errors per field — empty means no errors */
  const [errors, setErrors] = useState({})

  /* Submit lifecycle:
       'idle'    — form not yet submitted
       'loading' — request in flight
       'success' — customer created, SuccessCard visible
       'error'   — server or validation error */
  const [submitStatus, setSubmitStatus] = useState('idle')

  /* Data returned by the API after successful registration.
     Shown in SuccessCard: UUID, timestamp */
  const [successData, setSuccessData] = useState(null)

  /* Server-level error message — shown below the submit button */
  const [errorMessage, setErrorMessage] = useState('')


  /* ── handleChange ──────────────────────────────────────────────────────── */
  /* Called by GlassInput and GlassSelect whenever a field value changes.
     Also clears the error for that field so the red border disappears
     as soon as the user starts correcting their input. */
  const handleChange = useCallback((fieldId, value) => {
    setFormData(prev => ({ ...prev, [fieldId]: value }))

    /* Clear the error for this field as user types/selects */
    setErrors(prev => {
      if (!prev[fieldId]) return prev  /* nothing to clear */
      const next = { ...prev }
      delete next[fieldId]
      return next
    })
  }, [])


  /* ── handleSubmit ──────────────────────────────────────────────────────── */
  /* Called when the submit button is clicked.
     Validates → submits → handles success/error. */
  const handleSubmit = useCallback(async () => {

    /* Step 1: Validate all required fields */
    const validationErrors = validateForm(formData)
    if (Object.keys(validationErrors).length > 0) {
      /* Set all errors at once so the user sees everything that is wrong */
      setErrors(validationErrors)
      return  /* stop here — do not submit with invalid data */
    }

    /* Step 2: Clear any previous errors and start loading */
    setErrors({})
    setErrorMessage('')
    setSubmitStatus('loading')

    try {
      let responseData

      if (MOCK_MODE) {
        /* ── MOCK SUBMISSION ─────────────────────────────────────────────── */
        /* Simulate the network delay so the loading spinner is visible */
        await new Promise(resolve => setTimeout(resolve, MOCK_DELAY_MS))

        /* Simulate the JSON response that FastAPI would return:
             customer_id     — UUID generated by PostgreSQL
             registered_at   — server timestamp at moment of INSERT
             days_until_scoreable — how long until the scoring gate opens */
        responseData = {
          customer_id:          generateMockUUID(),
          registered_at:        new Date().toISOString(),
          days_until_scoreable: 30,
          status:               'created',
        }

      } else {
        /* ── REAL API SUBMISSION ─────────────────────────────────────────── */
        /* Convert city_tier from string ('1','2','3') to integer (1,2,3)
           because the DB schema stores it as SMALLINT, not VARCHAR */
        const payload = {
          ...formData,
          city_tier: formData.city_tier ? parseInt(formData.city_tier, 10) : null,
        }
        const BASE_URL = import.meta.env.VITE_API_URL || ''
        const response = await fetch(`${BASE_URL}/api/v1/customers/register`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify(payload),
        })

        if (!response.ok) {
          /* Parse the FastAPI error response body for a helpful message */
          const errorBody = await response.json().catch(() => ({}))
          throw new Error(
            errorBody.detail ?? `Server error ${response.status}`
          )
        }

        responseData = await response.json()
      }

      /* Step 3: Registration succeeded — store result and show SuccessCard */
      setSuccessData(responseData)
      setSubmitStatus('success')

      /* Step 4: Auto-reset the form after 5 seconds so the operator
         can immediately enter the next customer */
      setTimeout(() => {
        resetForm()
      }, 5000)

    } catch (err) {
      /* Any network error or server error lands here */
      setErrorMessage(err.message ?? 'Registration failed. Please try again.')
      setSubmitStatus('error')
    }
  }, [formData])


  /* ── resetForm ─────────────────────────────────────────────────────────── */
  /* Resets every piece of state back to its initial value.
     Called automatically after 5 seconds on success,
     and available to the component for manual reset. */
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
