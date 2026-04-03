/*
  src/CustomerEntryForm.jsx
  ─────────────────────────────────────────────────────────────────────────────
  ROOT COMPONENT: CustomerEntryForm

  This is the top-level component. It:
    1. Calls useCustomerForm() to get all state and handlers
    2. Renders the glassmorphism scene (background + outer panel)
    3. Assembles the inner form card using GlassInput, GlassSelect, SuccessCard
    4. Handles the field layout grid (full-width vs two-column)

  LAYOUT STRUCTURE:
    scene (dark background + ambient glow)
      └── outer-glass (frosted outer panel)
            ├── brand row (ChurnGuard logo)
            └── inner-card (form surface)
                  ├── heading
                  ├── form fields (mapped from FORM_FIELDS config)
                  ├── submit button
                  ├── error message (if any)
                  └── SuccessCard (if status = 'success')

  THIS COMPONENT DOES NOT CONTAIN FORM LOGIC.
  All state, validation, and submit handling is in useCustomerForm.js.
  ─────────────────────────────────────────────────────────────────────────────
*/

import './CustomerEntryForm.css'

/* Subcomponents — each in its own folder */
import GlassInput   from './components/GlassInput/GlassInput'
import GlassSelect  from './components/GlassSelect/GlassSelect'
import SuccessCard  from './components/SuccessCard/SuccessCard'

/* Hook — all form logic lives here */
import { useCustomerForm } from './hooks/useCustomerForm'

/* Config — field definitions (tells us what to render and in what order) */
import { FORM_FIELDS } from './data/formConfig'


export default function CustomerEntryForm() {

  /* Destructure everything the form needs from the hook.
     This component does not manage any state of its own. */
  const {
    formData,
    errors,
    submitStatus,
    successData,
    errorMessage,
    handleChange,
    handleSubmit,
    resetForm,
  } = useCustomerForm()

  /* Derived booleans — make JSX conditions readable */
  const isLoading = submitStatus === 'loading'
  const isSuccess = submitStatus === 'success'
  const isError   = submitStatus === 'error'

  /* Separate fields into two groups for layout:
       fullFields  — colSpan='full', each on its own row
       halfFields  — colSpan='half', two per row in a grid */
  const fullFields = FORM_FIELDS.filter(f => f.colSpan === 'full')
  const halfFields = FORM_FIELDS.filter(f => f.colSpan === 'half')


  return (
    /* SCENE — the full-page dark background with ambient glow */
    <div className="scene">

      {/* Ambient glow orbs — decorative, behind the glass */}
      <div className="scene__orb scene__orb--teal"   aria-hidden="true" />
      <div className="scene__orb scene__orb--indigo" aria-hidden="true" />

      {/* Decorative floating dots — mimic the reference image moisture droplets */}
      <span className="scene__dot" style={{ top: '18%', left: '9%' }}   aria-hidden="true" />
      <span className="scene__dot" style={{ top: '70%', left: '13%' }}  aria-hidden="true" />
      <span className="scene__dot" style={{ top: '35%', right: '11%' }} aria-hidden="true" />
      <span className="scene__dot" style={{ top: '60%', right: '7%' }}  aria-hidden="true" />
      <span className="scene__dot" style={{ top: '82%', right: '18%' }} aria-hidden="true" />


      {/* OUTER GLASS PANEL — the large frosted container */}
      <div className="outer-glass">

        {/* Top edge highlight — light catching the glass rim */}
        <div className="glass-highlight" aria-hidden="true" />

        {/* Brand row — logo mark + name */}
        <div className="brand-row">
          {/* 3×3 dot grid — the ChurnGuard mark */}
          <div className="brand-dots" aria-hidden="true">
            {Array.from({ length: 9 }).map((_, i) => (
              <span key={i} className="brand-dot" />
            ))}
          </div>
          <span className="brand-name">ChurnGuard</span>
          {/* Operational tag — identifies this as the data entry interface */}
          <span className="brand-tag">data entry</span>
        </div>


        {/* INNER CARD — the actual form surface */}
        <div className="inner-card">

          {/* Inner card top edge highlight */}
          <div className="glass-highlight" aria-hidden="true" />

          {/* Form heading */}
          <h1 className="form-heading">Register Customer</h1>
          <p className="form-subheading">to your ChurnGuard workspace</p>


          {/* FULL-WIDTH FIELDS (full_name sits alone on its row) */}
          {fullFields.map((field) => (
            <div key={field.id} className="field-full">
              <GlassInput
                id={field.id}
                label={field.label}
                value={formData[field.id]}
                placeholder={field.placeholder}
                error={errors[field.id] ?? ''}
                onChange={(val) => handleChange(field.id, val)}
                disabled={isLoading || isSuccess}
              />
            </div>
          ))}


          {/* TWO-COLUMN GRID FIELDS (pairs of dropdowns) */}
          {/* Group half-width fields into pairs of two for the two-column layout.
              slice(i, i+2) takes each consecutive pair from the array. */}
          {Array.from(
            { length: Math.ceil(halfFields.length / 2) },
            (_, i) => halfFields.slice(i * 2, i * 2 + 2)
          ).map((pair, rowIndex) => (
            <div key={rowIndex} className="field-row">
              {pair.map((field) => (
                <GlassSelect
                  key={field.id}
                  id={field.id}
                  label={field.label}
                  value={formData[field.id]}
                  options={field.options}
                  error={errors[field.id] ?? ''}
                  onChange={(val) => handleChange(field.id, val)}
                  disabled={isLoading || isSuccess}
                />
              ))}
            </div>
          ))}


          {/* DIVIDER */}
          <div className="form-divider" />


          {/* SUBMIT BUTTON */}
          <button
            className={`submit-btn ${isLoading ? 'submit-btn--loading' : ''} ${isSuccess ? 'submit-btn--success' : ''}`}
            onClick={handleSubmit}
            /* Disable during loading or after success (auto-reset in 5s) */
            disabled={isLoading || isSuccess}
            aria-busy={isLoading}
          >
            {isLoading ? (
              /* Loading state — spinner + text */
              <span className="submit-btn__loading">
                <span className="submit-btn__spinner" aria-hidden="true" />
                Registering...
              </span>
            ) : isSuccess ? (
              /* Success state — momentary confirmation before reset */
              <span>Customer Created</span>
            ) : (
              /* Default state */
              <span>Register Customer</span>
            )}
          </button>


          {/* SERVER ERROR MESSAGE — shown below button when API fails */}
          {isError && errorMessage && (
            <p className="form-error-message" role="alert">
              {errorMessage}
            </p>
          )}


          {/* SUCCESS CARD — slides in after successful registration */}
          {isSuccess && successData && (
            <SuccessCard
              data={successData}
              onReset={resetForm}
            />
          )}

        </div>
        {/* end inner-card */}

      </div>
      {/* end outer-glass */}

    </div>
    /* end scene */
  )
}
