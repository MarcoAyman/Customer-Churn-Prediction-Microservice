/*
  src/components/GlassSelect/GlassSelect.jsx
  ─────────────────────────────────────────────────────────────────────────────
  REUSABLE COMPONENT: Glass-styled dropdown select field.

  Used for all 6 dropdown fields:
    gender, marital_status, city_tier,
    preferred_login_device, preferred_payment_mode, preferred_order_cat

  WHY SEPARATE FROM GlassInput?
    Select and input behave differently in HTML — different events,
    different styling resets, different arrow icons.
    Keeping them separate means each is clean and focused.

  PROPS:
    id       — field identifier
    label    — label text above the dropdown
    value    — controlled value from form state
    options  — array of { value, label } from formConfig.js
    error    — validation error string
    onChange — function(value) called when selection changes
    disabled — disables during form submission
  ─────────────────────────────────────────────────────────────────────────────
*/

import './GlassSelect.css'

export default function GlassSelect({
  id,
  label,
  value,
  options = [],
  error = '',
  onChange,
  disabled = false,
}) {
  return (
    <div className="glass-select-wrapper">

      {/* Field label */}
      <label htmlFor={id} className="glass-field-label">
        <span className="glass-field-icon" aria-hidden="true" />
        {label}
      </label>

      {/* Select wrapper — needed to position the custom chevron arrow */}
      <div className="glass-select-outer">

        <select
          id={id}
          value={value}
          disabled={disabled}
          onChange={(e) => onChange(e.target.value)}
          className={`glass-select ${error ? 'glass-select--error' : ''} ${disabled ? 'glass-select--disabled' : ''}`}
          aria-invalid={!!error}
          aria-describedby={error ? `${id}-error` : undefined}
        >
          {/* Placeholder option — value '' so the required check catches it */}
          <option value="" disabled>
            — select —
          </option>

          {/* Render each option from the config array */}
          {options.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>

        {/* Custom chevron arrow — replaces the browser default arrow.
            Position absolute inside the wrapper div, right side. */}
        <span className="glass-select-arrow" aria-hidden="true">
          <svg
            width="10"
            height="6"
            viewBox="0 0 10 6"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              d="M1 1L5 5L9 1"
              stroke="rgba(255,255,255,0.35)"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </span>

      </div>

      {/* Validation error message */}
      {error && (
        <p id={`${id}-error`} className="glass-field-error" role="alert">
          {error}
        </p>
      )}

    </div>
  )
}
