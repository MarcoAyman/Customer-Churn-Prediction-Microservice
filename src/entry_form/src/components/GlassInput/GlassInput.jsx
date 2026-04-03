/*
  src/components/GlassInput/GlassInput.jsx
  ─────────────────────────────────────────────────────────────────────────────
  REUSABLE COMPONENT: Glass-styled text input field.

  Used for: full_name (the only free-text field in the form).
  Could also be used for any future text fields added to the form.

  PROPS:
    id          — field identifier, used for the htmlFor/id pair
    label       — label text shown above the input
    value       — controlled value from form state
    placeholder — hint text when empty
    error       — validation error string (shows red border + message)
    onChange    — function(value) called on every keystroke
    disabled    — disables the input during form submission
  ─────────────────────────────────────────────────────────────────────────────
*/

import './GlassInput.css'

export default function GlassInput({
  id,
  label,
  value,
  placeholder = '',
  error = '',
  onChange,
  disabled = false,
}) {
  return (
    /* Wrapper div — contains the label, input, and error message */
    <div className="glass-input-wrapper">

      {/* Field label — uppercase, small, muted */}
      <label htmlFor={id} className="glass-field-label">
        {/* Small teal square icon — consistent with the design system */}
        <span className="glass-field-icon" aria-hidden="true" />
        {label}
      </label>

      {/* The actual input element */}
      <input
        id={id}
        type="text"
        value={value}
        placeholder={placeholder}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
        /* Apply error class when a validation error exists for this field */
        className={`glass-input ${error ? 'glass-input--error' : ''} ${disabled ? 'glass-input--disabled' : ''}`}
        /* aria-invalid tells screen readers this field has an error */
        aria-invalid={!!error}
        aria-describedby={error ? `${id}-error` : undefined}
      />

      {/* Validation error message — only visible when error is set */}
      {error && (
        <p id={`${id}-error`} className="glass-field-error" role="alert">
          {error}
        </p>
      )}

    </div>
  )
}
