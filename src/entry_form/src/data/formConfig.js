/*
  src/data/formConfig.js
  ─────────────────────────────────────────────────────────────────────────────
  FORM CONFIGURATION — single source of truth for all field options.

  WHY THIS FILE EXISTS:
    All dropdown options, validation rules, and field metadata live here —
    not scattered across individual components.

    If a new payment method is added to the database ENUM, you add it here.
    The GlassSelect component never needs to change.

  MATCHES:
    Options here must match the PostgreSQL ENUMs defined in schema.sql exactly.
    The backend will reject any value not in the ENUM — so keeping these in sync
    is critical.
  ─────────────────────────────────────────────────────────────────────────────
*/


/* ── DROPDOWN OPTIONS PER FIELD ──────────────────────────────────────────── */

/* Gender — matches gender_enum in schema.sql */
export const GENDER_OPTIONS = [
  { value: 'Male',   label: 'Male' },
  { value: 'Female', label: 'Female' },
]

/* Marital status — matches marital_status_enum in schema.sql */
export const MARITAL_OPTIONS = [
  { value: 'Single',   label: 'Single' },
  { value: 'Married',  label: 'Married' },
  { value: 'Divorced', label: 'Divorced' },
]

/* City tier — ordinal 1/2/3 — stored as integer in DB */
export const CITY_TIER_OPTIONS = [
  { value: '1', label: 'Tier 1 — metro city' },
  { value: '2', label: 'Tier 2 — mid-size city' },
  { value: '3', label: 'Tier 3 — smaller city' },
]

/* Payment mode — matches payment_mode_enum in schema.sql */
export const PAYMENT_OPTIONS = [
  { value: 'Credit Card', label: 'Credit Card' },
  { value: 'Debit Card',  label: 'Debit Card' },
  { value: 'COD',         label: 'COD (Cash on Delivery)' },
  { value: 'E wallet',    label: 'E Wallet' },
  { value: 'UPI',         label: 'UPI' },
]

/* Login device — matches login_device_enum in schema.sql */
export const DEVICE_OPTIONS = [
  { value: 'Mobile Phone', label: 'Mobile Phone' },
  { value: 'Computer',     label: 'Computer' },
]

/* Order category — matches order_cat_enum in schema.sql */
export const ORDER_CAT_OPTIONS = [
  { value: 'Grocery',            label: 'Grocery' },
  { value: 'Fashion',            label: 'Fashion' },
  { value: 'Mobile',             label: 'Mobile' },
  { value: 'Laptop & Accessory', label: 'Laptop & Accessory' },
  { value: 'Others',             label: 'Others' },
]


/* ── FORM FIELD SCHEMA ───────────────────────────────────────────────────── */
/*
  Defines every field in the form:
    id        — matches the key in the form state object
    label     — shown above the input
    type      — 'text' (GlassInput) or 'select' (GlassSelect)
    options   — array of {value, label} for select fields
    required  — whether the field must be filled before submitting
    placeholder — hint text shown inside the input when empty
    colSpan   — 'full' = takes whole row, 'half' = two per row
*/
export const FORM_FIELDS = [
  {
    id:          'full_name',
    label:       'full name',
    type:        'text',
    required:    false,          /* optional — Kaggle rows have no names */
    placeholder: 'e.g. Sarah Mitchell',
    colSpan:     'full',         /* sits alone on its own row */
  },
  {
    id:       'gender',
    label:    'gender',
    type:     'select',
    options:  GENDER_OPTIONS,
    required: true,
    colSpan:  'half',
  },
  {
    id:       'marital_status',
    label:    'marital status',
    type:     'select',
    options:  MARITAL_OPTIONS,
    required: true,
    colSpan:  'half',
  },
  {
    id:       'city_tier',
    label:    'city tier',
    type:     'select',
    options:  CITY_TIER_OPTIONS,
    required: true,
    colSpan:  'half',
  },
  {
    id:       'preferred_login_device',
    label:    'login device',
    type:     'select',
    options:  DEVICE_OPTIONS,
    required: true,
    colSpan:  'half',
  },
  {
    id:       'preferred_payment_mode',
    label:    'payment mode',
    type:     'select',
    options:  PAYMENT_OPTIONS,
    required: true,
    colSpan:  'half',
  },
  {
    id:       'preferred_order_cat',
    label:    'order category',
    type:     'select',
    options:  ORDER_CAT_OPTIONS,
    required: true,
    colSpan:  'half',
  },
]


/* ── INITIAL FORM STATE ──────────────────────────────────────────────────── */
/*
  The starting value for every field — all empty strings.
  Used by useCustomerForm to initialise and reset the form.
  Generated dynamically from FORM_FIELDS so it never goes out of sync.
*/
export const INITIAL_FORM_STATE = FORM_FIELDS.reduce((acc, field) => {
  acc[field.id] = ''
  return acc
}, {})


/* ── CUSTOMER FEATURES CREATED AT REGISTRATION ───────────────────────────── */
/*
  These are the values inserted into customer_features at registration time.
  Shown in the SuccessCard so the operator knows what was created.
  In the real system, FastAPI computes and inserts these — they are
  displayed here for transparency.
*/
export const INITIAL_FEATURES = {
  tenure_months:          0.0,
  order_count:            0,
  day_since_last_order:   null,   /* null = no orders yet */
  complain:               false,
  satisfaction_score:     null,   /* null = not yet rated */
  coupon_used:            0,
  cashback_amount:        0.0,
  features_source:        'system',
}

/* How many days until a new customer becomes scoreable.
   Matches the is_scoreable gate: tenure_months >= 1 AND order_count >= 1 */
export const DAYS_UNTIL_SCOREABLE = 30
