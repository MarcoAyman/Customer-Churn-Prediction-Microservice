"""
config/cleaning_config.py
══════════════════════════════════════════════════════════════════════════════
Central configuration for the Stage 1 data cleaning pipeline.

WHY A SEPARATE CONFIG FILE?
  All cleaning rules, alias maps, and constraints live here — NOT scattered
  across the cleaning script. When a new dirty value appears in production
  data, you come to THIS file and add one line. You never touch the logic.
  This is the "open for extension, closed for modification" principle.

  The cleaning script imports from here. The FastAPI validation layer
  also imports from here. One source of truth for all data rules.
══════════════════════════════════════════════════════════════════════════════
"""

# ─────────────────────────────────────────────────────────────────────────────
# EXCEL FILE CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# The sheet inside the Excel file that contains the actual data
# (the other sheet 'Data Dict' is the data dictionary — not loaded)
EXCEL_SHEET_NAME = "E Comm"

# The column that uniquely identifies each customer in the Kaggle dataset.
# Used to detect and remove duplicate customers.
CUSTOMER_ID_COLUMN = "CustomerID"

# The target label column. Must be 0 or 1. Any row where this is missing
# or invalid is rejected — it is unusable for both training and evaluation.
TARGET_COLUMN = "Churn"


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORICAL ALIAS NORMALISATION MAPS
#
# PROBLEM FOUND IN THIS DATASET:
#   The Kaggle CSV contains duplicate representations of the same category.
#   If we do NOT fix these before inserting into the DB or before OHE,
#   the model treats them as separate categories — a silent, dangerous bug.
#
# EXAMPLE BUG:
#   If 'CC' and 'Credit Card' both exist when we one-hot encode:
#   → Column 'PreferredPaymentMode_CC'           gets created
#   → Column 'PreferredPaymentMode_Credit Card'  gets created
#   → The model learns two separate signals for the SAME human behaviour
#   → Feature importance is split between them — both are weakened
#   → At inference, a new 'CC' value hits the wrong column → silent error
#
# SOLUTION:
#   Map every known alias to one single canonical value BEFORE any processing.
#   The canonical value is chosen to be human-readable for the database.
# ─────────────────────────────────────────────────────────────────────────────

PAYMENT_MODE_ALIASES = {
    # Raw value found in data  →  Canonical value stored in DB
    "CC":               "Credit Card",   # 273 rows use abbreviation 'CC'
    "Cash on Delivery": "COD",           # 149 rows use full form — same as COD
    "COD":              "COD",           # 365 rows already correct
    "Credit Card":      "Credit Card",   # 1501 rows already correct
    "Debit Card":       "Debit Card",    # already correct
    "E wallet":         "E wallet",      # already correct
    "UPI":              "UPI",           # already correct
}

LOGIN_DEVICE_ALIASES = {
    # Raw value found in data  →  Canonical value stored in DB
    "Phone":        "Mobile Phone",  # 1231 rows use 'Phone' — same device as 'Mobile Phone'
    "Mobile Phone": "Mobile Phone",  # 2765 rows already correct
    "Computer":     "Computer",      # already correct
}

ORDER_CAT_ALIASES = {
    # Raw value found in data    →  Canonical value stored in DB
    "Mobile Phone":       "Mobile",              # 1271 rows — this is an ORDER CATEGORY (what
                                                  # the customer buys), NOT the login device.
                                                  # 'Mobile Phone' here means mobile phones
                                                  # as a product. Canonical name: 'Mobile'.
    "Mobile":             "Mobile",              # 809 rows already use short form
    "Grocery":            "Grocery",             # already correct
    "Fashion":            "Fashion",             # already correct
    "Laptop & Accessory": "Laptop & Accessory",  # already correct
    "Others":             "Others",              # already correct
}

# All alias maps bundled together: column name → its alias map
# The cleaning script iterates this dict — adding a new column to clean
# means adding one entry here, nothing else changes.
ALL_ALIAS_MAPS = {
    "PreferredPaymentMode": PAYMENT_MODE_ALIASES,
    "PreferredLoginDevice": LOGIN_DEVICE_ALIASES,
    "PreferedOrderCat":     ORDER_CAT_ALIASES,   # note: typo 'Prefered' is in original data
}

# The final valid set of values per categorical column, AFTER normalisation.
# Used for post-normalisation validation — if a value is not in this set,
# the normalisation map is incomplete and needs updating.
VALID_CATEGORICAL_VALUES = {
    "PreferredPaymentMode": {"COD", "Credit Card", "Debit Card", "E wallet", "UPI"},
    "PreferredLoginDevice": {"Mobile Phone", "Computer"},
    "PreferedOrderCat":     {"Grocery", "Fashion", "Mobile", "Laptop & Accessory", "Others"},
    "Gender":               {"Male", "Female"},
    "MaritalStatus":        {"Single", "Married", "Divorced"},
}


# ─────────────────────────────────────────────────────────────────────────────
# NULL IMPUTATION STRATEGY
#
# WHICH COLUMNS HAVE NULLS (from EDA on this specific dataset):
#   Tenure:                     264 nulls  (4.7%)
#   WarehouseToHome:            251 nulls  (4.5%)
#   HourSpendOnApp:             255 nulls  (4.5%)
#   OrderAmountHikeFromlastYear: 265 nulls (4.7%)
#   CouponUsed:                 256 nulls  (4.5%)
#   OrderCount:                 258 nulls  (4.6%)
#   DaySinceLastOrder:          307 nulls  (5.5%)
#
# WHY NOT DROP ROWS WITH NULLS?
#   DaySinceLastOrder alone has 307 nulls = 5.5% of the dataset.
#   Dropping all rows with any null would lose ~15-20% of training data.
#   That is 800-1100 rows we cannot afford to lose on a 5,630-row dataset.
#
# WHY MEDIAN, NOT MEAN?
#   WarehouseToHome:   ranges 5 to 127  — outliers pull the mean far right
#   CashbackAmount:    ranges 0 to 325  — right-skewed distribution
#   Median is the 50th percentile — unaffected by extreme values.
#   It represents the "typical" customer, which is what we want to impute.
#
# WHY ZERO FOR CouponUsed?
#   Zero coupons used is a completely valid real-world state — the customer
#   simply did not use any coupons that month. Imputing with median (which
#   would be a positive number) would falsely imply coupon activity.
#   Zero is the only defensible imputation here.
#
# NOTE ON OrderAmountHikeFromlastYear:
#   This column CAN be negative (customer spent LESS than last year).
#   Negative values are VALID and carry important churn signal.
#   We impute with median (which is positive here: 15.0) meaning we assume
#   the customer with missing data had average behaviour.
#   We NEVER clip this column — negatives must be preserved.
# ─────────────────────────────────────────────────────────────────────────────

# These columns get their nulls filled with the column's median value
IMPUTE_WITH_MEDIAN = [
    "Tenure",
    "WarehouseToHome",
    "HourSpendOnApp",
    "OrderAmountHikeFromlastYear",  # can be negative — median here is 15.0 (positive)
    "OrderCount",
    "DaySinceLastOrder",
]

# These columns get their nulls filled with zero
IMPUTE_WITH_ZERO = [
    "CouponUsed",   # zero = customer used no coupons — valid real state, not missing
]


# ─────────────────────────────────────────────────────────────────────────────
# NUMERIC RANGE CONSTRAINTS
#
# These are the PHYSICAL LIMITS of each numeric column — the hard boundaries
# that a value cannot exceed in the real world.
#
# WHY DO WE CLIP INSTEAD OF REJECT?
#   Clipping (e.g. SatisfactionScore=7 → 5) is safer than rejection because:
#   1. Most out-of-range values are data entry errors, not fundamentally
#      wrong records — the rest of the row is still valid.
#   2. Rejection loses training data. Clipping preserves the row.
#   3. We LOG every clip so you can see upstream data quality issues.
#
# EXCEPTION: OrderAmountHikeFromlastYear has NO lower bound clip.
#   This column represents YoY order value change. A value of -50%
#   means the customer spent 50% less than last year — critical churn signal.
#   Clipping it to 0 would destroy the signal. No lower clip applied.
#
# FORMAT: column_name → (minimum_allowed, maximum_allowed)
# Use None to skip one side of the boundary.
# ─────────────────────────────────────────────────────────────────────────────

NUMERIC_RANGE_CONSTRAINTS = {
    # Column                      Min    Max    Reason for bounds
    "CityTier":                  (1,     3),    # ordinal 1/2/3 only
    "SatisfactionScore":         (1,     5),    # Likert scale 1-5
    "Complain":                  (0,     1),    # binary flag
    "NumberOfDeviceRegistered":  (1,    10),    # physical device limit
    "NumberOfAddress":           (1,   100),    # reasonable address count
    "Tenure":                    (0,   200),    # months — max ~16 years
    "WarehouseToHome":           (0,   500),    # km — reasonable delivery range
    "HourSpendOnApp":            (0,    24),    # hours per day max
    "OrderCount":                (0,   500),    # orders per month
    "CouponUsed":                (0,   200),    # coupons per month
    "DaySinceLastOrder":         (0,   365),    # days — max 1 year
    "CashbackAmount":            (0, 10000),    # monetary — reasonable max
    # OrderAmountHikeFromlastYear: intentionally NO constraint — negatives valid
}


# ─────────────────────────────────────────────────────────────────────────────
# HARD REJECTION RULES
# Rows matching any of these conditions are DELETED from the dataset.
# These are unfixable — not even imputation can save them.
# ─────────────────────────────────────────────────────────────────────────────

# Churn must be exactly 0 or 1 — it is the target label.
# A missing or invalid label means we cannot train or evaluate on this row.
VALID_CHURN_VALUES = {0, 1}

# Gender must be one of these — demographic feature, cannot be inferred.
VALID_GENDER_VALUES = {"Male", "Female"}
