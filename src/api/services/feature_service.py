"""
src/api/services/feature_service.py
══════════════════════════════════════════════════════════════════════════════
FEATURE SERVICE — computes and inserts the initial customer_features row.

WHAT THIS SERVICE DOES:
  When a customer registers, two DB rows must be created:
    1. customers row        ← customer_service.py handles this
    2. customer_features row ← THIS service handles this

  The customer_features row starts with all zero/null values because:
    - tenure_months = 0         (just registered, 0 days passed)
    - order_count = 0           (no orders yet)
    - day_since_last_order = NULL (no orders to measure from)
    - complain = FALSE          (no complaints filed)
    - etc.

  These values will be updated by background jobs as the customer interacts
  with the platform:
    - Tenure: daily cron recomputes from registered_at
    - OrderCount: updated after each order
    - DaySinceLastOrder: updated after each order
    - Complain: set when a support ticket is raised

WHY A SEPARATE SERVICE AND NOT INLINE IN customer_service.py?
  Feature computation will become more complex as the system grows.
  Today it is all zeros. In the future it may pull data from other tables
  (order history, session logs) or apply business logic.
  Keeping it isolated means customer_service.py never needs to change
  when feature computation logic changes.
══════════════════════════════════════════════════════════════════════════════
"""

import logging
from datetime import datetime, timezone
from uuid import UUID

from database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

# Table name constant — if the table is ever renamed, change it here only
TABLE_CUSTOMER_FEATURES = "customer_features"


# ─────────────────────────────────────────────────────────────────────────────
# INITIAL FEATURE VALUES
# ─────────────────────────────────────────────────────────────────────────────

def build_initial_features(customer_id: UUID) -> dict:
    """
    Build the initial customer_features dict for a newly registered customer.

    All behavioral features start at zero or null because the customer
    has just registered and has no interaction history yet.

    The only non-null values are:
      - customer_id: the FK to the customers table (required)
      - features_computed_at: NOW() (when this row was created)
      - features_source: 'system' (computed by FastAPI, not seeded from Kaggle)

    Args:
        customer_id: UUID of the newly inserted customer

    Returns:
        dict: feature values ready for DB INSERT
    """
    now = datetime.now(timezone.utc)

    features = {
        # Foreign key — links this row to the customers table
        "customer_id":                      str(customer_id),

        # Behavioral features — all zero/null at registration
        # These will be updated by background jobs over time

        # Recomputed daily: (today - registered_at) in months
        # Starts at 0 — the customer just registered
        "tenure_months":                    0.0,

        # Will be set when the customer rates the service (1-5 scale)
        # NULL = not yet rated — acceptable initial state
        "satisfaction_score":               None,

        # Will be set to TRUE when a support ticket is raised
        # FALSE = no complaints yet — the starting assumption
        "complain":                         False,

        # Will be computed from the orders table after the first order
        # NULL = no delivery address known yet
        "warehouse_to_home":                None,

        # Number of saved delivery addresses — starts at 0
        "number_of_address":                1,   # minimum 1 per DB CHECK constraint

        # Hours per month on the app — starts at 0 (no sessions yet)
        "hour_spend_on_app":                None,

        # Devices registered to this account — starts at 1 (the device used to register)
        "number_of_device_registered":      1,

        # Days since last order — NULL because no orders exist yet
        "day_since_last_order":             None,

        # Number of orders in the last month — starts at 0
        "order_count":                      0,

        # Year-over-year order value change — NULL for new customers (no history)
        "order_amount_hike_from_last_year":  None,

        # Coupons used — starts at 0
        "coupon_used":                      0,

        # Total cashback earned — starts at 0.0
        "cashback_amount":                  0.0,

        # Metadata
        "features_computed_at":             now.isoformat(),
        # 'system' = computed by FastAPI background logic
        # Distinguishes from 'kaggle_seed' (bulk seeded) and 'synthetic' (generated)
        "features_source":                  "system",
    }

    logger.debug(
        f"  Built initial features for customer {customer_id}: "
        f"tenure=0.0, orders=0, complain=False"
    )

    return features


# ─────────────────────────────────────────────────────────────────────────────
# INSERT INITIAL FEATURES INTO DATABASE
# ─────────────────────────────────────────────────────────────────────────────

def insert_initial_features(
    db: DatabaseConnection,
    customer_id: UUID,
) -> dict:
    """
    Compute and insert the initial customer_features row for a new customer.

    This is called by customer_service.py immediately after the customers
    row is inserted — both rows must exist before the request returns.

    WHY INSERT IN THE SAME REQUEST?
      The customer_features row is required for the customer to appear
      correctly on the operations dashboard. If we deferred it to a
      background job and the job failed, the customer would have no
      features row and would cause errors in the batch scoring query.
      Inserting immediately guarantees consistency.

    Args:
        db:          connected DatabaseConnection
        customer_id: UUID of the newly inserted customer

    Returns:
        dict: the feature values that were inserted (used in the response)

    Raises:
        Exception: propagates DB errors to the calling service
    """
    logger.info(f"  Inserting initial customer_features for {customer_id}...")

    # Build the feature dict
    features = build_initial_features(customer_id)

    # Build the INSERT SQL from the dict keys
    # This approach is dynamic — adding a new feature to build_initial_features()
    # automatically includes it in the INSERT without touching this function
    columns = list(features.keys())
    values  = list(features.values())

    # Build parameterised SQL:
    # INSERT INTO customer_features (col1, col2, ...) VALUES (%s, %s, ...)
    # Using %s placeholders (NOT string formatting) prevents SQL injection
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"""
        INSERT INTO {TABLE_CUSTOMER_FEATURES} ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT (customer_id) DO NOTHING;
    """
    # ON CONFLICT DO NOTHING: if a features row already exists for this
    # customer (should not happen, but safety net), skip silently

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)   # parameterised — safe from injection
        conn.commit()   # commit the features row immediately

    logger.info(f"  ✓ customer_features row inserted for {customer_id}")

    # Return the features dict so customer_service can include it in the response
    # Remove customer_id from the returned dict — the caller already knows it
    return {k: v for k, v in features.items()
            if k not in ("customer_id", "features_computed_at", "features_source")}


# ─────────────────────────────────────────────────────────────────────────────
# RECOMPUTE TENURE (called by daily cron endpoint)
# ─────────────────────────────────────────────────────────────────────────────

def recompute_all_tenures(db: DatabaseConnection) -> int:
    """
    Update tenure_months for every active customer based on registered_at.

    Called by the daily cron endpoint: POST /api/v1/admin/refresh-tenure
    Also called by the GitHub Actions daily cron workflow.

    Formula: tenure_months = (TODAY - registered_at) / 30 days

    Why recompute from registered_at instead of just incrementing?
      Incrementing by 1/30 every day accumulates floating point drift.
      Recomputing from the source (registered_at) always gives the exact value.

    Args:
        db: connected DatabaseConnection

    Returns:
        int: number of customer rows updated
    """
    logger.info("Recomputing tenure_months for all active customers...")

    sql = """
        UPDATE customer_features cf
        SET
            tenure_months       = ROUND(
                EXTRACT(EPOCH FROM (NOW() - c.registered_at)) / 2592000.0,
                1
            ),
            -- 2592000 seconds = 30 days exactly
            -- ROUND to 1 decimal: 9.03 months → 9.0
            features_computed_at = NOW()
        FROM customers c
        WHERE cf.customer_id = c.id
          AND c.is_active = TRUE;
    """

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            # rowcount = how many rows were updated
            updated_count = cur.rowcount
        conn.commit()

    logger.info(f"  ✓ Updated tenure_months for {updated_count:,} customers")
    return updated_count
