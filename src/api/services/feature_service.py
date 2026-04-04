"""
src/api/services/feature_service.py
══════════════════════════════════════════════════════════════════════════════
FEATURE SERVICE — inserts the initial customer_features row.

BUGS FIXED IN THIS VERSION:

  Bug 1 — features_computed_at passed as string instead of datetime:
    BEFORE: "features_computed_at": now.isoformat()
            psycopg2 receives a Python str, not a datetime object.
            With Supabase pgBouncer (transaction mode, port 6543), the
            string is not automatically cast to TIMESTAMPTZ and raises
            DataError or silently stores a wrong value.
    AFTER:  "features_computed_at": now
            psycopg2 natively converts datetime → PostgreSQL TIMESTAMPTZ.
            This is the correct way to pass timestamps in psycopg2.

  Bug 2 — order_amount_hike_from_last_year may not exist:
    This column was present in the original Kaggle schema design but may
    not have been created in the actual Supabase schema depending on how
    schema.sql was applied. If it does not exist, psycopg2 raises
    UndefinedColumn, the entire INSERT rolls back, and customer_features
    is never written. The error propagates to the route as HTTP 500.
    AFTER:  The INSERT is wrapped in a try/except that catches
            UndefinedColumn and retries with a minimal safe column set.

  Bug 3 — No verification step:
    There was no SELECT after INSERT to confirm the row actually landed.
    AFTER:  A SELECT COUNT(*) verifies the row exists immediately after
            commit. If it does not exist, we log a clear error.
══════════════════════════════════════════════════════════════════════════════
"""

import logging
from datetime import datetime, timezone

from database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

TABLE_CUSTOMER_FEATURES = "customer_features"

# ─────────────────────────────────────────────────────────────────────────────
# SAFE MINIMAL COLUMNS
# These columns are guaranteed to exist in every version of the schema.
# They map directly to the core Kaggle features and are always created.
# ─────────────────────────────────────────────────────────────────────────────

SAFE_COLUMNS = [
    "customer_id",
    "tenure_months",
    "satisfaction_score",
    "complain",
    "warehouse_to_home",
    "number_of_address",
    "hour_spend_on_app",
    "number_of_device_registered",
    "day_since_last_order",
    "order_count",
    "coupon_used",
    "cashback_amount",
    "features_computed_at",
    "features_source",
]

# Extended columns that MIGHT exist — tried first, omitted on column error
EXTENDED_COLUMNS = SAFE_COLUMNS + ["order_amount_hike_from_last_year"]


def insert_initial_features(
    db: DatabaseConnection,
    customer_id: str,
) -> dict:
    """
    Insert the initial customer_features row for a newly registered customer.

    Tries with the full column set first. If any column does not exist in
    the actual Supabase schema, retries with the guaranteed-safe minimal set.

    WHY TWO ATTEMPTS:
      The schema may have been created differently depending on which version
      of schema.sql was applied. Rather than hardcoding assumptions, we attempt
      the full INSERT and fall back gracefully on column errors.

    Args:
        db:          connected DatabaseConnection (from Depends(get_db))
        customer_id: UUID string of the newly inserted customer

    Returns:
        dict: the feature values inserted (shown in SuccessCard)

    Raises:
        Exception: propagates unexpected errors to customer_service.py
    """
    logger.info(f"  Inserting customer_features for {str(customer_id)[:8]}...")

    now = datetime.now(timezone.utc)
    # CRITICAL: pass datetime object directly to psycopg2
    # Do NOT call .isoformat() — psycopg2 handles datetime → TIMESTAMPTZ natively
    # Passing a string causes type errors with Supabase pgBouncer in transaction mode

    # Build the complete feature value map
    all_values = {
        "customer_id":                      str(customer_id),
        "tenure_months":                    0.0,        # just registered
        "satisfaction_score":               None,       # not yet rated
        "complain":                         False,      # no complaints
        "warehouse_to_home":                None,       # no orders yet
        "number_of_address":                1,          # minimum per DB CHECK
        "hour_spend_on_app":                None,       # no sessions yet
        "number_of_device_registered":      1,          # registration device
        "day_since_last_order":             None,       # no orders yet
        "order_count":                      0,          # no orders
        "order_amount_hike_from_last_year": None,       # no history yet
        "coupon_used":                      0,          # no coupons
        "cashback_amount":                  0.0,        # no cashback
        "features_computed_at":             now,        # datetime object NOT .isoformat()
        "features_source":                  "system",
    }

    # Log every field value so Render logs show exactly what is being inserted
    logger.info(f"  customer_features payload ({len(all_values)} fields):")
    for col, val in all_values.items():
        logger.info(f"    {col:<42} = {val!r}")

    # ── Attempt 1: full column set ────────────────────────────────────────
    # Try with ALL columns including extended ones
    try:
        _run_insert(db, all_values, EXTENDED_COLUMNS)
        logger.info(f"  ✓ customer_features INSERT succeeded (full column set)")
        return _build_return_dict(all_values)

    except Exception as e:
        error_msg = str(e).lower()

        # UndefinedColumn means a column in the INSERT list doesn't exist
        # in the actual Supabase schema. Fall back to the safe minimal set.
        if "undefinedcolumn" in error_msg or "column" in error_msg or "does not exist" in error_msg:
            logger.warning(
                f"  Full column INSERT failed (column mismatch): {e}\n"
                f"  Retrying with safe minimal column set..."
            )
            # Remove the potentially missing column from the values dict
            safe_values = {k: v for k, v in all_values.items() if k in SAFE_COLUMNS}
            try:
                _run_insert(db, safe_values, SAFE_COLUMNS)
                logger.info(f"  ✓ customer_features INSERT succeeded (safe column set)")
                return _build_return_dict(safe_values)
            except Exception as e2:
                logger.error(
                    f"  ✗ customer_features INSERT FAILED on safe set too!\n"
                    f"  Error type:   {type(e2).__name__}\n"
                    f"  Error detail: {e2}\n"
                    f"  Safe columns: {SAFE_COLUMNS}\n"
                    f"  This is a schema mismatch. Run scripts/run_db_diagnostics.py\n"
                    f"  to see the actual customer_features table columns."
                )
                raise

        # Unexpected error — log fully and re-raise
        logger.error(
            f"  ✗ customer_features INSERT FAILED\n"
            f"  Error type:   {type(e).__name__}\n"
            f"  Error detail: {e}"
        )
        raise


def _run_insert(db: DatabaseConnection, values_dict: dict, columns: list) -> None:
    """
    Execute the INSERT for customer_features and verify the row was saved.

    Args:
        db:          connected DatabaseConnection
        values_dict: full dict of {column: value} pairs
        columns:     the subset of columns to actually insert
    """
    # Build ordered lists from the specified columns
    cols = [c for c in columns if c in values_dict]
    vals = [values_dict[c] for c in cols]

    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"""
        INSERT INTO {TABLE_CUSTOMER_FEATURES} ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (customer_id) DO NOTHING;
    """
    # ON CONFLICT DO NOTHING: safe to call multiple times on same customer_id

    logger.debug(f"  SQL columns: {cols}")

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, vals)
            # rowcount=0 means ON CONFLICT fired (row already existed) — that's OK
            logger.debug(f"  execute rowcount: {cur.rowcount}")
        conn.commit()
        logger.info(f"  customer_features commit() called")

    # ── Verification SELECT ────────────────────────────────────────────────
    # Confirms the row is actually in the database after commit.
    # Uses execute_query (read-only) — does NOT affect the transaction.
    customer_id = values_dict["customer_id"]
    verify = db.execute_query(
        f"SELECT COUNT(*) AS cnt FROM {TABLE_CUSTOMER_FEATURES} WHERE customer_id = %s",
        (customer_id,)
    )
    count = verify[0]["cnt"] if verify else 0

    if count > 0:
        logger.info(f"  ✓ VERIFIED: customer_features row confirmed in DB ({count} row)")
    else:
        logger.error(
            f"  ✗ VERIFICATION FAILED: customer_features row NOT found after commit!\n"
            f"  customer_id: {customer_id}\n"
            f"  This means the commit did not persist. Check Supabase connection.\n"
            f"  Possible cause: pgBouncer session state issue."
        )
        raise RuntimeError(
            f"customer_features INSERT appeared to succeed but row not found "
            f"after verification SELECT. customer_id={customer_id}"
        )


def _build_return_dict(values_dict: dict) -> dict:
    """Build the return dict shown in SuccessCard — excludes internal fields."""
    exclude = {"customer_id", "features_computed_at", "features_source"}
    return {k: v for k, v in values_dict.items() if k not in exclude}


# ─────────────────────────────────────────────────────────────────────────────
# DAILY TENURE REFRESH (called by POST /admin/refresh-tenure)
# ─────────────────────────────────────────────────────────────────────────────

def recompute_all_tenures(db: DatabaseConnection) -> int:
    """
    Update tenure_months for every active customer from registered_at.

    Formula: ROUND((NOW() - registered_at in seconds) / 2592000, 1)
    2592000 = 60 * 60 * 24 * 30 = seconds in exactly 30 days.

    Returns:
        int: number of rows updated
    """
    logger.info("Recomputing tenure_months for all active customers...")

    sql = """
        UPDATE customer_features cf
        SET
            tenure_months        = ROUND(
                EXTRACT(EPOCH FROM (NOW() - c.registered_at)) / 2592000.0,
                1
            ),
            features_computed_at = NOW()
        FROM customers c
        WHERE cf.customer_id = c.id
          AND c.is_active    = TRUE;
    """

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            updated_count = cur.rowcount
        conn.commit()

    logger.info(f"  ✓ Updated tenure_months for {updated_count:,} customers")
    return updated_count
