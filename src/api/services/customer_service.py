"""
src/api/services/customer_service.py
══════════════════════════════════════════════════════════════════════════════
CUSTOMER SERVICE — registration business logic.

HISTORY OF BUGS FIXED IN THIS VERSION:

  Bug 1 (original deployment):
    insert_data included columns that don't exist in Supabase schema:
    'email', 'password_hash', 'kaggle_customer_id', 'kaggle_churn_label'
    → psycopg2 raised UndefinedColumn → rollback → 500 → nothing saved
    SSE still fired because the queue write does not depend on DB commits.

  Bug 2 (db_fix deployment):
    _get_customers_columns() created a SECOND DatabaseConnection inside
    the request handler to dynamically detect schema columns.
    With the dashboard making 4+ concurrent DB requests, this second
    connection exhausted Supabase's free tier connection limit.
    → connection timeout → Render dropped the request with no response
    → browser showed "Failed to fetch" with no error detail.

THIS VERSION:
  - No second DatabaseConnection ever created
  - Reads directly from the Pydantic request object (.value for Enums)
  - Only the exact columns that exist in the Supabase schema
  - If full_name column is missing, tries without it (graceful degradation)
  - Every step logged with ✓ / ✗ so Render logs show exactly what happened
══════════════════════════════════════════════════════════════════════════════
"""

import logging
from datetime import datetime, timezone

from database.connection import DatabaseConnection
from src.api.models.customer import CustomerRegisterRequest, CustomerRegisterResponse
from src.api.services.feature_service import insert_initial_features
from src.api.services.sse_service import sse_service
from src.api.validators.data_integrity import (
    log_validation_summary,
    validate_customer_request,
)

logger = logging.getLogger(__name__)

TABLE_CUSTOMERS = "customers"


def register_customer(
    request: CustomerRegisterRequest,
    db: DatabaseConnection,
) -> CustomerRegisterResponse:
    """
    Execute the full customer registration flow.

    Uses ONLY the single database connection provided by get_db().
    Never opens a second connection — avoids exhausting Supabase pool.

    Steps:
      1. Log all incoming field values
      2. Run business integrity checks
      3. INSERT into customers table (RETURNING id for the UUID)
      4. INSERT into customer_features table
      5. Publish SSE event to dashboard live feed
      6. Return response
    """
    logger.info("─" * 55)
    logger.info("  CUSTOMER REGISTRATION — starting")
    logger.info("─" * 55)

    # ── Step 1: Log incoming fields ───────────────────────────────────────
    log_validation_summary(request)

    # ── Step 2: Integrity checks ──────────────────────────────────────────
    logger.info("  Step 2: Business integrity checks...")
    issues = validate_customer_request(request)
    if issues:
        msg = f"Data integrity checks failed: {'; '.join(issues)}"
        logger.error(f"  ✗ {msg}")
        raise ValueError(msg)
    logger.info("  ✓ Integrity checks passed")

    now = datetime.now(timezone.utc)

    # ── Step 3: INSERT into customers ─────────────────────────────────────
    logger.info("  Step 3: INSERT INTO customers...")

    # Try to include full_name first. If the column does not exist yet
    # (migration not run), fall back to inserting without it.
    customer_id = _insert_with_full_name(db, request, now)

    logger.info(f"  ✓ customers row committed: id={customer_id}")

    # ── Step 4: INSERT into customer_features ────────────────────────────
    logger.info("  Step 4: INSERT INTO customer_features...")
    initial_features = insert_initial_features(db, customer_id)
    logger.info(f"  ✓ customer_features row committed")

    # ── Step 5: Publish SSE event ─────────────────────────────────────────
    # In try/except — SSE failure must never mask a successful registration
    logger.info("  Step 5: Publishing SSE event...")
    try:
        sse_service.publish(
            event_type="new_customer",
            payload={
                "customer_id": str(customer_id),
                "full_name":   request.full_name or "—",
                "city_tier":   request.city_tier,
                "payment":     request.preferred_payment_mode.value,
                "device":      request.preferred_login_device.value,
            },
            db=db,
        )
        logger.info("  ✓ SSE event published")
    except Exception as e:
        logger.warning(f"  SSE publish failed (non-critical, customer IS saved): {e}")

    # ── Step 6: Build response ────────────────────────────────────────────
    response = CustomerRegisterResponse(
        customer_id=customer_id,
        registered_at=now,
        days_until_scoreable=30,
        status="created",
        initial_features=initial_features,
    )

    logger.info(f"  ✓ REGISTRATION COMPLETE — id={customer_id}")
    logger.info("─" * 55)
    return response


# ─────────────────────────────────────────────────────────────────────────────
# INSERT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _build_core_insert(request: CustomerRegisterRequest, now: datetime) -> tuple[list, list]:
    """
    Build the core column list and value list for the customers INSERT.
    These columns are guaranteed to exist in the schema (based on Kaggle data).
    Does NOT include full_name — that is added separately by the caller.

    Returns:
        (columns, values) — lists of equal length, ready for parameterised INSERT
    """
    columns = [
        "registered_at",
        "is_active",
        "role",
        "gender",
        "marital_status",
        "city_tier",
        "preferred_payment_mode",
        "preferred_login_device",
        "preferred_order_cat",
    ]
    values = [
        now,
        True,
        "customer",
        request.gender.value,               # Enum → string: "Male" / "Female"
        request.marital_status.value,        # Enum → string: "Single" etc.
        int(request.city_tier),              # Pydantic gives int but explicit cast
        request.preferred_payment_mode.value,
        request.preferred_login_device.value,
        request.preferred_order_cat.value,
    ]
    return columns, values


def _execute_insert(db: DatabaseConnection, columns: list, values: list) -> str:
    """
    Run the actual INSERT SQL and return the generated UUID.
    Commits inside the function. Raises on any DB error.
    """
    placeholders = ", ".join(["%s"] * len(columns))
    col_list     = ", ".join(columns)
    sql = f"""
        INSERT INTO {TABLE_CUSTOMERS} ({col_list})
        VALUES ({placeholders})
        RETURNING id;
    """

    logger.info(f"  Columns: {columns}")
    logger.info(f"  Values:  {values}")

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)
            result = cur.fetchone()
            if result is None:
                raise RuntimeError("INSERT RETURNING id returned nothing")
            customer_id = str(result[0])
        conn.commit()
        logger.info(f"  commit() called → id={customer_id}")

    return customer_id


def _insert_with_full_name(
    db: DatabaseConnection,
    request: CustomerRegisterRequest,
    now: datetime,
) -> str:
    """
    Try to INSERT with full_name. If full_name column does not exist yet
    (migration not run), fall back to inserting without it.

    This means:
      - After run_add_full_name.py is run: full_name is saved ✓
      - Before the migration:             registration still works ✓

    Args:
        db:      connected DatabaseConnection
        request: validated Pydantic request
        now:     server-side timestamp for registered_at

    Returns:
        str: UUID of the inserted row
    """
    columns, values = _build_core_insert(request, now)

    # Add full_name if the user provided one
    # If the column doesn't exist, we catch UndefinedColumn below
    if request.full_name is not None:
        columns = ["full_name"] + columns
        values  = [request.full_name] + values

    try:
        return _execute_insert(db, columns, values)

    except Exception as e:
        error_str = str(e).lower()

        # Check for UndefinedColumn — full_name column doesn't exist yet
        if "full_name" in error_str and (
            "column" in error_str or "undefined" in error_str
        ):
            logger.warning(
                "  full_name column not found — retrying without it.\n"
                "  Run: python scripts/run_add_full_name.py to add the column."
            )
            # Retry without full_name
            columns_no_name, values_no_name = _build_core_insert(request, now)
            return _execute_insert(db, columns_no_name, values_no_name)

        # Any other error — log the exact PostgreSQL message and re-raise
        logger.error(
            f"  ✗ INSERT INTO customers FAILED\n"
            f"  Error type:   {type(e).__name__}\n"
            f"  Error detail: {e}\n"
            f"  Columns:      {columns}\n"
            f"  This error will be returned as HTTP 500 to the form."
        )
        raise
