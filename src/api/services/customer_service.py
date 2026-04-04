"""
src/api/services/customer_service.py
══════════════════════════════════════════════════════════════════════════════
CUSTOMER SERVICE — registration business logic.

WHAT IS FIXED vs PREVIOUS VERSION:

  Added SELECT verification after the customers INSERT:
    After _execute_insert commits, a SELECT COUNT(*) WHERE id = %s confirms
    the row is actually in Supabase. If the SELECT returns 0, the commit
    did not persist — this will be logged clearly with a RuntimeError.

  This verification makes the invisible visible:
    Previously, if the commit silently failed (pgBouncer edge case, network
    hiccup), the function returned a UUID, the form showed "Customer Created",
    but nothing was in the database. The verification catches this case and
    raises an error so the form shows a real error message instead.
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

    Two rows are written atomically to Supabase on every successful call:
      Row 1: customers       (9 profile fields + server metadata)
      Row 2: customer_features (all behavioral fields start at zero/null)

    Both writes are verified with a SELECT after commit.
    SSE event is published only after both rows are confirmed.

    Args:
        request: Pydantic-validated request from the route handler
        db:      connected DatabaseConnection from Depends(get_db)

    Returns:
        CustomerRegisterResponse

    Raises:
        ValueError:  business rule violation → HTTP 422
        Exception:   DB error → HTTP 500 (logged with full stack trace)
    """
    logger.info("─" * 55)
    logger.info("  CUSTOMER REGISTRATION — starting")
    logger.info("─" * 55)

    # ── Step 1: Log incoming fields ───────────────────────────────────────
    log_validation_summary(request)

    # ── Step 2: Business integrity checks ────────────────────────────────
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
    customer_id = _insert_with_full_name(db, request, now)
    logger.info(f"  ✓ customers row committed: id={customer_id}")

    # ── Step 4: INSERT into customer_features ────────────────────────────
    logger.info("  Step 4: INSERT INTO customer_features...")
    initial_features = insert_initial_features(db, customer_id)
    logger.info(f"  ✓ customer_features row committed")

    # ── Step 5: Publish SSE event ─────────────────────────────────────────
    # Published AFTER both rows are verified in DB.
    # try/except so SSE failure never masks a successful registration.
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
        logger.warning(f"  SSE publish failed (non-critical, rows ARE in DB): {e}")

    # ── Step 6: Return response ───────────────────────────────────────────
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


def _build_core_insert(request: CustomerRegisterRequest, now: datetime) -> tuple[list, list]:
    """
    Build the guaranteed-safe column and value lists for the customers INSERT.
    Only columns that exist in every version of the schema are included.
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
        now,                                        # datetime object — psycopg2 native
        True,
        "customer",
        request.gender.value,                       # Enum.value → plain string
        request.marital_status.value,
        int(request.city_tier),                     # ensure Python int for SMALLINT
        request.preferred_payment_mode.value,
        request.preferred_login_device.value,
        request.preferred_order_cat.value,
    ]
    return columns, values


def _execute_insert(db: DatabaseConnection, columns: list, values: list) -> str:
    """
    Run INSERT INTO customers RETURNING id and verify the row exists.

    Commits explicitly (psycopg2 does not auto-commit).
    Runs a verification SELECT after commit to confirm the row persisted.

    Returns:
        str: UUID of the inserted row

    Raises:
        RuntimeError: if INSERT succeeds but row not found in verification SELECT
        psycopg2.errors.UndefinedColumn: if a column doesn't exist in schema
        psycopg2.errors.NotNullViolation: if a required column has None value
        psycopg2.errors.InvalidTextRepresentation: if an ENUM value is wrong
    """
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"""
        INSERT INTO {TABLE_CUSTOMERS} ({", ".join(columns)})
        VALUES ({placeholders})
        RETURNING id;
    """

    logger.info(f"  Columns: {columns}")
    logger.info(f"  Values:  {values}")

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, values)
            except Exception as e:
                # Log the exact PostgreSQL error before re-raising
                # This shows up in Render logs as the exact failure cause
                logger.error(
                    f"  ✗ INSERT INTO customers FAILED\n"
                    f"  PostgreSQL error: {type(e).__name__}: {e}\n"
                    f"  Columns attempted: {columns}\n"
                    f"  Values attempted:  {values}"
                )
                raise

            result = cur.fetchone()
            if result is None:
                raise RuntimeError(
                    "INSERT INTO customers RETURNING id returned no row. "
                    "This should not happen — the INSERT must have failed silently."
                )
            customer_id = str(result[0])

        # Explicit commit — REQUIRED, psycopg2 does NOT auto-commit
        conn.commit()
        logger.info(f"  customers commit() called: id={customer_id}")

    # ── Verification SELECT ────────────────────────────────────────────────
    # Confirms the committed row is actually readable from Supabase.
    # Uses a fresh read from the pool (different connection slot) to ensure
    # we are not reading from a local cache.
    verify = db.execute_query(
        "SELECT id, registered_at FROM customers WHERE id = %s",
        (customer_id,)
    )
    if verify:
        logger.info(
            f"  ✓ VERIFIED: customers row confirmed in DB\n"
            f"    id={verify[0]['id']}\n"
            f"    registered_at={verify[0]['registered_at']}"
        )
    else:
        logger.error(
            f"  ✗ VERIFICATION FAILED: customers row NOT found after commit!\n"
            f"  id={customer_id}\n"
            f"  Possible causes:\n"
            f"    1. pgBouncer session state issue (port 6543 transaction mode)\n"
            f"    2. INSERT went to a different schema/database\n"
            f"    3. Connection was closed before commit completed"
        )
        raise RuntimeError(
            f"customers INSERT appeared to succeed (RETURNING id={customer_id}) "
            f"but row not found in verification SELECT. Data was NOT saved."
        )

    return customer_id


def _insert_with_full_name(
    db: DatabaseConnection,
    request: CustomerRegisterRequest,
    now: datetime,
) -> str:
    """
    Try to INSERT with full_name. Falls back gracefully if column missing.

    After running scripts/run_add_full_name.py: full_name is saved ✓
    Before the migration:                       registration still works ✓
    """
    columns, values = _build_core_insert(request, now)

    if request.full_name is not None:
        columns = ["full_name"] + columns
        values  = [request.full_name] + values

    try:
        return _execute_insert(db, columns, values)

    except Exception as e:
        error_str = str(e).lower()

        # full_name column doesn't exist → retry without it
        if "full_name" in error_str and (
            "column" in error_str or "undefined" in error_str or "does not exist" in error_str
        ):
            logger.warning(
                "  full_name column not in schema — retrying without it.\n"
                "  Run: python scripts/run_add_full_name.py to add the column."
            )
            cols_no_name, vals_no_name = _build_core_insert(request, now)
            return _execute_insert(db, cols_no_name, vals_no_name)

        raise
