"""
src/api/services/customer_service.py
══════════════════════════════════════════════════════════════════════════════
CUSTOMER SERVICE — all business logic for customer registration.

THIS IS THE HEART OF THE REGISTRATION FLOW.

WHAT IT DOES:
  1. Receives the validated request from the route handler
  2. Runs the data integrity check (business rules beyond Pydantic)
  3. Runs Stage 1 cleaning rules (same as seed pipeline) via clean_single_record
  4. Inserts the customers row into the database
  5. Calls feature_service to insert the customer_features row
  6. Publishes a 'new_customer' SSE event to the dashboard feed
  7. Returns a structured response

WHY DOES THIS SERVICE CALL clean_single_record()?
  The cleaning rules in Stage 1 (stage1_clean.py) are the canonical
  rules for what data is acceptable. The entry form dropdowns prevent
  most dirty values, but applying the same cleaning function ensures
  the database always receives consistently formatted values regardless
  of the input path (Kaggle CSV or frontend form).
  One set of cleaning rules, used in both paths.

ROUTE CALLS SERVICE — NOT THE REVERSE:
  The route handler in routes/customers.py has exactly one job:
  receive the HTTP request and call this function.
  ALL logic lives here. Routes are thin.
══════════════════════════════════════════════════════════════════════════════
"""

import logging
from datetime import datetime, timezone

from database.connection import DatabaseConnection
from src.api.models.customer import CustomerInsertData, CustomerRegisterRequest, CustomerRegisterResponse
from src.api.services.feature_service import insert_initial_features
from src.api.services.sse_service import sse_service
from src.api.validators.data_integrity import (
    log_validation_summary,
    validate_customer_request,
)

logger = logging.getLogger(__name__)

# Table name constant
TABLE_CUSTOMERS = "customers"


# ─────────────────────────────────────────────────────────────────────────────
# MAIN REGISTRATION FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def register_customer(
    request: CustomerRegisterRequest,
    db: DatabaseConnection,
) -> CustomerRegisterResponse:
    """
    Execute the full customer registration flow.

    This is the only function the route handler needs to call.
    It orchestrates validation → DB insert → features → SSE event.

    Args:
        request: validated Pydantic model from the route handler
        db:      connected DatabaseConnection from the dependency injector

    Returns:
        CustomerRegisterResponse: all data the frontend needs to show SuccessCard

    Raises:
        ValueError: if business-level validation fails
        Exception:  propagates unexpected DB errors to the route handler
    """
    logger.info("─" * 50)
    logger.info("  CUSTOMER REGISTRATION — starting flow")
    logger.info("─" * 50)

    # ── Step 1: Log what came in ───────────────────────────────────────────
    log_validation_summary(request)

    # ── Step 2: Business-level integrity checks ────────────────────────────
    logger.info("  Step 2: Running data integrity checks...")
    issues = validate_customer_request(request)

    if issues:
        # Raise ValueError — route handler catches this and returns HTTP 422
        error_msg = f"Data integrity checks failed: {'; '.join(issues)}"
        logger.error(f"  ✗ {error_msg}")
        raise ValueError(error_msg)

    logger.info("  ✓ Data integrity checks passed")

    # ── Step 3: Apply Stage 1 cleaning rules ──────────────────────────────
    # This normalises values using the same rules as the CSV seeding pipeline.
    # For form data, this mainly strips whitespace and ensures consistent
    # string formats — the dropdowns already prevent dirty aliases.
    logger.info("  Step 3: Applying Stage 1 cleaning rules...")
    cleaned_dict, cleaning_errors = _apply_cleaning_rules(request)

    if cleaning_errors:
        logger.warning(f"  Cleaning found issues: {cleaning_errors}")
        # Cleaning errors are warnings for form data — the dropdowns
        # prevent the worst cases. Log but proceed.

    logger.info("  ✓ Cleaning rules applied")

    # ── Step 4: Build the DB insert dict ──────────────────────────────────
    logger.info("  Step 4: Building customers INSERT payload...")

    now = datetime.now(timezone.utc)  # single timestamp for all fields

    insert_data = {
        # System-generated fields — never provided by the user
        "registered_at": now.isoformat(),
        "is_active":     True,
        "role":          "customer",

        # User-provided fields (cleaned)
        "full_name":               cleaned_dict.get("full_name"),
        "gender":                  cleaned_dict.get("gender"),
        "marital_status":          cleaned_dict.get("marital_status"),
        "city_tier":               cleaned_dict.get("city_tier"),
        "preferred_payment_mode":  cleaned_dict.get("preferred_payment_mode"),
        "preferred_login_device":  cleaned_dict.get("preferred_login_device"),
        "preferred_order_cat":     cleaned_dict.get("preferred_order_cat"),

        # Kaggle fields — NULL for all live registrations
        "kaggle_customer_id": None,
        "email":              None,     # form does not collect email (no auth)
        "password_hash":      None,
        "kaggle_churn_label": None,
    }

    # ── Step 5: Insert into customers table ───────────────────────────────
    logger.info("  Step 5: Inserting into customers table...")
    customer_id = _insert_customer_row(db, insert_data)

    logger.info(f"  ✓ Customer inserted with ID: {customer_id}")

    # ── Step 6: Insert initial customer_features row ──────────────────────
    # This MUST happen in the same request — the dashboard will error if
    # a customer exists without a corresponding features row.
    logger.info("  Step 6: Creating initial customer_features row...")
    initial_features = insert_initial_features(db, customer_id)

    logger.info("  ✓ customer_features row created")

    # ── Step 7: Publish SSE event to the dashboard feed ───────────────────
    # This fires asynchronously — the response does not wait for it.
    logger.info("  Step 7: Publishing new_customer SSE event...")
    sse_service.publish(
        event_type="new_customer",
        payload={
            "customer_id": str(customer_id),
            "full_name":   cleaned_dict.get("full_name") or "Unknown",
            "city_tier":   cleaned_dict.get("city_tier"),
            "payment":     cleaned_dict.get("preferred_payment_mode"),
            "device":      cleaned_dict.get("preferred_login_device"),
        },
        db=db,   # pass db so the event is also persisted to sse_events table
    )
    logger.info("  ✓ SSE event published")

    # ── Step 8: Build and return the response ─────────────────────────────
    logger.info("  Step 8: Building response...")

    response = CustomerRegisterResponse(
        customer_id=customer_id,
        registered_at=now,
        days_until_scoreable=30,   # tenure >= 1 month gate
        status="created",
        initial_features=initial_features,
    )

    logger.info("─" * 50)
    logger.info(f"  ✓ REGISTRATION COMPLETE — customer {customer_id}")
    logger.info("─" * 50)

    return response


# ─────────────────────────────────────────────────────────────────────────────
# PRIVATE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _apply_cleaning_rules(
    request: CustomerRegisterRequest,
) -> tuple[dict, list]:
    """
    Apply Stage 1 cleaning rules to the request data.

    Imports clean_single_record from the existing pipeline module.
    This is the same function used by the Kaggle CSV seeding pipeline —
    one source of truth for cleaning rules across both data paths.

    Args:
        request: validated Pydantic model

    Returns:
        Tuple of (cleaned_dict, errors_list)
    """
    # Convert the Pydantic model to a plain dict for the cleaning function.
    # .value extracts the string value from Enum fields (e.g. Gender.male → "Male")
    raw_dict = {
        "full_name":               request.full_name,
        "gender":                  request.gender.value,
        "marital_status":          request.marital_status.value,
        "city_tier":               request.city_tier,
        "preferred_payment_mode":  request.preferred_payment_mode.value,
        "preferred_login_device":  request.preferred_login_device.value,
        "preferred_order_cat":     request.preferred_order_cat.value,
    }

    try:
        # Import the cleaning function from Stage 1 pipeline
        # This is the same clean_single_record used in seed_database.py
        from src.pipeline.stage1_clean import clean_single_record
        cleaned, errors = clean_single_record(raw_dict)
        return cleaned, errors
    except ImportError:
        # If stage1_clean is not importable (e.g. running API standalone),
        # fall back to using the raw dict — Pydantic already validated it
        logger.warning(
            "  Could not import stage1_clean — using raw validated data. "
            "This is acceptable if the entry form uses constrained dropdowns."
        )
        return raw_dict, []


def _insert_customer_row(
    db: DatabaseConnection,
    insert_data: dict,
) -> str:
    """
    Execute the INSERT statement for the customers table.

    Returns the UUID string generated by PostgreSQL.
    Uses RETURNING id to get the generated UUID in the same query —
    no second round-trip needed.

    Args:
        db:          connected DatabaseConnection
        insert_data: dict of column→value pairs to insert

    Returns:
        str: the UUID string of the inserted customer row
    """
    columns      = list(insert_data.keys())
    values       = list(insert_data.values())
    placeholders = ", ".join(["%s"] * len(columns))

    # RETURNING id: PostgreSQL returns the generated UUID in the same query
    # This is much faster than a separate SELECT after INSERT
    sql = f"""
        INSERT INTO {TABLE_CUSTOMERS} ({", ".join(columns)})
        VALUES ({placeholders})
        RETURNING id;
    """

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)
            # fetchone() returns one row — the (id,) tuple from RETURNING
            result = cur.fetchone()
            if result is None:
                raise RuntimeError("INSERT returned no ID — unexpected database error")
            customer_id = result[0]   # the UUID value
        conn.commit()   # commit the customers INSERT

    return customer_id
