"""
src/api/validators/data_integrity.py
══════════════════════════════════════════════════════════════════════════════
DATA INTEGRITY VALIDATOR — third line of defense.

THE THREE LINES OF DEFENSE:
  1. Pydantic (models/customer.py)     — rejects wrong types and ENUM values
                                          BEFORE the request reaches a route
  2. This validator                    — checks business rules that Pydantic
                                          cannot express (e.g. cross-field checks,
                                          range constraints from cleaning_config)
  3. PostgreSQL ENUMs + CHECK constraints — final hard stop at DB level

WHY DO WE NEED THIS IF PYDANTIC ALREADY VALIDATES?
  Pydantic validates types and ENUM membership.
  But it cannot check things like:
    - "is this full_name suspiciously long?"
    - "is city_tier consistent with the allowed range in cleaning_config?"
    - "does this request look like a bot submission?"
  Those are business rules, not type rules. They live here.

  Also: this validator runs the same cleaning_config rules as Stage 1
  (stage1_clean.py). This ensures the same data quality rules apply to
  BOTH the Kaggle CSV seeding path AND the live frontend registration path.

USAGE:
  from src.api.validators.data_integrity import validate_customer_request
  issues = validate_customer_request(request)
  if issues:
      raise HTTPException(422, detail=issues)
══════════════════════════════════════════════════════════════════════════════
"""

import logging
from typing import List

from src.api.models.customer import CustomerRegisterRequest

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTRAINTS — mirror the rules from config/cleaning_config.py
# ─────────────────────────────────────────────────────────────────────────────

# These must stay in sync with NUMERIC_RANGE_CONSTRAINTS in cleaning_config.py
FIELD_CONSTRAINTS = {
    "city_tier":    {"min": 1,   "max": 3},
    "full_name":    {"max_len": 255},
}

# Suspicious patterns in full_name that suggest bot/test submissions
SUSPICIOUS_NAME_PATTERNS = [
    "test", "asdf", "qwerty", "123", "admin", "null", "undefined",
]

# Minimum full_name length if provided (single-character names rejected)
MIN_NAME_LENGTH = 2


# ─────────────────────────────────────────────────────────────────────────────
# MAIN VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

def validate_customer_request(request: CustomerRegisterRequest) -> List[str]:
    """
    Run business-level integrity checks on a customer registration request.

    Returns a list of issue strings. An empty list means the data passed
    all checks. A non-empty list should result in a 422 response.

    These checks run AFTER Pydantic validation — the request object already
    has correct types and valid ENUM values when this function is called.

    Args:
        request: validated CustomerRegisterRequest from Pydantic

    Returns:
        List[str]: list of issue descriptions, empty if all checks pass
    """
    issues: List[str] = []

    logger.debug(f"Running data integrity checks for registration request")

    # ── Check 1: city_tier range ──────────────────────────────────────────────
    # Pydantic already enforces ge=1, le=3, so this should never fire.
    # We keep it as a safety net in case the constraint is removed from the model.
    city_tier = request.city_tier
    constraints = FIELD_CONSTRAINTS["city_tier"]
    if not (constraints["min"] <= city_tier <= constraints["max"]):
        issue = (
            f"city_tier must be between {constraints['min']} and {constraints['max']}, "
            f"got {city_tier}"
        )
        issues.append(issue)
        logger.warning(f"  Integrity check failed: {issue}")

    # ── Check 2: full_name length ─────────────────────────────────────────────
    if request.full_name is not None:
        name = request.full_name.strip()

        # Too short — single character is not a real name
        if len(name) < MIN_NAME_LENGTH:
            issue = (
                f"full_name must be at least {MIN_NAME_LENGTH} characters, "
                f"got '{name}' ({len(name)} char)"
            )
            issues.append(issue)
            logger.warning(f"  Integrity check failed: {issue}")

        # Too long — exceeds the VARCHAR(255) column width
        elif len(name) > FIELD_CONSTRAINTS["full_name"]["max_len"]:
            issue = (
                f"full_name must be under {FIELD_CONSTRAINTS['full_name']['max_len']} "
                f"characters, got {len(name)}"
            )
            issues.append(issue)
            logger.warning(f"  Integrity check failed: {issue}")

        # Suspicious content check — catches test/bot submissions
        # Only log a warning — do NOT reject (legitimate names could contain
        # substrings like 'test' e.g. "Anastasia")
        name_lower = name.lower()
        for pattern in SUSPICIOUS_NAME_PATTERNS:
            if name_lower == pattern:
                # Exact match only — 'test' is rejected, 'testing' is not
                logger.warning(
                    f"  Suspicious full_name value: '{name}'. "
                    f"Accepting but flagging for review."
                )
                break

    # ── Check 3: Cross-field consistency ─────────────────────────────────────
    # Example: if we had warehouse_to_home, we could check it is > 0.
    # For now: no cross-field issues possible with the current 7 fields.
    # This block is a placeholder for future business rules.

    # ── Log outcome ───────────────────────────────────────────────────────────
    if not issues:
        logger.debug("  All integrity checks passed")
    else:
        logger.warning(f"  {len(issues)} integrity issue(s) found")

    return issues


def log_validation_summary(request: CustomerRegisterRequest) -> None:
    """
    Log a clean summary of the incoming registration data.
    Called by the route handler for observability — every registration
    attempt is logged so you can debug issues without touching the DB.

    Args:
        request: the validated request object
    """
    logger.info("  Registration request received:")
    logger.info(f"    full_name:              {request.full_name or '(not provided)'}")
    logger.info(f"    gender:                 {request.gender.value}")
    logger.info(f"    marital_status:         {request.marital_status.value}")
    logger.info(f"    city_tier:              {request.city_tier}")
    logger.info(f"    preferred_payment_mode: {request.preferred_payment_mode.value}")
    logger.info(f"    preferred_login_device: {request.preferred_login_device.value}")
    logger.info(f"    preferred_order_cat:    {request.preferred_order_cat.value}")
