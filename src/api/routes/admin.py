"""
src/api/routes/admin.py
══════════════════════════════════════════════════════════════════════════════
ADMIN ROUTER — dashboard data endpoints.

FIX APPLIED — _make_json_safe():
  PostgreSQL returns Python types that are NOT natively JSON-serialisable:
    - NUMERIC / ROUND()  → Python Decimal  → must become float
    - TIMESTAMPTZ        → Python datetime → must become ISO string
    - UUID               → Python UUID     → must become str

  Without explicit conversion, FastAPI's encoder occasionally fails on
  nested dicts, producing nulls or 500 errors that crash the React tree.

  Every endpoint now runs its result through _make_json_safe() before
  wrapping in APIResponse. This is the single fix for the black-screen crash.

TIMING CONSTANTS (visible to the caller):
  React Query polls:   /overview, /at-risk, /last-batch  every 60 seconds
                       /churn-trend, /drift              every 120 seconds
  These are defined in useDashboardData.js — not in this file.
══════════════════════════════════════════════════════════════════════════════
"""

import decimal
import logging
import uuid
from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException, status

from database.connection import DatabaseConnection
from src.api.dependencies import get_db, verify_admin
from src.api.models.responses import APIResponse
from src.api.services.feature_service import recompute_all_tenures
from src.api.services.sse_service import sse_service

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/admin",
    tags=["Admin Dashboard"],
    # Every route in this router requires X-Admin-Key header.
    # verify_admin raises HTTP 403 if the key is missing or wrong.
    dependencies=[Depends(verify_admin)],
)


# ─────────────────────────────────────────────────────────────────────────────
# TYPE SERIALISATION HELPER
#
# WHY THIS EXISTS:
#   psycopg2 returns PostgreSQL types as Python objects that json.dumps()
#   and even FastAPI's jsonable_encoder cannot always handle when they appear
#   inside a plain dict (as opposed to inside a Pydantic model).
#
#   This function walks the entire data structure and converts every
#   non-JSON-native value to its closest JSON-native equivalent:
#     decimal.Decimal → float   (NUMERIC, ROUND() results)
#     datetime        → str     (TIMESTAMPTZ columns)
#     date            → str     (DATE columns)
#     uuid.UUID       → str     (UUID primary keys)
#     bool            → bool    (BOOLEAN — already JSON-safe, kept as-is)
#     None            → None    (NULL — already JSON-safe, kept as-is)
#
#   Called at the END of every admin route, just before APIResponse().
# ─────────────────────────────────────────────────────────────────────────────

def _make_json_safe(obj):
    """
    Recursively convert non-JSON-serialisable PostgreSQL types to
    JSON-native Python primitives.

    Handles: dict, list, Decimal, datetime, date, UUID, bool, int, float, str, None.
    Everything else is stringified as a fallback.

    Args:
        obj: any Python value returned by psycopg2

    Returns:
        A JSON-serialisable version of the same value.
    """
    if isinstance(obj, dict):
        # Recurse into every value in the dict
        return {key: _make_json_safe(val) for key, val in obj.items()}

    elif isinstance(obj, list):
        # Recurse into every item in the list
        return [_make_json_safe(item) for item in obj]

    elif isinstance(obj, decimal.Decimal):
        # NUMERIC / ROUND() / computed columns → Python float
        # e.g. churn_probability=Decimal("0.7823") → 0.7823
        # e.g. high_risk_pct=Decimal("16.8")      → 16.8
        return float(obj)

    elif isinstance(obj, datetime):
        # TIMESTAMPTZ columns → ISO 8601 string
        # e.g. registered_at=datetime(2026,3,15,...) → "2026-03-15T23:47:57+00:00"
        return obj.isoformat()

    elif isinstance(obj, date):
        # DATE columns (from ::DATE casts) → ISO date string
        # e.g. batch_date=date(2026,3,15) → "2026-03-15"
        return obj.isoformat()

    elif isinstance(obj, uuid.UUID):
        # UUID primary keys → lowercase hex string
        # e.g. customer_id=UUID("e5fdab1d-...") → "e5fdab1d-..."
        return str(obj)

    elif obj is None or isinstance(obj, (bool, int, float, str)):
        # Already JSON-safe — return unchanged
        # NOTE: bool must be checked BEFORE int because bool is a subclass of int
        return obj

    else:
        # Unknown type — stringify as a safe fallback, log for visibility
        logger.warning(
            f"_make_json_safe: unknown type {type(obj).__name__} = {obj!r} — stringified"
        )
        return str(obj)


# ─────────────────────────────────────────────────────────────────────────────
# KPI OVERVIEW — powers the four top cards
# React Query polling interval: 60 seconds (set in useDashboardData.js)
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/overview",
    response_model=APIResponse,
    summary="KPI summary for dashboard cards",
)
def get_kpi_overview(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch KPI summary from the v_current_risk_summary view.
    Returns a single row: total_customers, risk tier counts and %, last_scored_at.

    The view always returns exactly one row (COUNT(*) guarantees it).
    Even with zero customers, it returns a row of zeros.
    """
    logger.info("GET /admin/overview")
    try:
        rows = db.execute_query("SELECT * FROM v_current_risk_summary;")

        # The view always returns one row — take it or default to empty dict
        raw = rows[0] if rows else {}

        # Convert all Decimal, datetime, UUID types to JSON-safe primitives
        data = _make_json_safe(raw)

        logger.info(
            f"  KPI overview: total={data.get('total_customers', 0)}, "
            f"high={data.get('high_risk_count', 0)}, "
            f"high_pct={data.get('high_risk_pct', 0)}"
        )

        return APIResponse(success=True, data=data)

    except Exception as e:
        logger.error(f"  Failed to fetch KPI overview: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch KPI data")


# ─────────────────────────────────────────────────────────────────────────────
# RISK DISTRIBUTION — powers the horizontal bar chart
# React Query polling interval: 60 seconds
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/risk-distribution",
    response_model=APIResponse,
    summary="Risk tier counts for bar chart",
)
def get_risk_distribution(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch risk tier breakdown from v_current_risk_summary.
    Returns a list of {tier, count, color} objects for Recharts.

    The counts come from the KPI summary view (one query, reshaped into
    the array format that Recharts BarChart expects).
    """
    logger.info("GET /admin/risk-distribution")
    try:
        rows = db.execute_query("SELECT * FROM v_current_risk_summary;")
        raw = rows[0] if rows else {}
        summary = _make_json_safe(raw)

        # Reshape from {high_risk_count: 948, ...} into the array Recharts expects
        # Each item: { tier: string, count: int, color: hex }
        distribution = [
            {
                "tier":  "HIGH",
                "count": summary.get("high_risk_count", 0) or 0,
                "color": "#ef4444",
            },
            {
                "tier":  "MEDIUM",
                "count": summary.get("medium_risk_count", 0) or 0,
                "color": "#f59e0b",
            },
            {
                "tier":  "LOW",
                "count": summary.get("low_risk_count", 0) or 0,
                "color": "#10b981",
            },
            {
                "tier":  "ONBOARDING",
                "count": summary.get("onboarding_count", 0) or 0,
                "color": "#3b82f6",
            },
        ]

        logger.info(f"  Risk distribution: {[(d['tier'], d['count']) for d in distribution]}")
        return APIResponse(success=True, data=distribution)

    except Exception as e:
        logger.error(f"  Failed to fetch risk distribution: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch risk distribution")


# ─────────────────────────────────────────────────────────────────────────────
# CHURN TREND — powers the line chart
# React Query polling interval: 120 seconds
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/churn-trend",
    response_model=APIResponse,
    summary="Churn rate per batch cycle (line chart)",
)
def get_churn_trend(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch churn rate history from the v_churn_trend view.
    Returns the last 10 completed batch cycles with % HIGH risk each cycle.

    If no batch runs exist yet, returns an empty array.
    The frontend ChurnTrendChart shows "last 0 batches" in that case.
    """
    logger.info("GET /admin/churn-trend")
    try:
        rows = db.execute_query("SELECT * FROM v_churn_trend;")

        # _make_json_safe handles: batch_date (date) → "2026-03-15",
        #                          high_risk_pct (Decimal) → float,
        #                          duration_seconds (int)  → int
        data = _make_json_safe(rows)

        logger.info(f"  Churn trend: {len(data)} batch cycles returned")
        return APIResponse(success=True, data=data)

    except Exception as e:
        logger.error(f"  Failed to fetch churn trend: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch churn trend")


# ─────────────────────────────────────────────────────────────────────────────
# TOP AT-RISK — powers the at-risk customer table
# React Query polling interval: 60 seconds
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/at-risk",
    response_model=APIResponse,
    summary="Top 20 at-risk customers",
)
def get_top_at_risk(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch top 20 customers by churn probability from the v_top_at_risk view.
    Joined with profile and feature data for the dashboard table.

    For kaggle-seeded customers:
      - display_id: "#" + first 8 chars of UUID
      - churn_probability: 1.0 (HIGH) or 0.0 (LOW) — binary kaggle labels
      - top_reason: null (no SHAP values for seed data)

    For live-registered customers after a real model runs:
      - churn_probability: float 0.0–1.0 from model.predict_proba()
      - top_reason: derived from shap_top_reasons JSONB field
    """
    logger.info("GET /admin/at-risk")
    try:
        rows = db.execute_query("SELECT * FROM v_top_at_risk;")

        # _make_json_safe converts: UUID → str, datetime → ISO, Decimal → float
        safe_rows = _make_json_safe(rows)

        # Add display_id (short customer identifier for the table)
        # and extract the top SHAP reason from the JSON field
        for row in safe_rows:
            # Short display ID from UUID — "#e5fdab1d" format
            cid = row.get("customer_id", "")
            row["display_id"] = f"#{str(cid)[:8]}" if cid else "#unknown"

            # Extract top reason from shap_top_reasons JSONB
            # shap_top_reasons is a list: [{"feature": "...", "impact": 0.2}, ...]
            # For kaggle seed data this is null — default to "–"
            shap = row.get("shap_top_reasons")
            if isinstance(shap, list) and len(shap) > 0:
                row["top_reason"] = shap[0].get("feature", "—")
            else:
                row["top_reason"] = "—"

        logger.info(f"  At-risk: {len(safe_rows)} customers returned")
        return APIResponse(success=True, data=safe_rows)

    except Exception as e:
        logger.error(f"  Failed to fetch at-risk customers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch at-risk customers")


# ─────────────────────────────────────────────────────────────────────────────
# DRIFT MONITOR — powers the PSI feature drift table
# React Query polling interval: 120 seconds
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/drift",
    response_model=APIResponse,
    summary="Feature drift PSI values",
)
def get_drift_report(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch the latest PSI drift values per feature from the most recent batch run.

    If no batch run has completed yet (first deployment), returns placeholder
    rows with psi_value=null and drift_level="pending" so the frontend table
    shows "pending" instead of being empty.

    The kaggle_baseline seeding run did NOT compute drift (drift_checked=False),
    so the first real drift data appears after the first live model batch run.
    """
    logger.info("GET /admin/drift")
    try:
        # Find the most recent completed batch run
        batch_rows = db.execute_query("""
            SELECT id FROM batch_runs
            WHERE status = 'completed'
            ORDER BY started_at DESC
            LIMIT 1;
        """)

        if not batch_rows:
            # No completed batch runs yet — return pending placeholders
            logger.info("  No completed batch run yet — returning pending drift rows")
            placeholder_features = [
                "DaySinceLastOrder",
                "Tenure",
                "OrderCount",
                "SatisfactionScore",
                "CouponUsed",
                "CashbackAmount",
                "HourSpendOnApp",
                "WarehouseToHome",
                "OrderAmountHikeFromlastYear",
                "Complain",
            ]
            data = [
                {
                    "feature_name":   f,
                    "psi_value":      None,
                    "drift_level":    "pending",
                    "reference_mean": None,
                    "current_mean":   None,
                }
                for f in placeholder_features
            ]
            return APIResponse(success=True, data=data)

        # Get drift values for the most recent batch run
        batch_id = str(batch_rows[0]["id"])   # UUID → string
        rows = db.execute_query(
            """
            SELECT
                feature_name,
                psi_value,
                drift_level,
                reference_mean,
                current_mean
            FROM drift_reports
            WHERE batch_run_id = %s
            ORDER BY psi_value DESC NULLS LAST;
            """,
            (batch_id,),
        )

        # _make_json_safe converts Decimal psi_value/reference_mean/current_mean → float
        data = _make_json_safe(rows)

        logger.info(f"  Drift: {len(data)} features from batch {batch_id[:8]}")
        return APIResponse(success=True, data=data)

    except Exception as e:
        logger.error(f"  Failed to fetch drift data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch drift data")


# ─────────────────────────────────────────────────────────────────────────────
# LAST BATCH — powers the health bar
# React Query polling interval: 60 seconds
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/last-batch",
    response_model=APIResponse,
    summary="Most recent batch run info",
)
def get_last_batch(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch the most recent batch run record for the health bar.

    For a freshly deployed system with only kaggle seed data:
      - model_version: "kaggle_baseline"
      - status: "completed" (from the seed run)
      - drift_alert_fired: false (seed run doesn't check drift)
      - customers_scored: 5630

    After first live model batch:
      - model_version: "v1.0.0" (or whatever version was registered)
      - drift_alert_fired: true/false depending on PSI values
    """
    logger.info("GET /admin/last-batch")
    try:
        rows = db.execute_query("""
            SELECT
                id,
                model_version,
                triggered_by,
                status,
                started_at,
                completed_at,
                duration_seconds,
                customers_scored,
                high_risk_count,
                medium_risk_count,
                low_risk_count,
                drift_alert_fired,
                error_message
            FROM batch_runs
            ORDER BY started_at DESC
            LIMIT 1;
        """)

        if not rows:
            logger.info("  No batch runs found")
            return APIResponse(success=True, data=None, message="No batch runs yet")

        # _make_json_safe converts: UUID → str, datetime → ISO, bool → bool
        data = _make_json_safe(rows[0])

        logger.info(
            f"  Last batch: model={data.get('model_version')}, "
            f"status={data.get('status')}, "
            f"scored={data.get('customers_scored', 0)}"
        )
        return APIResponse(success=True, data=data)

    except Exception as e:
        logger.error(f"  Failed to fetch last batch: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch last batch info")


# ─────────────────────────────────────────────────────────────────────────────
# REFRESH TENURE — called by daily GitHub Actions cron
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/refresh-tenure",
    response_model=APIResponse,
    summary="Recompute tenure_months for all active customers",
)
def refresh_tenure(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Recompute tenure_months for every active customer from registered_at.
    Called by daily GitHub Actions cron via POST /api/v1/admin/refresh-tenure.

    Formula: tenure_months = (NOW() - registered_at) / 30 days
    Returns: { customers_updated: int }
    """
    logger.info("POST /admin/refresh-tenure — triggered")
    try:
        updated_count = recompute_all_tenures(db)
        return APIResponse(
            success=True,
            data={"customers_updated": updated_count},
            message=f"Tenure recomputed for {updated_count:,} customers",
        )
    except Exception as e:
        logger.error(f"  Tenure refresh failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Tenure refresh failed")


# ─────────────────────────────────────────────────────────────────────────────
# SSE STATUS — diagnostic endpoint
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/sse-status",
    response_model=APIResponse,
    summary="SSE queue diagnostics",
)
def get_sse_status() -> APIResponse:
    """
    Return SSE queue size and event count.
    No DB needed. Used for monitoring.
    """
    return APIResponse(success=True, data=sse_service.get_status())
