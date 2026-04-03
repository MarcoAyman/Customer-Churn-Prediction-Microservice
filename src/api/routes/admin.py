"""
src/api/routes/admin.py
══════════════════════════════════════════════════════════════════════════════
ADMIN ROUTER — dashboard data endpoints.

All endpoints here require X-Admin-Key header authentication.
They serve the data that powers the operations dashboard charts, tables,
and KPI cards via React Query polling.

ENDPOINTS:
  GET  /api/v1/admin/overview          → KPI summary (v_current_risk_summary)
  GET  /api/v1/admin/risk-distribution → bar chart data
  GET  /api/v1/admin/churn-trend       → line chart data (v_churn_trend)
  GET  /api/v1/admin/at-risk           → top 20 table (v_top_at_risk)
  GET  /api/v1/admin/drift             → PSI table (drift_reports)
  GET  /api/v1/admin/last-batch        → last batch run info
  POST /api/v1/admin/refresh-tenure    → trigger daily tenure recomputation
  GET  /api/v1/admin/sse-status        → SSE queue diagnostics
══════════════════════════════════════════════════════════════════════════════
"""

import logging

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
    # All routes in this router require admin authentication.
    # verify_admin is called for every request before the handler runs.
    dependencies=[Depends(verify_admin)],
)


# ─────────────────────────────────────────────────────────────────────────────
# KPI OVERVIEW — powers the four top cards
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/overview",
    response_model=APIResponse,
    summary="KPI summary for dashboard cards",
)
def get_kpi_overview(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch KPI summary from the v_current_risk_summary view.
    Returns: total_customers, high/medium/low/onboarding counts and %, last_scored_at.
    """
    logger.info("GET /admin/overview")
    try:
        # Query the pre-built view — FastAPI never constructs this query itself
        rows = db.execute_query("SELECT * FROM v_current_risk_summary;")

        # The view returns exactly one row
        data = rows[0] if rows else {}

        logger.info(f"  KPI overview: total={data.get('total_customers', 0)}, "
                    f"high={data.get('high_risk_count', 0)}")

        return APIResponse(success=True, data=data)

    except Exception as e:
        logger.error(f"  Failed to fetch KPI overview: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch KPI data")


# ─────────────────────────────────────────────────────────────────────────────
# RISK DISTRIBUTION — powers the horizontal bar chart
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/risk-distribution",
    response_model=APIResponse,
    summary="Risk tier counts for bar chart",
)
def get_risk_distribution(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch risk tier breakdown from v_current_risk_summary.
    Returns a list of {tier, count, color} dicts for Recharts.
    """
    logger.info("GET /admin/risk-distribution")
    try:
        rows = db.execute_query("SELECT * FROM v_current_risk_summary;")
        summary = rows[0] if rows else {}

        # Reshape into the array format Recharts expects
        distribution = [
            {"tier": "HIGH",       "count": summary.get("high_risk_count", 0),    "color": "#ef4444"},
            {"tier": "MEDIUM",     "count": summary.get("medium_risk_count", 0),  "color": "#f59e0b"},
            {"tier": "LOW",        "count": summary.get("low_risk_count", 0),     "color": "#10b981"},
            {"tier": "ONBOARDING", "count": summary.get("onboarding_count", 0),   "color": "#3b82f6"},
        ]

        return APIResponse(success=True, data=distribution)

    except Exception as e:
        logger.error(f"  Failed to fetch risk distribution: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch risk distribution")


# ─────────────────────────────────────────────────────────────────────────────
# CHURN TREND — powers the line chart
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/churn-trend",
    response_model=APIResponse,
    summary="Churn rate per batch cycle (line chart)",
)
def get_churn_trend(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch churn rate history from the v_churn_trend view.
    Returns last 10 completed batch cycles with % HIGH risk each cycle.
    """
    logger.info("GET /admin/churn-trend")
    try:
        rows = db.execute_query("SELECT * FROM v_churn_trend;")

        # Convert datetime objects to ISO strings for JSON serialisation
        for row in rows:
            if "batch_date" in row and row["batch_date"] is not None:
                row["batch_date"] = str(row["batch_date"])

        logger.info(f"  Churn trend: {len(rows)} batch cycles returned")
        return APIResponse(success=True, data=rows)

    except Exception as e:
        logger.error(f"  Failed to fetch churn trend: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch churn trend")


# ─────────────────────────────────────────────────────────────────────────────
# TOP AT-RISK — powers the at-risk customer table
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/at-risk",
    response_model=APIResponse,
    summary="Top 20 at-risk customers",
)
def get_top_at_risk(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch top 20 customers by churn probability from v_top_at_risk view.
    Joined with profile and feature data for the dashboard table.
    """
    logger.info("GET /admin/at-risk")
    try:
        rows = db.execute_query("SELECT * FROM v_top_at_risk;")

        # Serialise non-JSON-safe types
        for row in rows:
            # UUID → string
            if "customer_id" in row:
                row["customer_id"] = str(row["customer_id"])
            # datetime → ISO string
            if "last_scored_at" in row and row["last_scored_at"]:
                row["last_scored_at"] = row["last_scored_at"].isoformat()
            if "registered_at" in row and row["registered_at"]:
                row["registered_at"] = row["registered_at"].isoformat()
            # Add a display_id for the table (short form)
            if "customer_id" in row:
                row["display_id"] = f"#{str(row['customer_id'])[:8]}"

        logger.info(f"  At-risk: {len(rows)} customers returned")
        return APIResponse(success=True, data=rows)

    except Exception as e:
        logger.error(f"  Failed to fetch at-risk customers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch at-risk customers")


# ─────────────────────────────────────────────────────────────────────────────
# DRIFT MONITOR — powers the PSI feature drift table
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/drift",
    response_model=APIResponse,
    summary="Feature drift PSI values",
)
def get_drift_report(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch the latest PSI drift values per feature from the most recent batch run.
    If no batch run has completed yet, returns placeholder pending rows.
    """
    logger.info("GET /admin/drift")
    try:
        # Get the most recent completed batch run
        batch_rows = db.execute_query("""
            SELECT id FROM batch_runs
            WHERE status = 'completed'
            ORDER BY started_at DESC
            LIMIT 1;
        """)

        if not batch_rows:
            # No batch run yet — return placeholder rows so the dashboard
            # shows "pending" instead of an empty table
            logger.info("  No completed batch run — returning placeholder drift rows")
            placeholder_features = [
                "DaySinceLastOrder", "Tenure", "OrderCount", "SatisfactionScore",
                "CouponUsed", "CashbackAmount", "HourSpendOnApp",
                "WarehouseToHome", "OrderAmountHikeFromlastYear", "Complain",
            ]
            data = [
                {
                    "feature_name":    f,
                    "psi_value":       None,
                    "drift_level":     "pending",
                    "reference_mean":  None,
                    "current_mean":    None,
                }
                for f in placeholder_features
            ]
            return APIResponse(success=True, data=data)

        # Get drift values for the most recent batch run
        batch_id = batch_rows[0]["id"]
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
            (str(batch_id),),
        )

        logger.info(f"  Drift: {len(rows)} features from batch {str(batch_id)[:8]}")
        return APIResponse(success=True, data=rows)

    except Exception as e:
        logger.error(f"  Failed to fetch drift data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch drift data")


# ─────────────────────────────────────────────────────────────────────────────
# LAST BATCH — powers the health bar
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/last-batch",
    response_model=APIResponse,
    summary="Most recent batch run info",
)
def get_last_batch(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    """
    Fetch the most recent batch run record for the health bar.
    Returns status, timing, risk counts, and drift alert flag.
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
            return APIResponse(success=True, data=None, message="No batch runs yet")

        batch = rows[0]
        # Serialise datetime fields
        for field in ("started_at", "completed_at"):
            if batch.get(field):
                batch[field] = batch[field].isoformat()
        if batch.get("id"):
            batch["id"] = str(batch["id"])

        return APIResponse(success=True, data=batch)

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
    Called by the daily GitHub Actions cron workflow.
    Returns the number of customers updated.
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
# SSE STATUS — diagnostic endpoint for monitoring
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/sse-status",
    response_model=APIResponse,
    summary="SSE queue diagnostics",
)
def get_sse_status() -> APIResponse:
    """
    Return current SSE queue size and published event count.
    Useful for monitoring the live event feed health.
    No DB connection needed.
    """
    return APIResponse(success=True, data=sse_service.get_status())
