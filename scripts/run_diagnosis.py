"""
scripts/run_diagnosis.py
══════════════════════════════════════════════════════════════════════════════
FULL SYSTEM DIAGNOSIS

Tests everything in order:
  1. Database connection
  2. All table row counts
  3. All views — what SQL they use and what they return
  4. A sample INSERT + SELECT + DELETE (proves writes work)
  5. API health endpoint (proves Render is awake)
  6. Every admin endpoint (proves each data path works)
  7. SSE endpoint reachability

Run this locally. It talks directly to Supabase AND to the Render API.
Output tells you exactly what is broken and what is working.

USAGE:
  python scripts/run_diagnosis.py

REQUIRES:
  - .env file with DATABASE_URL set
  - RENDER_URL environment variable OR hardcode your Render URL below
══════════════════════════════════════════════════════════════════════════════
"""

import json
import logging
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── CONFIGURE THESE ────────────────────────────────────────────────────────
RENDER_URL  = os.environ.get("RENDER_URL", "https://churnguard-api-9am4.onrender.com")
ADMIN_KEY   = os.environ.get("ADMIN_API_KEY", "35ed4d2c0931c33b3a3bd695c677fbbf65eaf75cf46651bc7587f4596a3f9e16")
# ──────────────────────────────────────────────────────────────────────────


PASS  = "  ✓"
FAIL  = "  ✗"
WARN  = "  ⚠"
INFO  = "  →"

results = []   # collect (test_name, passed) tuples


def record(name: str, passed: bool) -> None:
    results.append((name, passed))
    symbol = PASS if passed else FAIL
    logger.info(f"{symbol} {name}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────────────────────

def test_db_connection() -> "DatabaseConnection | None":
    logger.info("\n" + "═" * 55)
    logger.info("  SECTION 1: Database Connection")
    logger.info("═" * 55)

    try:
        from database.connection import DatabaseConnection
        db = DatabaseConnection()
        db.connect()
        healthy = db.health_check()
        record("DB connect + health check (SELECT 1)", healthy)
        return db
    except Exception as e:
        record(f"DB connect FAILED: {e}", False)
        logger.error(f"{FAIL} Cannot proceed without DB connection. Check DATABASE_URL.")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — TABLE ROW COUNTS
# ─────────────────────────────────────────────────────────────────────────────

def test_table_counts(db) -> None:
    logger.info("\n" + "═" * 55)
    logger.info("  SECTION 2: Table Row Counts")
    logger.info("═" * 55)

    tables = [
        "customers",
        "customer_features",
        "predictions",
        "model_versions",
        "batch_runs",
        "drift_reports",
        "sse_events",
    ]

    for table in tables:
        try:
            rows = db.execute_query(f"SELECT COUNT(*) AS cnt FROM {table};")
            count = rows[0]["cnt"]
            ok = count > 0 or table in ("drift_reports", "sse_events", "batch_runs")
            logger.info(f"  {'✓' if ok else '⚠'} {table:<30} {count:>8,} rows")
            record(f"Table {table} accessible", True)
        except Exception as e:
            record(f"Table {table} ERROR: {e}", False)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — VIEW DEFINITIONS AND OUTPUT
# ─────────────────────────────────────────────────────────────────────────────

def test_views(db) -> None:
    logger.info("\n" + "═" * 55)
    logger.info("  SECTION 3: View Definitions and Output")
    logger.info("═" * 55)

    views = [
        "v_current_risk_summary",
        "v_top_at_risk",
        "v_churn_trend",
        "v_customer_ml_features",
    ]

    for view in views:
        logger.info(f"\n  Checking view: {view}")

        # Get the view definition from pg_views
        try:
            defn_rows = db.execute_query(
                "SELECT definition FROM pg_views WHERE viewname = %s AND schemaname = 'public';",
                (view,)
            )
            if defn_rows:
                defn = defn_rows[0]["definition"]
                # Print first 300 chars of the definition
                preview = defn[:300].replace("\n", " ").replace("  ", " ")
                logger.info(f"  {INFO} Definition preview: {preview}...")
            else:
                logger.warning(f"  {WARN} View not found in pg_views!")
                record(f"View {view} exists", False)
                continue
        except Exception as e:
            logger.error(f"  {FAIL} Could not read view definition: {e}")

        # Query the view
        try:
            rows = db.execute_query(f"SELECT * FROM {view} LIMIT 5;")
            if rows:
                logger.info(f"  {PASS} {view} returned {len(rows)} row(s)")
                logger.info(f"  {INFO} First row: {dict(list(rows[0].items())[:5])}...")
                record(f"View {view} returns data", True)
            else:
                logger.warning(f"  {WARN} {view} returned 0 rows — possible view bug")
                record(f"View {view} returns data", False)
        except Exception as e:
            logger.error(f"  {FAIL} {view} query FAILED: {e}")
            record(f"View {view} queryable", False)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — SAMPLE INSERT + VERIFY + DELETE
# ─────────────────────────────────────────────────────────────────────────────

def test_write_roundtrip(db) -> None:
    logger.info("\n" + "═" * 55)
    logger.info("  SECTION 4: Write Round-Trip (INSERT → SELECT → DELETE)")
    logger.info("═" * 55)

    now = datetime.now(timezone.utc)
    customer_id = None

    try:
        # INSERT into customers
        logger.info(f"  {INFO} Attempting INSERT INTO customers...")
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO customers (
                        registered_at, is_active, role,
                        gender, marital_status, city_tier,
                        preferred_payment_mode, preferred_login_device,
                        preferred_order_cat
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id;
                    """,
                    (now, True, 'customer', 'Male', 'Single', 2,
                     'Credit Card', 'Mobile Phone', 'Grocery')
                )
                result = cur.fetchone()
                customer_id = str(result[0])
            conn.commit()
        logger.info(f"  {PASS} INSERT + commit succeeded: id={customer_id[:8]}...")

        # SELECT back
        verify = db.execute_query(
            "SELECT id, registered_at, gender FROM customers WHERE id = %s",
            (customer_id,)
        )
        if verify:
            logger.info(f"  {PASS} SELECT confirmed row exists in DB")
            record("customers INSERT + SELECT verify", True)
        else:
            logger.error(f"  {FAIL} Row NOT found after commit — pgBouncer issue?")
            record("customers INSERT + SELECT verify", False)
            return

        # INSERT customer_features
        logger.info(f"  {INFO} Attempting INSERT INTO customer_features...")
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO customer_features (
                        customer_id, tenure_months, complain,
                        order_count, coupon_used, cashback_amount,
                        number_of_address, number_of_device_registered,
                        features_computed_at, features_source
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (customer_id) DO NOTHING;
                    """,
                    (customer_id, 0.0, False, 0, 0, 0.0, 1, 1, now, 'diagnosis_test')
                )
            conn.commit()

        vf = db.execute_query(
            "SELECT customer_id FROM customer_features WHERE customer_id = %s",
            (customer_id,)
        )
        if vf:
            logger.info(f"  {PASS} customer_features row confirmed in DB")
            record("customer_features INSERT + SELECT verify", True)
        else:
            logger.error(f"  {FAIL} customer_features row NOT found after commit")
            record("customer_features INSERT + SELECT verify", False)

    except Exception as e:
        logger.error(f"  {FAIL} Write round-trip FAILED: {type(e).__name__}: {e}")
        record("Write round-trip", False)

    finally:
        # Always clean up test data
        if customer_id:
            try:
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "DELETE FROM customer_features WHERE customer_id = %s",
                            (customer_id,)
                        )
                        cur.execute(
                            "DELETE FROM customers WHERE id = %s",
                            (customer_id,)
                        )
                    conn.commit()
                logger.info(f"  {INFO} Test rows deleted (clean up complete)")
            except Exception as e:
                logger.warning(f"  {WARN} Cleanup failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — ACTUAL COLUMN NAMES (what really exists)
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_columns(db) -> None:
    logger.info("\n" + "═" * 55)
    logger.info("  SECTION 5: Actual Column Names in Supabase")
    logger.info("═" * 55)

    for table in ("customers", "customer_features"):
        rows = db.execute_query(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (table,)
        )
        logger.info(f"\n  {table} ({len(rows)} columns):")
        for r in rows:
            nullable = "" if r["is_nullable"] == "YES" else " NOT NULL"
            logger.info(f"    {r['column_name']:<42} {r['data_type']}{nullable}")

        # Check for full_name specifically
        has_full_name = any(r["column_name"] == "full_name" for r in rows)
        logger.info(f"\n  full_name column: {'EXISTS ✓' if has_full_name else 'MISSING ✗'}")
        record(f"{table}.full_name exists", has_full_name)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — API ENDPOINTS (via HTTP from local machine to Render)
# ─────────────────────────────────────────────────────────────────────────────

def api_get(path: str, admin: bool = False, timeout: int = 15) -> dict | None:
    """Make a GET request to the Render API, return JSON or None on failure."""
    url = f"{RENDER_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if admin:
        headers["X-Admin-Key"] = ADMIN_KEY
    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        logger.error(f"    HTTP {e.code}: {e.reason}")
        return None
    except Exception as e:
        logger.error(f"    {type(e).__name__}: {e}")
        return None


def test_api_endpoints() -> None:
    logger.info("\n" + "═" * 55)
    logger.info(f"  SECTION 6: API Endpoints → {RENDER_URL}")
    logger.info("═" * 55)

    endpoints = [
        ("/api/v1/health",                   False,  "Health check (no auth)"),
        ("/api/v1/admin/overview",           True,   "KPI overview"),
        ("/api/v1/admin/risk-distribution",  True,   "Risk distribution"),
        ("/api/v1/admin/churn-trend",        True,   "Churn trend"),
        ("/api/v1/admin/at-risk",            True,   "At-risk customers"),
        ("/api/v1/admin/drift",              True,   "Drift monitor"),
        ("/api/v1/admin/last-batch",         True,   "Last batch"),
        ("/api/v1/admin/sse-status",         True,   "SSE status"),
    ]

    for path, admin, name in endpoints:
        logger.info(f"\n  Testing: {name} ({path})")
        resp = api_get(path, admin=admin)

        if resp is None:
            record(f"API: {name}", False)
            continue

        if "status" in resp:  # health endpoint
            ok = resp.get("status") in ("healthy", "degraded")
            db_ok = resp.get("db_connected", False)
            logger.info(f"    status={resp.get('status')}, db_connected={db_ok}, env={resp.get('environment')}")
            record(f"API: {name}", ok)
        elif resp.get("success"):
            data = resp.get("data")
            if isinstance(data, list):
                logger.info(f"    success=True, rows={len(data)}")
            elif isinstance(data, dict):
                preview = {k: v for k, v in list(data.items())[:4]}
                logger.info(f"    success=True, data={preview}...")
            else:
                logger.info(f"    success=True, data={data}")
            record(f"API: {name}", True)
        else:
            logger.warning(f"    Response: {resp}")
            record(f"API: {name}", False)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 — FINAL SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def print_summary() -> None:
    logger.info("\n" + "═" * 55)
    logger.info("  DIAGNOSIS SUMMARY")
    logger.info("═" * 55)

    passed = [n for n, ok in results if ok]
    failed = [n for n, ok in results if not ok]

    logger.info(f"\n  PASSED ({len(passed)}):")
    for n in passed:
        logger.info(f"    ✓ {n}")

    if failed:
        logger.info(f"\n  FAILED ({len(failed)}):")
        for n in failed:
            logger.info(f"    ✗ {n}")

    logger.info(f"\n  Score: {len(passed)}/{len(results)}")

    if not failed:
        logger.info("\n  ✓ ALL CHECKS PASSED")
        logger.info("  The system is healthy. If the dashboard looks broken,")
        logger.info("  it is likely a frontend code issue, not a backend issue.")
    else:
        logger.info("\n  ✗ FAILURES FOUND")
        logger.info("  The items above tell you exactly what is broken.")
        logger.info("  Fix the failures in order — each one may unblock the next.")

    logger.info("═" * 55)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 55)
    logger.info("  CHURNGUARD — FULL SYSTEM DIAGNOSIS")
    logger.info(f"  API: {RENDER_URL}")
    logger.info(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    # Section 1: DB connection
    db = test_db_connection()

    if db:
        # Section 2: Row counts
        test_table_counts(db)

        # Section 3: View definitions + output
        test_views(db)

        # Section 4: Write round-trip
        test_write_roundtrip(db)

        # Section 5: Actual column names
        test_schema_columns(db)

        db.disconnect()
    else:
        logger.error("Skipping DB sections — no connection.")

    # Section 6: API endpoints (independent of DB connection above)
    test_api_endpoints()

    # Section 7: Summary
    print_summary()


if __name__ == "__main__":
    main()
