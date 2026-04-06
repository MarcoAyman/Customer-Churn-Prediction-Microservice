"""
scripts/run_seed_batch_run.py — v4
Reads the actual CHECK constraint on triggered_by before inserting.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main() -> None:
    from database.connection import DatabaseConnection

    logger.info("=" * 55)
    logger.info("  SEED: batch_runs table")
    logger.info("=" * 55)

    with DatabaseConnection() as db:

        # ── Step 1: Already seeded? ────────────────────────────────────────
        rows = db.execute_query("SELECT COUNT(*) AS cnt FROM batch_runs;")
        if rows[0]["cnt"] > 0:
            logger.info(f"  Already seeded ({rows[0]['cnt']} rows) — nothing to do")
            return
        logger.info("  batch_runs is empty — will insert")

        # ── Step 2: Read all CHECK constraints on batch_runs ──────────────
        # This shows us EXACTLY what values are allowed for triggered_by
        logger.info("Step 2: Reading CHECK constraints on batch_runs...")
        constraints = db.execute_query(
            """
            SELECT conname, pg_get_constraintdef(oid) AS definition
            FROM pg_constraint
            WHERE conrelid = 'batch_runs'::regclass
              AND contype  = 'c';
            """
        )
        logger.info(f"  Found {len(constraints)} CHECK constraint(s):")
        for c in constraints:
            logger.info(f"    {c['conname']}: {c['definition']}")

        # Extract allowed triggered_by values from the constraint definition
        # The constraint looks like:
        # CHECK (triggered_by = ANY (ARRAY['manual', 'cron', ...]))
        triggered_by_value = "manual"   # safe default — almost always allowed
        for c in constraints:
            defn = c["definition"].lower()
            if "triggered_by" in defn:
                logger.info(f"  triggered_by constraint: {c['definition']}")
                # Try to parse allowed values from the constraint text
                # e.g. ARRAY['manual'::text, 'cron'::text, 'api'::text]
                import re
                matches = re.findall(r"'([^']+)'", c["definition"])
                if matches:
                    logger.info(f"  Allowed triggered_by values: {matches}")
                    # Pick the first value (most likely 'manual' or 'cron')
                    triggered_by_value = matches[0]
                    logger.info(f"  Will use: '{triggered_by_value}'")
                break

        # ── Step 3: Get model_version_id ───────────────────────────────────
        logger.info("Step 3: Looking up kaggle_baseline...")
        model_version_id = None
        try:
            mv = db.execute_query(
                "SELECT id, version FROM model_versions LIMIT 5;"
            )
            for r in mv:
                if "kaggle" in str(r.get("version", "")).lower():
                    model_version_id = str(r["id"])
                    logger.info(f"  Found: id={model_version_id[:8]}...")
                    break
        except Exception as e:
            logger.warning(f"  model_versions lookup failed: {e}")

        # ── Step 4: Get generated + insertable columns ─────────────────────
        logger.info("Step 4: Finding generated columns to exclude...")
        try:
            gen = db.execute_query(
                """
                SELECT attname FROM pg_attribute
                WHERE attrelid = 'batch_runs'::regclass
                  AND attgenerated != '' AND attnum > 0;
                """
            )
            generated = {r["attname"] for r in gen}
        except Exception:
            generated = {"duration_seconds"}
        logger.info(f"  Generated (excluded): {generated}")

        all_cols = db.execute_query(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'batch_runs'
            ORDER BY ordinal_position;
            """
        )
        insertable = {r["column_name"] for r in all_cols} - generated - {"id"}

        # ── Step 5: Build and run INSERT ───────────────────────────────────
        seed_started   = datetime(2026, 3, 15, 23, 47, 50, tzinfo=timezone.utc)
        seed_completed = datetime(2026, 3, 15, 23, 47, 57, tzinfo=timezone.utc)

        candidates = {
            "model_version_id":  model_version_id,
            "model_version":     "kaggle_baseline",
            "triggered_by":      triggered_by_value,   # ← from CHECK constraint
            "status":            "completed",
            "started_at":        seed_started,
            "completed_at":      seed_completed,
            "customers_scored":  5630,
            "high_risk_count":   948,
            "medium_risk_count": 0,
            "low_risk_count":    4682,
            "drift_checked":     False,
            "drift_alert_fired": False,
            "error_message":     None,
        }

        insert_data = {k: v for k, v in candidates.items() if k in insertable}

        logger.info(f"Step 5: Inserting {len(insert_data)} fields:")
        for k, v in insert_data.items():
            logger.info(f"  {k:<30} = {v!r}")

        cols   = list(insert_data.keys())
        vals   = list(insert_data.values())
        pholds = ", ".join(["%s"] * len(cols))

        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO batch_runs ({', '.join(cols)}) "
                    f"VALUES ({pholds}) RETURNING id;",
                    vals,
                )
                batch_run_id = str(cur.fetchone()[0])
            conn.commit()

        logger.info(f"  ✓ Inserted: id={batch_run_id}")

        # ── Step 6: Verify ─────────────────────────────────────────────────
        verify = db.execute_query(
            "SELECT model_version, status, customers_scored, "
            "high_risk_count, duration_seconds FROM batch_runs WHERE id = %s;",
            (batch_run_id,)
        )
        if verify:
            r = verify[0]
            logger.info("  ✓ Confirmed in Supabase:")
            for k, v in r.items():
                logger.info(f"    {k}: {v}")
        else:
            logger.error("  ✗ Row not found after commit")
            sys.exit(1)

        # ── Step 7: v_churn_trend ──────────────────────────────────────────
        trend = db.execute_query("SELECT * FROM v_churn_trend;")
        logger.info(f"  v_churn_trend: {len(trend)} row(s) {'✓' if trend else '⚠ still empty'}")

    logger.info("=" * 55)
    logger.info("  DONE — health bar will now show model + last batch info")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
