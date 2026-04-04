"""
scripts/run_add_full_name.py
══════════════════════════════════════════════════════════════════════════════
DATABASE MIGRATION — Add full_name column to customers table.

WHY THIS IS NEEDED:
  The original customers table was seeded from the Kaggle e-commerce dataset.
  Kaggle data has no customer names — so the original schema did not include
  a full_name column.

  Live registrations through the entry form DO collect a full name.
  Without this column, any INSERT that includes full_name in its column
  list will raise psycopg2.errors.UndefinedColumn and be rolled back.
  This is silently swallowed by the route's except Exception handler,
  meaning the customer is never written to the database.

WHAT THIS SCRIPT DOES:
  Step 1: Checks if full_name already exists (safe to run multiple times)
  Step 2: If missing, runs ALTER TABLE customers ADD COLUMN full_name TEXT
  Step 3: Verifies the column was added successfully
  Step 4: Logs confirmation

  IF COLUMN ALREADY EXISTS:
    The script detects this and exits safely without making any changes.
    It is safe to run this script multiple times.

SQL EXECUTED:
  ALTER TABLE customers ADD COLUMN IF NOT EXISTS full_name TEXT;

  TEXT type: allows any length string.
  NULL allowed: Kaggle-seeded rows have no name (NULL is correct for them).
  No default: NULL is the correct default for historical rows.

USAGE:
  From project root:
    python scripts/run_add_full_name.py
══════════════════════════════════════════════════════════════════════════════
"""

import logging
import sys
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


def column_exists(db, table: str, column: str) -> bool:
    """
    Check if a column already exists in a table.

    Uses information_schema — always available, no special permissions needed.
    Returns True if the column exists, False if it needs to be created.

    Args:
        db:     connected DatabaseConnection
        table:  table name to check
        column: column name to look for

    Returns:
        bool: True if column exists
    """
    rows = db.execute_query(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = %s
          AND column_name  = %s;
        """,
        (table, column),
    )
    return rows[0]["cnt"] > 0


def add_full_name_column(db) -> bool:
    """
    Add full_name TEXT column to the customers table.

    Uses IF NOT EXISTS so it is safe to run even if the column already exists.
    PostgreSQL will skip the ALTER TABLE silently in that case.

    Args:
        db: connected DatabaseConnection

    Returns:
        bool: True if the column was added (or already existed)

    Raises:
        Exception: propagates any unexpected DB error
    """
    logger.info("  Executing: ALTER TABLE customers ADD COLUMN IF NOT EXISTS full_name TEXT")

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE customers
                ADD COLUMN IF NOT EXISTS full_name TEXT;
                """
            )
            # IF NOT EXISTS: PostgreSQL adds the column if missing,
            # or does nothing if it already exists.
            # No error either way — safe to run multiple times.
        conn.commit()
        # Commit is required — DDL statements (ALTER TABLE) are NOT
        # auto-committed in psycopg2. Without this, the change is rolled back
        # when the connection is returned to the pool.

    logger.info("  ALTER TABLE committed to Supabase ✓")
    return True


def add_full_name_index(db) -> None:
    """
    Add a GIN index on full_name for text search performance.
    Optional — skipped silently if it already exists.

    A plain text search (LIKE '%name%') on an unindexed TEXT column
    would do a full table scan. A btree index is sufficient for
    exact-match and prefix queries from the dashboard search box.

    Args:
        db: connected DatabaseConnection
    """
    logger.info("  Adding btree index on full_name (CREATE INDEX IF NOT EXISTS)...")
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_customers_full_name
                    ON customers (full_name);
                    """
                )
                # IF NOT EXISTS: safe to run multiple times
            conn.commit()
        logger.info("  Index created (or already existed) ✓")
    except Exception as e:
        # Index creation is non-critical — log and continue
        logger.warning(f"  Index creation skipped: {e}")


def main() -> None:
    from database.connection import DatabaseConnection

    logger.info("=" * 60)
    logger.info("  MIGRATION: Add full_name to customers table")
    logger.info("=" * 60)

    with DatabaseConnection() as db:

        # ── Step 1: Check current state ────────────────────────────────────
        logger.info("Step 1: Checking if full_name column already exists...")

        if column_exists(db, "customers", "full_name"):
            logger.info("  ✓ full_name column ALREADY EXISTS — no migration needed")
            logger.info("  The migration is safe to re-run but nothing changed.")

        else:
            logger.info("  full_name column DOES NOT EXIST — migration needed")

            # ── Step 2: Add the column ─────────────────────────────────────
            logger.info("Step 2: Adding full_name column...")
            add_full_name_column(db)

            # ── Step 3: Verify it was added ────────────────────────────────
            logger.info("Step 3: Verifying the column was added...")

            if column_exists(db, "customers", "full_name"):
                logger.info("  ✓ full_name column CONFIRMED in Supabase")
            else:
                logger.error("  ✗ Column not found after ALTER TABLE — check Supabase logs")
                sys.exit(1)

            # ── Step 4: Add index (non-critical) ───────────────────────────
            logger.info("Step 4: Adding search index on full_name...")
            add_full_name_index(db)

        # ── Verify with a sample of existing rows ──────────────────────────
        logger.info("Step 5: Spot-checking existing rows...")
        rows = db.execute_query(
            """
            SELECT id, full_name, gender, registered_at
            FROM customers
            ORDER BY registered_at DESC
            LIMIT 3;
            """
        )
        for r in rows:
            logger.info(
                f"  Row: id={str(r['id'])[:8]}... "
                f"full_name={r['full_name']!r} "
                f"gender={r['gender']}"
            )
        # Expected: full_name=None for Kaggle-seeded rows (no names in dataset)
        # After live registrations: full_name='Marco Hanna' etc.

    logger.info("=" * 60)
    logger.info("  MIGRATION COMPLETE")
    logger.info("  Kaggle rows: full_name = NULL (correct — no names in dataset)")
    logger.info("  New registrations: full_name = whatever operator entered")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
