"""
scripts/run_db_diagnostics.py
══════════════════════════════════════════════════════════════════════════════
DATABASE DIAGNOSTIC SCRIPT

PURPOSE:
  Run this FIRST before any migration or fix.
  It connects to the live Supabase database and prints the exact column
  names and types of the customers and customer_features tables.

  This tells you precisely which columns exist so you know exactly what
  INSERT statements will succeed and which will fail with UndefinedColumn.

WHY THIS IS IMPORTANT:
  The customer_service.py INSERT builds its SQL dynamically from a dict.
  If ANY key in that dict does not match a real column name, psycopg2
  raises errors.UndefinedColumn and the entire INSERT is rolled back.
  The error is swallowed by the route's except Exception handler
  and the customer never reaches the database.

USAGE:
  From project root:
    python scripts/run_db_diagnostics.py

OUTPUT:
  Prints all column names, types, nullable status for:
    - customers table
    - customer_features table
  Also checks whether full_name column exists.
══════════════════════════════════════════════════════════════════════════════
"""

import logging
import sys
from pathlib import Path

# Add project root to sys.path so database/ and config/ packages are found
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def inspect_table(db, table_name: str) -> list[dict]:
    """
    Query information_schema to get exact column definitions for a table.

    information_schema.columns is a PostgreSQL system view that always
    exists and does not require special permissions. It shows every
    column, its data type, and whether it allows NULL.

    Args:
        db:         connected DatabaseConnection
        table_name: name of the table to inspect

    Returns:
        list of dicts with keys: column_name, data_type, is_nullable, column_default
    """
    logger.info(f"  Inspecting table: {table_name}")

    rows = db.execute_query(
        """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = %s
        ORDER BY ordinal_position;
        """,
        (table_name,),
    )

    if not rows:
        logger.warning(f"  Table '{table_name}' not found in public schema!")
        return []

    return rows


def check_full_name_exists(columns: list[dict]) -> bool:
    """
    Check whether the full_name column exists in the columns list.

    Args:
        columns: list of column dicts from inspect_table()

    Returns:
        bool: True if full_name column exists
    """
    return any(col["column_name"] == "full_name" for col in columns)


def print_table_columns(table_name: str, columns: list[dict]) -> None:
    """
    Print a formatted table of column definitions to stdout.

    Args:
        table_name: display name for the header
        columns:    list of column dicts from inspect_table()
    """
    print()
    print(f"  ┌─ TABLE: {table_name} ({len(columns)} columns) ─────────────────────────┐")
    print(f"  {'COLUMN NAME':<35} {'TYPE':<25} {'NULLABLE':<10} {'DEFAULT'}")
    print(f"  {'─' * 35} {'─' * 25} {'─' * 10} {'─' * 20}")

    for col in columns:
        name     = col["column_name"]
        dtype    = col["data_type"]
        nullable = col["is_nullable"]
        default  = str(col.get("column_default") or "")[:30]

        # Highlight the full_name column if it exists
        marker = " ← full_name" if name == "full_name" else ""
        print(f"  {name:<35} {dtype:<25} {nullable:<10} {default}{marker}")

    print()


def main() -> None:
    from database.connection import DatabaseConnection

    logger.info("=" * 60)
    logger.info("  CHURNGUARD — DATABASE DIAGNOSTIC")
    logger.info("=" * 60)

    with DatabaseConnection() as db:

        # ── Inspect customers table ────────────────────────────────────────
        logger.info("Inspecting customers table...")
        customer_cols = inspect_table(db, "customers")
        print_table_columns("customers", customer_cols)

        # Check for full_name column
        if check_full_name_exists(customer_cols):
            logger.info("  ✓ full_name column EXISTS in customers table")
        else:
            logger.warning("  ✗ full_name column MISSING from customers table")
            logger.warning("    Run: python scripts/run_add_full_name.py to add it")

        # Print just the column names as a Python list — useful for copy-pasting
        # into customer_service.py insert_data dict
        customer_col_names = [col["column_name"] for col in customer_cols]
        print(f"  customers column names (copy-paste ready):")
        print(f"  {customer_col_names}")
        print()

        # ── Inspect customer_features table ───────────────────────────────
        logger.info("Inspecting customer_features table...")
        feature_cols = inspect_table(db, "customer_features")
        print_table_columns("customer_features", feature_cols)

        feature_col_names = [col["column_name"] for col in feature_cols]
        print(f"  customer_features column names (copy-paste ready):")
        print(f"  {feature_col_names}")
        print()

        # ── Check predictions table (where kaggle scores live) ─────────────
        logger.info("Checking predictions table row count...")
        rows = db.execute_query("SELECT COUNT(*) AS cnt FROM predictions;")
        logger.info(f"  predictions table: {rows[0]['cnt']:,} rows")

        # ── Check customers count ───────────────────────────────────────────
        logger.info("Checking customers table row count...")
        rows = db.execute_query("SELECT COUNT(*) AS cnt FROM customers;")
        logger.info(f"  customers table: {rows[0]['cnt']:,} rows")

        # ── Check most recent customer ─────────────────────────────────────
        logger.info("Fetching most recently registered customer...")
        rows = db.execute_query(
            """
            SELECT id, registered_at, gender, city_tier
            FROM customers
            ORDER BY registered_at DESC
            LIMIT 1;
            """
        )
        if rows:
            r = rows[0]
            logger.info(
                f"  Latest customer: id={str(r['id'])[:8]}... "
                f"registered_at={r['registered_at']} "
                f"gender={r['gender']} "
                f"city_tier={r['city_tier']}"
            )
        else:
            logger.warning("  No customers found in table")

    logger.info("=" * 60)
    logger.info("  DIAGNOSTIC COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
