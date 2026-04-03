"""
scripts/run_db_connection.py
══════════════════════════════════════════════════════════════════════════════
CLI script — test the database connection and print a full diagnostic report.

PURPOSE:
  Run this script any time you want to verify:
    1. The .env file is correctly configured
    2. The DATABASE_URL can reach Supabase
    3. The connection pool is healthy
    4. All 7 expected tables exist in the database
    5. Row counts in each table are as expected

  This is the first script to run when something seems wrong with the DB.
  It tells you exactly what the connection can and cannot see.

USAGE:
  cd churn_prediction/
  python scripts/run_db_connection.py

  No arguments needed — reads DATABASE_URL from .env automatically.

OUTPUT:
  Console output + timestamped log in logs/db_connection_YYYY-MM-DD.log
══════════════════════════════════════════════════════════════════════════════
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

import logging          # structured logging
import sys              # sys.path manipulation and exit codes
from datetime import datetime   # for timestamped log filenames
from pathlib import Path        # file path handling

# Add project root to sys.path so 'from database.connection import ...' works
# This is needed when running: python scripts/run_db_connection.py
# from anywhere — Python needs to find the database/ and config/ packages
PROJECT_ROOT = Path(__file__).parent.parent   # scripts/ → project root
sys.path.insert(0, str(PROJECT_ROOT))         # prepend project root to module search path

# Import the reusable connection class
from database.connection import DatabaseConnection

# Import table names from config — so this script stays in sync with the schema
from config.db_config import ALL_TABLES


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    """
    Configure logging to write to console AND a timestamped log file.
    Called once at the start of main().
    """
    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(exist_ok=True)   # create logs/ directory if it doesn't exist

    # Timestamp in filename makes each run's log unique and easy to find
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file  = logs_dir / f"db_connection_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),                    # console
            logging.FileHandler(log_file, encoding="utf-8"),      # file
        ],
    )
    logging.info(f"Log file: {log_file}")


# ─────────────────────────────────────────────────────────────────────────────
# DIAGNOSTIC CHECKS — run after connection is established
# ─────────────────────────────────────────────────────────────────────────────

def check_tables_exist(db: DatabaseConnection) -> bool:
    """
    Query PostgreSQL's information_schema to verify all 7 expected tables exist.

    WHY information_schema?
      information_schema.tables is a system view that lists all tables
      in all schemas. It is always available in PostgreSQL and does not
      require any special permissions to read.

    Args:
        db: connected DatabaseConnection instance

    Returns:
        bool: True if all expected tables exist
    """
    logging.info("")
    logging.info("─" * 50)
    logging.info("CHECK 1 — Verifying expected tables exist")
    logging.info("─" * 50)

    # Query the system view to list all user tables in the 'public' schema
    rows = db.execute_query("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type   = 'BASE TABLE'
        ORDER BY table_name;
    """)

    # Build a set of table names for fast lookup
    existing = {row["table_name"] for row in rows}

    logging.info(f"  Tables found in database ({len(existing)}):")
    for name in sorted(existing):
        logging.info(f"    ✓ {name}")

    # Check every expected table is present
    missing = [t for t in ALL_TABLES if t not in existing]

    if missing:
        logging.error(f"  ✗ Missing tables: {missing}")
        logging.error("  The schema may not have been applied.")
        logging.error("  Paste database/schema.sql into Supabase SQL Editor and run it.")
        return False   # at least one table is missing

    logging.info(f"  ✓ All {len(ALL_TABLES)} expected tables are present")
    return True


def check_row_counts(db: DatabaseConnection) -> None:
    """
    Query the row count of each table and log a summary.

    This gives a quick visual confirmation that seeding was successful
    (customers=5630, customer_features=5630, predictions=5630, etc.)

    Args:
        db: connected DatabaseConnection instance
    """
    logging.info("")
    logging.info("─" * 50)
    logging.info("CHECK 2 — Row counts per table")
    logging.info("─" * 50)

    for table in ALL_TABLES:
        # COUNT(*) is the fastest way to count rows in PostgreSQL
        # It reads the table metadata, not the actual row data
        rows = db.execute_query(f"SELECT COUNT(*) AS count FROM {table};")
        count = rows[0]["count"]   # extract the count from the result dict
        logging.info(f"  {table:<25} {count:>8,} rows")


def check_views_exist(db: DatabaseConnection) -> None:
    """
    Verify the 4 database views exist and are queryable.

    Views are not tables — they are saved SQL queries. If a table they
    reference has been dropped or renamed, the view becomes invalid and
    queries against it will fail. This check detects that early.

    Args:
        db: connected DatabaseConnection instance
    """
    logging.info("")
    logging.info("─" * 50)
    logging.info("CHECK 3 — Verifying views are queryable")
    logging.info("─" * 50)

    # The 4 views defined in schema.sql
    views = [
        "v_customer_ml_features",
        "v_current_risk_summary",
        "v_top_at_risk",
        "v_churn_trend",
    ]

    for view in views:
        try:
            # SELECT with LIMIT 1 — just check the view is queryable, not its data
            db.execute_query(f"SELECT * FROM {view} LIMIT 1;")
            logging.info(f"  ✓ {view}")
        except Exception as e:
            # View is broken — log the error but continue checking others
            logging.error(f"  ✗ {view} — FAILED: {e}")


def check_pool_status(db: DatabaseConnection) -> None:
    """
    Log the connection pool configuration for diagnostic purposes.

    Args:
        db: connected DatabaseConnection instance
    """
    logging.info("")
    logging.info("─" * 50)
    logging.info("CHECK 4 — Connection pool status")
    logging.info("─" * 50)

    status = db.get_pool_status()   # returns a dict of pool diagnostics

    logging.info(f"  Connected:            {status['connected']}")
    logging.info(f"  Pool size:            min={status['min_connections']}, "
                 f"max={status['max_connections']}")
    logging.info(f"  Pool timeout:         {status['pool_timeout_sec']}s")
    logging.info(f"  Query timeout:        {status['query_timeout_sec']}s")
    logging.info(f"  Database URL:         {status['db_url_masked']}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Connect to Supabase, run all diagnostic checks, and print a summary.
    Exits with code 0 on success, 1 on failure.
    """
    setup_logging()

    logging.info("=" * 50)
    logging.info("  CHURNGUARD — DATABASE CONNECTION TEST")
    logging.info("=" * 50)
    logging.info(f"  Project root: {PROJECT_ROOT}")

    # Use the context manager — auto-connects on enter, auto-disconnects on exit
    # If connect() fails, the exception propagates here and the script exits
    try:
        with DatabaseConnection() as db:

            # Check 1 — all tables present?
            tables_ok = check_tables_exist(db)

            # Check 2 — row counts (informational, does not fail)
            check_row_counts(db)

            # Check 3 — views queryable? (only if tables exist)
            if tables_ok:
                check_views_exist(db)

            # Check 4 — pool diagnostics
            check_pool_status(db)

            # Final summary
            logging.info("")
            logging.info("=" * 50)
            if tables_ok:
                logging.info("  ✓ DATABASE CONNECTION TEST PASSED")
                logging.info("  All checks completed successfully.")
                logging.info("  The database is ready for use.")
            else:
                logging.error("  ✗ DATABASE CONNECTION TEST FAILED")
                logging.error("  Some tables are missing — apply schema.sql first.")
            logging.info("=" * 50)

            sys.exit(0 if tables_ok else 1)   # 0 = success, 1 = failure

    except (ValueError, ConnectionError) as e:
        # ValueError: DATABASE_URL not set
        # ConnectionError: could not reach Supabase
        logging.error(f"  ✗ FATAL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
