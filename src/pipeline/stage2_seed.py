"""
src/pipeline/stage2_seed.py
══════════════════════════════════════════════════════════════════════════════
STAGE 2 — DATABASE SEEDING

PURPOSE:
  Read the cleaned CSV (output of Stage 1) and insert all rows into
  the Supabase PostgreSQL database across the correct tables.

  This script is the bridge between the cleaned local data and the live
  production database. After this runs, Supabase has all 5,630 customers
  with their features and kaggle ground truth predictions.

WHAT THIS SCRIPT DOES (in order):
  Step 1 → Load the cleaned CSV
  Step 2 → Connect to Supabase
  Step 3 → Pre-flight checks (tables exist? already seeded?)
  Step 4 → Insert kaggle_baseline into model_versions
  Step 5 → Insert rows into customers table
  Step 6 → Insert rows into customer_features table
  Step 7 → Insert rows into predictions table (kaggle ground truth)
  Step 8 → Post-seeding verification (count checks)

HOW DATA IS SPLIT FROM THE 20-COLUMN CSV INTO 3 TABLES:

  CSV (20 columns)
       │
       ├──→ customers (8 columns from CSV + 4 computed)
       │      kaggle_customer_id, gender, marital_status, city_tier,
       │      preferred_payment_mode, preferred_login_device,
       │      preferred_order_cat, kaggle_churn_label
       │      + is_active=TRUE, role='customer', registered_at (backdated)
       │
       ├──→ customer_features (12 columns from CSV + 3 computed)
       │      tenure_months, satisfaction_score, complain,
       │      warehouse_to_home, number_of_address, hour_spend_on_app,
       │      number_of_device_registered, day_since_last_order,
       │      order_count, order_amount_hike_from_last_year,
       │      coupon_used, cashback_amount
       │      + customer_id (UUID), features_computed_at, features_source
       │
       └──→ predictions (derived from Churn label — kaggle baseline)
              churn_probability (1.0 if churned, 0.0 if retained),
              churn_label, risk_tier, ground_truth, ground_truth_source
              + customer_id (UUID), model_version='kaggle_baseline'

USED BY:
  scripts/run_seeding.py   — CLI entry point
══════════════════════════════════════════════════════════════════════════════
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

import logging           # structured, levelled log output
import os                # reading environment variables
import sys               # sys.exit() on critical failures
import time              # time.sleep() for retry delays
import uuid              # uuid.uuid4() for generating UUIDs in Python
from dataclasses import dataclass, field   # structured result container
from datetime import datetime, timedelta, timezone  # date arithmetic for registered_at
from pathlib import Path                   # file path handling
from typing import Optional                # type hints

import pandas as pd      # reading the cleaned CSV

# psycopg2: the standard Python driver for PostgreSQL
# Installed via: pip install psycopg2-binary python-dotenv
try:
    import psycopg2                          # PostgreSQL database adapter
    import psycopg2.extras                   # execute_values() for batch inserts
    from psycopg2.extensions import connection as PGConnection  # type hint
except ImportError:
    print("ERROR: psycopg2 is not installed.")
    print("Run: pip install psycopg2-binary python-dotenv")
    sys.exit(1)

# python-dotenv: reads .env file and loads DATABASE_URL into os.environ
try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: python-dotenv is not installed.")
    print("Run: pip install psycopg2-binary python-dotenv")
    sys.exit(1)

# Import all constants from config — no magic numbers in this script
from config.db_config import (
    ALL_TABLES,
    CHUNK_SIZE,
    CLEANED_CSV_PATH,
    CUSTOMER_FEATURES_COLUMN_MAP,
    CUSTOMERS_COLUMN_MAP,
    DAYS_PER_MONTH,
    ENV_FILE_PATH,
    KAGGLE_BASELINE_ALGORITHM,
    KAGGLE_BASELINE_NOTE,
    KAGGLE_BASELINE_THRESHOLD,
    KAGGLE_BASELINE_VERSION,
    MAX_CHUNK_RETRIES,
    RETRY_DELAY_SECONDS,
    TABLE_CUSTOMER_FEATURES,
    TABLE_CUSTOMERS,
    TABLE_MODEL_VERSIONS,
    TABLE_PREDICTIONS,
    get_risk_tier,
)

# ─────────────────────────────────────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────────────────────────────────────

# Module-level logger — name is 'src.pipeline.stage2_seed'
# The CLI runner (run_seeding.py) configures the handlers (console + file)
# This module just logs — it does not configure where logs go
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SEEDING REPORT — DATA CLASS
#
# WHY A DATACLASS?
#   After seeding completes, the caller gets a structured summary:
#   how many rows were inserted into each table, how long it took,
#   whether any errors occurred.
#   A dataclass is cleaner than a raw dict — it has typed fields and
#   a summary method for printing.
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SeedingReport:
    """Structured summary of what the seeding pipeline did."""

    # Row counts per table
    customers_inserted:         int = 0
    customer_features_inserted: int = 0
    predictions_inserted:       int = 0

    # Timing
    started_at:    Optional[datetime] = None
    completed_at:  Optional[datetime] = None

    # Error tracking
    errors:        list = field(default_factory=list)
    success:       bool = False

    @property
    def duration_seconds(self) -> Optional[float]:
        """How long the seeding took, in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def print_summary(self) -> None:
        """Log a clean, readable summary of everything that was done."""
        logger.info("=" * 60)
        logger.info("  SEEDING REPORT — STAGE 2 COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Status:               {'✓ SUCCESS' if self.success else '✗ FAILED'}")
        if self.duration_seconds:
            logger.info(f"  Duration:             {self.duration_seconds:.1f} seconds")
        logger.info("")
        logger.info("  Rows inserted per table:")
        logger.info(f"    customers:            {self.customers_inserted:>6,}")
        logger.info(f"    customer_features:    {self.customer_features_inserted:>6,}")
        logger.info(f"    predictions:          {self.predictions_inserted:>6,}")
        logger.info(f"    model_versions:       {'1 (kaggle_baseline placeholder)':>6}")
        logger.info("")
        logger.info(f"  Total rows inserted:  "
                    f"{self.customers_inserted + self.customer_features_inserted + self.predictions_inserted + 1:,}")
        if self.errors:
            logger.error("")
            logger.error(f"  Errors ({len(self.errors)}):")
            for err in self.errors:
                logger.error(f"    ✗ {err}")
        logger.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — LOAD CLEANED CSV
# ─────────────────────────────────────────────────────────────────────────────

def load_cleaned_data() -> pd.DataFrame:
    """
    Load the cleaned CSV produced by Stage 1 (stage1_clean.py).

    WHY LOAD FROM CSV AND NOT FROM THE RAW EXCEL?
      This script trusts Stage 1 already ran and produced a clean file.
      Each stage is responsible for exactly one job.
      If this script re-cleaned the data itself, we would have duplicate
      cleaning logic in two places — the #1 cause of subtle inconsistencies.

    WHY VALIDATE THE CSV HERE BEFORE PROCEEDING?
      A corrupt or empty CSV could silently insert zero rows into the DB
      without any error. We fail loudly here with a clear message rather
      than silently producing an empty database.

    Returns:
        pd.DataFrame: the cleaned data, ready for splitting into tables

    Raises:
        SystemExit: if the file does not exist or is empty
    """
    logger.info("─" * 60)
    logger.info("  STEP 1 — LOADING CLEANED CSV")
    logger.info("─" * 60)
    logger.info(f"  Source: {CLEANED_CSV_PATH}")

    # Check the file exists — fail immediately with a helpful message if not
    if not CLEANED_CSV_PATH.exists():
        logger.error(f"  ✗ Cleaned CSV not found: {CLEANED_CSV_PATH}")
        logger.error("  Have you run Stage 1 first?")
        logger.error("  Run: python scripts/run_cleaning.py")
        sys.exit(1)

    # Load the CSV into a DataFrame
    df = pd.read_csv(CLEANED_CSV_PATH)

    # Validate it is not empty
    if len(df) == 0:
        logger.error("  ✗ Cleaned CSV is empty — Stage 1 may have failed")
        sys.exit(1)

    # Validate all expected columns exist
    # If the schema changed in Stage 1, we want to know immediately
    expected_columns = set(CUSTOMERS_COLUMN_MAP.keys()) | set(CUSTOMER_FEATURES_COLUMN_MAP.keys())
    missing_cols = expected_columns - set(df.columns)
    if missing_cols:
        logger.error(f"  ✗ Missing columns in cleaned CSV: {missing_cols}")
        logger.error("  The CSV may be from an older version of Stage 1.")
        sys.exit(1)

    logger.info(f"  ✓ Loaded {len(df):,} rows × {len(df.columns)} columns")
    logger.info(f"  Churn distribution:")
    logger.info(f"    Retained (0): {(df['Churn'] == 0).sum():,}")
    logger.info(f"    Churned  (1): {(df['Churn'] == 1).sum():,}")
    logger.info(f"  Remaining nulls: {df.isnull().sum().sum()} (must be 0)")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — CONNECT TO SUPABASE
# ─────────────────────────────────────────────────────────────────────────────

def connect_to_database() -> PGConnection:
    """
    Load DATABASE_URL from .env and establish a psycopg2 connection.

    WHY USE A .env FILE?
      The DATABASE_URL contains your Supabase password.
      Hardcoding it in the script means it ends up in git history forever —
      a security risk even if you delete it later.
      The .env file is excluded from git via .gitignore.
      python-dotenv reads it and puts DATABASE_URL into os.environ.

    WHY PORT 6543 (POOLER) AND NOT 5432 (DIRECT)?
      Port 5432 = direct connection. Each request holds a persistent
      connection. Supabase free tier allows ~20 direct connections.
      Render, GitHub Actions, and local scripts would quickly exhaust them.

      Port 6543 = pgBouncer connection pooler. Multiple application
      connections share a small pool of real DB connections efficiently.
      This is how Supabase is designed to be used with cloud deployments.
      Always use 6543 for any non-local connection.

    Returns:
        psycopg2 connection object (autocommit=False by default)

    Raises:
        SystemExit: if DATABASE_URL is missing or connection fails
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 2 — CONNECTING TO SUPABASE")
    logger.info("─" * 60)

    # Load .env file — this populates os.environ with DATABASE_URL
    if ENV_FILE_PATH.exists():
        load_dotenv(ENV_FILE_PATH)
        logger.info(f"  Loaded environment from: {ENV_FILE_PATH}")
    else:
        # Try loading from current directory as fallback
        load_dotenv()
        logger.info("  Loaded environment from current directory .env")

    # Read DATABASE_URL from environment
    db_url = os.environ.get("DATABASE_URL")

    if not db_url:
        logger.error("  ✗ DATABASE_URL is not set")
        logger.error("  Steps to fix:")
        logger.error("    1. Copy .env.example to .env")
        logger.error("    2. Fill in your Supabase connection string (port 6543)")
        logger.error("    3. Re-run this script")
        sys.exit(1)

    # Log a masked version of the URL (hide the password for security)
    # Example URL: postgresql://postgres.abc:PASSWORD@host:6543/postgres
    # We show: postgresql://postgres.abc:****@host:6543/postgres
    masked_url = _mask_password(db_url)
    logger.info(f"  Connecting to: {masked_url}")

    # Validate that port 6543 is used (not 5432)
    if ":5432" in db_url:
        logger.warning("  ⚠ WARNING: You are using port 5432 (direct connection)")
        logger.warning("    Supabase recommends port 6543 (pgBouncer pooler) for")
        logger.warning("    all non-local connections. Update your DATABASE_URL.")
        logger.warning("    Proceeding anyway — but switch to 6543 for production.")

    # Attempt the connection
    try:
        conn = psycopg2.connect(db_url)
        logger.info("  ✓ Connected to Supabase successfully")
        return conn

    except psycopg2.OperationalError as e:
        # OperationalError = cannot reach the server or wrong credentials
        logger.error(f"  ✗ Failed to connect to database: {e}")
        logger.error("  Common causes:")
        logger.error("    - Wrong DATABASE_URL (check Supabase Settings → Database)")
        logger.error("    - Wrong password in the connection string")
        logger.error("    - Network issue (VPN, firewall)")
        logger.error("    - Supabase project is paused (free tier pauses after inactivity)")
        sys.exit(1)


def _mask_password(url: str) -> str:
    """
    Replace the password in a database URL with '****' for safe logging.

    Example:
      Input:  postgresql://postgres.abc:mypassword@host:6543/postgres
      Output: postgresql://postgres.abc:****@host:6543/postgres
    """
    # Find the ':' before the password and the '@' after it
    if "@" in url and ":" in url:
        try:
            # Split on '@' to isolate credentials from host
            credentials, host_part = url.rsplit("@", 1)
            # Split credentials on ':' — last part is the password
            cred_parts = credentials.split(":")
            # Replace everything after the second ':' with '****'
            cred_parts[-1] = "****"
            return ":".join(cred_parts) + "@" + host_part
        except Exception:
            return "postgresql://****:****@****"  # fallback if parsing fails
    return url


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — PRE-FLIGHT CHECKS
# ─────────────────────────────────────────────────────────────────────────────

def run_preflight_checks(
    conn: PGConnection,
    dry_run: bool = False,
) -> bool:
    """
    Verify the database is ready to receive data before inserting anything.

    CHECKS PERFORMED:
      1. All 7 tables exist (schema was applied correctly)
      2. The database is not already seeded (prevents duplicate inserts)

    WHY CHECK FOR EXISTING DATA?
      If you run the seeding script twice by mistake, you would insert
      5,630 duplicate customers. The DB has a UNIQUE constraint on
      kaggle_customer_id, so the second run would fail mid-way through
      with a constraint violation error.

      Better to detect this early and warn clearly:
      "Database already has data. Use --reset to wipe and re-seed."

    Args:
        conn:    active psycopg2 connection
        dry_run: if True, skip the already-seeded check (for testing)

    Returns:
        bool: True = all checks passed, safe to proceed
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 3 — PRE-FLIGHT CHECKS")
    logger.info("─" * 60)

    # Use a cursor — a cursor is the object that executes SQL queries
    # 'with' ensures it is closed after the block even if an error occurs
    with conn.cursor() as cur:

        # ── Check 1: All 7 tables exist ───────────────────────────────────────
        logger.info("  Check 1: Verifying all 7 tables exist...")

        # Query PostgreSQL's information_schema to list all tables
        # information_schema.tables is a system view — it lists every table
        # in every schema. We filter to 'public' (the default schema).
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)

        # Fetch all results and build a set of table names for fast lookup
        existing_tables = {row[0] for row in cur.fetchall()}

        # Check every expected table is present
        missing_tables = [t for t in ALL_TABLES if t not in existing_tables]

        if missing_tables:
            logger.error(f"  ✗ Missing tables: {missing_tables}")
            logger.error("  The schema has not been applied, or was applied incompletely.")
            logger.error("  Steps to fix:")
            logger.error("    1. Open Supabase → SQL Editor")
            logger.error("    2. Paste the contents of database/schema.sql")
            logger.error("    3. Click Run")
            logger.error("    4. Re-run this seeding script")
            return False  # signal failure — do not proceed

        logger.info(f"  ✓ All {len(ALL_TABLES)} tables found: {sorted(existing_tables)}")

        # ── Check 2: Not already seeded ───────────────────────────────────────
        if not dry_run:
            logger.info("")
            logger.info("  Check 2: Checking if database already has data...")

            # Count rows in the customers table
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_CUSTOMERS};")
            customer_count = cur.fetchone()[0]  # fetchone() gets one row, [0] gets the count

            if customer_count > 0:
                logger.warning(f"  ⚠ Database already has {customer_count:,} customers")
                logger.warning("  To re-seed: run with --reset flag to wipe data first")
                logger.warning("  Example: python scripts/run_seeding.py --reset")
                return False  # signal: already seeded, abort

            logger.info("  ✓ Database is empty — safe to proceed with seeding")
        else:
            logger.info("  [DRY RUN] Skipping already-seeded check")

    return True   # all checks passed


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — INSERT MODEL VERSION PLACEHOLDER
# ─────────────────────────────────────────────────────────────────────────────

def insert_model_version_placeholder(
    conn: PGConnection,
    dry_run: bool = False,
) -> None:
    """
    Insert a 'kaggle_baseline' row into the model_versions table.

    WHY IS THIS NEEDED?
      The predictions table stores model_version = 'kaggle_baseline' for
      all seeded predictions. The model_versions table is the registry of
      all known model versions. Without this placeholder row:
        - The predictions table references a model version that does not
          formally exist in the registry
        - The dashboard "current model" card would show nothing
        - The seeding script would need special-case logic to skip FK checks

      The placeholder makes the data consistent: kaggle_baseline is a
      legitimate (if unusual) model version — it is the ground truth
      baseline against which all real models will be compared.

    Args:
        conn:    active psycopg2 connection
        dry_run: if True, log the action but do not execute
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 4 — INSERTING MODEL VERSION PLACEHOLDER")
    logger.info("─" * 60)
    logger.info(f"  Version: '{KAGGLE_BASELINE_VERSION}'")
    logger.info("  This is NOT a real trained model — it is the Kaggle label baseline.")
    logger.info("  It exists so the predictions table has a valid model_version reference.")

    if dry_run:
        logger.info("  [DRY RUN] Would insert: model_versions row for 'kaggle_baseline'")
        return

    with conn.cursor() as cur:
        # INSERT with ON CONFLICT DO NOTHING:
        # If this script is run again (e.g. after a partial failure and --reset),
        # the second INSERT for 'kaggle_baseline' would fail with a UNIQUE violation
        # because 'version' is UNIQUE in the model_versions table.
        # ON CONFLICT DO NOTHING silently skips the insert if the row already exists.
        cur.execute(f"""
            INSERT INTO {TABLE_MODEL_VERSIONS} (
                version,
                status,
                algorithm,
                decision_threshold,
                promotion_notes,
                train_date
            ) VALUES (
                %(version)s,
                'archived',           -- kaggle baseline is archived from the start — it is not a live model
                %(algorithm)s,
                %(threshold)s,
                %(notes)s,
                CURRENT_DATE
            )
            ON CONFLICT (version) DO NOTHING;   -- safe to re-run without error
        """, {
            "version":   KAGGLE_BASELINE_VERSION,
            "algorithm": KAGGLE_BASELINE_ALGORITHM,
            "threshold": KAGGLE_BASELINE_THRESHOLD,
            "notes":     KAGGLE_BASELINE_NOTE,
        })

    # Commit this single row before proceeding to bulk inserts
    # If the bulk inserts fail later, this row stays — that is fine
    conn.commit()
    logger.info("  ✓ model_versions placeholder inserted and committed")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — BUILD ROWS FOR EACH TABLE
#
# WHY SEPARATE BUILD FUNCTIONS FROM INSERT FUNCTIONS?
#   The build functions transform DataFrame rows into Python dicts.
#   The insert function takes any list of dicts and inserts them.
#   This separation means:
#   - Build functions are testable without a database connection
#   - The insert function is reusable for any table
#   - Each function does exactly one thing
# ─────────────────────────────────────────────────────────────────────────────

def build_customers_rows(df: pd.DataFrame) -> list[dict]:
    """
    Transform the cleaned DataFrame into a list of dicts ready for
    INSERT into the customers table.

    KEY DECISIONS:
      - A new UUID is generated in Python for each row using uuid.uuid4().
        Why Python and not PostgreSQL's gen_random_uuid()?
        Because we need the UUID immediately after inserting customers,
        to use as customer_id in customer_features and predictions rows.
        If PostgreSQL generated it, we would need a RETURNING clause and
        a second round-trip to get it back. Generating in Python means
        we have the UUID before the INSERT and can use it everywhere.

      - registered_at is backdated using Tenure:
        registered_at = NOW() - (tenure_months × 30 days)
        This means the daily tenure-recomputation cron will reproduce
        the correct tenure value from registered_at.

      - Complain (0/1 integer in CSV) stays as int here — the customers
        table does not have a Complain column. Complain goes to customer_features.

    Args:
        df: the cleaned DataFrame (5,630 rows × 20 columns)

    Returns:
        list of dicts, each matching the customers table INSERT columns
        Also returns the UUID assigned to each row (keyed by kaggle_customer_id)
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 5a — BUILDING customers ROWS")
    logger.info("─" * 60)
    logger.info(f"  Mapping {len(df):,} CSV rows → customers table format")
    logger.info("  Generating UUIDs in Python (needed for FK in features + predictions)")
    logger.info("  Backdating registered_at from Tenure (tenure × 30 days before now)")

    rows = []          # final list of dicts to insert
    now = datetime.now(timezone.utc)  # current UTC timestamp, computed once for all rows

    for _, csv_row in df.iterrows():
        # Generate a UUID for this customer in Python — not in PostgreSQL
        # This UUID will be reused as customer_id in customer_features and predictions
        customer_uuid = str(uuid.uuid4())

        # Backdate registered_at based on Tenure.
        # If Tenure = 9 months, registered_at = now - (9 × 30 days) = 270 days ago.
        # This means when the daily cron recomputes Tenure tomorrow, it will
        # compute (tomorrow - registered_at) ≈ 9.03 months ≈ 9 months. Consistent.
        tenure_months = float(csv_row["Tenure"])
        registered_at = now - timedelta(days=tenure_months * DAYS_PER_MONTH)

        row = {
            # UUID generated in Python — sent as string, PostgreSQL stores as UUID
            "id":                       customer_uuid,

            # Original Kaggle CustomerID stored for traceability
            "kaggle_customer_id":       int(csv_row["CustomerID"]),

            # Static profile fields — mapped from CSV columns via CUSTOMERS_COLUMN_MAP
            "gender":                   csv_row["Gender"],
            "marital_status":           csv_row["MaritalStatus"],
            "city_tier":                int(csv_row["CityTier"]),
            "preferred_payment_mode":   csv_row["PreferredPaymentMode"],
            "preferred_login_device":   csv_row["PreferredLoginDevice"],
            "preferred_order_cat":      csv_row["PreferedOrderCat"],  # note: CSV typo preserved

            # The Kaggle churn label — historical reference only, not a live prediction
            "kaggle_churn_label":       int(csv_row["Churn"]),

            # Computed values — not in the CSV
            "is_active":                True,
            "role":                     "customer",
            "registered_at":            registered_at.isoformat(),

            # These are NULL for all Kaggle rows — only live registrations have them
            "email":                    None,
            "full_name":                None,
            "password_hash":            None,
        }
        rows.append(row)

    logger.info(f"  ✓ Built {len(rows):,} customer rows")
    logger.info(f"  Sample (first row):")
    sample = {k: v for k, v in list(rows[0].items())[:8]}  # first 8 fields for brevity
    for k, v in sample.items():
        logger.info(f"    {k}: {v}")

    return rows


def build_customer_features_rows(
    df: pd.DataFrame,
    customers_rows: list[dict],
) -> list[dict]:
    """
    Build rows for the customer_features table.

    KEY DECISIONS:
      - customer_id is taken from customers_rows (the UUID we generated in Python)
        This is why we build customers_rows first and pass them in here.
        The order matters: customers must be inserted before customer_features
        because of the FOREIGN KEY constraint.

      - Complain is stored as a boolean in the DB (not 0/1 integer).
        The CSV has 0 and 1. We convert: 0 → False, 1 → True.
        PostgreSQL enforces BOOLEAN type — inserting an integer would work
        but is semantically imprecise. False/True is more honest.

      - All float columns use Python float() cast to ensure psycopg2
        does not confuse numpy.float64 with Python float.
        psycopg2 handles Python native types reliably. numpy types can
        sometimes produce unexpected behaviour with parameter binding.

    Args:
        df:              the cleaned DataFrame
        customers_rows:  the built customers rows (to extract UUIDs)

    Returns:
        list of dicts, each matching the customer_features table INSERT columns
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 5b — BUILDING customer_features ROWS")
    logger.info("─" * 60)
    logger.info(f"  Mapping {len(df):,} CSV rows → customer_features table format")
    logger.info("  Converting Complain (0/1 int) → boolean (False/True)")
    logger.info("  Casting all numeric values to Python native types (not numpy types)")

    now = datetime.now(timezone.utc)  # timestamp for features_computed_at

    rows = []

    for i, (_, csv_row) in enumerate(df.iterrows()):
        # Get the UUID we generated for this customer in build_customers_rows()
        # The ordering is guaranteed: row i in df → row i in customers_rows
        customer_uuid = customers_rows[i]["id"]

        row = {
            # FK to customers table — the UUID we generated in Python
            "customer_id":                      customer_uuid,

            # Behavioral features — all from the CSV, cast to Python native types
            "tenure_months":                    float(csv_row["Tenure"]),
            "satisfaction_score":               int(csv_row["SatisfactionScore"]),

            # Convert 0/1 integer → Python bool for BOOLEAN column
            # int(csv_row["Complain"]) == 1 evaluates to True or False
            "complain":                         bool(int(csv_row["Complain"])),

            "warehouse_to_home":                float(csv_row["WarehouseToHome"]),
            "number_of_address":                int(csv_row["NumberOfAddress"]),
            "hour_spend_on_app":                float(csv_row["HourSpendOnApp"]),
            "number_of_device_registered":      int(csv_row["NumberOfDeviceRegistered"]),
            "day_since_last_order":             int(csv_row["DaySinceLastOrder"]),
            "order_count":                      int(csv_row["OrderCount"]),

            # This column CAN be negative — do not clamp or abs() it
            # A negative value means the customer spent less than last year
            "order_amount_hike_from_last_year": float(csv_row["OrderAmountHikeFromlastYear"]),

            "coupon_used":                      int(csv_row["CouponUsed"]),
            "cashback_amount":                  float(csv_row["CashbackAmount"]),

            # Metadata — computed at insert time
            "features_computed_at":             now.isoformat(),
            "features_source":                  "kaggle_seed",  # all seeded rows use this source
        }
        rows.append(row)

    logger.info(f"  ✓ Built {len(rows):,} customer_features rows")
    logger.info(f"  Complain=True count: {sum(1 for r in rows if r['complain']):,}")
    logger.info(f"  Complain=False count: {sum(1 for r in rows if not r['complain']):,}")

    return rows


def build_predictions_rows(
    df: pd.DataFrame,
    customers_rows: list[dict],
) -> list[dict]:
    """
    Build kaggle baseline prediction rows for the predictions table.

    WHY SEED PREDICTIONS FROM KAGGLE LABELS?
      From day one, the system has 5,630 labeled predictions it can
      evaluate against. This gives you:
        - Immediate model evaluation without waiting for live data
        - A baseline to compare future model versions against
        - Confirmed churners (948 rows) to tune your threshold against

    HOW WE CONSTRUCT THE PROBABILITY:
      The Kaggle dataset has binary labels (0 or 1) — not probabilities.
      For the baseline predictions:
        - Churn = 1 (churned)  → churn_probability = 1.0
        - Churn = 0 (retained) → churn_probability = 0.0

      This is deliberately extreme — it is NOT a calibrated probability.
      It represents "we know with certainty what happened" for the training
      data. Real model probabilities will be continuous (e.g. 0.73).

      The risk tier is derived from this probability using get_risk_tier():
        - 1.0 → HIGH (≥ 0.70)
        - 0.0 → LOW  (< 0.45)

    Args:
        df:              the cleaned DataFrame
        customers_rows:  built customer rows (to extract UUIDs)

    Returns:
        list of dicts, each matching the predictions table INSERT columns
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 5c — BUILDING predictions ROWS (kaggle baseline)")
    logger.info("─" * 60)
    logger.info("  Kaggle labels → churn_probability: Churn=1 → 1.0,  Churn=0 → 0.0")
    logger.info("  ground_truth filled immediately from kaggle_churn_label")
    logger.info("  ground_truth_source = 'kaggle' for all seeded predictions")

    now = datetime.now(timezone.utc)  # predicted_at timestamp
    rows = []

    for i, (_, csv_row) in enumerate(df.iterrows()):
        # Get the UUID we generated for this customer
        customer_uuid = customers_rows[i]["id"]

        # Binary churn label from Kaggle (0 or 1)
        churn_int = int(csv_row["Churn"])

        # Convert binary label to a proxy probability:
        #   1 (churned)  → 1.0  (certain churn — known fact)
        #   0 (retained) → 0.0  (certain retention — known fact)
        churn_probability = float(churn_int)

        # Boolean churn label (True = predicted to churn)
        churn_label = bool(churn_int)

        # Derive risk tier from the probability using the shared config function
        risk_tier = get_risk_tier(churn_probability)

        row = {
            # FK to customers table
            "customer_id":          customer_uuid,

            # batch_run_id = NULL for kaggle seed predictions
            # (there is no batch run — this was a one-time seeding operation)
            "batch_run_id":         None,

            # Prediction output
            "churn_probability":    churn_probability,
            "churn_label":          churn_label,
            "risk_tier":            risk_tier,
            "threshold_used":       KAGGLE_BASELINE_THRESHOLD,
            "model_version":        KAGGLE_BASELINE_VERSION,

            # SHAP reasons: NULL for baseline predictions — we have no SHAP values
            # for labels that came from a dataset rather than a model
            "shap_top_reasons":     None,

            # Feature snapshot: empty for baseline — features are in customer_features table
            "features_snapshot":    "{}",  # PostgreSQL JSONB — empty JSON object

            # Ground truth — filled immediately because we KNOW what happened (Kaggle told us)
            "ground_truth":         churn_label,   # same as churn_label for kaggle rows
            "ground_truth_source":  "kaggle",
            "labeled_at":           now.isoformat(),

            # Metadata
            "prediction_type":      "batch",
            "latency_ms":           None,    # no real inference latency for seeded rows
            "predicted_at":         now.isoformat(),
        }
        rows.append(row)

    churned_count  = sum(1 for r in rows if r["churn_label"])
    retained_count = sum(1 for r in rows if not r["churn_label"])
    logger.info(f"  ✓ Built {len(rows):,} prediction rows")
    logger.info(f"    HIGH risk (churned):  {churned_count:,}")
    logger.info(f"    LOW risk (retained):  {retained_count:,}")

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — INSERT INTO DATABASE (CHUNKED BATCH INSERTS)
# ─────────────────────────────────────────────────────────────────────────────

def insert_rows_in_chunks(
    conn: PGConnection,
    table_name: str,
    rows: list[dict],
    dry_run: bool = False,
) -> int:
    """
    Insert a list of row dicts into a database table in chunks.

    WHY CHUNKS?
      Inserting 5,630 rows in a single INSERT would create one massive
      database transaction. If it fails at row 5,000, we lose everything
      and must restart from zero.

      Inserting in chunks of CHUNK_SIZE (500) means:
        - We commit after every chunk → only re-do the failed chunk
        - We log progress every 500 rows → you can see it working
        - Memory usage is bounded → the full list is not held in one query
        - Supabase free tier connections stay healthy

    WHY psycopg2.extras.execute_values()?
      This is much faster than calling cur.execute() in a loop.
      execute_values() constructs one INSERT statement with multiple
      value rows:
        INSERT INTO table (col1, col2) VALUES (v1, v2), (v3, v4), ...
      This is 10-50x faster than individual INSERT statements.

    WHY RETRY ON FAILURE?
      Transient network errors or Supabase connection timeouts can cause
      a chunk to fail even though the data is valid. Retrying 3 times
      handles these without requiring manual intervention.

    Args:
        conn:       active psycopg2 connection
        table_name: target table name (e.g. 'customers')
        rows:       list of dicts — each dict is one row to insert
        dry_run:    if True, log what would happen but do not insert

    Returns:
        int: total number of rows successfully inserted
    """
    if not rows:
        logger.warning(f"  No rows to insert into {table_name} — skipping")
        return 0

    if dry_run:
        logger.info(f"  [DRY RUN] Would insert {len(rows):,} rows into {table_name}")
        logger.info(f"  [DRY RUN] Column names: {list(rows[0].keys())}")
        return 0

    # Extract column names from the first row dict
    # All rows have the same keys — built by the build_*_rows() functions above
    columns = list(rows[0].keys())

    # Build the INSERT SQL template:
    # INSERT INTO customers (id, gender, ...) VALUES %s
    # The %s placeholder is filled by execute_values() with each row's values
    insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT DO NOTHING;
    """
    # ON CONFLICT DO NOTHING: if a row already exists (e.g. re-running after partial failure),
    # skip it silently rather than raising a UNIQUE constraint violation error.
    # This makes the insert operation idempotent — safe to run twice.

    total_inserted = 0     # running count of successfully inserted rows
    total_chunks = (len(rows) + CHUNK_SIZE - 1) // CHUNK_SIZE  # ceiling division

    logger.info(f"  Inserting {len(rows):,} rows into '{table_name}'")
    logger.info(f"  Chunk size: {CHUNK_SIZE} → {total_chunks} chunks")

    # Split the full rows list into chunks of CHUNK_SIZE
    for chunk_index in range(0, len(rows), CHUNK_SIZE):

        # Slice the rows list to get this chunk
        chunk = rows[chunk_index: chunk_index + CHUNK_SIZE]

        # Convert each dict to a tuple of values in the same column order
        # execute_values() expects a list of tuples, not a list of dicts
        values = [tuple(row[col] for col in columns) for row in chunk]

        chunk_num = (chunk_index // CHUNK_SIZE) + 1   # human-readable chunk number (1-indexed)

        # Attempt the insert with retries for transient failures
        for attempt in range(1, MAX_CHUNK_RETRIES + 1):
            try:
                with conn.cursor() as cur:
                    # execute_values inserts all rows in one SQL statement
                    psycopg2.extras.execute_values(
                        cur,          # cursor to execute on
                        insert_sql,   # SQL template with %s placeholder
                        values,       # list of value tuples
                        page_size=CHUNK_SIZE,  # how many rows per VALUES clause
                    )

                # Commit after each successful chunk
                # This is the most important line — it saves the chunk permanently
                # If we crash after this commit, this chunk is safe
                conn.commit()

                total_inserted += len(chunk)
                logger.info(
                    f"    Chunk {chunk_num}/{total_chunks}: "
                    f"{len(chunk)} rows inserted "
                    f"[{total_inserted:,}/{len(rows):,} total]"
                )
                break  # success — exit the retry loop

            except psycopg2.Error as e:
                # Database error on this chunk
                logger.error(f"    Chunk {chunk_num} attempt {attempt}/{MAX_CHUNK_RETRIES} failed: {e}")

                # Roll back the failed transaction so the connection is clean
                conn.rollback()

                if attempt < MAX_CHUNK_RETRIES:
                    logger.info(f"    Retrying in {RETRY_DELAY_SECONDS} seconds...")
                    time.sleep(RETRY_DELAY_SECONDS)  # wait before retry
                else:
                    # All retries exhausted — this chunk truly failed
                    logger.error(f"    ✗ Chunk {chunk_num} failed after {MAX_CHUNK_RETRIES} attempts")
                    logger.error(f"    Failed on rows {chunk_index} to {chunk_index + len(chunk)}")
                    raise  # re-raise to let the caller handle the failure

    logger.info(f"  ✓ Inserted {total_inserted:,} rows into '{table_name}'")
    return total_inserted


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — POST-SEEDING VERIFICATION
# ─────────────────────────────────────────────────────────────────────────────

def verify_seeding_results(
    conn: PGConnection,
    expected_customers: int,
) -> bool:
    """
    Query the database after seeding to confirm row counts are correct.

    WHY VERIFY AFTER INSERT?
      ON CONFLICT DO NOTHING means failed inserts are silently skipped.
      Without verification, we could have inserted 5,100 rows out of 5,630
      and never know — the script would exit with "success."

      This step counts rows in each table and compares against expected.
      If counts don't match, it logs exactly what is missing.

    Args:
        conn:               active psycopg2 connection
        expected_customers: how many rows we tried to insert (5,630)

    Returns:
        bool: True = counts match expectations, False = mismatch detected
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 7 — POST-SEEDING VERIFICATION")
    logger.info("─" * 60)

    all_passed = True

    with conn.cursor() as cur:

        # Check customers count
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_CUSTOMERS};")
        customers_count = cur.fetchone()[0]
        status = "✓" if customers_count == expected_customers else "✗"
        logger.info(f"  {status} customers:         {customers_count:>6,} (expected {expected_customers:,})")
        if customers_count != expected_customers:
            all_passed = False

        # Check customer_features count (should match customers 1:1)
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_CUSTOMER_FEATURES};")
        features_count = cur.fetchone()[0]
        status = "✓" if features_count == expected_customers else "✗"
        logger.info(f"  {status} customer_features: {features_count:>6,} (expected {expected_customers:,})")
        if features_count != expected_customers:
            all_passed = False

        # Check predictions count (should also match customers)
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_PREDICTIONS};")
        predictions_count = cur.fetchone()[0]
        status = "✓" if predictions_count == expected_customers else "✗"
        logger.info(f"  {status} predictions:       {predictions_count:>6,} (expected {expected_customers:,})")
        if predictions_count != expected_customers:
            all_passed = False

        # Check model_versions placeholder
        cur.execute(
            f"SELECT COUNT(*) FROM {TABLE_MODEL_VERSIONS} WHERE version = %s;",
            (KAGGLE_BASELINE_VERSION,)
        )
        mv_count = cur.fetchone()[0]
        status = "✓" if mv_count == 1 else "✗"
        logger.info(f"  {status} model_versions:    {mv_count:>6,} (expected 1 — kaggle_baseline)")
        if mv_count != 1:
            all_passed = False

        # Check churn distribution in predictions (948 HIGH, 4682 LOW)
        cur.execute(f"""
            SELECT risk_tier, COUNT(*)
            FROM {TABLE_PREDICTIONS}
            GROUP BY risk_tier
            ORDER BY risk_tier;
        """)
        tier_counts = cur.fetchall()
        logger.info("")
        logger.info("  Risk tier distribution in predictions:")
        for tier, count in tier_counts:
            logger.info(f"    {tier:<12} {count:>6,}")

    if all_passed:
        logger.info("")
        logger.info("  ✓ ALL VERIFICATION CHECKS PASSED")
    else:
        logger.error("")
        logger.error("  ✗ VERIFICATION FAILED — some row counts do not match")
        logger.error("  This may indicate some rows were skipped (ON CONFLICT DO NOTHING)")
        logger.error("  Check the logs above for chunk-level errors")

    return all_passed


# ─────────────────────────────────────────────────────────────────────────────
# RESET HELPER — wipe all seeded data before re-seeding
# ─────────────────────────────────────────────────────────────────────────────

def reset_seeded_data(conn: PGConnection) -> None:
    """
    Delete all seeded data from the database.
    Used when re-seeding from scratch (--reset flag).

    WHY DELETE IN THIS ORDER?
      Foreign key constraints require deleting child rows before parent rows.
      predictions references customers (FK: predictions.customer_id → customers.id)
      customer_features references customers (FK: customer_features.customer_id → customers.id)

      Order: predictions → customer_features → customers → model_versions
      Deleting in any other order raises a FK constraint violation.

    NOTE: This only deletes Kaggle-seeded data (features_source = 'kaggle_seed').
    Live customers registered through the frontend are NOT deleted.
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  RESET — CLEARING EXISTING SEEDED DATA")
    logger.info("─" * 60)
    logger.warning("  ⚠ This will delete all Kaggle-seeded data from the database")
    logger.warning("  Live customers (features_source = 'system') are NOT affected")

    with conn.cursor() as cur:

        # Step 1: Delete predictions for kaggle-seeded customers
        cur.execute(f"""
            DELETE FROM {TABLE_PREDICTIONS}
            WHERE ground_truth_source = 'kaggle';
        """)
        pred_deleted = cur.rowcount  # rowcount = how many rows were deleted
        logger.info(f"  Deleted {pred_deleted:,} rows from predictions")

        # Step 2: Delete customer_features for kaggle-seeded customers
        cur.execute(f"""
            DELETE FROM {TABLE_CUSTOMER_FEATURES}
            WHERE features_source = 'kaggle_seed';
        """)
        feat_deleted = cur.rowcount
        logger.info(f"  Deleted {feat_deleted:,} rows from customer_features")

        # Step 3: Delete kaggle customers (kaggle_customer_id IS NOT NULL)
        cur.execute(f"""
            DELETE FROM {TABLE_CUSTOMERS}
            WHERE kaggle_customer_id IS NOT NULL;
        """)
        cust_deleted = cur.rowcount
        logger.info(f"  Deleted {cust_deleted:,} rows from customers")

        # Step 4: Delete the kaggle_baseline model version placeholder
        cur.execute(f"""
            DELETE FROM {TABLE_MODEL_VERSIONS}
            WHERE version = %s;
        """, (KAGGLE_BASELINE_VERSION,))
        mv_deleted = cur.rowcount
        logger.info(f"  Deleted {mv_deleted:,} rows from model_versions")

    conn.commit()  # commit all deletions as one transaction
    logger.info("  ✓ Reset complete — database is empty and ready for re-seeding")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def run_seeding_pipeline(
    dry_run: bool = False,
    reset: bool = False,
) -> SeedingReport:
    """
    Run all 7 seeding steps in order.
    This is the single function called by the CLI runner (run_seeding.py).

    Args:
        dry_run: if True — log everything but do not write to the database
        reset:   if True — delete existing seeded data before inserting

    Returns:
        SeedingReport with counts and status
    """
    report = SeedingReport(started_at=datetime.now(timezone.utc))

    logger.info("=" * 60)
    logger.info("  STAGE 2 — DATABASE SEEDING")
    logger.info("=" * 60)

    if dry_run:
        logger.info("  MODE: DRY RUN — no data will be written to the database")
    if reset:
        logger.info("  MODE: RESET — existing seeded data will be deleted first")

    conn = None  # initialise to None so finally block can check safely

    try:
        # Step 1: Load CSV
        df = load_cleaned_data()

        # Step 2: Connect
        conn = connect_to_database()

        # Optional reset — delete existing seeded data before re-inserting
        if reset and not dry_run:
            reset_seeded_data(conn)

        # Step 3: Pre-flight checks
        checks_passed = run_preflight_checks(conn, dry_run=dry_run)
        if not checks_passed:
            logger.error("  Pre-flight checks failed — aborting seeding")
            report.errors.append("Pre-flight checks failed")
            return report

        # Step 4: Insert model version placeholder
        insert_model_version_placeholder(conn, dry_run=dry_run)

        # Step 5a: Build customer rows (UUIDs generated here)
        customers_rows = build_customers_rows(df)

        # Step 5b: Build customer_features rows (uses UUIDs from Step 5a)
        features_rows = build_customer_features_rows(df, customers_rows)

        # Step 5c: Build predictions rows (uses UUIDs from Step 5a)
        predictions_rows = build_predictions_rows(df, customers_rows)

        # Step 6: Insert into database
        # ORDER MATTERS: customers first (parent), then features and predictions (children)
        # Inserting children before parents would violate FOREIGN KEY constraints

        logger.info("")
        logger.info("─" * 60)
        logger.info("  STEP 6 — INSERTING INTO DATABASE")
        logger.info("─" * 60)
        logger.info("  Insert order: customers → customer_features → predictions")
        logger.info("  (parent table before child tables — required by FK constraints)")

        report.customers_inserted = insert_rows_in_chunks(
            conn, TABLE_CUSTOMERS, customers_rows, dry_run
        )
        report.customer_features_inserted = insert_rows_in_chunks(
            conn, TABLE_CUSTOMER_FEATURES, features_rows, dry_run
        )
        report.predictions_inserted = insert_rows_in_chunks(
            conn, TABLE_PREDICTIONS, predictions_rows, dry_run
        )

        # Step 7: Verify
        if not dry_run:
            verified = verify_seeding_results(conn, expected_customers=len(df))
            if not verified:
                report.errors.append("Post-seeding verification failed — row count mismatch")

        report.success = len(report.errors) == 0
        report.completed_at = datetime.now(timezone.utc)
        report.print_summary()

        return report

    except Exception as e:
        # Catch any unexpected error — log it and return a failed report
        logger.error(f"  ✗ Unexpected error during seeding: {e}", exc_info=True)
        report.errors.append(str(e))
        report.completed_at = datetime.now(timezone.utc)
        report.print_summary()
        return report

    finally:
        # Always close the database connection — even if an error occurred
        # 'finally' runs regardless of whether try succeeded or except fired
        if conn is not None:
            conn.close()
            logger.info("  Database connection closed")
