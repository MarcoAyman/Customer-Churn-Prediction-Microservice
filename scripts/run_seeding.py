"""
scripts/run_seeding.py
══════════════════════════════════════════════════════════════════════════════
CLI entry point for Stage 2 Database Seeding.

USAGE:
  # Normal run — insert 5,630 Kaggle customers into Supabase:
  python scripts/run_seeding.py

  # Dry run — see what would happen without touching the database:
  python scripts/run_seeding.py --dry-run

  # Reset and re-seed — wipe existing Kaggle data and re-insert:
  python scripts/run_seeding.py --reset

PRE-REQUISITES:
  1. Stage 1 must have run: python scripts/run_cleaning.py
     → produces data/cleaned/ecommerce_churn_clean.csv

  2. Schema must be applied to Supabase:
     → Paste database/schema.sql into Supabase SQL Editor and Run

  3. .env file must exist with DATABASE_URL:
     → Copy .env.example to .env and fill in your Supabase connection string
     → Use port 6543 (pooler), NOT port 5432 (direct)

  4. Dependencies must be installed:
     → pip install psycopg2-binary python-dotenv pandas openpyxl

OUTPUT:
  Supabase database populated with:
    - 5,630 rows in customers
    - 5,630 rows in customer_features
    - 5,630 rows in predictions (kaggle baseline ground truth)
    - 1 row  in model_versions  (kaggle_baseline placeholder)

  logs/seeding_YYYY-MM-DD_HH-MM-SS.log  ← full log file
══════════════════════════════════════════════════════════════════════════════
"""

import argparse         # command-line argument parsing
import logging          # logging configuration
import sys              # sys.path and sys.exit
from datetime import datetime   # for timestamped log filenames
from pathlib import Path        # file path handling

# Add project root to sys.path so imports from src/ and config/ work
# This is needed when running: python scripts/run_seeding.py
# from the project root directory
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.stage2_seed import run_seeding_pipeline


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns argparse.Namespace with:
      .dry_run: bool — log actions but do not write to DB
      .reset:   bool — wipe existing seeded data before re-inserting
    """
    parser = argparse.ArgumentParser(
        description="ChurnGuard Stage 2 — Seed Supabase database from cleaned CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_seeding.py              # normal run
  python scripts/run_seeding.py --dry-run    # preview without writing
  python scripts/run_seeding.py --reset      # wipe and re-seed
        """,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",   # flag — present = True, absent = False
        default=False,
        help=(
            "Log all actions without writing anything to the database. "
            "Use this to verify the script is working before the real run."
        ),
    )

    parser.add_argument(
        "--reset",
        action="store_true",
        default=False,
        help=(
            "Delete all existing Kaggle-seeded data before re-inserting. "
            "Use this if you need to re-seed after fixing a data issue. "
            "WARNING: permanently deletes all kaggle_seed rows."
        ),
    )

    return parser.parse_args()


def setup_logging(dry_run: bool = False) -> None:
    """
    Configure logging to write to both console and a timestamped log file.
    """
    # Create logs directory if it doesn't exist
    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(exist_ok=True)

    # Build timestamped log filename
    # dry_run runs get a '_dryrun' suffix so they are easy to identify
    timestamp  = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    suffix     = "_dryrun" if dry_run else ""
    log_file   = logs_dir / f"seeding_{timestamp}{suffix}.log"

    # Configure root logger
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),                   # console
            logging.FileHandler(log_file, encoding="utf-8"),     # file
        ],
    )

    logging.info(f"Log file: {log_file}")


def main() -> None:
    """Main entry point — parse args, setup logging, run pipeline."""
    args = parse_arguments()
    setup_logging(dry_run=args.dry_run)

    logging.info(f"Project root:  {PROJECT_ROOT}")
    logging.info(f"Dry run:       {args.dry_run}")
    logging.info(f"Reset:         {args.reset}")

    # Run the full seeding pipeline
    report = run_seeding_pipeline(
        dry_run=args.dry_run,
        reset=args.reset,
    )

    # Exit with appropriate code
    # 0 = success (CI/CD treats this as OK)
    # 1 = failure (CI/CD treats this as an error — triggers alert)
    exit_code = 0 if report.success or args.dry_run else 1

    if exit_code == 0:
        logging.info("")
        logging.info("══════════════════════════════════════════════════════════════")
        logging.info("  STAGE 2 COMPLETE")
        logging.info("  Supabase is now populated and ready.")
        logging.info("  Next step: verify in Supabase Table Editor")
        logging.info("  Then: run EDA → python notebooks/01_eda.ipynb")
        logging.info("══════════════════════════════════════════════════════════════")
    else:
        logging.error("  Stage 2 failed — check errors above")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
