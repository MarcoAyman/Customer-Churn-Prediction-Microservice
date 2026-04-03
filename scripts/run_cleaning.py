"""
scripts/run_cleaning.py
══════════════════════════════════════════════════════════════════════════════
CLI entry point for Stage 1 Data Cleaning.

This script is the ONLY way to trigger the cleaning pipeline from the
command line. It sets up logging (console + log file), then calls the
pipeline orchestrator in src/pipeline/stage1_clean.py.

WHY SEPARATE CLI SCRIPT FROM THE PIPELINE MODULE?
  The pipeline module (stage1_clean.py) is a library — it has functions
  that other modules import and call (the API, tests, notebooks).
  This script is the command-line interface to that library.
  They have separate responsibilities. Mixing them would mean the API
  importing argparse — wrong abstraction.

USAGE:
  python scripts/run_cleaning.py

  From project root (recommended):
  cd churn_prediction && python scripts/run_cleaning.py

OUTPUT:
  data/cleaned/ecommerce_churn_clean.csv   ← clean data ready for DB
  logs/cleaning_YYYY-MM-DD_HH-MM-SS.log   ← full log with timestamps
══════════════════════════════════════════════════════════════════════════════
"""

import logging          # logging library — structured output
import sys              # sys.path manipulation and exit codes
from datetime import datetime  # for timestamped log filenames
from pathlib import Path       # file path handling

# Add the project root to sys.path so we can import from src/ and config/
# This is needed when running as: python scripts/run_cleaning.py
# (Python doesn't automatically include the parent directory in the module path)
PROJECT_ROOT = Path(__file__).parent.parent  # scripts/ → project root
sys.path.insert(0, str(PROJECT_ROOT))        # add to front of module search path

from src.pipeline.stage1_clean import run_cleaning_pipeline  # the actual pipeline


def setup_logging() -> None:
    """
    Configure logging to write to BOTH the console AND a timestamped log file.

    WHY BOTH?
      Console: you see it in real-time while the script runs.
      File:    permanent record. If something goes wrong in production,
               you can go back and read exactly what happened and when.

    WHY TIMESTAMPED FILE NAME?
      You may run cleaning multiple times. Timestamped filenames mean
      each run has its own log — you never overwrite previous logs.
    """
    # Create logs directory if it doesn't exist
    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(exist_ok=True)

    # Build a timestamp string for the log filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = logs_dir / f"cleaning_{timestamp}.log"

    # Log format: timestamp + level + message
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    date_format = "%H:%M:%S"

    # Configure the root logger to write to both destinations
    logging.basicConfig(
        level=logging.INFO,          # INFO and above (WARNING, ERROR, CRITICAL)
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(sys.stdout),           # console output
            logging.FileHandler(log_file, encoding="utf-8"),  # file output
        ],
    )

    logging.info(f"Log file: {log_file}")


def main() -> None:
    """
    Main entry point. Sets up logging, defines paths, runs the pipeline.
    """
    setup_logging()

    # Define input and output paths relative to project root
    input_path  = str(PROJECT_ROOT / "data" / "raw" / "E_Commerce_Dataset.xlsx")
    output_path = str(PROJECT_ROOT / "data" / "cleaned" / "ecommerce_churn_clean.csv")

    logging.info(f"Project root: {PROJECT_ROOT}")
    logging.info(f"Input:  {input_path}")
    logging.info(f"Output: {output_path}")

    # Run the full 9-step cleaning pipeline
    df_clean, report = run_cleaning_pipeline(
        input_path=input_path,
        output_path=output_path,
    )

    # Final confirmation
    logging.info("")
    logging.info("══════════════════════════════════════════════════════════════")
    logging.info("  STAGE 1 COMPLETE")
    logging.info(f"  Clean data: {output_path}")
    logging.info(f"  Rows ready for DB: {len(df_clean):,}")
    logging.info("  Next step: Run scripts/run_seeding.py to inject into Supabase")
    logging.info("══════════════════════════════════════════════════════════════")

    sys.exit(0)  # exit code 0 = success


if __name__ == "__main__":
    main()
