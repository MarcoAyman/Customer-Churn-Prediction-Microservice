"""
run_feature_engineering.py — CLI entry point for Stage 3.2

Run from project root:
    python scripts/run_feature_engineering.py
    python scripts/run_feature_engineering.py --skip-sync

Exit codes:
    0  success
    1  unhandled exception — check traceback in logs/
"""
from __future__ import annotations

import argparse
import logging
import sys
import traceback
from datetime import datetime
from pathlib import Path


class FERunner:
    """
    CLI wrapper for FeatureEngPipeline.
    Handles sys.path, logging setup, DatabaseConnection, and exit codes.
    """

    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    DEFAULT_REPORTS_REPO = (
        Path.home() / "Desktop" / "Customer-Churn-Prediction-Microservice-Reports"
    )

    def __init__(self) -> None:
        self.args = self._parse_args()
        self._setup_path()
        self.log_path = self._setup_logging()

    def _parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="Stage 3.2 — Feature Engineering for ChurnGuard"
        )
        parser.add_argument(
            "--reports-repo", default=str(self.DEFAULT_REPORTS_REPO),
            help="Path to the reports GitHub repo (default: %(default)s)",
        )
        parser.add_argument(
            "--skip-sync", action="store_true", default=False,
            help="Skip copying reports and git push",
        )
        return parser.parse_args()

    def _setup_path(self) -> None:
        if str(self.PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(self.PROJECT_ROOT))

    def _setup_logging(self) -> Path:
        log_dir = self.PROJECT_ROOT / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = log_dir / f"feature_eng_{timestamp}.log"

        root = logging.getLogger()
        root.setLevel(logging.INFO)
        root.handlers.clear()

        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(fmt)
        root.addHandler(console)

        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)

        logging.getLogger("matplotlib").setLevel(logging.WARNING)
        logging.getLogger("numexpr").setLevel(logging.WARNING)

        return log_path

    def run(self) -> int:
        from database.connection import DatabaseConnection
        from src.ml.feature_eng.orchestrator import FeatureEngPipeline

        logging.info("╔" + "═" * 68 + "╗")
        logging.info("║  CHURNGUARD — STAGE 3.2 — FEATURE ENGINEERING" + " " * 21 + "║")
        logging.info(f"║  Log → {str(self.log_path):<61}║")
        logging.info("╚" + "═" * 68 + "╝")

        try:
            with DatabaseConnection() as db:
                pipeline = FeatureEngPipeline(
                    db=db,
                    reports_repo=self.args.reports_repo,
                )

                if self.args.skip_sync:
                    # Monkey-patch sync to no-op when --skip-sync is set
                    from src.ml.feature_eng.orchestrator import FeatureEngReporter
                    FeatureEngReporter.sync_to_reports_repo = lambda self: True
                    logging.info("[FERunner] --skip-sync: git push disabled")

                results = pipeline.run()

            logging.info(
                f"[FERunner] Stage 3.2 complete — "
                f"{results['X_train'].shape[1]} features | "
                f"splits saved to data/splits/"
            )
            return 0

        except Exception:
            logging.error("[FERunner] Uncaught exception:")
            logging.error(traceback.format_exc())
            return 1


if __name__ == "__main__":
    sys.exit(FERunner().run())
