"""
scripts/run_batch_scoring.py — CLI entry point for Stage 3.5 batch scoring.

Execution order:
    1. Load model artifacts from HF Hub
    2. INSERT batch_runs row (status='running')   ← MUST be before predictions
    3. Load eligible customers from Supabase
    4. Score all customers
    5. Write predictions (FK batch_run_id now exists)
    6. PSI drift check
    7. UPDATE batch_runs row (status='completed', fill real counts)

Run from project root:
    python scripts/run_batch_scoring.py
    python scripts/run_batch_scoring.py --skip-drift
    python scripts/run_batch_scoring.py --triggered-by github_actions

Exit codes:
    0  success, no drift
    2  success, drift detected
    1  unhandled exception
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path


class BatchRunner:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent

    def __init__(self) -> None:
        self.args     = self._parse_args()
        self._setup_path()
        self.log_path = self._setup_logging()

    def _parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description="Stage 3.5 — Batch Scoring")
        parser.add_argument("--skip-drift", action="store_true", default=False)
        parser.add_argument("--triggered-by", default="manual",
                            choices=["manual", "github_actions"])
        return parser.parse_args()

    def _setup_path(self) -> None:
        if str(self.PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(self.PROJECT_ROOT))

    def _setup_logging(self) -> Path:
        log_dir = self.PROJECT_ROOT / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path  = log_dir / f"batch_{timestamp}.log"
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
        for noisy in ("matplotlib", "numexpr", "lightgbm", "xgboost"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
        return log_path

    def run(self) -> int:
        from database.connection import DatabaseConnection
        from src.ml.batch.batch_scorer import BatchScorer, DriftMonitor

        batch_run_id = str(uuid.uuid4())
        t_start      = time.perf_counter()

        logging.info("╔" + "═" * 68 + "╗")
        logging.info("║  CHURNGUARD — BATCH SCORING" + " " * 41 + "║")
        logging.info(f"║  batch_run_id={batch_run_id:<55}║")
        logging.info("╚" + "═" * 68 + "╝")

        drift_detected  = False
        max_psi_feature = None
        max_psi_value   = None
        n_eligible = n_scored = n_high = n_medium = n_low = 0
        drift_checked = False

        try:
            with DatabaseConnection() as db:
                scorer = BatchScorer(db=db)

                # ── 1. Load artifacts ─────────────────────────────────────────
                logging.info("\n" + "─" * 70)
                logging.info("▸ LOADING PRODUCTION MODEL ARTIFACTS")
                logging.info("─" * 70)
                scorer.load_artifacts()

                # ── 2. INSERT batch_runs row FIRST ────────────────────────────
                # predictions.batch_run_id is a FK to batch_runs.id
                # The row must exist before any predictions reference it.
                # We insert with status='running' and update to 'completed' at end.
                logging.info("\n" + "─" * 70)
                logging.info("▸ CREATING BATCH RUN RECORD (status=running)")
                logging.info("─" * 70)
                scorer.create_batch_run(
                    batch_run_id = batch_run_id,
                    triggered_by = self.args.triggered_by,
                )

                # ── 3. Load eligible customers ────────────────────────────────
                logging.info("\n" + "─" * 70)
                logging.info("▸ LOADING ELIGIBLE CUSTOMERS FROM SUPABASE")
                logging.info("─" * 70)
                df         = scorer.load_customers()
                n_eligible = len(df)

                if n_eligible == 0:
                    logging.info("No eligible customers — nothing to score.")
                    scorer.complete_batch_run(
                        batch_run_id=batch_run_id, n_eligible=0, n_scored=0,
                        n_high=0, n_medium=0, n_low=0,
                        drift_checked=False, drift_alert=False,
                        max_psi_feature=None, max_psi_value=None,
                        elapsed_s=time.perf_counter() - t_start,
                    )
                    return 0

                # ── 4. Score ──────────────────────────────────────────────────
                logging.info("\n" + "─" * 70)
                logging.info("▸ SCORING ALL ELIGIBLE CUSTOMERS")
                logging.info("─" * 70)
                predictions, X_batch = scorer.score_all(df)
                n_scored = len(predictions)
                n_high   = sum(1 for p in predictions if p["risk_tier"] == "HIGH")
                n_medium = sum(1 for p in predictions if p["risk_tier"] == "MEDIUM")
                n_low    = sum(1 for p in predictions if p["risk_tier"] == "LOW")

                # ── 5. Write predictions ──────────────────────────────────────
                logging.info("\n" + "─" * 70)
                logging.info("▸ WRITING PREDICTIONS TO SUPABASE")
                logging.info("─" * 70)
                scorer.write_predictions(predictions, batch_run_id)

                # ── 6. PSI Drift check ────────────────────────────────────────
                if not self.args.skip_drift and scorer.reference_distribution:
                    logging.info("\n" + "─" * 70)
                    logging.info("▸ PSI DRIFT CHECK")
                    logging.info("─" * 70)
                    feat_names = scorer.reference_distribution.get("feature_names", [])
                    drift_checked = True
                    if feat_names:
                        drift_detected, drift_info = DriftMonitor().check(
                            X_batch=X_batch,
                            ref_dist=scorer.reference_distribution,
                            feature_names=feat_names,
                            db=db,
                            batch_run_id=batch_run_id,
                        )
                        max_psi_feature = drift_info.get("max_psi_feature")
                        max_psi_value   = drift_info.get("max_psi_value")
                    else:
                        logging.warning("No feature_names in reference_distribution — skipping PSI")
                        drift_checked = False
                else:
                    logging.info("Drift check skipped")

                # ── 7. Update batch_runs to completed ─────────────────────────
                logging.info("\n" + "─" * 70)
                logging.info("▸ UPDATING BATCH RUN SUMMARY")
                logging.info("─" * 70)
                scorer.complete_batch_run(
                    batch_run_id    = batch_run_id,
                    n_eligible      = n_eligible,
                    n_scored        = n_scored,
                    n_high          = n_high,
                    n_medium        = n_medium,
                    n_low           = n_low,
                    drift_checked   = drift_checked,
                    drift_alert     = drift_detected,
                    max_psi_feature = max_psi_feature,
                    max_psi_value   = max_psi_value,
                    elapsed_s       = time.perf_counter() - t_start,
                )

            elapsed = time.perf_counter() - t_start
            logging.info("\n" + "═" * 70)
            logging.info("▸ BATCH SCORING COMPLETE")
            logging.info("═" * 70)
            logging.info(f"  batch_run_id:  {batch_run_id}")
            logging.info(f"  model_version: {scorer.model_version}")
            logging.info(f"  eligible:      {n_eligible:,}")
            logging.info(f"  scored:        {n_scored:,}")
            logging.info(f"  HIGH:          {n_high:,}  ({n_high/max(n_scored,1)*100:.1f}%)")
            logging.info(f"  MEDIUM:        {n_medium:,}  ({n_medium/max(n_scored,1)*100:.1f}%)")
            logging.info(f"  LOW:           {n_low:,}  ({n_low/max(n_scored,1)*100:.1f}%)")
            logging.info(f"  drift alert:   {drift_detected}")
            logging.info(f"  elapsed:       {elapsed:.1f}s")
            logging.info("═" * 70)

            return 2 if drift_detected else 0

        except Exception:
            logging.error("[BatchRunner] Uncaught exception:")
            logging.error(traceback.format_exc())
            return 1


if __name__ == "__main__":
    sys.exit(BatchRunner().run())
