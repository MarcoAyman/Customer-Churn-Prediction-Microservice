"""
run_training.py — CLI entry point for Stage 3.3

Trains XGBoost (v1.0.0 → production) then LightGBM (v1.1.0 → staging).

WHY two models in one run:
    Training both gives you a real versioning experience:
    - XGBoost is the production champion (better recall on this dataset)
    - LightGBM is the staging challenger (could be promoted next retraining)
    - Both appear in MLflow for comparison
    - Both appear in Supabase model_versions with different statuses
    - Both appear in HF Hub under different version subfolders

Run from project root:
    python scripts/run_training.py
    python scripts/run_training.py --skip-register   ← train + evaluate only, no HF Hub push

Exit codes:
    0  success — both models trained and registered
    1  unhandled exception
"""
from __future__ import annotations

import argparse
import logging
import sys
import traceback
from datetime import datetime
from pathlib import Path


class TrainingRunner:
    """
    CLI wrapper for the full Stage 3.3 training pipeline.

    Execution order:
        1. XGBoost: tune → train → evaluate → threshold → SHAP → MLflow → register (production)
        2. LightGBM: tune → train → evaluate → threshold → SHAP → MLflow → register (staging)
        3. Print champion comparison
    """

    PROJECT_ROOT = Path(__file__).resolve().parent.parent

    # ── Model version strings ─────────────────────────────────────────────────
    XGB_VERSION  = "v1.0.0"   # XGBoost → production
    LGBM_VERSION = "v1.1.0"   # LightGBM → staging

    def __init__(self) -> None:
        self.args     = self._parse_args()
        self._setup_path()
        self.log_path = self._setup_logging()

    def _parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="Stage 3.3 — Model Training for ChurnGuard"
        )
        parser.add_argument(
            "--skip-register", action="store_true", default=False,
            help="Train and evaluate but skip HF Hub push + Supabase write",
        )
        parser.add_argument(
            "--xgb-only", action="store_true", default=False,
            help="Train XGBoost only (skip LightGBM)",
        )
        return parser.parse_args()

    def _setup_path(self) -> None:
        if str(self.PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(self.PROJECT_ROOT))

    def _setup_logging(self) -> Path:
        log_dir = self.PROJECT_ROOT / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path  = log_dir / f"training_{timestamp}.log"

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

        logging.info("╔" + "═" * 68 + "╗")
        logging.info("║  CHURNGUARD — STAGE 3.3 — MODEL TRAINING" + " " * 26 + "║")
        logging.info(f"║  XGBoost={self.XGB_VERSION} (production) · LightGBM={self.LGBM_VERSION} (staging)" + " " * 16 + "║")
        logging.info(f"║  Log → {str(self.log_path):<61}║")
        logging.info("╚" + "═" * 68 + "╝")

        try:
            with DatabaseConnection() as db:
                # ── XGBoost (production) ──────────────────────────────────────
                xgb_results = self._train_model(
                    model_type="xgboost",
                    version=self.XGB_VERSION,
                    status="production",
                    db=db,
                )

                # ── LightGBM (staging) ────────────────────────────────────────
                if not self.args.xgb_only:
                    lgbm_results = self._train_model(
                        model_type="lightgbm",
                        version=self.LGBM_VERSION,
                        status="staging",
                        db=db,
                    )
                    self._print_champion_comparison(xgb_results, lgbm_results)
                else:
                    logging.info("[TrainingRunner] --xgb-only flag set — skipping LightGBM")

            return 0

        except Exception:
            logging.error("[TrainingRunner] Uncaught exception:")
            logging.error(traceback.format_exc())
            return 1

    def _train_model(
        self, model_type: str, version: str, status: str, db
    ) -> dict:
        """
        Full pipeline for one model: tune → train → evaluate → threshold → SHAP → register.

        Input:
            model_type: 'xgboost' or 'lightgbm'
            version:    version string (e.g. 'v1.0.0')
            status:     'production' or 'staging'
            db:         DatabaseConnection

        Output: results dict with metrics + threshold + mlflow_run_id
        """
        import numpy as np
        from src.ml.training.trainer import (
            OptunaTuner, XGBoostTrainer, LightGBMTrainer,
        )
        from src.ml.training.evaluator import (
            ModelEvaluator, ThresholdSelector, SHAPExplainer,
        )
        from src.ml.training.mlflow_tracker import (
            MLflowExperiment, MLflowRunLogger,
        )
        from src.ml.training.register import ModelRegistrar

        algo_label = "XGBoost" if model_type == "xgboost" else "LightGBM"
        staging    = Path("models/staging") / version

        logging.info("")
        logging.info("═" * 70)
        logging.info(f"▸ TRAINING {algo_label} → {version} → status={status}")
        logging.info("═" * 70)

        # ── 1. Trainer setup ─────────────────────────────────────────────────
        if model_type == "xgboost":
            trainer = XGBoostTrainer(version=version)
        else:
            trainer = LightGBMTrainer(version=version)

        X_train, y_train, X_val, y_val = trainer.load_data()

        # ── 2. Optuna hyperparameter tuning ──────────────────────────────────
        logging.info("")
        logging.info("─" * 70)
        logging.info(f"▸ OPTUNA TUNING — {algo_label}")
        logging.info("─" * 70)
        tuner      = OptunaTuner(model_type=model_type)
        best_params = tuner.tune(X_train, y_train)

        # ── 3. Train on full training fold ────────────────────────────────────
        logging.info("")
        logging.info("─" * 70)
        logging.info(f"▸ TRAINING {algo_label} ON FULL TRAIN FOLD")
        logging.info("─" * 70)
        model = trainer.train(X_train, y_train, best_params)
        trainer.save_model()
        manifest = trainer.save_run_manifest(best_params, X_train)

        # XGBoost also saves reference_distribution for drift monitoring
        if model_type == "xgboost":
            logging.info("")
            logging.info("─" * 70)
            logging.info("▸ SAVING REFERENCE DISTRIBUTION (for PSI drift monitoring)")
            logging.info("─" * 70)
            trainer.save_reference_distribution(X_train)

        # ── 4. Threshold selection ────────────────────────────────────────────
        logging.info("")
        logging.info("─" * 70)
        logging.info("▸ THRESHOLD SELECTION FROM PR CURVE (val set)")
        logging.info("─" * 70)
        selector  = ThresholdSelector(staging_dir=staging)
        threshold = selector.select(model, X_val, y_val)

        # ── 5. Evaluate on val + test ─────────────────────────────────────────
        logging.info("")
        logging.info("─" * 70)
        logging.info("▸ EVALUATION — val + test sets")
        logging.info("─" * 70)
        evaluator = ModelEvaluator(staging_dir=staging)
        metrics   = evaluator.evaluate(model, X_val, y_val, threshold, version)

        # ── 6. SHAP ───────────────────────────────────────────────────────────
        logging.info("")
        logging.info("─" * 70)
        logging.info("▸ SHAP FEATURE IMPORTANCE")
        logging.info("─" * 70)
        shap_exp = SHAPExplainer(staging_dir=staging)
        shap_summary = shap_exp.explain(model, trainer.feature_names)

        # ── 7. MLflow tracking ────────────────────────────────────────────────
        logging.info("")
        logging.info("─" * 70)
        logging.info(f"▸ MLFLOW LOGGING — run_name={algo_label}_{version}")
        logging.info("─" * 70)
        experiment   = MLflowExperiment().get_or_create()
        run_id = None

        with MLflowRunLogger(
            experiment_id=experiment.experiment_id,
            run_name=f"{algo_label}_{version}",
            tags={"algorithm": algo_label, "version": version, "status": status},
        ) as run_logger:
            run_logger.log_params(best_params)
            run_logger.log_metrics(metrics["test"])
            run_logger.log_metrics(
                {f"val_{k}": v for k, v in metrics["val"].items()
                 if isinstance(v, (int, float)) and k not in ("set", "n_samples")}
            )
            run_logger.log_artifact(staging / "model.pkl")
            run_logger.log_artifact(staging / "metrics.json")
            run_logger.log_artifact(staging / "thresholds.yaml")
            run_logger.log_artifact(staging / "shap_summary.json")
            run_logger.log_artifact(staging / "run_manifest.json")
            run_logger.log_dict(best_params, "best_hyperparameters.json")
            run_id = run_logger.run_id

        # ── 8. Register ───────────────────────────────────────────────────────
        if not self.args.skip_register:
            logging.info("")
            logging.info("─" * 70)
            logging.info(f"▸ REGISTERING {algo_label} → HF Hub + Supabase")
            logging.info("─" * 70)
            registrar = ModelRegistrar(
                db=db,
                staging_dir=staging,
            )
            reg_result = registrar.register(
                model         = model,
                metrics       = metrics,
                threshold     = threshold,
                algorithm     = algo_label,
                mlflow_run_id = run_id or "skipped",
                status        = status,
                feature_names = trainer.feature_names,
            )
        else:
            logging.info("[TrainingRunner] --skip-register — skipping HF Hub + Supabase")
            reg_result = {"artifact_path": "skipped", "status": status}

        return {
            "version":     version,
            "algorithm":   algo_label,
            "status":      status,
            "metrics":     metrics,
            "threshold":   threshold,
            "mlflow_run":  run_id,
            "shap_top3":   shap_summary.get("top3_features", []),
            "reg_result":  reg_result,
        }

    def _print_champion_comparison(self, xgb: dict, lgbm: dict) -> None:
        """Print a side-by-side comparison of XGBoost vs LightGBM to console."""
        logging.info("")
        logging.info("═" * 70)
        logging.info("▸ CHAMPION COMPARISON: XGBoost (production) vs LightGBM (staging)")
        logging.info("═" * 70)

        xgb_t  = xgb["metrics"]["test"]
        lgbm_t = lgbm["metrics"]["test"]

        metrics_to_compare = [
            ("AUC-ROC",    "auc_roc"),
            ("PR-AUC",     "pr_auc"),
            ("Recall",     "recall"),
            ("Precision",  "precision"),
            ("F1",         "f1"),
            ("Brier",      "brier_score"),
        ]

        logging.info(f"{'Metric':<18} {'XGBoost v1.0.0':>16} {'LightGBM v1.1.0':>16} {'Winner':>8}")
        logging.info("─" * 62)
        for label, key in metrics_to_compare:
            xv  = xgb_t.get(key, 0)
            lv  = lgbm_t.get(key, 0)
            # Lower is better for Brier score
            if key == "brier_score":
                winner = "XGBoost" if xv <= lv else "LightGBM"
            else:
                winner = "XGBoost" if xv >= lv else "LightGBM"
            logging.info(f"{label:<18} {xv:>16.4f} {lv:>16.4f} {winner:>8}")

        logging.info("─" * 62)
        logging.info(f"{'Threshold':<18} {xgb['threshold']:>16.4f} {lgbm['threshold']:>16.4f}")
        logging.info(f"{'Status':<18} {'production':>16} {'staging':>16}")
        logging.info("")
        logging.info(f"XGBoost SHAP top-3: {xgb['shap_top3']}")
        logging.info(f"LightGBM SHAP top-3: {lgbm['shap_top3']}")
        logging.info("")
        logging.info("Production model: XGBoost v1.0.0")
        logging.info("To promote LightGBM: update model_versions SET status='production' WHERE version='v1.1.0'")
        logging.info("Then restart Render service to hot-swap the model.")
        logging.info("═" * 70)


if __name__ == "__main__":
    sys.exit(TrainingRunner().run())
