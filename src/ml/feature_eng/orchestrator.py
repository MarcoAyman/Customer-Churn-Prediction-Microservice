"""
orchestrator.py — Stage 3.2: Feature Engineering Orchestrator

Classes:
    FeatureEngReporter   — saves reports + copies to reports repo
    FeatureEngPipeline   — main entry point, wires all Stage 3.2 classes together

Execution order inside FeatureEngPipeline.run():
    1. Load data from Supabase (reuses TrainingDataLoader from Stage 3.1)
    2. StratifiedSplitter → train / val / test raw DataFrames
    3. FeatureEngineer.fit(train) → learn encoding rates + thresholds
    4. FeatureEngineer.transform(train/val/test) → engineered feature matrices
    5. PreprocessorBuilder.fit(X_train) → fit scaler + OHE on train only
    6. PreprocessorBuilder.transform(all three) → final numpy arrays
    7. Save splits as parquet + artifacts as pkl/json
    8. FeatureEngReporter.save_reports() → JSON reports
    9. FeatureEngReporter.sync_to_reports_repo() → copy + git push
"""
from __future__ import annotations

import json
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

from src.ml.eda.utils.signal_logger import SignalLogger
from src.ml.feature_eng.engineer import FeatureEngineer, ENGINEERED_FEATURES
from src.ml.feature_eng.pipeline import StratifiedSplitter, PreprocessorBuilder


class FeatureEngReporter(SignalLogger):
    """
    Saves feature engineering reports as JSON and syncs to the reports repo.

    Input:  results dict from FeatureEngPipeline.run()
    Output: JSON files in local reports dir + git push to reports repo

    Reports produced:
        feature_summary.json  — what features exist, their source, EDA justification
        split_summary.json    — train/val/test sizes and churn rates
        (feature_engineering_config.json is saved directly by FeatureEngineer.save_config)
    """

    def __init__(
        self,
        local_reports_dir: str | Path,
        reports_repo_dir: str | Path,
    ) -> None:
        super().__init__()
        self.local_dir = Path(local_reports_dir)
        self.repo_dir  = Path(reports_repo_dir) / "feature_eng_reports"
        self.local_dir.mkdir(parents=True, exist_ok=True)

    def save_feature_summary(
        self,
        X_train: np.ndarray,
        feature_names: list,
        fe_config: Dict,
        split_summary: Dict,
    ) -> Dict:
        """
        Build and save feature_summary.json.

        Input:  X_train array, feature names, config and split dicts
        Output: summary dict written to feature_summary.json
        """
        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "split": split_summary,
            "features": {
                "total_after_preprocessing":  int(X_train.shape[1]),
                "total_feature_names":        len(feature_names),
                "feature_names":              feature_names,
            },
            "engineering_decisions": {
                "ordinal_encoded": {
                    k: v
                    for k, v in fe_config.get("encoding_decisions", {})
                    .get("ordinal_encoded", {}).items()
                },
                "one_hot_encoded": fe_config.get("encoding_decisions", {})
                                            .get("one_hot_encoded", {}),
                "passthrough": fe_config.get("encoding_decisions", {})
                                        .get("passthrough_numeric", {}),
            },
            "feature_groups": fe_config.get("feature_groups", {}),
            "fitted_thresholds": fe_config.get("fitted_thresholds", {}),
            "engineered_feature_list": ENGINEERED_FEATURES,
        }

        output_path = self.local_dir / "feature_summary.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        self._signal("Writing to", str(output_path), "feature_summary.json")
        return summary

    def sync_to_reports_repo(self) -> bool:
        """
        Copy local feature_eng reports to the reports GitHub repo and push.

        Input:  local reports dir (set in __init__)
        Output: True on success, False on failure

        Steps:
            1. shutil.copytree → copies all JSONs to feature_eng_reports/
            2. git add . → stage all changes
            3. git commit -m "..." → commit with timestamp
            4. git push origin main → push to GitHub
        """
        self._signal("Syncing to", str(self.repo_dir))

        if not self.repo_dir.parent.exists():
            self._warn(
                f"Reports repo not found: {self.repo_dir.parent}\n"
                "               Create the repo first or set correct path."
            )
            return False

        # Copy reports
        shutil.copytree(
            src=self.local_dir,
            dst=self.repo_dir,
            dirs_exist_ok=True,
        )
        n_json = len(list(self.repo_dir.rglob("*.json")))
        self._signal("Writing to", str(self.repo_dir), f"{n_json} JSON files copied")

        # Git push
        repo_root = self.repo_dir.parent
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        commit_msg = f"feature engineering reports updated — {timestamp}"

        return self._git_push(repo_root, commit_msg)

    def _git_push(self, repo_root: Path, commit_msg: str) -> bool:
        """Run git add / commit / push in the reports repo."""
        steps = [
            (["git", "add", "."],                   "git add ."),
            (["git", "commit", "-m", commit_msg],   "git commit"),
            (["git", "push", "origin", "main"],     "git push origin main"),
        ]
        for cmd, label in steps:
            # Skip commit if nothing changed
            if label == "git commit":
                check = subprocess.run(
                    ["git", "status", "--porcelain"],
                    cwd=repo_root, capture_output=True, text=True,
                )
                if not check.stdout.strip():
                    self._signal("Skipping", "git commit — nothing changed")
                    continue

            result = subprocess.run(
                cmd, cwd=repo_root, capture_output=True, text=True,
            )
            if result.returncode == 0:
                self._signal("✓ Done", label)
            else:
                self._warn(f"{label} failed:\n{result.stderr.strip()}")
                return False

        self._signal("DONE", "reports repo synced to GitHub")
        return True


class FeatureEngPipeline(SignalLogger):
    """
    Main entry point for Stage 3.2 — wires all components together.

    Input:  DatabaseConnection, config paths
    Output: X_train, X_val, X_test numpy arrays + all artifacts saved

    Artifacts produced (in models/artifacts/):
        preprocessor.pkl                  — sklearn Pipeline (scaler + OHE)
        feature_names.json                — ordered output feature list
        feature_engineering_config.json   — all fitted thresholds + encoding rates

    Data splits saved (in data/splits/):
        X_train.npy, X_val.npy, X_test.npy
        y_train.npy, y_val.npy, y_test.npy

    Reports saved and synced:
        feature_summary.json
        split_summary.json
    """

    def __init__(
        self,
        db,
        artifacts_dir: str | Path = "models/artifacts",
        splits_dir:    str | Path = "data/splits",
        local_reports: str | Path = "src/ml/feature_eng/reports",
        reports_repo:  str | Path = (
            "/home/marco-hanna/Desktop/"
            "Customer-Churn-Prediction-Microservice-Reports"
        ),
        model_version: str = "kaggle_baseline",
    ) -> None:
        super().__init__()
        self.db             = db
        self.artifacts_dir  = Path(artifacts_dir)
        self.splits_dir     = Path(splits_dir)
        self.local_reports  = Path(local_reports)
        self.reports_repo   = Path(reports_repo)
        self.model_version  = model_version

        for d in [self.artifacts_dir, self.splits_dir, self.local_reports]:
            d.mkdir(parents=True, exist_ok=True)

    def run(self) -> Dict:
        """
        Execute the full Stage 3.2 pipeline.

        Input:  DatabaseConnection (set in __init__)
        Output: dict with X_train, X_val, X_test, y_train, y_val, y_test + paths

        Execution order:
            1. Load from Supabase
            2. Split (stratified 70/15/15)
            3. Fit FeatureEngineer on train
            4. Transform all three folds
            5. Fit PreprocessorBuilder on X_train_engineered
            6. Transform all three → final numpy arrays
            7. Save splits + artifacts
            8. Save reports + sync to GitHub
        """
        from database.connection import DatabaseConnection
        from src.ml.eda.dataset.loader import TrainingDataLoader

        t_start = time.perf_counter()
        self._banner("STAGE 3.2 — FEATURE ENGINEERING")

        # ── 1. LOAD ───────────────────────────────────────────────────────────
        self._banner("LOAD DATA FROM SUPABASE")
        loader = TrainingDataLoader(self.db, model_version=self.model_version)
        df = loader.load()

        # ── 2. SPLIT ─────────────────────────────────────────────────────────
        self._banner("STRATIFIED SPLIT (70 / 15 / 15)")
        splitter = StratifiedSplitter(target_col="churn")
        df_train, df_val, df_test = splitter.split(df)

        # Extract targets before feature engineering
        y_train = df_train["churn"].values.astype(int)
        y_val   = df_val["churn"].values.astype(int)
        y_test  = df_test["churn"].values.astype(int)

        # ── 3. FEATURE ENGINEERING ───────────────────────────────────────────
        self._banner("FEATURE ENGINEERING (5 GROUPS + TARGET ENCODING)")
        engineer = FeatureEngineer(artifacts_dir=self.artifacts_dir)
        engineer.fit(df_train)

        X_train_fe = engineer.transform(df_train)
        X_val_fe   = engineer.transform(df_val)
        X_test_fe  = engineer.transform(df_test)

        self._signal(
            "Feature shape",
            f"X_train {X_train_fe.shape} | X_val {X_val_fe.shape} | X_test {X_test_fe.shape}",
        )

        # ── 4. PREPROCESSING ─────────────────────────────────────────────────
        self._banner("PREPROCESSING (SCALE + ONE-HOT) — FIT ON TRAIN ONLY")
        preprocessor = PreprocessorBuilder(artifacts_dir=self.artifacts_dir)
        preprocessor.fit(X_train_fe)

        X_train = preprocessor.transform(X_train_fe)
        X_val   = preprocessor.transform(X_val_fe)
        X_test  = preprocessor.transform(X_test_fe)

        self._signal(
            "Final shape",
            f"X_train {X_train.shape} | X_val {X_val.shape} | X_test {X_test.shape}",
            f"{X_train.shape[1]} features total",
        )

        # ── 5. SAVE ARTIFACTS ─────────────────────────────────────────────────
        self._banner("SAVING ARTIFACTS")
        fe_config = engineer.save_config(
            self.artifacts_dir / "feature_engineering_config.json"
        )
        preprocessor.save()
        split_summary = splitter.save_split_summary(
            df_train, df_val, df_test,
            self.local_reports / "split_summary.json",
        )

        # ── 6. SAVE SPLITS ────────────────────────────────────────────────────
        self._banner("SAVING SPLITS TO DISK")
        self._save_splits(X_train, X_val, X_test, y_train, y_val, y_test)

        # ── 7. REPORTS ────────────────────────────────────────────────────────
        self._banner("SAVING REPORTS")
        reporter = FeatureEngReporter(
            local_reports_dir=self.local_reports,
            reports_repo_dir=self.reports_repo,
        )
        feature_summary = reporter.save_feature_summary(
            X_train,
            preprocessor.feature_names,
            fe_config,
            split_summary,
        )
        # Also copy feature_engineering_config.json to reports dir
        shutil.copy(
            self.artifacts_dir / "feature_engineering_config.json",
            self.local_reports / "feature_engineering_config.json",
        )

        # ── 8. SYNC TO GITHUB ─────────────────────────────────────────────────
        self._banner("SYNCING TO REPORTS REPO + GIT PUSH")
        reporter.sync_to_reports_repo()

        # ── DONE ──────────────────────────────────────────────────────────────
        elapsed = time.perf_counter() - t_start
        self._banner("STAGE 3.2 COMPLETE")
        self._signal(
            "DONE",
            f"{X_train.shape[1]} features | "
            f"train={len(X_train):,} | val={len(X_val):,} | test={len(X_test):,}",
            f"{elapsed:.1f}s",
        )

        return {
            "X_train": X_train, "X_val": X_val, "X_test": X_test,
            "y_train": y_train, "y_val": y_val, "y_test": y_test,
            "feature_names": preprocessor.feature_names,
            "feature_summary": feature_summary,
            "split_summary": split_summary,
        }

    # ── private helpers ──────────────────────────────────────────────────────

    def _save_splits(
        self,
        X_train: np.ndarray, X_val: np.ndarray, X_test: np.ndarray,
        y_train: np.ndarray, y_val: np.ndarray, y_test: np.ndarray,
    ) -> None:
        """Save all six arrays as .npy files for fast reloading during training."""
        splits = {
            "X_train": X_train, "X_val": X_val, "X_test": X_test,
            "y_train": y_train, "y_val": y_val, "y_test": y_test,
        }
        for name, arr in splits.items():
            path = self.splits_dir / f"{name}.npy"
            np.save(path, arr)
            self._signal("Writing to", str(path), f"shape={arr.shape}")

    def _banner(self, title: str) -> None:
        self.logger.info("")
        self.logger.info("─" * 70)
        self.logger.info(f"▸ {title}")
        self.logger.info("─" * 70)
