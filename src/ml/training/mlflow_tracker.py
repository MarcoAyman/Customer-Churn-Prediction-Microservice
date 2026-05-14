"""
mlflow_tracker.py — Stage 3.3: MLflow Experiment Tracking

Classes:
    MLflowExperiment  — creates/gets the MLflow experiment
    MLflowRunLogger   — logs params, metrics, and artifacts to a single run

WHY MLflow is separate from the trainer:
    MLflow is local experiment tracking only. It records every run so
    you can compare XGBoost vs LightGBM side by side in the MLflow UI.
    It does NOT serve predictions in production — that role belongs to
    HuggingFace Hub (storage) + Supabase model_versions (registry).

MLflow version: 3.12.0

Usage:
    experiment = MLflowExperiment(name="churnguard").get_or_create()
    with MLflowRunLogger(experiment_id=experiment.experiment_id, run_name="xgb_v1.0.0") as run_logger:
        run_logger.log_params(best_params)
        run_logger.log_metrics(metrics["test"])
        run_logger.log_artifact(model_path)
        run_id = run_logger.run_id

To view the UI:
    mlflow ui --port 5000
    open http://localhost:5000
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from src.ml.eda.utils.signal_logger import SignalLogger


class MLflowExperiment(SignalLogger):
    """
    Creates or retrieves an MLflow experiment by name.

    WHY: MLflow experiments group related runs together.
    All ChurnGuard model training runs live in the 'churnguard-churn-prediction'
    experiment so they can be compared side by side in the MLflow UI.

    Input:  experiment name string
    Output: mlflow.entities.Experiment object
    """

    DEFAULT_EXPERIMENT_NAME = "churnguard-churn-prediction"
    DEFAULT_TRACKING_URI    = "mlruns"   # local SQLite tracking server

    def __init__(
        self,
        experiment_name: str  = DEFAULT_EXPERIMENT_NAME,
        tracking_uri:    str  = DEFAULT_TRACKING_URI,
    ) -> None:
        """
        Args:
            experiment_name: MLflow experiment name (groups all ChurnGuard runs)
            tracking_uri:    local path for MLflow tracking server storage
        """
        super().__init__()
        self.experiment_name = experiment_name
        self.tracking_uri    = tracking_uri

    def get_or_create(self):
        """
        Return the MLflow experiment, creating it if it doesn't exist yet.

        Output: mlflow.entities.Experiment
        """
        import mlflow
        mlflow.set_tracking_uri(self.tracking_uri)

        experiment = mlflow.get_experiment_by_name(self.experiment_name)
        if experiment is None:
            experiment_id = mlflow.create_experiment(self.experiment_name)
            experiment    = mlflow.get_experiment(experiment_id)
            self._signal(
                "Created",
                f"MLflow experiment '{self.experiment_name}'",
                f"id={experiment_id} | tracking_uri={self.tracking_uri}",
            )
        else:
            self._signal(
                "Using",
                f"MLflow experiment '{self.experiment_name}'",
                f"id={experiment.experiment_id} | {self.tracking_uri}",
            )

        self._signal(
            "View UI",
            "run: mlflow ui --port 5000",
            "then open http://localhost:5000 to compare XGBoost vs LightGBM",
        )
        return experiment


class MLflowRunLogger(SignalLogger):
    """
    Context-manager wrapper around an MLflow run.

    Logs hyperparameters, metrics, and artifact files to the active run.
    On exit, the run is ended cleanly regardless of success or failure.

    Usage:
        with MLflowRunLogger(experiment_id, run_name="xgb_v1.0.0") as logger:
            logger.log_params({"max_depth": 5, "lr": 0.08})
            logger.log_metrics({"auc_roc": 0.87, "recall": 0.81})
            logger.log_artifact(Path("models/staging/v1.0.0/model.pkl"))
            run_id = logger.run_id   ← store this in model_versions table

    Input:  experiment_id (from MLflowExperiment.get_or_create())
    Output: run_id string — stored in Supabase model_versions.mlflow_run_id
    """

    def __init__(
        self,
        experiment_id: str,
        run_name:      str,
        tags:          Optional[Dict] = None,
    ) -> None:
        """
        Args:
            experiment_id: from MLflowExperiment.get_or_create().experiment_id
            run_name:      human-readable name, e.g. 'xgb_v1.0.0'
            tags:          optional dict of string tags for this run
        """
        super().__init__()
        self.experiment_id = experiment_id
        self.run_name      = run_name
        self.tags          = tags or {}
        self._run          = None
        self._run_id:      Optional[str] = None

    # ── context manager ──────────────────────────────────────────────────────

    def __enter__(self) -> "MLflowRunLogger":
        import mlflow
        self._run = mlflow.start_run(
            experiment_id=self.experiment_id,
            run_name=self.run_name,
            tags=self.tags,
        )
        self._run_id = self._run.info.run_id
        self._signal(
            "Started",
            f"MLflow run '{self.run_name}'",
            f"run_id={self._run_id}",
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        import mlflow
        mlflow.end_run()
        if exc_type is None:
            self._signal("Ended", f"MLflow run '{self.run_name}'", f"run_id={self._run_id}")
        else:
            self._warn(f"Run '{self.run_name}' ended with exception: {exc_val}")
        return False   # do not suppress exceptions

    # ── logging methods ──────────────────────────────────────────────────────

    def log_params(self, params: Dict[str, Any]) -> None:
        """
        Log hyperparameters to MLflow.

        Input:  dict of hyperparameter name → value (from OptunaTuner best_params)
        Effect: appears in the MLflow UI 'Params' tab for this run
        """
        import mlflow

        # MLflow requires string keys and primitive values
        clean = {str(k): str(v) for k, v in params.items()}
        mlflow.log_params(clean)
        self._signal(
            "Logged params",
            f"{len(clean)} hyperparameters",
            " | ".join(f"{k}={v}" for k, v in list(clean.items())[:5]),
        )

    def log_metrics(self, metrics: Dict[str, float], step: Optional[int] = None) -> None:
        """
        Log evaluation metrics to MLflow.

        Input:  dict of metric name → float value (from ModelEvaluator)
        Effect: appears in the MLflow UI 'Metrics' tab for this run
        """
        import mlflow

        numeric = {k: float(v) for k, v in metrics.items()
                   if isinstance(v, (int, float)) and k not in ("set", "n_samples")}
        if step is not None:
            for k, v in numeric.items():
                mlflow.log_metric(k, v, step=step)
        else:
            mlflow.log_metrics(numeric)

        def _fmt(v) -> str:
            # Safe formatter: :.4f for numbers, str() for anything else.
            # Needed because log_metrics is called with both test metrics
            # (keys: auc_roc, recall, precision) AND val-prefixed metrics
            # (keys: val_auc_roc, val_recall...) where .get() returns None.
            return f"{v:.4f}" if isinstance(v, (int, float)) else "n/a"

        self._signal(
            "Logged metrics",
            f"{len(numeric)} metrics",
            f"auc_roc={_fmt(metrics.get('auc_roc'))} "
            f"recall={_fmt(metrics.get('recall'))} "
            f"precision={_fmt(metrics.get('precision'))}",
        )

    def log_artifact(self, path: str | Path, artifact_path: Optional[str] = None) -> None:
        """
        Log a file (pkl, json, yaml) to MLflow artifact storage.

        Input:
            path:          local path to the file to upload
            artifact_path: optional subdirectory inside the MLflow run's artifact store

        Effect: file appears in the MLflow UI 'Artifacts' tab
        WHY: MLflow keeps a local copy of all artifacts per run so experiments
             are self-contained and reproducible.
        """
        import mlflow

        path = Path(path)
        if not path.exists():
            self._warn(f"Artifact not found, skipping: {path}")
            return

        mlflow.log_artifact(str(path), artifact_path=artifact_path)
        size_kb = path.stat().st_size / 1024
        self._signal(
            "Logged artifact",
            str(path),
            f"{size_kb:.1f} KB → MLflow run {self._run_id}",
        )

    def log_dict(self, d: Dict, filename: str) -> None:
        """
        Log a dict directly to MLflow as a JSON artifact.

        Input:  dict + filename (e.g. 'metrics_summary.json')
        Effect: JSON file appears in MLflow artifacts
        """
        import mlflow
        mlflow.log_dict(d, filename)
        self._signal("Logged dict", filename, f"{len(d)} keys → MLflow artifacts")

    @property
    def run_id(self) -> Optional[str]:
        """MLflow run_id — stored in Supabase model_versions.mlflow_run_id."""
        return self._run_id
