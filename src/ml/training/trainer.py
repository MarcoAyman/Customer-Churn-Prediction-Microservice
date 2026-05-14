"""
trainer.py — Stage 3.3: Model Training

Classes:
    OptunaTuner      — 20-trial Bayesian hyperparameter search (shared by both models)
    XGBoostTrainer   — trains XGBoost with tuned params, saves model + reference_distribution
    LightGBMTrainer  — trains LightGBM with tuned params, saves model

Every file read prints: what file, what data, what it is used for.
Every file write prints: what file, what is in it.

Key design decisions:
    - scale_pos_weight computed from y_train.npy (NOT hardcoded)
      because the exact ratio depends on the actual stratified split.
    - Optuna objective: maximize recall, subject to precision >= 0.60.
      This matches the business goal: catch as many churners as possible
      while keeping false-alarm rate acceptable.
    - reference_distribution.pkl is saved by XGBoostTrainer because it must
      reflect the TRAINING distribution. It is used in Day 3 drift monitoring
      (PSI comparison against new batch data).
"""
from __future__ import annotations

import json
import pickle
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np

from src.ml.eda.utils.signal_logger import SignalLogger


class OptunaTuner(SignalLogger):
    """
    Bayesian hyperparameter search using Optuna.

    Shared by both XGBoostTrainer and LightGBMTrainer — accepts a model_type
    argument to switch the search space.

    Input:  X_train, y_train numpy arrays + model_type string
    Output: best_params dict — passed directly to the corresponding Trainer

    Objective: maximize recall @ threshold, subject to precision >= 0.60.
    Uses 5-fold StratifiedKFold on the training set only.
    Val/test sets are NEVER touched during tuning.
    """

    N_TRIALS:        int   = 20      # 20 trials → good balance of speed vs quality
    CV_FOLDS:        int   = 5       # 5-fold stratified CV inside training set
    RANDOM_STATE:    int   = 42
    PRECISION_FLOOR: float = 0.60    # precision must be >= this to accept a recall score
    OPTUNA_SEED:     int   = 42

    def __init__(self, model_type: str, artifacts_dir: str | Path = "models/artifacts") -> None:
        """
        Args:
            model_type:    'xgboost' or 'lightgbm'
            artifacts_dir: where to read scale_pos_weight info from
        """
        super().__init__()
        if model_type not in ("xgboost", "lightgbm"):
            raise ValueError(f"model_type must be 'xgboost' or 'lightgbm', got '{model_type}'")
        self.model_type    = model_type
        self.artifacts_dir = Path(artifacts_dir)

    def tune(self, X_train: np.ndarray, y_train: np.ndarray) -> Dict[str, Any]:
        """
        Run Optuna search and return best hyperparameters.

        Input:
            X_train: (n_samples, n_features) — from data/splits/X_train.npy
            y_train: (n_samples,)            — from data/splits/y_train.npy

        Output:
            best_params dict ready to pass to XGBoost/LightGBM constructor
        """
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        # Compute scale_pos_weight from the actual training labels
        # (never hardcode — the ratio depends on the stratified split)
        n_neg = int((y_train == 0).sum())   # retained customers
        n_pos = int((y_train == 1).sum())   # churned customers
        scale_pos_weight = n_neg / n_pos
        self._signal(
            "Computed",
            "scale_pos_weight",
            f"n_retained={n_neg:,} / n_churned={n_pos:,} = {scale_pos_weight:.4f}",
        )

        self._signal(
            "Starting",
            f"Optuna search — {self.model_type}",
            f"{self.N_TRIALS} trials · {self.CV_FOLDS}-fold CV · objective=recall@precision≥{self.PRECISION_FLOOR}",
        )
        t0 = time.perf_counter()

        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(seed=self.OPTUNA_SEED),
            pruner=optuna.pruners.MedianPruner(n_startup_trials=5, n_warmup_steps=3),
        )

        study.optimize(
            lambda trial: self._objective(trial, X_train, y_train, scale_pos_weight),
            n_trials=self.N_TRIALS,
            show_progress_bar=False,
        )

        best = study.best_trial
        elapsed = time.perf_counter() - t0
        self._signal(
            "DONE",
            f"best recall={best.value:.4f} at trial #{best.number}",
            f"{elapsed:.1f}s · {self.N_TRIALS} trials completed",
        )

        # Always inject the fixed params that must not be tuned
        best_params = best.params.copy()
        best_params["scale_pos_weight"] = scale_pos_weight
        best_params["random_state"]     = self.RANDOM_STATE
        best_params["eval_metric"]      = "logloss"

        return best_params

    def _objective(
        self,
        trial,
        X_train: np.ndarray,
        y_train: np.ndarray,
        scale_pos_weight: float,
    ) -> float:
        """Single Optuna trial — returns recall score (0 if precision floor not met)."""
        from sklearn.model_selection import StratifiedKFold
        from sklearn.metrics import precision_score, recall_score

        params = self._suggest_params(trial, scale_pos_weight)
        model  = self._build_model(params)

        skf     = StratifiedKFold(n_splits=self.CV_FOLDS, shuffle=True, random_state=self.RANDOM_STATE)
        recalls = []

        for fold_idx, (train_idx, val_idx) in enumerate(skf.split(X_train, y_train)):
            X_f_train, X_f_val = X_train[train_idx], X_train[val_idx]
            y_f_train, y_f_val = y_train[train_idx], y_train[val_idx]

            model.fit(X_f_train, y_f_train)
            y_pred = model.predict(X_f_val)

            prec = precision_score(y_f_val, y_pred, zero_division=0)
            rec  = recall_score(y_f_val, y_pred, zero_division=0)

            # Precision floor: if precision is below 0.60, this trial is invalid
            # Return 0 so Optuna deprioritises it
            if prec < self.PRECISION_FLOOR:
                recalls.append(0.0)
            else:
                recalls.append(rec)

            # Optuna pruner: report intermediate value and prune bad trials early
            trial.report(float(np.mean(recalls)), fold_idx)
            if trial.should_prune():
                import optuna
                raise optuna.exceptions.TrialPruned()

        return float(np.mean(recalls))

    def _suggest_params(self, trial, scale_pos_weight: float) -> Dict[str, Any]:
        """Suggest hyperparameters for the current trial (model-type specific)."""
        if self.model_type == "xgboost":
            return {
                "max_depth":          trial.suggest_int("max_depth", 3, 8),
                "learning_rate":      trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "n_estimators":       trial.suggest_int("n_estimators", 100, 500),
                "subsample":          trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree":   trial.suggest_float("colsample_bytree", 0.6, 1.0),
                "min_child_weight":   trial.suggest_int("min_child_weight", 1, 10),
                "reg_alpha":          trial.suggest_float("reg_alpha", 1e-4, 1.0, log=True),
                "reg_lambda":         trial.suggest_float("reg_lambda", 1e-4, 1.0, log=True),
                "scale_pos_weight":   scale_pos_weight,
                "random_state":       self.RANDOM_STATE,
                "eval_metric":        "logloss",
                "use_label_encoder":  False,
            }
        else:  # lightgbm
            return {
                "max_depth":          trial.suggest_int("max_depth", 3, 8),
                "learning_rate":      trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "n_estimators":       trial.suggest_int("n_estimators", 100, 500),
                "num_leaves":         trial.suggest_int("num_leaves", 20, 100),
                "subsample":          trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree":   trial.suggest_float("colsample_bytree", 0.6, 1.0),
                "min_child_samples":  trial.suggest_int("min_child_samples", 10, 50),
                "reg_alpha":          trial.suggest_float("reg_alpha", 1e-4, 1.0, log=True),
                "reg_lambda":         trial.suggest_float("reg_lambda", 1e-4, 1.0, log=True),
                "class_weight":       "balanced",
                "random_state":       self.RANDOM_STATE,
                "verbose":            -1,
            }

    def _build_model(self, params: Dict):
        """Instantiate the model class with given params (for CV inside objective)."""
        if self.model_type == "xgboost":
            from xgboost import XGBClassifier
            return XGBClassifier(**{k: v for k, v in params.items()
                                    if k != "use_label_encoder"})
        else:
            from lightgbm import LGBMClassifier
            return LGBMClassifier(**params)


class XGBoostTrainer(SignalLogger):
    """
    Trains XGBoost on the full training fold using hyperparameters from OptunaTuner.

    Input files read:
        data/splits/X_train.npy       — 3,940 × 38 training feature matrix
        data/splits/y_train.npy       — 3,940 churn labels (0/1)
        models/artifacts/feature_names.json — 38 feature names (for SHAP + reports)
        models/artifacts/feature_engineering_config.json — validates features are correct

    Output files written:
        models/staging/{version}/model.pkl              — trained XGBoost estimator
        models/artifacts/reference_distribution.pkl     — training feature stats for PSI drift
        models/staging/{version}/run_manifest.json      — reproducibility record
    """

    def __init__(
        self,
        splits_dir:    str | Path = "data/splits",
        artifacts_dir: str | Path = "models/artifacts",
        staging_dir:   str | Path = "models/staging",
        version:       str         = "v1.0.0",
    ) -> None:
        super().__init__()
        self.splits_dir    = Path(splits_dir)
        self.artifacts_dir = Path(artifacts_dir)
        self.staging_dir   = Path(staging_dir) / version
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.version       = version
        self._model        = None
        self._feature_names: list = []

    def load_data(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Load training and validation splits from disk.

        Returns: (X_train, y_train, X_val, y_val)
        Val is loaded here so the evaluator can use it for threshold selection.
        Test is NOT loaded — only touched at final evaluation.
        """
        self._signal(
            "Reading from",
            str(self.splits_dir / "X_train.npy"),
            "purpose: 3,940 × 38 training feature matrix → fit XGBoost",
        )
        X_train = np.load(self.splits_dir / "X_train.npy")

        self._signal(
            "Reading from",
            str(self.splits_dir / "y_train.npy"),
            "purpose: 3,940 churn labels → compute scale_pos_weight + fit target",
        )
        y_train = np.load(self.splits_dir / "y_train.npy")

        self._signal(
            "Reading from",
            str(self.splits_dir / "X_val.npy"),
            "purpose: 845 × 38 validation features → ThresholdSelector (PR curve)",
        )
        X_val = np.load(self.splits_dir / "X_val.npy")

        self._signal(
            "Reading from",
            str(self.splits_dir / "y_val.npy"),
            "purpose: 845 validation labels → ThresholdSelector",
        )
        y_val = np.load(self.splits_dir / "y_val.npy")

        self._signal(
            "Reading from",
            str(self.artifacts_dir / "feature_names.json"),
            "purpose: 38 ordered feature names → SHAP value labelling + reports",
        )
        with open(self.artifacts_dir / "feature_names.json") as f:
            self._feature_names = json.load(f)["feature_names"]

        self._signal(
            "Reading from",
            str(self.artifacts_dir / "feature_engineering_config.json"),
            "purpose: validate encoding decisions (ordinal_cramers_v_threshold, fitted thresholds)"
            " are consistent with training data",
        )
        with open(self.artifacts_dir / "feature_engineering_config.json") as f:
            fe_config = json.load(f)

        self._signal(
            "Validated",
            "feature_engineering_config.json",
            f"ordinal_cramers_v_threshold={fe_config['encoding_decisions']['ordinal_cramers_v_threshold']} "
            f"| warehouse_log_median={fe_config['fitted_thresholds']['warehouse_log_median']} "
            f"| address_iqr_cap={fe_config['fitted_thresholds']['address_iqr_cap']}",
        )

        self._signal(
            "Data loaded",
            f"X_train={X_train.shape} | X_val={X_val.shape}",
            f"churn_rate_train={y_train.mean():.4f} | churn_rate_val={y_val.mean():.4f}",
        )
        return X_train, y_train, X_val, y_val

    def train(self, X_train: np.ndarray, y_train: np.ndarray, best_params: Dict) -> Any:
        """
        Train XGBoost on the full training fold with Optuna's best params.

        Input:
            X_train:     (3,940, 38) — full training feature matrix
            y_train:     (3,940,)    — training churn labels
            best_params: dict from OptunaTuner.tune()

        Output:
            Fitted XGBClassifier (also stored as self._model)
        """
        from xgboost import XGBClassifier

        self._signal(
            "Training",
            "XGBoost on full training fold",
            f"n_samples={len(X_train):,} | n_features={X_train.shape[1]} | "
            f"scale_pos_weight={best_params.get('scale_pos_weight', '?'):.4f}",
        )

        # Log the key hyperparameters so they appear in the console
        self._signal(
            "Hyperparameters",
            f"max_depth={best_params.get('max_depth')} "
            f"lr={best_params.get('learning_rate'):.4f} "
            f"n_est={best_params.get('n_estimators')} "
            f"subsample={best_params.get('subsample'):.2f} "
            f"colsample={best_params.get('colsample_bytree'):.2f}",
        )

        t0 = time.perf_counter()
        safe_params = {k: v for k, v in best_params.items()
                       if k not in ("use_label_encoder", "eval_metric")}
        safe_params["eval_metric"] = "logloss"

        model = XGBClassifier(**safe_params)
        model.fit(X_train, y_train, verbose=False)

        elapsed = time.perf_counter() - t0
        self._model = model
        self._signal("DONE", "XGBoost training complete", f"{elapsed:.1f}s")
        return model

    def save_model(self) -> Path:
        """
        Save the trained model to models/staging/{version}/model.pkl.

        Output file:
            models/staging/v1.0.0/model.pkl
            — sklearn-compatible XGBClassifier
            — loaded by FastAPI on startup to serve predictions
            — uploaded to HF Hub by ModelRegistrar
        """
        if self._model is None:
            raise RuntimeError("Call train() before save_model().")
        path = self.staging_dir / "model.pkl"
        with open(path, "wb") as f:
            pickle.dump(self._model, f)
        self._signal(
            "Writing to",
            str(path),
            "model.pkl — trained XGBClassifier · used by API + HF Hub upload",
        )
        return path

    def save_reference_distribution(self, X_train: np.ndarray) -> Path:
        """
        Save training feature statistics for PSI drift detection in Day 3.

        WHY: Drift monitoring (Stage 3.5) compares the distribution of
        new batch data against the training distribution using PSI
        (Population Stability Index). PSI needs a reference — that reference
        is the training data statistics captured here.

        Output file:
            models/artifacts/reference_distribution.pkl
            — dict with mean, std, percentiles per feature
            — uploaded to HF Hub alongside the model
            — loaded by batch.py every time a batch scoring run completes

        Input:  X_train numpy array (3,940 × 38)
        Output: Path to the saved .pkl file
        """
        ref_dist = {
            "feature_names":  self._feature_names,
            "n_samples":      int(X_train.shape[0]),
            "n_features":     int(X_train.shape[1]),
            "mean":           X_train.mean(axis=0).tolist(),
            "std":            X_train.std(axis=0).tolist(),
            "percentiles": {
                "p5":  np.percentile(X_train, 5,  axis=0).tolist(),
                "p25": np.percentile(X_train, 25, axis=0).tolist(),
                "p50": np.percentile(X_train, 50, axis=0).tolist(),
                "p75": np.percentile(X_train, 75, axis=0).tolist(),
                "p95": np.percentile(X_train, 95, axis=0).tolist(),
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        path = self.artifacts_dir / "reference_distribution.pkl"
        with open(path, "wb") as f:
            pickle.dump(ref_dist, f)
        self._signal(
            "Writing to",
            str(path),
            "reference_distribution.pkl — training feature stats · used by PSI drift monitor",
        )
        return path

    def save_run_manifest(self, best_params: Dict, X_train: np.ndarray) -> Dict:
        """
        Save run_manifest.json for reproducibility.

        WHY: Without this, it is impossible to know which data, which code,
        which random seeds produced this model. The manifest lets anyone
        reproduce the exact result by cloning the repo at the recorded git SHA
        and running with the same seeds and parameters.

        Output file:
            models/staging/{version}/run_manifest.json
            — uploaded to HF Hub
            — referenced by model_versions.artifact_path in Supabase
        """
        import subprocess
        import sys

        try:
            git_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
            ).decode().strip()
        except Exception:
            git_sha = "unknown"

        try:
            import xgboost
            xgb_version = xgboost.__version__
        except Exception:
            xgb_version = "unknown"

        import hashlib
        data_hash = hashlib.sha256(X_train.tobytes()).hexdigest()[:16]

        manifest = {
            "version":       self.version,
            "algorithm":     "XGBoost",
            "trained_at":    datetime.now(timezone.utc).isoformat(),
            "git_sha":       git_sha,
            "data": {
                "X_train_shape": list(X_train.shape),
                "X_train_hash":  data_hash,
                "source":        "data/splits/X_train.npy",
            },
            "hyperparameters": {
                k: v for k, v in best_params.items()
                if k not in ("eval_metric", "use_label_encoder")
            },
            "environment": {
                "python":  sys.version.split()[0],
                "xgboost": xgb_version,
            },
            "seeds": {
                "optuna":       42,
                "random_state": 42,
                "cv_folds":     5,
            },
        }
        path = self.staging_dir / "run_manifest.json"
        with open(path, "w") as f:
            json.dump(manifest, f, indent=2)
        self._signal(
            "Writing to",
            str(path),
            "run_manifest.json — git SHA, data hash, params, seeds · reproducibility record",
        )
        return manifest

    @property
    def model(self):
        return self._model

    @property
    def feature_names(self) -> list:
        return self._feature_names


class LightGBMTrainer(SignalLogger):
    """
    Trains LightGBM on the full training fold using hyperparameters from OptunaTuner.

    Mirrors XGBoostTrainer structure. Used to produce v1.1.0 (staging) so Marco
    gets the full production versioning experience: one production model (XGBoost),
    one staging model (LightGBM) ready for promotion.

    Input files read:
        data/splits/X_train.npy         — same training data as XGBoost
        data/splits/y_train.npy         — same labels
        data/splits/X_val.npy           — for threshold selection
        data/splits/y_val.npy
        models/artifacts/feature_names.json — same 38 features

    Output files written:
        models/staging/{version}/model.pkl         — trained LGBMClassifier
        models/staging/{version}/run_manifest.json — reproducibility record
    """

    def __init__(
        self,
        splits_dir:    str | Path = "data/splits",
        artifacts_dir: str | Path = "models/artifacts",
        staging_dir:   str | Path = "models/staging",
        version:       str         = "v1.1.0",
    ) -> None:
        super().__init__()
        self.splits_dir    = Path(splits_dir)
        self.artifacts_dir = Path(artifacts_dir)
        self.staging_dir   = Path(staging_dir) / version
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.version       = version
        self._model        = None
        self._feature_names: list = []

    def load_data(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Load splits (same as XGBoostTrainer — reads from same .npy files)."""
        self._signal(
            "Reading from",
            str(self.splits_dir / "X_train.npy"),
            "purpose: 3,940 × 38 training features → fit LightGBM · same split as XGBoost (random_state=42)",
        )
        X_train = np.load(self.splits_dir / "X_train.npy")

        self._signal(
            "Reading from",
            str(self.splits_dir / "y_train.npy"),
            "purpose: 3,940 churn labels → class_weight='balanced' handles imbalance in LightGBM",
        )
        y_train = np.load(self.splits_dir / "y_train.npy")

        self._signal(
            "Reading from",
            str(self.splits_dir / "X_val.npy"),
            "purpose: 845 validation features → ThresholdSelector (PR curve) after training",
        )
        X_val = np.load(self.splits_dir / "X_val.npy")

        self._signal(
            "Reading from",
            str(self.splits_dir / "y_val.npy"),
            "purpose: 845 validation labels → PR curve threshold selection",
        )
        y_val = np.load(self.splits_dir / "y_val.npy")

        self._signal(
            "Reading from",
            str(self.artifacts_dir / "feature_names.json"),
            "purpose: 38 feature names → SHAP TreeExplainer labelling",
        )
        with open(self.artifacts_dir / "feature_names.json") as f:
            self._feature_names = json.load(f)["feature_names"]

        return X_train, y_train, X_val, y_val

    def train(self, X_train: np.ndarray, y_train: np.ndarray, best_params: Dict) -> Any:
        """
        Train LightGBM on the full training fold.

        Key difference from XGBoost: LightGBM uses class_weight='balanced'
        instead of scale_pos_weight. Both achieve the same imbalance correction —
        LightGBM's API just uses a different parameter name.

        Input:
            X_train, y_train: full training fold numpy arrays
            best_params:      dict from OptunaTuner.tune(model_type='lightgbm')
        Output:
            Fitted LGBMClassifier
        """
        from lightgbm import LGBMClassifier

        self._signal(
            "Training",
            "LightGBM on full training fold",
            f"n_samples={len(X_train):,} | n_features={X_train.shape[1]} | "
            f"imbalance_strategy=class_weight='balanced'",
        )
        self._signal(
            "Hyperparameters",
            f"max_depth={best_params.get('max_depth')} "
            f"lr={best_params.get('learning_rate'):.4f} "
            f"n_est={best_params.get('n_estimators')} "
            f"num_leaves={best_params.get('num_leaves')}",
        )

        t0 = time.perf_counter()
        model = LGBMClassifier(**best_params)
        model.fit(X_train, y_train)
        elapsed = time.perf_counter() - t0
        self._model = model
        self._signal("DONE", "LightGBM training complete", f"{elapsed:.1f}s")
        return model

    def save_model(self) -> Path:
        """
        Save LightGBM model to models/staging/{version}/model.pkl.

        Output: models/staging/v1.1.0/model.pkl
            — staging model, NOT promoted to production yet
            — uploaded to HF Hub under v1.1.0/ subfolder
        """
        if self._model is None:
            raise RuntimeError("Call train() before save_model().")
        path = self.staging_dir / "model.pkl"
        with open(path, "wb") as f:
            pickle.dump(self._model, f)
        self._signal(
            "Writing to",
            str(path),
            "model.pkl — trained LGBMClassifier · staging version (not production)",
        )
        return path

    def save_run_manifest(self, best_params: Dict, X_train: np.ndarray) -> Dict:
        """Save LightGBM run_manifest.json — same structure as XGBoost."""
        import subprocess, sys, hashlib

        try:
            git_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
            ).decode().strip()
        except Exception:
            git_sha = "unknown"

        try:
            import lightgbm
            lgbm_version = lightgbm.__version__
        except Exception:
            lgbm_version = "unknown"

        data_hash = hashlib.sha256(X_train.tobytes()).hexdigest()[:16]

        manifest = {
            "version":    self.version,
            "algorithm":  "LightGBM",
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "git_sha":    git_sha,
            "data": {
                "X_train_shape": list(X_train.shape),
                "X_train_hash":  data_hash,
                "source":        "data/splits/X_train.npy",
            },
            "hyperparameters": best_params,
            "environment":     {"python": sys.version.split()[0], "lightgbm": lgbm_version},
            "seeds":           {"optuna": 42, "random_state": 42, "cv_folds": 5},
        }
        path = self.staging_dir / "run_manifest.json"
        with open(path, "w") as f:
            json.dump(manifest, f, indent=2)
        self._signal(
            "Writing to",
            str(path),
            "run_manifest.json — git SHA, data hash, LightGBM params · reproducibility record",
        )
        return manifest

    @property
    def model(self):
        return self._model

    @property
    def feature_names(self) -> list:
        return self._feature_names
