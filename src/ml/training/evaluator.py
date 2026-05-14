"""
evaluator.py — Stage 3.3: Model Evaluation + Threshold Selection + SHAP

Classes:
    ModelEvaluator    — computes all metrics on val + test sets
    ThresholdSelector — finds optimal decision threshold from PR curve on VAL only
    SHAPExplainer     — computes SHAP values using TreeExplainer (XGBoost/LightGBM native)

Key design:
    - ThresholdSelector uses VAL set ONLY (test is locked)
    - SHAPExplainer samples 500 rows from training for speed
    - All outputs written to models/staging/{version}/

Every file read/written prints what it is and why.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from src.ml.eda.utils.signal_logger import SignalLogger


class ModelEvaluator(SignalLogger):
    """
    Computes a full suite of classification metrics on val and test sets.

    WHY both val and test:
        - Val metrics: used for threshold selection (interactive during tuning)
        - Test metrics: the official, final, one-time performance report

    Input files read:
        data/splits/X_test.npy — LOCKED test set, touched ONCE here
        data/splits/y_test.npy — test labels

    Output files written:
        models/staging/{version}/metrics.json — all metrics for both val + test
    """

    def __init__(self, staging_dir: str | Path, splits_dir: str | Path = "data/splits") -> None:
        super().__init__()
        self.staging_dir = Path(staging_dir)
        self.splits_dir  = Path(splits_dir)

    def evaluate(
        self,
        model: Any,
        X_val: np.ndarray,
        y_val: np.ndarray,
        threshold: float,
        version: str,
    ) -> Dict:
        """
        Compute all metrics on BOTH val and test sets using the selected threshold.

        WHY threshold parameter: the threshold was selected from the PR curve
        on the val set (by ThresholdSelector). All binary metrics (recall, precision,
        F1) depend on this threshold. AUC metrics are threshold-independent.

        Input:
            model:     fitted XGBoost or LightGBM classifier
            X_val:     845 × 38 validation features (already loaded by trainer)
            y_val:     845 validation labels
            threshold: decision threshold from ThresholdSelector (e.g. 0.38)
            version:   model version string (e.g. 'v1.0.0')

        Output:
            metrics dict (also written to metrics.json)
        """
        from sklearn.metrics import (
            roc_auc_score, average_precision_score,
            precision_score, recall_score, f1_score,
            confusion_matrix, brier_score_loss,
        )

        self._signal(
            "Reading from",
            str(self.splits_dir / "X_test.npy"),
            "purpose: LOCKED test set · touched ONCE for final metrics · 845 × 38 features",
        )
        X_test = np.load(self.splits_dir / "X_test.npy")

        self._signal(
            "Reading from",
            str(self.splits_dir / "y_test.npy"),
            "purpose: 845 test labels · ground truth for final AUC, recall, precision",
        )
        y_test = np.load(self.splits_dir / "y_test.npy")

        self._signal(
            "Evaluating",
            f"threshold={threshold:.4f}",
            f"val ({len(y_val):,} rows) + test ({len(y_test):,} rows)",
        )

        def _compute_set_metrics(X, y, label):
            """Compute full metrics for one set."""
            proba = model.predict_proba(X)[:, 1]
            pred  = (proba >= threshold).astype(int)
            cm    = confusion_matrix(y, pred)
            tn, fp, fn, tp = cm.ravel()
            return {
                "set":              label,
                "n_samples":        int(len(y)),
                "n_churned":        int(y.sum()),
                "auc_roc":          float(roc_auc_score(y, proba)),
                "pr_auc":           float(average_precision_score(y, proba)),
                "brier_score":      float(brier_score_loss(y, proba)),
                "threshold":        float(threshold),
                "recall":           float(recall_score(y, pred, zero_division=0)),
                "precision":        float(precision_score(y, pred, zero_division=0)),
                "f1":               float(f1_score(y, pred, zero_division=0)),
                "true_positives":   int(tp),
                "false_positives":  int(fp),
                "true_negatives":   int(tn),
                "false_negatives":  int(fn),
                "business_cost_score": float(fn * 5 + fp * 1),
                # business_cost_score: each missed churner (FN) costs 5×
                # more than a false alarm (FP) — incentivises recall
            }

        val_metrics  = _compute_set_metrics(X_val,  y_val,  "val")
        test_metrics = _compute_set_metrics(X_test, y_test, "test")

        metrics = {
            "version":    version,
            "algorithm":  model.__class__.__name__,
            "threshold":  threshold,
            "val":        val_metrics,
            "test":       test_metrics,
            # Summary of gate results (from STAGE3_ML_README §2)
            "gates": {
                "auc_roc_gte_0.85":      test_metrics["auc_roc"] >= 0.85,
                "recall_gte_0.80":       test_metrics["recall"]  >= 0.80,
                "precision_gte_0.60":    test_metrics["precision"] >= 0.60,
                "overfit_gap_lt_0.05":   abs(
                    val_metrics["auc_roc"] - test_metrics["auc_roc"]
                ) < 0.05,
            },
        }

        # Print a readable summary to console
        self._signal(
            "Test metrics",
            f"AUC={test_metrics['auc_roc']:.4f} "
            f"Recall={test_metrics['recall']:.4f} "
            f"Precision={test_metrics['precision']:.4f} "
            f"F1={test_metrics['f1']:.4f}",
        )
        self._signal(
            "Gates",
            f"AUC≥0.85:{metrics['gates']['auc_roc_gte_0.85']} "
            f"Recall≥0.80:{metrics['gates']['recall_gte_0.80']} "
            f"Precision≥0.60:{metrics['gates']['precision_gte_0.60']} "
            f"OverfitGap<0.05:{metrics['gates']['overfit_gap_lt_0.05']}",
        )

        # Write metrics.json
        path = self.staging_dir / "metrics.json"
        with open(path, "w") as f:
            json.dump(metrics, f, indent=2)
        self._signal(
            "Writing to",
            str(path),
            "metrics.json — AUC/Recall/Precision/F1 for val+test · uploaded to HF Hub",
        )
        return metrics


class ThresholdSelector(SignalLogger):
    """
    Selects the operating decision threshold from the precision-recall curve on the VAL set.

    WHY val only: The threshold is a model decision — if we selected it on the test
    set, the test performance would be optimistically biased. Val is the designated
    set for all decisions that don't involve raw model fitting.

    Business rule: find the LOWEST threshold t* such that:
        recall(t*)   >= 0.80   (catch 4 in 5 churners)
        precision(t*) >= 0.60   (no more than 40% false alarms)

    If no single threshold satisfies both, the one maximising recall while
    keeping precision as close to 0.60 as possible is selected.

    Output files written:
        models/staging/{version}/thresholds.yaml — the selected threshold + risk tiers
    """

    RECALL_TARGET:    float = 0.80
    PRECISION_FLOOR:  float = 0.60

    def __init__(self, staging_dir: str | Path) -> None:
        super().__init__()
        self.staging_dir = Path(staging_dir)

    def select(
        self,
        model: Any,
        X_val: np.ndarray,
        y_val: np.ndarray,
    ) -> float:
        """
        Plot PR curve on val, select threshold, write thresholds.yaml.

        Input:
            model: fitted classifier
            X_val: 845 × 38 val features
            y_val: 845 val labels
        Output:
            selected threshold float
        """
        import yaml
        from sklearn.metrics import precision_recall_curve

        self._signal(
            "Computing",
            "PR curve on val set",
            f"target: recall≥{self.RECALL_TARGET} AND precision≥{self.PRECISION_FLOOR}",
        )

        proba = model.predict_proba(X_val)[:, 1]
        precisions, recalls, thresholds = precision_recall_curve(y_val, proba)

        # Find threshold that meets both constraints
        best_threshold = None
        best_recall    = 0.0

        for prec, rec, thr in zip(precisions[:-1], recalls[:-1], thresholds):
            if prec >= self.PRECISION_FLOOR and rec >= self.RECALL_TARGET:
                if rec > best_recall:
                    best_recall    = rec
                    best_threshold = thr

        # Fallback: if no threshold meets both gates,
        # take the one with highest recall at or above precision floor
        if best_threshold is None:
            self._warn(
                "No threshold meets both recall≥0.80 AND precision≥0.60. "
                "Falling back to best recall with precision closest to 0.60."
            )
            mask = precisions[:-1] >= self.PRECISION_FLOOR
            if mask.any():
                best_idx       = np.argmax(recalls[:-1][mask])
                best_threshold = thresholds[mask][best_idx]
                best_recall    = recalls[:-1][mask][best_idx]
            else:
                # Last resort: use 0.50 default
                best_threshold = 0.50
                self._warn("Precision floor never met. Using default threshold=0.50.")

        # Risk tier bounds derived from the selected threshold
        high_cutoff   = min(best_threshold + 0.05, 0.95)
        medium_cutoff = max(best_threshold - 0.15, 0.05)

        thresholds_data = {
            "version":           str(self.staging_dir.name),
            "decision_threshold": float(best_threshold),
            "recall_at_threshold": float(best_recall),
            "risk_tiers": {
                "HIGH":   f">= {high_cutoff:.3f}",
                "MEDIUM": f">= {medium_cutoff:.3f} and < {high_cutoff:.3f}",
                "LOW":    f"< {medium_cutoff:.3f}",
            },
            "selection_criteria": {
                "recall_target":   self.RECALL_TARGET,
                "precision_floor": self.PRECISION_FLOOR,
            },
        }

        path = self.staging_dir / "thresholds.yaml"
        with open(path, "w") as f:
            yaml.dump(thresholds_data, f, default_flow_style=False)

        self._signal(
            "Writing to",
            str(path),
            f"thresholds.yaml — decision_threshold={best_threshold:.4f} "
            f"recall={best_recall:.4f} · used by API + batch scorer",
        )
        self._signal(
            "Risk tiers",
            f"HIGH≥{high_cutoff:.3f} | MEDIUM≥{medium_cutoff:.3f} | LOW below",
        )
        return float(best_threshold)


class SHAPExplainer(SignalLogger):
    """
    Computes SHAP (SHapley Additive exPlanations) feature importance using TreeExplainer.

    WHY TreeExplainer: XGBoost and LightGBM are tree-based models. TreeExplainer
    is exact (not approximate) and runs in milliseconds — much faster than
    KernelExplainer which would be needed for non-tree models.

    WHY SHAP: Standard feature importance (gain/split) tells you which features
    the model uses overall. SHAP tells you HOW MUCH each feature contributed
    to each individual prediction. This is what powers the "top-3 reason codes"
    in the API response (e.g. "Customer flagged because: high complain_risk,
    low tenure_months, Mobile order category").

    Input files read:
        data/splits/X_train.npy         — sampled 500 rows for SHAP computation
        models/artifacts/feature_names.json — 38 names for labelling SHAP values

    Output files written:
        models/staging/{version}/shap_summary.json — global feature importance
    """

    SHAP_SAMPLE_N: int = 500   # TreeExplainer is fast — 500 rows is enough for global importance

    def __init__(
        self,
        staging_dir:   str | Path,
        splits_dir:    str | Path = "data/splits",
        artifacts_dir: str | Path = "models/artifacts",
    ) -> None:
        super().__init__()
        self.staging_dir   = Path(staging_dir)
        self.splits_dir    = Path(splits_dir)
        self.artifacts_dir = Path(artifacts_dir)

    def explain(self, model: Any, feature_names: List[str]) -> Dict:
        """
        Compute SHAP values and save shap_summary.json.

        Input:
            model:         fitted XGBoost or LightGBM classifier
            feature_names: 38 ordered feature names from feature_names.json

        Output:
            shap_summary dict (also written to shap_summary.json)
        """
        import shap

        self._signal(
            "Reading from",
            str(self.splits_dir / "X_train.npy"),
            f"purpose: sample {self.SHAP_SAMPLE_N} rows for SHAP TreeExplainer "
            f"(global feature importance computation)",
        )
        X_train = np.load(self.splits_dir / "X_train.npy")

        # Sample for speed — SHAP is exact on trees but still benefits from sampling
        # for the global summary plot computation
        np.random.seed(42)
        idx      = np.random.choice(len(X_train), size=min(self.SHAP_SAMPLE_N, len(X_train)), replace=False)
        X_sample = X_train[idx]

        self._signal(
            "Computing",
            "SHAP TreeExplainer",
            f"sample={len(X_sample)} rows | {len(feature_names)} features",
        )
        t0 = time.perf_counter()

        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_sample)

        # For binary classification, shap_values may be a list [class0, class1]
        # We want class 1 (churn=1) SHAP values
        if isinstance(shap_values, list):
            shap_vals = shap_values[1]
        else:
            shap_vals = shap_values

        # Mean absolute SHAP value per feature = global importance
        mean_abs_shap = np.abs(shap_vals).mean(axis=0)

        # Build sorted feature importance list
        importance = sorted(
            [
                {"feature": name, "mean_abs_shap": float(val), "rank": 0}
                for name, val in zip(feature_names, mean_abs_shap)
            ],
            key=lambda x: -x["mean_abs_shap"],
        )
        for i, item in enumerate(importance):
            item["rank"] = i + 1

        # Dominance check: no single feature should exceed 40% of total importance
        total_shap = sum(x["mean_abs_shap"] for x in importance)
        for item in importance:
            item["importance_pct"] = float(item["mean_abs_shap"] / total_shap * 100)

        top1_pct = importance[0]["importance_pct"]
        if top1_pct > 40:
            self._warn(
                f"Feature '{importance[0]['feature']}' dominates at {top1_pct:.1f}% of SHAP importance. "
                "This may indicate leakage or over-reliance on a single signal."
            )

        elapsed = time.perf_counter() - t0
        summary = {
            "version":          str(self.staging_dir.name),
            "n_samples":        len(X_sample),
            "feature_importance": importance,
            "top3_features":    [x["feature"] for x in importance[:3]],
            "dominance_warning": top1_pct > 40,
            "top1_importance_pct": top1_pct,
        }

        path = self.staging_dir / "shap_summary.json"
        with open(path, "w") as f:
            json.dump(summary, f, indent=2)

        self._signal(
            "Writing to",
            str(path),
            f"shap_summary.json — {len(feature_names)} features ranked by |SHAP| · {elapsed:.1f}s",
        )
        self._signal(
            "Top 3 features",
            " | ".join(f"{x['feature']}={x['mean_abs_shap']:.4f}" for x in importance[:3]),
        )
        return summary
