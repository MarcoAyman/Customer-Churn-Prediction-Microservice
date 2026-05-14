"""
class_balance.py — analyses the churn label distribution at the dataset level.

Produces:
    1. A donut chart of churned vs retained (matches dashboard aesthetic)
    2. Imbalance ratio with interpretation
    3. Stage-3.5 handling recommendation based on severity

This is intentionally separate from SanityChecker: sanity asks "is the data
healthy?" while ClassBalanceAnalyzer asks "how do we MODEL with this
imbalance?" — both are valid questions with different audiences.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd

from src.ml.eda.reporting.manifest_writer import ManifestWriter
from src.ml.eda.reporting.plot_saver import PlotSaver
from src.ml.eda.utils.signal_logger import SignalLogger


class ClassBalanceAnalyzer(SignalLogger):
    """Analyse churn label distribution and recommend a handling strategy."""

    # Imbalance bands drive the modeling recommendation.
    BALANCED_MAX: float = 2.0    # <2:1  →  balanced, no action needed
    MODERATE_MAX: float = 5.0    # <5:1  →  moderate, class weights are enough
    HEAVY_MAX: float = 10.0      # <10:1 →  heavy, consider SMOTE
    # >=10:1                      →  severe, multiple strategies stacked

    def __init__(
        self,
        df: pd.DataFrame,
        target_col: str,
        plot_saver: PlotSaver,
        manifest: ManifestWriter,
    ) -> None:
        super().__init__()
        self.df = df
        self.target_col = target_col
        self.plot_saver = plot_saver
        self.manifest = manifest

    def analyze(self) -> Dict:
        self._signal("Analyzing", "class balance")

        counts = self.df[self.target_col].value_counts().sort_index()
        total = int(counts.sum())
        retained = int(counts.get(0, 0))
        churned = int(counts.get(1, 0))

        churn_rate = float(churned / total) if total else 0.0
        retention_rate = float(retained / total) if total else 0.0
        ratio = float(retained / churned) if churned > 0 else float("inf")

        severity, recommendation = self._build_recommendation(ratio)

        plot_info = self._plot_donut(retained, churned, churn_rate)

        return {
            "total":              total,
            "retained_count":     retained,
            "churned_count":      churned,
            "retention_rate":     retention_rate,
            "churn_rate":         churn_rate,
            "imbalance_ratio":    ratio,
            "imbalance_severity": severity,
            "plot":               plot_info,
            "narrative": (
                f"Dataset holds {total:,} customers: {retained:,} retained and "
                f"{churned:,} churned ({churn_rate*100:.2f}% churn rate). "
                f"Imbalance ratio = {ratio:.2f}:1. Severity: {severity}."
            ),
            "recommendation": recommendation,
        }

    def save(self, output_path: str | Path) -> Dict:
        result = self.analyze()
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        self._signal(
            "Writing to", str(output_path),
            f"severity={result['imbalance_severity']}  ratio={result['imbalance_ratio']:.2f}:1",
        )
        return result

    # ---------- plot ----------

    def _plot_donut(self, retained: int, churned: int, churn_rate: float) -> Dict:
        colors = self.plot_saver.COLORS

        fig, ax = plt.subplots(figsize=(6, 5))
        sizes = [retained, churned]
        labels = [f"Retained\n{retained:,}", f"Churned\n{churned:,}"]
        ax.pie(
            sizes,
            labels=labels,
            colors=[colors["green"], colors["red"]],
            startangle=90,
            autopct="%1.1f%%",
            pctdistance=0.78,
            wedgeprops={"width": 0.38, "edgecolor": colors["panel"], "linewidth": 2},
            textprops={"color": colors["text"], "fontsize": 10},
        )
        # Centre text — total + churn rate
        ax.text(
            0, 0.06,
            f"{retained + churned:,}",
            ha="center", va="center",
            color=colors["text"], fontsize=18, fontweight="bold",
        )
        ax.text(
            0, -0.10,
            f"{churn_rate*100:.2f}% churn",
            ha="center", va="center",
            color=colors["text_dim"], fontsize=10,
        )
        ax.set_title("Class balance — churned vs retained")

        caption = (
            "Donut chart of the churn label. The hollow ring shows the proportional split; "
            "the centre displays the total count and overall churn rate. "
            "Green = retained, red = churned."
        )
        path = self.plot_saver.save(fig, "global__class_balance__donut")
        self.manifest.add(path, caption, None, "donut", "global")
        return {"path": path, "caption": caption}

    # ---------- recommendation logic ----------

    def _build_recommendation(self, ratio: float) -> tuple[str, Dict]:
        if ratio < self.BALANCED_MAX:
            severity = "balanced"
            strategy = "none"
            rationale = (
                f"Ratio {ratio:.2f}:1 is close to balanced. No special handling needed — "
                "default training should work fine."
            )
            stage_3_5_plan = ["Stage 3.5: use default class balance — no weighting, no resampling."]
        elif ratio < self.MODERATE_MAX:
            severity = "moderate"
            strategy = "algorithmic_weights"
            rationale = (
                f"Ratio {ratio:.2f}:1 is mildly imbalanced. Algorithmic class weights alone "
                "typically handle this well. Avoid resampling unless metrics disappoint."
            )
            stage_3_5_plan = [
                "XGBoost / LightGBM: set scale_pos_weight = (n_retained / n_churned).",
                "Logistic Regression / MLP: set class_weight='balanced'.",
                "Compare against threshold tuning on a calibrated model.",
            ]
        elif ratio < self.HEAVY_MAX:
            severity = "heavy"
            strategy = "weights_plus_threshold_tuning"
            rationale = (
                f"Ratio {ratio:.2f}:1 is heavily imbalanced. Combine algorithmic weights "
                "with explicit threshold selection — the default 0.5 will collapse recall."
            )
            stage_3_5_plan = [
                "Apply scale_pos_weight / class_weight as above.",
                "In Stage 3.8, select the operating threshold via the PR curve (recall ≥ 0.80 gate).",
                "Experiment: SMOTE on the training fold to compare against algorithmic weighting.",
            ]
        else:
            severity = "severe"
            strategy = "stack_multiple_strategies"
            rationale = (
                f"Ratio {ratio:.2f}:1 is severely imbalanced. A single strategy will not suffice. "
                "Stack algorithmic weights, resampling, and threshold tuning — and validate each."
            )
            stage_3_5_plan = [
                "Apply scale_pos_weight / class_weight.",
                "Resample the training fold with SMOTE or ADASYN (never touch val/test).",
                "Select operating threshold from the PR curve.",
                "Consider focal loss for the MLP challenger.",
                "Use PR-AUC (Average Precision) as the primary tuning metric — not ROC-AUC.",
            ]

        return severity, {
            "strategy_label":   strategy,
            "severity":         severity,
            "rationale":        rationale,
            "stage_3_5_plan":   stage_3_5_plan,
        }
