"""
numeric.py — deep analysis for a single numeric feature.

Produces, for one column:
    1. Histogram with mean / median reference lines (shape diagnosis)
    2. Box plot split by churn (separation diagnosis)
    3. Summary stats: mean, median, std, p1/p5/p25/p75/p95/p99, min/max, skew, kurt
    4. Mann-Whitney U test (non-parametric, robust to skewed distributions)
    5. Cohen's d effect size with pooled std
    6. IQR outlier report — DESCRIPTIVE ONLY (Stage 1 already sanity-clipped;
       actual transforms are decided in Stage 3.2, not here)
    7. Rule-based recommendation: signal strength, transform suggestion,
       outlier handling.

Mann-Whitney chosen over Welch's t-test because most customer-behaviour
features in this dataset are heavily skewed — t-test assumptions (normality)
would be violated and the p-values unreliable.
"""
from __future__ import annotations

from typing import Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

from src.ml.eda.analyzers.base import FeatureAnalyzer


class NumericAnalyzer(FeatureAnalyzer):
    """Per-feature analyzer for numeric columns."""

    # |Cohen's d| thresholds for the recommendation.
    D_STRONG: float = 0.8
    D_MODERATE: float = 0.5
    D_WEAK: float = 0.2

    # Skewness thresholds for the transform suggestion.
    SKEW_HEAVY: float = 1.5
    SKEW_MODERATE: float = 0.8

    def _analyze(self) -> Dict:
        feature = self.feature_name
        target = self.target_col
        df = self.df
        series = df[feature].dropna().astype(float)

        # --- summary stats ---
        summary = self._compute_summary(series)

        # --- split by churn ---
        retained, churned = self._churn_split()
        retained = retained.dropna().astype(float)
        churned  = churned.dropna().astype(float)

        # --- Mann-Whitney U (two-sided, non-parametric) ---
        try:
            u_stat, p_value = mannwhitneyu(retained, churned, alternative="two-sided")
        except ValueError:
            # Happens when one side is empty or all-identical — very unlikely here.
            u_stat, p_value = 0.0, 1.0

        # --- Cohen's d with pooled std ---
        cohens_d = self._cohens_d(retained, churned)

        split_stats = {
            "retained": {
                "n":      int(len(retained)),
                "mean":   float(retained.mean())   if len(retained) else 0.0,
                "median": float(retained.median()) if len(retained) else 0.0,
                "std":    float(retained.std(ddof=1)) if len(retained) > 1 else 0.0,
            },
            "churned": {
                "n":      int(len(churned)),
                "mean":   float(churned.mean())   if len(churned) else 0.0,
                "median": float(churned.median()) if len(churned) else 0.0,
                "std":    float(churned.std(ddof=1)) if len(churned) > 1 else 0.0,
            },
        }

        # --- outlier audit (DESCRIPTIVE only — no data modification) ---
        outlier_stats = self._outlier_report(series, summary)

        # --- plots ---
        histogram_plot = self._plot_histogram(series, summary)
        boxplot_plot = self._plot_box_by_churn(retained, churned)

        # --- narrations ---
        dist_narr = self.narrator.distribution_shape(
            summary["skewness"], summary["mean"], summary["median"]
        )
        mw_narr = self.narrator.mann_whitney(p_value, u_stat)
        d_narr = self.narrator.cohens_d(cohens_d)
        outlier_narr = self.narrator.outliers(
            outlier_stats["outlier_count"],
            int(series.count()),
            summary["p99"],
            summary["max"],
        )
        box_narr = self.narrator.boxplot_by_churn(
            split_stats["retained"]["median"],
            split_stats["churned"]["median"],
            cohens_d,
        )

        # --- recommendation ---
        recommendation = self._build_recommendation(
            cohens_d, p_value, summary["skewness"], outlier_stats["outlier_pct"]
        )

        return {
            "type": "numeric",
            "summary_stats": summary,
            "split_by_churn": split_stats,
            "histogram_plot": histogram_plot,
            "boxplot":        boxplot_plot,
            "statistical_tests": {
                "mann_whitney_u": {
                    "statistic":  float(u_stat),
                    "p_value":    float(p_value),
                    "narration":  mw_narr,
                },
                "cohens_d": {
                    "value":     cohens_d,
                    "narration": d_narr,
                },
            },
            "outliers": {**outlier_stats, "narration": outlier_narr},
            "narrative": {
                "distribution": dist_narr,
                "boxplot":      box_narr,
                "separation":   f"{mw_narr} {d_narr}",
                "outliers":     outlier_narr,
            },
            "recommendation": recommendation,
        }

    # ---------- statistics ----------

    @staticmethod
    def _compute_summary(series: pd.Series) -> Dict[str, float]:
        return {
            "count":     int(series.count()),
            "mean":      float(series.mean()),
            "median":    float(series.median()),
            "std":       float(series.std(ddof=1)) if len(series) > 1 else 0.0,
            "min":       float(series.min()),
            "p1":        float(series.quantile(0.01)),
            "p5":        float(series.quantile(0.05)),
            "p25":       float(series.quantile(0.25)),
            "p75":       float(series.quantile(0.75)),
            "p95":       float(series.quantile(0.95)),
            "p99":       float(series.quantile(0.99)),
            "max":       float(series.max()),
            "skewness":  float(series.skew()),
            "kurtosis":  float(series.kurtosis()),
        }

    @staticmethod
    def _cohens_d(retained: pd.Series, churned: pd.Series) -> float:
        """Cohen's d with pooled std. Positive = churners have higher values."""
        n1, n2 = len(retained), len(churned)
        if n1 < 2 or n2 < 2:
            return 0.0
        m1, m2 = retained.mean(), churned.mean()
        s1, s2 = retained.std(ddof=1), churned.std(ddof=1)
        pooled = np.sqrt(((n1 - 1) * s1 ** 2 + (n2 - 1) * s2 ** 2) / (n1 + n2 - 2))
        return float((m2 - m1) / pooled) if pooled > 0 else 0.0

    @staticmethod
    def _outlier_report(series: pd.Series, summary: Dict[str, float]) -> Dict:
        q1, q3 = summary["p25"], summary["p75"]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        mask = (series < lower_bound) | (series > upper_bound)
        count = int(mask.sum())
        total = int(series.count())
        pct = float(count / total * 100) if total else 0.0
        return {
            "method":           "IQR (1.5 × IQR fence)",
            "iqr_lower_bound":  float(lower_bound),
            "iqr_upper_bound":  float(upper_bound),
            "outlier_count":    count,
            "outlier_pct":      pct,
        }

    # ---------- plotting ----------

    def _plot_histogram(
        self,
        series: pd.Series,
        summary: Dict[str, float],
    ) -> Dict[str, str]:
        feature = self.feature_name
        colors = self.plot_saver.COLORS

        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.hist(
            series,
            bins=40,
            color=colors["blue"],
            alpha=0.75,
            edgecolor=colors["panel"],
            linewidth=0.6,
        )
        ax.axvline(
            summary["mean"], color=colors["orange"], linestyle="--", linewidth=1.2,
            label=f"Mean = {summary['mean']:.2f}",
        )
        ax.axvline(
            summary["median"], color=colors["purple"], linestyle="--", linewidth=1.2,
            label=f"Median = {summary['median']:.2f}",
        )
        ax.set_xlabel(feature)
        ax.set_ylabel("count")
        ax.set_title(f"{feature} — distribution (skew = {summary['skewness']:+.2f})")
        ax.legend(loc="upper right")

        caption = (
            f"Histogram of {feature} across {summary['count']:,} customers. "
            f"Orange = mean, purple = median. The gap between them confirms skew direction: "
            f"mean > median ⇒ right-skewed, mean < median ⇒ left-skewed."
        )
        path = self.plot_saver.save(fig, f"num__{feature}__histogram")
        self.manifest.add(path, caption, feature, "histogram", "numeric")
        return {"path": path, "caption": caption}

    def _plot_box_by_churn(
        self,
        retained: pd.Series,
        churned: pd.Series,
    ) -> Dict[str, str]:
        feature = self.feature_name
        colors = self.plot_saver.COLORS

        fig, ax = plt.subplots(figsize=(6, 4.8))
        bp = ax.boxplot(
            [retained, churned],
            tick_labels=["Retained (0)", "Churned (1)"],
            patch_artist=True,
            widths=0.55,
            medianprops={"color": colors["text"], "linewidth": 1.6},
            flierprops={
                "marker": "o", "markersize": 3,
                "markerfacecolor": colors["text_dim"],
                "markeredgecolor": colors["text_dim"], "alpha": 0.5,
            },
            whiskerprops={"color": colors["text_dim"]},
            capprops={"color": colors["text_dim"]},
        )
        # Fill each box with its churn-class colour.
        for patch, color in zip(bp["boxes"], [colors["green"], colors["red"]]):
            patch.set_facecolor(color)
            patch.set_edgecolor(color)
            patch.set_alpha(0.65)

        ax.set_ylabel(feature)
        ax.set_title(f"{feature} — distribution split by churn")

        caption = (
            "Box plot reads as: box = 25th–75th percentile (the middle 50%), line inside box "
            "= median, whiskers extend to 1.5 × IQR, dots beyond = outliers. Large vertical "
            "separation between the two medians = strong churn predictor. "
            "Box overlap that is still high means the feature alone does NOT distinguish the two groups."
        )
        path = self.plot_saver.save(fig, f"num__{feature}__boxplot_churn")
        self.manifest.add(path, caption, feature, "boxplot_by_churn", "numeric")
        return {"path": path, "caption": caption}

    # ---------- recommendation logic ----------

    def _build_recommendation(
        self,
        cohens_d: float,
        p_value: float,
        skewness: float,
        outlier_pct: float,
    ) -> Dict:
        abs_d = abs(cohens_d)
        abs_skew = abs(skewness)

        # Signal strength: combine effect size with significance.
        if abs_d >= self.D_STRONG and p_value < 0.01:
            signal = "strong"
        elif abs_d >= self.D_MODERATE and p_value < 0.01:
            signal = "moderate"
        elif abs_d >= self.D_WEAK and p_value < 0.05:
            signal = "weak"
        else:
            signal = "negligible"

        # Transform suggestion based on skewness + outlier mass.
        if abs_skew >= self.SKEW_HEAVY:
            transform = "log1p"
            transform_reason = f"heavy skew (skewness = {skewness:+.2f})"
        elif abs_skew >= self.SKEW_MODERATE and outlier_pct >= 5:
            transform = "log1p (optional)"
            transform_reason = "moderate skew combined with substantial outlier tail"
        else:
            transform = "none"
            transform_reason = "distribution close to symmetric OR outlier mass minimal"

        # Outlier-handling hint.
        if outlier_pct < 1:
            outlier_action = "no additional outlier treatment"
        elif outlier_pct < 5:
            outlier_action = "Stage 1 clip is sufficient; trees handle raw values fine"
        else:
            outlier_action = "consider p99 cap or log transform in Stage 3.2"

        action = "keep_low_priority" if signal == "negligible" else "keep"

        return {
            "action":          action,
            "signal_strength": signal,
            "transform":       transform,
            "transform_reason": transform_reason,
            "outlier_action":  outlier_action,
            "rationale": (
                f"Signal: {signal} (|Cohen's d| = {abs_d:.2f}, p = {p_value:.4g}). "
                f"Transform: {transform} — {transform_reason}. "
                f"Outlier handling: {outlier_action}."
            ),
            "engineering_ideas": [],
        }
