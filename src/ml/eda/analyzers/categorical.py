"""
categorical.py — deep analysis for a single categorical feature.

Produces, for one column:
    1. Category distribution (pie if 2-cat, horizontal bar if 3+)
    2. 100% stacked bar of churn vs retained per category (the signal plot)
    3. Chi-squared test vs churn (with expected-frequency reliability flag)
    4. Cramér's V effect size
    5. Rule-based recommendation for Stage 3.2:
         - 'engineer_ordinal'   — strong association, build ordinal by churn rate
         - 'keep_one_hot'       — default; add hotspot flags if any
         - plus a list of hotspot categories (churn rate > 1.5× overall)

The heart is the 100% stacked bar: each category reaches 100% and the red
portion's length = churn rate inside that category. An orange dashed line at
the overall retention rate makes hotspots jump out visually.
"""
from __future__ import annotations
import warnings

from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency

from src.ml.eda.analyzers.base import FeatureAnalyzer


class CategoricalAnalyzer(FeatureAnalyzer):
    """Per-feature analyzer for categorical columns."""

    PIE_MAX_CATEGORIES: int = 2        # up to 2 cats → pie; 3+ → horizontal bar
    HOTSPOT_LIFT: float = 1.5          # churn rate > overall × this → hotspot
    MIN_EXPECTED_FREQ: int = 5         # chi-squared reliability threshold

    @staticmethod
    def _format_key(value) -> str:
        """Render a category value as a clean string.

        Handles pandas' habit of upcasting int group-keys to float64 (so 3 → 3.0).
        Also normalises bool → '0'/'1' for display consistency.
        """
        if isinstance(value, bool):
            return str(int(value))
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    def _analyze(self) -> Dict:
        feature = self.feature_name
        target = self.target_col
        df = self.df

        # --- distribution ---
        vc = df[feature].astype(object).value_counts(dropna=False)
        total = int(vc.sum())
        distribution = [
            {"category": self._format_key(cat), "count": int(n), "pct": float(n / total)}
            for cat, n in vc.items()
        ]

        # --- per-category churn ---
        per_cat = (
            df.groupby(df[feature].astype(object))[target]
              .agg(count="count", churned="sum", churn_rate="mean")
              .reset_index()
        )
        per_cat_dict: Dict[str, Dict] = {
            self._format_key(row[feature]): {
                "count":      int(row["count"]),
                "churned":    int(row["churned"]),
                "retained":   int(row["count"] - row["churned"]),
                "churn_rate": float(row["churn_rate"]),
            }
            for _, row in per_cat.iterrows()
        }
        overall_rate = float(df[target].mean())

        # --- statistical tests ---
        contingency = pd.crosstab(df[feature].astype(object), df[target])
        chi2, p_value, dof, expected = chi2_contingency(contingency)
        chi_sq_reliable = bool((expected >= self.MIN_EXPECTED_FREQ).all())

        n = int(contingency.values.sum())
        min_dim = min(contingency.shape) - 1
        cramers_v = float(np.sqrt(chi2 / (n * min_dim))) if min_dim > 0 and n > 0 else 0.0

        # --- plots ---
        distribution_plot = self._plot_distribution(vc)
        stacked_plot = self._plot_stacked_churn(per_cat_dict, overall_rate)

        # --- narrations ---
        chi_narr = self.narrator.chi_squared(p_value, chi2, dof)
        if not chi_sq_reliable:
            chi_narr += (
                " ⚠ Some expected frequencies are below 5 — the chi-squared p-value "
                "is approximate here; treat it as directional rather than exact."
            )
        v_narr = self.narrator.cramers_v(cramers_v)
        rate_narr = self.narrator.category_churn_rates(
            {k: v["churn_rate"] for k, v in per_cat_dict.items()},
            overall_rate,
            hotspot_lift=self.HOTSPOT_LIFT,
        )

        dist_narr = (
            f"'{feature}' has {len(vc)} category/ies across {total:,} customers. "
            f"Most common: '{vc.index[0]}' ({vc.iloc[0]/total*100:.1f}%)."
            + (
                f" Least common: '{vc.index[-1]}' ({vc.iloc[-1]/total*100:.1f}%)."
                if len(vc) > 1 else ""
            )
        )

        # --- recommendation ---
        recommendation = self._build_recommendation(
            per_cat_dict, overall_rate, cramers_v, p_value
        )

        return {
            "type": "categorical",
            "n_categories": len(vc),
            "distribution": distribution,
            "distribution_plot": distribution_plot,
            "per_category_churn": per_cat_dict,
            "stacked_churn_plot": stacked_plot,
            "overall_churn_rate": overall_rate,
            "statistical_tests": {
                "chi_squared": {
                    "statistic":    float(chi2),
                    "p_value":      float(p_value),
                    "dof":          int(dof),
                    "reliable":     chi_sq_reliable,
                    "narration":    chi_narr,
                },
                "cramers_v": {
                    "value":     cramers_v,
                    "narration": v_narr,
                },
            },
            "narrative": {
                "distribution": dist_narr,
                "churn_rates":  rate_narr,
                "association":  f"{chi_narr} {v_narr}",
            },
            "recommendation": recommendation,
        }

    # ---------- plotting ----------

    def _plot_distribution(self, vc: pd.Series) -> Dict[str, str]:
        feature = self.feature_name
        n_cats = len(vc)
        colors = self.plot_saver.COLORS

        if n_cats <= self.PIE_MAX_CATEGORIES:
            fig, ax = plt.subplots(figsize=(5, 5))
            # Use distinct colours per slice.
            slice_colors = [colors["blue"], colors["purple"]][:n_cats]
            ax.pie(
                vc.values,
                labels=[self._format_key(x) for x in vc.index],
                autopct="%1.1f%%",
                colors=slice_colors,
                startangle=90,
                textprops={"color": colors["text"], "fontsize": 10},
                wedgeprops={"edgecolor": colors["panel"], "linewidth": 1.5},
            )
            ax.set_title(f"{feature} — category distribution")
        else:
            fig, ax = plt.subplots(figsize=(8, max(4, n_cats * 0.45)))
            # Reverse so largest category sits at the top.
            ax.barh(
                [self._format_key(x) for x in vc.index][::-1],
                vc.values[::-1],
                color=colors["blue"],
                edgecolor=colors["panel"],
            )
            ax.set_xlabel("count")
            ax.set_title(f"{feature} — category distribution")

        caption = (
            f"Distribution of {n_cats} categories across {int(vc.sum()):,} customers. "
            f"{'Pie' if n_cats <= self.PIE_MAX_CATEGORIES else 'Horizontal bar'} chart chosen "
            f"for readability at this category count."
        )
        path = self.plot_saver.save(fig, f"cat__{feature}__distribution")
        self.manifest.add(path, caption, feature, "distribution", "categorical")
        return {"path": path, "caption": caption}

    def _plot_stacked_churn(
        self,
        per_cat: Dict[str, Dict],
        overall_rate: float,
    ) -> Dict[str, str]:
        feature = self.feature_name
        colors = self.plot_saver.COLORS

        # Sort categories by churn rate descending so the worst is at the top.
        sorted_cats = sorted(per_cat.items(), key=lambda kv: -kv[1]["churn_rate"])
        cats = [c for c, _ in sorted_cats]
        retained_pct = [per_cat[c]["retained"] / per_cat[c]["count"] * 100 for c in cats]
        churn_pct    = [per_cat[c]["churn_rate"] * 100 for c in cats]

        fig, ax = plt.subplots(figsize=(9, max(4, len(cats) * 0.5)))
        y = np.arange(len(cats))
        ax.barh(y, retained_pct, label="Retained (0)", color=colors["green"])
        ax.barh(y, churn_pct, left=retained_pct, label="Churned (1)", color=colors["red"])

        # Overall-retention reference line — crossing to its right = hotspot.
        ax.axvline(
            (1 - overall_rate) * 100,
            color=colors["orange"],
            linestyle="--",
            linewidth=1.2,
            label=f"Overall retention ({(1-overall_rate)*100:.1f}%)",
        )

        # Inline churn-rate labels on each bar for quick reading.
        for i, c in enumerate(cats):
            rate_text = f"{per_cat[c]['churn_rate']*100:.1f}%"
            ax.text(
                101, i, rate_text, va="center", ha="left",
                color=colors["text_dim"], fontsize=9,
            )

        ax.set_yticks(y)
        ax.set_yticklabels(cats)
        ax.set_xlim(0, 108)   # leave room for the inline labels
        ax.set_xlabel("percentage of customers in category")
        ax.set_title(f"{feature} — churn rate by category (100% stacked)")
        ax.legend(loc="lower right")
        ax.invert_yaxis()     # highest-churn category at the top

        caption = (
            "Each bar spans 100% of customers inside that category. Red = churn rate. "
            "Orange dashed line = overall retention baseline — any category whose red "
            "segment extends PAST this line churns more than average (a hotspot)."
        )
        path = self.plot_saver.save(fig, f"cat__{feature}__churn_stacked")
        self.manifest.add(path, caption, feature, "churn_stacked_bar", "categorical")
        return {"path": path, "caption": caption}

    # ---------- recommendation logic ----------

    def _build_recommendation(
        self,
        per_cat: Dict[str, Dict],
        overall_rate: float,
        cramers_v: float,
        p_value: float,
    ) -> Dict:
        hotspots: List[str] = [
            cat for cat, stats in per_cat.items()
            if stats["churn_rate"] > overall_rate * self.HOTSPOT_LIFT
        ]

        engineering_ideas: List[str] = []
        hotspot_flag_text = (
            f"Create binary flag(s) for hotspot categories: {', '.join(hotspots)}."
            if hotspots else ""
        )

        if p_value >= 0.05:
            action = "keep_one_hot"
            rationale = (
                f"No statistically significant association with churn (p = {p_value:.3f}). "
                "Keep via one-hot for completeness, but do not expect strong predictive signal."
            )
        elif cramers_v >= 0.3:
            action = "engineer_ordinal"
            rationale = (
                f"Strong association (Cramér's V = {cramers_v:.3f}). "
                "Ordinal encoding by historical churn rate will give tree models a dense "
                "monotonic signal instead of sparse one-hot columns."
            )
            engineering_ideas.append(
                "Stage 3.2: map each category to its churn rate (from this EDA) as an ordinal column."
            )
            if hotspot_flag_text:
                engineering_ideas.append(hotspot_flag_text)
        elif cramers_v >= 0.1:
            action = "keep_one_hot"
            rationale = (
                f"Moderate association (Cramér's V = {cramers_v:.3f}). "
                "One-hot is sufficient; tree models will pick up the interactions."
            )
            if hotspot_flag_text:
                engineering_ideas.append(hotspot_flag_text)
        else:
            action = "keep_one_hot"
            rationale = (
                f"Weak association (Cramér's V = {cramers_v:.3f}). "
                "One-hot encode for completeness but deprioritise in feature selection."
            )

        return {
            "action":             action,
            "rationale":          rationale,
            "engineering_ideas":  engineering_ideas,
            "hotspot_categories": hotspots,
            "hotspot_lift_used":  self.HOTSPOT_LIFT,
        }
