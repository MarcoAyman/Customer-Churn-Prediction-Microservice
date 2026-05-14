"""
narrator.py — translates raw statistical results into human-readable prose.

This is the "teaching" class. Every analyzer feeds its raw numbers
(p-values, effect sizes, skewness, outlier counts) into StatisticalNarrator
and gets back a plain-English interpretation. The text is saved into the
feature's JSON and rendered under the relevant plot in the dashboard.

Why a separate class: keeping narration logic in ONE place means terminology
and thresholds are consistent across every feature. Change "moderate" to
"medium" here, and every feature in the report updates.

Stateless. All methods are @classmethod — feel free to call directly.

Convention bands used throughout:
    Cohen's d       <0.2 negligible | <0.5 small | <0.8 medium | else large
    Cramér's V      <0.1 weak       | <0.3 moderate          | else strong
    p-value         <1e-4 highly sig | <0.01 significant | <0.05 marginal | else n.s.
"""
from __future__ import annotations

from typing import Dict, List, Tuple


class StatisticalNarrator:
    """Pure translator from statistical results to explanatory prose."""

    # (upper_bound_exclusive, label) — last tuple catches everything above.
    COHEN_D_BANDS: List[Tuple[float, str]] = [
        (0.2, "negligible"),
        (0.5, "small"),
        (0.8, "medium"),
        (float("inf"), "large"),
    ]

    CRAMERS_V_BANDS: List[Tuple[float, str]] = [
        (0.1, "weak"),
        (0.3, "moderate"),
        (float("inf"), "strong"),
    ]

    # ---------- effect-size interpretation ----------

    @classmethod
    def _band_label(cls, value: float, bands: List[Tuple[float, str]]) -> str:
        """Return the label whose upper bound value falls below."""
        abs_v = abs(value)
        for upper, label in bands:
            if abs_v < upper:
                return label
        return bands[-1][1]

    @classmethod
    def cohens_d(cls, d: float) -> str:
        magnitude = cls._band_label(d, cls.COHEN_D_BANDS)
        if d == 0:
            direction = "no mean difference between groups"
        elif d > 0:
            direction = "values are higher in churners (positive shift)"
        else:
            direction = "values are higher in retained customers (negative shift)"
        return f"Cohen's d = {d:+.2f} — {magnitude} effect size. {direction.capitalize()}."

    @classmethod
    def cramers_v(cls, v: float) -> str:
        magnitude = cls._band_label(v, cls.CRAMERS_V_BANDS)
        return f"Cramér's V = {v:.3f} — {magnitude} association with churn."

    # ---------- significance-test interpretation ----------

    @classmethod
    def _p_verdict(cls, p: float) -> str:
        if p < 1e-4:
            return "Highly significant (p < 0.0001)"
        if p < 0.01:
            return f"Significant (p = {p:.4f})"
        if p < 0.05:
            return f"Marginally significant (p = {p:.4f})"
        return f"Not statistically significant (p = {p:.3f})"

    @classmethod
    def mann_whitney(cls, p_value: float, u_stat: float) -> str:
        verdict = cls._p_verdict(p_value)
        return (
            f"Mann-Whitney U = {u_stat:,.0f}. {verdict}. "
            f"This non-parametric test asks whether churn and retained customers "
            f"have the same distribution for this feature."
        )

    @classmethod
    def chi_squared(cls, p_value: float, statistic: float, dof: int) -> str:
        verdict = cls._p_verdict(p_value)
        return (
            f"χ² = {statistic:.2f} on {dof} df. {verdict}. "
            f"Tests whether the category proportions differ between churn and retained."
        )

    # ---------- distribution interpretation ----------

    @classmethod
    def distribution_shape(
        cls,
        skewness: float,
        mean: float,
        median: float,
    ) -> str:
        """Describe distribution shape from skewness + mean/median gap."""
        abs_skew = abs(skewness)
        if abs_skew < 0.5:
            shape = "approximately symmetric"
        elif abs_skew < 1.0:
            direction = "right" if skewness > 0 else "left"
            shape = f"moderately {direction}-skewed (a noticeable {direction}-side tail)"
        else:
            direction = "right" if skewness > 0 else "left"
            shape = (
                f"strongly {direction}-skewed — most values clustered on the "
                f"{'low' if direction == 'right' else 'high'} end with a long "
                f"{direction}-side tail"
            )

        if abs_skew < 0.3:
            comment = ""
        elif skewness > 0:
            comment = f" Mean ({mean:.2f}) > median ({median:.2f}) — confirms the right skew."
        else:
            comment = f" Mean ({mean:.2f}) < median ({median:.2f}) — confirms the left skew."

        return f"Distribution is {shape} (skewness = {skewness:+.2f}).{comment}"

    @classmethod
    def outliers(
        cls,
        outlier_count: int,
        total_n: int,
        p99_value: float,
        max_value: float,
    ) -> str:
        pct = (outlier_count / total_n * 100) if total_n else 0.0
        if pct < 1:
            severity = "minimal"
            action = (
                "No additional outlier treatment needed beyond Stage 1's sanity clip. "
                "Raw values are safe to feed into any model family."
            )
        elif pct < 5:
            severity = "moderate"
            action = (
                "Tree-based models (XGBoost, LightGBM) handle this natively — no action needed. "
                "For scale-sensitive models (MLP, Logistic Regression), consider log1p transform "
                "at the Stage 3.2 feature-engineering step."
            )
        else:
            severity = "substantial"
            action = (
                "The long tail is material. Recommended: log1p transform, OR cap at the 99th "
                "percentile inside the preprocessing pipeline. Decision deferred to Stage 3.2 "
                "(different treatment per model family may be appropriate)."
            )

        return (
            f"{outlier_count:,} outliers by the IQR rule ({pct:.1f}% of rows). "
            f"p99 = {p99_value:.2f}, max = {max_value:.2f}. "
            f"Severity: {severity}. {action}"
        )

    # ---------- categorical interpretation ----------

    @classmethod
    def category_churn_rates(
        cls,
        per_category_rates: Dict[str, float],
        overall_rate: float,
        hotspot_lift: float = 1.5,
    ) -> str:
        """Narrate the per-category churn rates — highest / lowest / hotspots."""
        if not per_category_rates:
            return "No category data to narrate."

        sorted_cats = sorted(per_category_rates.items(), key=lambda kv: -kv[1])
        highest_cat, highest_rate = sorted_cats[0]
        lowest_cat, lowest_rate = sorted_cats[-1]

        lift_high = (highest_rate / overall_rate) if overall_rate else float("inf")
        lift_low = (lowest_rate / overall_rate) if overall_rate else 0.0

        lines = [
            (
                f"Overall churn rate = {overall_rate*100:.1f}%. "
                f"Highest-churn category: '{highest_cat}' at {highest_rate*100:.1f}% "
                f"({lift_high:.2f}× the baseline). "
                f"Lowest-churn category: '{lowest_cat}' at {lowest_rate*100:.1f}% "
                f"({lift_low:.2f}× the baseline)."
            )
        ]

        hotspots = [
            cat for cat, rate in per_category_rates.items()
            if rate > overall_rate * hotspot_lift
        ]
        if hotspots:
            lines.append(
                f"Hotspot categories (churn rate > {hotspot_lift}× baseline): "
                f"{', '.join(hotspots)}. Strong candidates for a binary flag feature "
                f"in Stage 3.2."
            )

        return " ".join(lines)

    @classmethod
    def boxplot_by_churn(
        cls,
        retained_median: float,
        churned_median: float,
        cohens_d: float,
    ) -> str:
        """Narrate the box plot: median shift + whether separation is visually strong."""
        shift = churned_median - retained_median
        direction = "higher" if shift > 0 else "lower"
        abs_d = abs(cohens_d)

        if abs_d >= 0.8:
            verdict = "A STRONG visual separator of churn."
        elif abs_d >= 0.5:
            verdict = "A moderately strong separator — visible but not dominant."
        elif abs_d >= 0.2:
            verdict = "A weak separator — some shift but substantial overlap between groups."
        else:
            verdict = (
                "No meaningful separation — this feature alone does not distinguish "
                "churners from retained customers."
            )

        return (
            f"Churned median ({churned_median:.2f}) is {abs(shift):.2f} {direction} than "
            f"retained median ({retained_median:.2f}). {verdict}"
        )
