"""
cohorts.py — churn rate sliced by meaningful customer segments.

Takes a declarative list of cohort specs and produces:
    1. A bar chart of churn rate per cohort bucket (per spec)
    2. A list of "hotspot" cohorts where churn rate > HOTSPOT_LIFT × overall
    3. A recommendation: which cohorts warrant their own binary feature flag

This formalises Marco's idea of "look at each category vs churn" as a data
structure: future cohorts get added to the SPEC list, no code change required.

Default specs cover:
    - tenure buckets (seeds tenure_segment ordinal in Stage 3.2)
    - city_tier
    - preferred_payment
Stage 3.2 reads the hotspot list and auto-generates candidate flag features.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.ml.eda.reporting.manifest_writer import ManifestWriter
from src.ml.eda.reporting.plot_saver import PlotSaver
from src.ml.eda.utils.signal_logger import SignalLogger


class CohortAnalyzer(SignalLogger):
    """Run churn analysis across multiple cohort definitions."""

    HOTSPOT_LIFT: float = 2.0  # churn rate > overall × this → hotspot cohort

    @staticmethod
    def _format_key(value) -> str:
        """Clean category-key render: 3.0 → '3', True → '1', otherwise str(value)."""
        if isinstance(value, bool):
            return str(int(value))
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    # Default cohort specs — Marco can override at construction time.
    DEFAULT_SPECS: List[Dict] = [
        {
            "name":    "tenure_bucket",
            "type":    "numeric_bucket",
            "feature": "tenure_months",
            "bins":    [-0.1, 6, 24, 10_000],
            "labels":  ["new (0–6mo)", "growing (6–24mo)", "loyal (24+mo)"],
            "purpose": "Feeds the ordinal `tenure_segment` feature in Stage 3.2.",
        },
        {
            "name":    "city_tier",
            "type":    "categorical",
            "feature": "city_tier",
            "purpose": "Tier-1/2/3 cities often have different churn dynamics.",
        },
        {
            "name":    "preferred_payment_mode",
            "type":    "categorical",
            "feature": "preferred_payment_mode",
            "purpose": "COD users are a known higher-churn segment.",
        },
    ]

    def __init__(
        self,
        df: pd.DataFrame,
        target_col: str,
        plot_saver: PlotSaver,
        manifest: ManifestWriter,
        cohort_specs: Optional[List[Dict]] = None,
    ) -> None:
        super().__init__()
        self.df = df
        self.target_col = target_col
        self.plot_saver = plot_saver
        self.manifest = manifest
        self.specs = cohort_specs if cohort_specs is not None else self.DEFAULT_SPECS

    def analyze(self) -> Dict:
        overall_rate = float(self.df[self.target_col].mean())
        cohorts: List[Dict] = []
        all_hotspots: List[Dict] = []

        for spec in self.specs:
            feature = spec.get("feature")
            if feature not in self.df.columns:
                self._warn(f"cohort '{spec.get('name')}': feature '{feature}' not in DataFrame — skipping")
                continue

            self._signal("Analyzing", f"cohort: {spec['name']}")
            cohort_result = self._analyze_single(spec, overall_rate)
            cohorts.append(cohort_result)
            all_hotspots.extend([
                {**h, "cohort_name": spec["name"]}
                for h in cohort_result["hotspots"]
            ])

        return {
            "overall_churn_rate": overall_rate,
            "hotspot_lift":       self.HOTSPOT_LIFT,
            "cohorts":            cohorts,
            "all_hotspots":       all_hotspots,
            "narrative":          self._build_narrative(cohorts, all_hotspots),
            "recommendation":     self._build_recommendation(all_hotspots),
        }

    def save(self, output_path: str | Path) -> Dict:
        result = self.analyze()
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, default=str)
        self._signal(
            "Writing to", str(output_path),
            f"{len(result['cohorts'])} cohorts  |  hotspots={len(result['all_hotspots'])}",
        )
        return result

    # ---------- per-spec analysis ----------

    def _analyze_single(self, spec: Dict, overall_rate: float) -> Dict:
        feature = spec["feature"]
        target = self.target_col

        if spec["type"] == "numeric_bucket":
            buckets = pd.cut(
                self.df[feature],
                bins=spec["bins"],
                labels=spec["labels"],
                include_lowest=True,
            )
            grouped = self.df.assign(_bucket=buckets).groupby("_bucket", observed=False)
        elif spec["type"] == "categorical":
            grouped = self.df.groupby(self.df[feature].astype(object))
        else:
            raise ValueError(f"Unknown cohort type: {spec['type']}")

        stats = grouped[target].agg(count="count", churned="sum", churn_rate="mean")
        stats = stats.dropna(subset=["churn_rate"])

        buckets_out: Dict[str, Dict] = {}
        hotspots: List[Dict] = []
        for bucket_name, row in stats.iterrows():
            count = int(row["count"])
            if count == 0:
                continue
            churn_rate = float(row["churn_rate"])
            lift = churn_rate / overall_rate if overall_rate > 0 else 0
            bucket_info = {
                "count":      count,
                "churned":    int(row["churned"]),
                "retained":   count - int(row["churned"]),
                "churn_rate": churn_rate,
                "lift":       round(lift, 2),
            }
            buckets_out[self._format_key(bucket_name)] = bucket_info
            if lift > self.HOTSPOT_LIFT:
                hotspots.append({
                    "bucket":     self._format_key(bucket_name),
                    "churn_rate": churn_rate,
                    "lift":       round(lift, 2),
                    "count":      count,
                })

        plot_info = self._plot_cohort(spec, buckets_out, overall_rate)

        return {
            "name":     spec["name"],
            "feature":  feature,
            "type":     spec["type"],
            "purpose":  spec.get("purpose", ""),
            "buckets":  buckets_out,
            "hotspots": hotspots,
            "plot":     plot_info,
        }

    # ---------- plot ----------

    def _plot_cohort(
        self,
        spec: Dict,
        buckets: Dict[str, Dict],
        overall_rate: float,
    ) -> Dict:
        colors = self.plot_saver.COLORS
        name = spec["name"]
        if not buckets:
            # Empty cohort — produce a placeholder and return.
            fig, ax = plt.subplots(figsize=(5, 3))
            ax.text(0.5, 0.5, "no data", ha="center", va="center", color=colors["text_dim"])
            ax.set_axis_off()
            caption = f"{name}: no buckets produced."
            path = self.plot_saver.save(fig, f"cohort__{name}__empty")
            self.manifest.add(path, caption, None, "cohort_bar", "global")
            return {"path": path, "caption": caption}

        # Preserve the bucket order as provided (important for tenure_bucket).
        labels = list(buckets.keys())
        rates = [buckets[k]["churn_rate"] * 100 for k in labels]
        counts = [buckets[k]["count"] for k in labels]

        # Colour hotspot bars red, normals blue.
        bar_colors = [
            colors["red"] if r > overall_rate * 100 * self.HOTSPOT_LIFT else colors["blue"]
            for r in rates
        ]

        fig, ax = plt.subplots(figsize=(max(6, len(labels) * 0.9), 4.5))
        x = np.arange(len(labels))
        bars = ax.bar(x, rates, color=bar_colors, edgecolor=colors["panel"])

        # Overall-rate reference line
        ax.axhline(
            overall_rate * 100,
            color=colors["orange"], linestyle="--", linewidth=1.2,
            label=f"Overall ({overall_rate*100:.1f}%)",
        )
        # Hotspot threshold
        ax.axhline(
            overall_rate * 100 * self.HOTSPOT_LIFT,
            color=colors["red"], linestyle=":", linewidth=1,
            label=f"Hotspot threshold ({self.HOTSPOT_LIFT}×)",
        )

        # Annotate each bar with churn % and count
        for bar, rate, count in zip(bars, rates, counts):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.5,
                f"{rate:.1f}%\nn={count:,}",
                ha="center", va="bottom",
                color=colors["text_dim"], fontsize=9,
            )

        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=20, ha="right")
        ax.set_ylabel("churn rate (%)")
        ax.set_title(f"Cohort: {name} — churn rate by bucket")
        ax.legend(loc="upper right")
        # Give headroom for the annotations
        ax.set_ylim(0, max(rates + [overall_rate * 100 * 1.2]) * 1.25)

        caption = (
            f"Churn rate per bucket of {spec['feature']}. Orange dashed = overall rate "
            f"({overall_rate*100:.1f}%). Red dotted = {self.HOTSPOT_LIFT}× hotspot threshold. "
            "Red bars have cleared the hotspot line — candidate segments for a binary flag in Stage 3.2."
        )
        path = self.plot_saver.save(fig, f"cohort__{name}")
        self.manifest.add(path, caption, None, "cohort_bar", "global")
        return {"path": path, "caption": caption}

    # ---------- narrative + recommendation ----------

    def _build_narrative(self, cohorts: List[Dict], all_hotspots: List[Dict]) -> str:
        if not cohorts:
            return "No cohorts could be analyzed (missing features in DataFrame)."

        parts = [f"Analysed {len(cohorts)} cohort definition(s)."]
        if all_hotspots:
            examples = [
                f"{h['cohort_name']}='{h['bucket']}' ({h['churn_rate']*100:.1f}%, {h['lift']:.2f}×)"
                for h in all_hotspots[:4]
            ]
            parts.append(
                f"Found {len(all_hotspots)} hotspot bucket(s) exceeding "
                f"{self.HOTSPOT_LIFT}× the baseline churn rate: "
                f"{'; '.join(examples)}."
            )
        else:
            parts.append(
                "No buckets cleared the hotspot threshold — churn is distributed fairly "
                "evenly across the analysed cohorts."
            )
        return " ".join(parts)

    def _build_recommendation(self, all_hotspots: List[Dict]) -> Dict:
        if not all_hotspots:
            return {
                "flag_candidates": [],
                "rationale": (
                    "No cohort hotspots identified. No cohort-derived binary flags "
                    "recommended for Stage 3.2."
                ),
            }

        flag_candidates = [
            {
                "cohort":      h["cohort_name"],
                "bucket":      h["bucket"],
                "churn_rate":  h["churn_rate"],
                "lift":        h["lift"],
                "count":       h["count"],
                "suggested_feature_name": f"is_{h['cohort_name']}_{h['bucket']}".replace(" ", "_").lower(),
            }
            for h in all_hotspots
        ]
        return {
            "flag_candidates": flag_candidates,
            "rationale": (
                f"{len(flag_candidates)} hotspot cohorts identified. Each warrants a "
                "binary flag feature in Stage 3.2 — the model will learn a much sharper "
                "decision boundary than with the underlying categorical column alone."
            ),
        }
