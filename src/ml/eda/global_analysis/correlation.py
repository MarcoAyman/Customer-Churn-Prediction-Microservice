"""
correlation.py — numeric feature redundancy analysis.

Produces:
    1. Pearson correlation heatmap of all numeric features
    2. List of strongly correlated pairs (|r| > configurable threshold)
    3. VIF (Variance Inflation Factor) per feature, computed as diag(inv(corr))
       — no sklearn dependency, just numpy
    4. A drop-candidate recommendation per high-VIF / high-correlation feature

Why VIF on top of pairwise Pearson: a feature can be uncorrelated with any
single other feature yet perfectly predicted by a linear combination of them.
VIF > 10 is the standard cutoff for problematic multicollinearity.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.ml.eda.reporting.manifest_writer import ManifestWriter
from src.ml.eda.reporting.plot_saver import PlotSaver
from src.ml.eda.utils.signal_logger import SignalLogger


class CorrelationAnalyzer(SignalLogger):
    """Pairwise Pearson + VIF for all numeric features."""

    STRONG_CORR: float = 0.85    # |r| above this → recommend dropping one of the pair
    MODERATE_CORR: float = 0.50  # |r| above this → list as 'worth knowing'
    HIGH_VIF: float = 10.0       # VIF above this → flag as multicollinear

    def __init__(
        self,
        df: pd.DataFrame,
        numeric_cols: List[str],
        plot_saver: PlotSaver,
        manifest: ManifestWriter,
    ) -> None:
        super().__init__()
        # Only keep columns that actually exist in df.
        self.numeric_cols = [c for c in numeric_cols if c in df.columns]
        self.df = df[self.numeric_cols].dropna()
        self.plot_saver = plot_saver
        self.manifest = manifest

    def analyze(self) -> Dict:
        self._signal(
            "Analyzing", "numeric correlations",
            f"{len(self.numeric_cols)} features × {len(self.df):,} rows",
        )

        # Pearson correlation matrix
        corr_matrix = self.df.corr(method="pearson")

        # Extract pair list
        pairs = self._extract_pairs(corr_matrix)

        # VIF — diagonal of the inverse of the correlation matrix
        vif_scores = self._compute_vif(corr_matrix)

        # Plot
        heatmap_plot = self._plot_heatmap(corr_matrix)

        # Recommendation
        recommendation = self._build_recommendation(pairs, vif_scores)

        strong_pairs = [p for p in pairs if abs(p["pearson_r"]) >= self.STRONG_CORR]
        moderate_pairs = [
            p for p in pairs
            if self.MODERATE_CORR <= abs(p["pearson_r"]) < self.STRONG_CORR
        ]

        narrative = self._build_narrative(strong_pairs, vif_scores)

        return {
            "features_analyzed":  self.numeric_cols,
            "sample_size":        int(len(self.df)),
            "correlation_matrix": corr_matrix.round(4).to_dict(),
            "heatmap_plot":       heatmap_plot,
            "strong_pairs":       strong_pairs,
            "moderate_pairs":     moderate_pairs,
            "vif_scores":         vif_scores,
            "high_vif_features":  [f for f, v in vif_scores.items() if v and v > self.HIGH_VIF],
            "narrative":          narrative,
            "recommendation":     recommendation,
            "thresholds_used": {
                "strong_correlation": self.STRONG_CORR,
                "moderate_correlation": self.MODERATE_CORR,
                "high_vif":             self.HIGH_VIF,
            },
        }

    def save(self, output_path: str | Path) -> Dict:
        result = self.analyze()
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, default=str)

        n_strong = len(result["strong_pairs"])
        n_high_vif = len(result["high_vif_features"])
        self._signal(
            "Writing to", str(output_path),
            f"strong_pairs={n_strong}  high_vif={n_high_vif}",
        )
        return result

    # ---------- pair extraction ----------

    def _extract_pairs(self, corr_matrix: pd.DataFrame) -> List[Dict]:
        """Extract upper-triangular pairs sorted by |r| descending."""
        pairs = []
        cols = corr_matrix.columns.tolist()
        for i, a in enumerate(cols):
            for b in cols[i + 1:]:
                r = float(corr_matrix.loc[a, b])
                if np.isnan(r):
                    continue
                abs_r = abs(r)
                if abs_r >= self.STRONG_CORR:
                    severity = "high"
                elif abs_r >= self.MODERATE_CORR:
                    severity = "moderate"
                else:
                    severity = "low"
                pairs.append({
                    "feature_a": a,
                    "feature_b": b,
                    "pearson_r": round(r, 4),
                    "abs_r":     round(abs_r, 4),
                    "severity":  severity,
                })
        pairs.sort(key=lambda p: p["abs_r"], reverse=True)
        return pairs

    # ---------- VIF ----------

    def _compute_vif(self, corr_matrix: pd.DataFrame) -> Dict[str, float]:
        """VIF_i = the i-th diagonal element of the inverse correlation matrix."""
        try:
            inv = np.linalg.inv(corr_matrix.values)
            diag = np.diag(inv)
            return {col: float(v) for col, v in zip(corr_matrix.columns, diag)}
        except np.linalg.LinAlgError:
            self._warn(
                "Correlation matrix is singular — perfect multicollinearity detected. "
                "VIF cannot be computed. Inspect strong_pairs for the culprit."
            )
            return {col: None for col in corr_matrix.columns}

    # ---------- plot ----------

    def _plot_heatmap(self, corr_matrix: pd.DataFrame) -> Dict:
        colors = self.plot_saver.COLORS
        n = len(corr_matrix)

        fig, ax = plt.subplots(figsize=(max(7, n * 0.55), max(5.5, n * 0.5)))
        # Build a two-tone diverging colormap between red, panel, and blue.
        from matplotlib.colors import LinearSegmentedColormap
        cmap = LinearSegmentedColormap.from_list(
            "divergent_dark",
            [colors["red"], colors["panel"], colors["blue"]],
            N=256,
        )
        im = ax.imshow(
            corr_matrix.values, cmap=cmap, vmin=-1, vmax=1, aspect="auto",
        )
        cbar = fig.colorbar(im, ax=ax, shrink=0.85)
        cbar.ax.tick_params(colors=colors["text_dim"])
        cbar.outline.set_edgecolor(colors["text_dim"])

        # Ticks
        ax.set_xticks(range(n))
        ax.set_yticks(range(n))
        ax.set_xticklabels(corr_matrix.columns, rotation=60, ha="right", color=colors["text_dim"])
        ax.set_yticklabels(corr_matrix.index, color=colors["text_dim"])

        # Annotate cells (only if the matrix is small enough to read)
        if n <= 14:
            for i in range(n):
                for j in range(n):
                    value = corr_matrix.values[i, j]
                    text_color = colors["text"] if abs(value) > 0.4 else colors["text_dim"]
                    ax.text(
                        j, i, f"{value:.2f}",
                        ha="center", va="center",
                        color=text_color, fontsize=8,
                    )

        ax.set_title("Pearson correlation — numeric features")
        ax.grid(False)

        caption = (
            "Pearson correlation matrix across numeric features. Red = negative correlation, "
            "blue = positive correlation, neutral background = near-zero. "
            "Values inside the cells are the correlation coefficients. "
            "Pairs with |r| ≥ 0.85 are flagged as redundant and are candidates for dropping."
        )
        path = self.plot_saver.save(fig, "global__correlation__heatmap")
        self.manifest.add(path, caption, None, "heatmap", "global")
        return {"path": path, "caption": caption}

    # ---------- narrative + recommendation ----------

    def _build_narrative(
        self,
        strong_pairs: List[Dict],
        vif_scores: Dict[str, float],
    ) -> str:
        parts = []

        if strong_pairs:
            pair_summaries = [
                f"{p['feature_a']}↔{p['feature_b']} (r = {p['pearson_r']:+.2f})"
                for p in strong_pairs[:5]
            ]
            parts.append(
                f"{len(strong_pairs)} feature pair(s) exceed |r| = {self.STRONG_CORR}: "
                f"{'; '.join(pair_summaries)}."
            )
        else:
            parts.append(
                f"No pair exceeds |r| = {self.STRONG_CORR}. Features look pairwise-independent."
            )

        high_vif = [(f, v) for f, v in vif_scores.items() if v and v > self.HIGH_VIF]
        if high_vif:
            high_vif.sort(key=lambda kv: -kv[1])
            vif_summaries = [f"{f} (VIF = {v:.2f})" for f, v in high_vif[:5]]
            parts.append(
                f"{len(high_vif)} feature(s) have VIF > {self.HIGH_VIF}: "
                f"{'; '.join(vif_summaries)}. These are candidates for removal "
                "or combination."
            )
        elif all(v is not None for v in vif_scores.values()):
            max_vif_pair = max(
                ((f, v) for f, v in vif_scores.items() if v is not None),
                key=lambda kv: kv[1],
                default=(None, None),
            )
            if max_vif_pair[1] is not None:
                parts.append(
                    f"All VIF scores below {self.HIGH_VIF} — highest is "
                    f"{max_vif_pair[0]} at {max_vif_pair[1]:.2f}. No multicollinearity concerns."
                )

        return " ".join(parts)

    def _build_recommendation(
        self,
        pairs: List[Dict],
        vif_scores: Dict[str, float],
    ) -> Dict:
        strong_pairs = [p for p in pairs if p["severity"] == "high"]
        drop_candidates: List[Dict] = []

        # For each strong pair, flag the one with HIGHER VIF as the drop candidate
        # (the feature that's more "redundant with the rest" goes).
        seen = set()
        for pair in strong_pairs:
            a, b = pair["feature_a"], pair["feature_b"]
            vif_a = vif_scores.get(a) or 0
            vif_b = vif_scores.get(b) or 0
            drop = a if vif_a >= vif_b else b
            keep = b if drop == a else a
            if drop not in seen:
                drop_candidates.append({
                    "drop":      drop,
                    "keep":      keep,
                    "pearson_r": pair["pearson_r"],
                    "drop_vif":  round(vif_scores.get(drop) or 0, 2),
                    "keep_vif":  round(vif_scores.get(keep) or 0, 2),
                    "reason": (
                        f"Redundant with '{keep}' (r = {pair['pearson_r']:+.2f}); "
                        f"'{drop}' has the higher VIF."
                    ),
                })
                seen.add(drop)

        if drop_candidates:
            rationale = (
                f"{len(drop_candidates)} feature(s) flagged for potential removal due to "
                "strong pairwise correlation. Revisit in Stage 3.2 feature engineering: "
                "the higher-VIF partner of each pair is the suggested drop."
            )
        else:
            rationale = (
                "No drop candidates from correlation analysis. All numeric features carry "
                "independent signal and should be retained going into Stage 3.2."
            )

        return {
            "drop_candidates": drop_candidates,
            "rationale":       rationale,
        }
