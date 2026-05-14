"""
base.py — abstract base class for per-feature analyzers.

Every feature in the training DataFrame goes through a subclass of
FeatureAnalyzer. The base defines the common protocol:
    1. analyze()  — run statistical analysis, produce plots, return a dict
    2. save()     — write that dict to reports/features/<feature_name>.json

Subclasses implement _analyze() and are free to define their own plot methods.
Both CategoricalAnalyzer and NumericAnalyzer conform to this contract so the
orchestrator can iterate over a heterogeneous list of feature names and call
.save() on each without knowing which type it is.

The JSON each analyzer produces has a common top-level shape:
    {
        "feature_name":       "<col>",
        "analyzer":           "CategoricalAnalyzer" | "NumericAnalyzer",
        "type":               "categorical" | "numeric",
        ...type-specific keys...
        "recommendation":     { "action": "keep" | "..." , "rationale": "...", ... }
    }
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from src.ml.eda.reporting.manifest_writer import ManifestWriter
from src.ml.eda.reporting.narrator import StatisticalNarrator
from src.ml.eda.reporting.plot_saver import PlotSaver
from src.ml.eda.utils.signal_logger import SignalLogger


class FeatureAnalyzer(SignalLogger):
    """Abstract base for per-feature analyzers.

    Holds references to shared services (plot saver, narrator, manifest) so that
    every analyzer emits into the same reports tree with consistent styling and
    terminology.
    """

    def __init__(
        self,
        feature_name: str,
        df: pd.DataFrame,
        target_col: str,
        plot_saver: PlotSaver,
        narrator: StatisticalNarrator,
        manifest: ManifestWriter,
    ) -> None:
        super().__init__()
        self.feature_name = feature_name
        self.df = df
        self.target_col = target_col
        self.plot_saver = plot_saver
        self.narrator = narrator
        self.manifest = manifest

    # ---------- public protocol ----------

    def analyze(self) -> Dict:
        """Run subclass analysis, stamp metadata, return the result dict."""
        self._signal("Analyzing", self.feature_name)
        result = self._analyze()
        result["feature_name"] = self.feature_name
        result["analyzer"] = self.__class__.__name__
        return result

    def save(self, output_dir: str | Path) -> Dict:
        """Analyze + write <feature>.json to output_dir. Returns the result."""
        result = self.analyze()
        output_path = Path(output_dir) / f"{self.feature_name}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, default=str)

        action = result.get("recommendation", {}).get("action", "n/a")
        self._signal(
            "Writing to",
            str(output_path),
            f"action={action}",
        )
        return result

    # ---------- for subclasses ----------

    def _analyze(self) -> Dict:
        """Subclasses must implement — return the analysis result dict."""
        raise NotImplementedError("Subclasses must implement _analyze()")

    def _churn_split(self) -> Tuple[pd.Series, pd.Series]:
        """Return (retained_values, churned_values) for this feature."""
        retained = self.df.loc[self.df[self.target_col] == 0, self.feature_name]
        churned  = self.df.loc[self.df[self.target_col] == 1, self.feature_name]
        return retained, churned
