"""
eda_orchestrator.py — the single entry point for Stage 3.1 EDA.

SupabaseEDA.run() is what scripts/run_eda.py calls. It:
    1. Loads the training DataFrame from Supabase (TrainingDataLoader)
    2. Runs SanityChecker on the loaded data
    3. Runs global analyses:
         - ClassBalanceAnalyzer      → reports/class_balance.json
         - CorrelationAnalyzer       → reports/correlation.json
         - CohortAnalyzer            → reports/cohorts.json
    4. Runs per-feature analyzers:
         - CategoricalAnalyzer for each categorical col → reports/features/<col>.json
         - NumericAnalyzer for each numeric col        → reports/features/<col>.json
    5. Writes the figure manifest             → reports/figures/manifest.json
    6. Extracts category_churn_rates.json     → reports/category_churn_rates.json
       (consumed by Stage 3.2 feature engineering)
    7. Writes summary.json                    → reports/summary.json
       (the top-level aggregate the dashboard reads first)

Exit codes (when wrapped by scripts/run_eda.py):
    0 — success, all green
    2 — sanity check returned 'warn' (pipeline still completed)
    1 — unexpected exception raised
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from database.connection import DatabaseConnection
from src.ml.eda.analyzers.categorical import CategoricalAnalyzer
from src.ml.eda.analyzers.numeric import NumericAnalyzer
from src.ml.eda.dataset.loader import TrainingDataLoader
from src.ml.eda.dataset.sanity import SanityChecker
from src.ml.eda.global_analysis.class_balance import ClassBalanceAnalyzer
from src.ml.eda.global_analysis.cohorts import CohortAnalyzer
from src.ml.eda.global_analysis.correlation import CorrelationAnalyzer
from src.ml.eda.reporting.manifest_writer import ManifestWriter
from src.ml.eda.reporting.narrator import StatisticalNarrator
from src.ml.eda.reporting.plot_saver import PlotSaver
from src.ml.eda.utils.signal_logger import SignalLogger


class SupabaseEDA(SignalLogger):
    """Stage 3.1 EDA orchestrator. One call to run() produces all artifacts."""

    def __init__(
        self,
        db: DatabaseConnection,
        reports_root: str | Path = "src/ml/eda/reports",
        model_version: str = "kaggle_baseline",
    ) -> None:
        super().__init__()
        self.db = db
        self.reports_root = Path(reports_root)
        self.model_version = model_version

    def run(self) -> Dict:
        """Execute the full EDA pipeline. Returns the top-level summary dict."""
        t_start = time.perf_counter()
        self._banner("STAGE 3.1 — SUPABASE EDA")

        # Fresh reports tree — don't let stale artifacts from a prior run mix in.
        self._prepare_reports_tree()

        # ─── 1. LOAD ───
        loader = TrainingDataLoader(self.db, model_version=self.model_version)
        df = loader.load()

        # ─── 2. SANITY ───
        self._banner("SANITY CHECK")
        sanity = SanityChecker(
            df=df,
            categorical_cols=loader.CATEGORICAL_COLS,
            numeric_cols=loader.NUMERIC_COLS,
            target_col=loader.TARGET_COL,
        )
        sanity_result = sanity.save(self.reports_root / "sanity.json")

        # Shared services live for the whole run.
        plot_saver = PlotSaver(reports_root=self.reports_root)
        narrator = StatisticalNarrator()
        manifest = ManifestWriter()

        # ─── 3. GLOBAL ANALYSES ───
        self._banner("GLOBAL: CLASS BALANCE")
        class_balance = ClassBalanceAnalyzer(
            df=df, target_col=loader.TARGET_COL,
            plot_saver=plot_saver, manifest=manifest,
        )
        class_balance_result = class_balance.save(self.reports_root / "class_balance.json")

        self._banner("GLOBAL: CORRELATION + VIF")
        correlation = CorrelationAnalyzer(
            df=df, numeric_cols=loader.NUMERIC_COLS,
            plot_saver=plot_saver, manifest=manifest,
        )
        correlation_result = correlation.save(self.reports_root / "correlation.json")

        self._banner("GLOBAL: COHORTS")
        cohorts = CohortAnalyzer(
            df=df, target_col=loader.TARGET_COL,
            plot_saver=plot_saver, manifest=manifest,
        )
        cohorts_result = cohorts.save(self.reports_root / "cohorts.json")

        # ─── 4. PER-FEATURE ANALYSES ───
        self._banner("PER-FEATURE: CATEGORICAL")
        categorical_results: Dict[str, Dict] = {}
        for col in loader.CATEGORICAL_COLS:
            if col not in df.columns:
                self._warn(f"skipping missing categorical column: {col}")
                continue
            analyzer = CategoricalAnalyzer(
                feature_name=col, df=df, target_col=loader.TARGET_COL,
                plot_saver=plot_saver, narrator=narrator, manifest=manifest,
            )
            categorical_results[col] = analyzer.save(self.reports_root / "features")

        self._banner("PER-FEATURE: NUMERIC")
        numeric_results: Dict[str, Dict] = {}
        for col in loader.NUMERIC_COLS:
            if col not in df.columns:
                self._warn(f"skipping missing numeric column: {col}")
                continue
            analyzer = NumericAnalyzer(
                feature_name=col, df=df, target_col=loader.TARGET_COL,
                plot_saver=plot_saver, narrator=narrator, manifest=manifest,
            )
            numeric_results[col] = analyzer.save(self.reports_root / "features")

        # ─── 5. MANIFEST ───
        self._banner("ARTIFACTS")
        manifest.write(self.reports_root / "figures" / "manifest.json")

        # ─── 6. CATEGORY CHURN RATES (for Stage 3.2) ───
        self._write_category_churn_rates(categorical_results)

        # ─── 7. SUMMARY ───
        summary = self._build_summary(
            t_start=t_start,
            df_shape=df.shape,
            loader=loader,
            sanity=sanity_result,
            class_balance=class_balance_result,
            correlation=correlation_result,
            cohorts=cohorts_result,
            categorical_results=categorical_results,
            numeric_results=numeric_results,
        )
        summary["counts"]["figures"] = manifest.count
        summary_path = self.reports_root / "summary.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        self._signal(
            "Writing to", str(summary_path),
            f"features={summary['counts']['features']}  figures={summary['counts']['figures']}",
        )

        # ─── FINAL SIGNAL ───
        elapsed = time.perf_counter() - t_start
        self._banner("DONE")
        self._signal(
            "DONE",
            f"EDA complete — status={summary['status']}",
            f"{elapsed:.1f}s  |  sanity={sanity_result['status']}",
        )
        return summary

    # ---------- helpers ----------

    def _prepare_reports_tree(self) -> None:
        """Create the reports directory layout. Never deletes — appends or overwrites."""
        (self.reports_root / "features").mkdir(parents=True, exist_ok=True)
        (self.reports_root / "figures").mkdir(parents=True, exist_ok=True)

    def _write_category_churn_rates(self, categorical_results: Dict[str, Dict]) -> None:
        """Aggregate per-category churn rates for Stage 3.2 consumption.

        This is the file Stage 3.2's FeatureEngineer reads to build the
        `category_volatility_score` ordinal mapping. Format:
            {
                "preferred_order_cat": {"Grocery": 0.05, "Mobile": 0.22, ...},
                "preferred_payment": {"COD": 0.35, ...},
                ...
            }
        """
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "by_feature": {},
        }
        for feature, result in categorical_results.items():
            payload["by_feature"][feature] = {
                cat: stats["churn_rate"]
                for cat, stats in result["per_category_churn"].items()
            }
        output_path = self.reports_root / "category_churn_rates.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        self._signal(
            "Writing to", str(output_path),
            f"{len(payload['by_feature'])} categorical features",
        )

    def _build_summary(
        self,
        t_start: float,
        df_shape: tuple,
        loader: TrainingDataLoader,
        sanity: Dict,
        class_balance: Dict,
        correlation: Dict,
        cohorts: Dict,
        categorical_results: Dict[str, Dict],
        numeric_results: Dict[str, Dict],
    ) -> Dict:
        """Build the top-level summary.json the dashboard reads first."""
        # Per-feature compact records for dashboard sidebar
        feature_records: List[Dict] = []
        for col, r in categorical_results.items():
            feature_records.append({
                "name":               col,
                "type":               "categorical",
                "n_categories":       r["n_categories"],
                "cramers_v":          r["statistical_tests"]["cramers_v"]["value"],
                "p_value":            r["statistical_tests"]["chi_squared"]["p_value"],
                "action":             r["recommendation"]["action"],
                "hotspot_categories": r["recommendation"]["hotspot_categories"],
            })
        for col, r in numeric_results.items():
            feature_records.append({
                "name":            col,
                "type":            "numeric",
                "cohens_d":        r["statistical_tests"]["cohens_d"]["value"],
                "p_value":         r["statistical_tests"]["mann_whitney_u"]["p_value"],
                "signal_strength": r["recommendation"]["signal_strength"],
                "transform":       r["recommendation"]["transform"],
                "action":          r["recommendation"]["action"],
            })

        # Strong-signal features for the "top predictors" section
        strong_numeric = sorted(
            [r for r in feature_records
             if r["type"] == "numeric"
             and r["signal_strength"] in ("strong", "moderate")],
            key=lambda r: -abs(r["cohens_d"]),
        )
        strong_categorical = sorted(
            [r for r in feature_records
             if r["type"] == "categorical"
             and r["cramers_v"] is not None
             and r["cramers_v"] >= 0.1],
            key=lambda r: -r["cramers_v"],
        )

        # Overall status
        status = "pass"
        if sanity["status"] != "pass":
            status = "warn"

        return {
            "generated_at":   datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(time.perf_counter() - t_start, 2),
            "status":         status,
            "model_version":  self.model_version,
            "dataset": {
                "rows":             int(df_shape[0]),
                "columns":          int(df_shape[1]),
                "categorical_cols": loader.CATEGORICAL_COLS,
                "numeric_cols":     loader.NUMERIC_COLS,
                "target_col":       loader.TARGET_COL,
            },
            "counts": {
                "features":        len(feature_records),
                "categorical":     len(categorical_results),
                "numeric":         len(numeric_results),
                "hotspot_cohorts": len(cohorts.get("all_hotspots", [])),
                "strong_pairs":    len(correlation.get("strong_pairs", [])),
                "high_vif":        len(correlation.get("high_vif_features", [])),
                "figures":         0,   # filled below from manifest if present
            },
            "sanity":        {"status": sanity["status"], "warnings": sanity.get("warnings", [])},
            "class_balance": {
                "churn_rate":         class_balance["churn_rate"],
                "imbalance_ratio":    class_balance["imbalance_ratio"],
                "imbalance_severity": class_balance["imbalance_severity"],
                "strategy_label":     class_balance["recommendation"]["strategy_label"],
            },
            "features":          feature_records,
            "top_predictors": {
                "numeric":     strong_numeric[:5],
                "categorical": strong_categorical[:5],
            },
            "multicollinearity": {
                "strong_pairs":     correlation.get("strong_pairs", []),
                "drop_candidates":  correlation.get("recommendation", {}).get("drop_candidates", []),
            },
            "cohort_hotspots":   cohorts.get("all_hotspots", []),
            "narratives": {
                "class_balance": class_balance["narrative"],
                "correlation":   correlation["narrative"],
                "cohorts":       cohorts["narrative"],
            },
        }

    def _banner(self, title: str) -> None:
        """Visible section break in the log output."""
        self.logger.info("")
        self.logger.info("─" * 70)
        self.logger.info(f"▸ {title}")
        self.logger.info("─" * 70)
