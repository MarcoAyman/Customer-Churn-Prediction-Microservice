"""
sanity.py — defensive checks on the loaded training DataFrame.

This is NOT cleaning. It verifies Stage 1 actually did its job:
    - the expected 5,630 rows are present
    - no duplicate customer_ids
    - no NULLs in any feature column
    - numeric columns are numeric
    - churn target has exactly {0, 1} values

Any violation is logged loudly and written into reports/sanity.json with
status='warn'. Marco's decision rule: if sanity fails, fix Stage 1 and re-seed
— do not patch it here.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd

from src.ml.eda.utils.signal_logger import SignalLogger


class SanityChecker(SignalLogger):
    """Runs post-load sanity checks and writes a machine-readable report."""

    # From CHURNGUARD_PROJECT_DOC §6 — Stage 2 seeded exactly 5,630 rows.
    EXPECTED_ROWS: int = 5630

    def __init__(
        self,
        df: pd.DataFrame,
        categorical_cols: List[str],
        numeric_cols: List[str],
        target_col: str = "churn",
    ) -> None:
        super().__init__()
        self.df = df
        self.categorical_cols = categorical_cols
        self.numeric_cols = numeric_cols
        self.target_col = target_col

    def check(self) -> Dict:
        """Run all sanity checks. Returns a dict suitable for JSON export."""
        df = self.df
        result: Dict = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rows": int(len(df)),
            "expected_rows": self.EXPECTED_ROWS,
            "row_count_delta": int(len(df) - self.EXPECTED_ROWS),
            "columns": list(df.columns),
            "n_features": len(self.categorical_cols) + len(self.numeric_cols),
            "duplicate_ids": int(df.index.duplicated().sum()),
            "null_counts": {},
            "dtype_issues": [],
            "target_values": sorted(df[self.target_col].dropna().unique().tolist()),
            "target_distribution": self._target_distribution(df),
            "status": "pass",
            "warnings": [],
        }

        # Null audit — Stage 1 guarantees zero, so any null is a regression.
        for col in df.columns:
            nulls = int(df[col].isna().sum())
            result["null_counts"][col] = nulls
            if nulls > 0:
                result["warnings"].append(f"{col}: {nulls} nulls found (Stage 1 should guarantee zero)")

        # Dtype audit on numeric features.
        for col in self.numeric_cols:
            if col not in df.columns:
                result["warnings"].append(f"expected numeric column '{col}' not found in DataFrame")
                continue
            if not pd.api.types.is_numeric_dtype(df[col]):
                issue = f"{col} has non-numeric dtype '{df[col].dtype}'"
                result["dtype_issues"].append(issue)
                result["warnings"].append(issue)

        # Categorical presence audit (no dtype check — object/category both fine).
        for col in self.categorical_cols:
            if col not in df.columns:
                result["warnings"].append(f"expected categorical column '{col}' not found in DataFrame")

        # Target sanity.
        if sorted(result["target_values"]) != [0, 1]:
            result["warnings"].append(
                f"target '{self.target_col}' has unexpected values: {result['target_values']} "
                f"(expected [0, 1])"
            )

        # Duplicate ids are fatal for training — every customer must be unique.
        if result["duplicate_ids"] > 0:
            result["warnings"].append(
                f"{result['duplicate_ids']} duplicate customer_ids — training would double-count customers"
            )

        # Row count mismatch — not fatal but worth flagging.
        if result["row_count_delta"] != 0:
            result["warnings"].append(
                f"row count delta: {result['row_count_delta']:+d} "
                f"(loaded {result['rows']:,} vs expected {self.EXPECTED_ROWS:,})"
            )

        if result["warnings"]:
            result["status"] = "warn"

        return result

    def _target_distribution(self, df: pd.DataFrame) -> Dict:
        counts = df[self.target_col].value_counts()
        total = int(counts.sum()) or 1
        return {
            "churned_count": int(counts.get(1, 0)),
            "retained_count": int(counts.get(0, 0)),
            "churn_rate": float(counts.get(1, 0) / total),
            "retention_rate": float(counts.get(0, 0) / total),
            "imbalance_ratio_retained_to_churned": (
                float(counts.get(0, 0) / counts.get(1, 1)) if counts.get(1, 0) else None
            ),
        }

    def save(self, output_path: str | Path) -> Dict:
        """Run checks and write results to JSON. Returns the result dict."""
        result = self.check()
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)

        self._signal(
            "Writing to",
            str(output_path),
            f"status={result['status']}",
        )

        # Emit each warning so nothing is silently buried in the JSON.
        for warning in result["warnings"]:
            self._warn(warning)

        rate = result["target_distribution"]["churn_rate"]
        self._signal(
            "DONE",
            f"sanity={result['status']}",
            f"churn_rate={rate*100:.2f}%  |  rows={result['rows']:,}",
        )
        return result
