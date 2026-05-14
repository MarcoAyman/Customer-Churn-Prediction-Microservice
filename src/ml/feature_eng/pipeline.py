"""
pipeline.py — Stage 3.2: Data Splitting + Preprocessing Pipeline

Classes:
    StratifiedSplitter   — splits raw data into train / val / test
    PreprocessorBuilder  — builds and fits the sklearn Pipeline on train only

Design rule: split BEFORE fitting the preprocessor.
Fitting the preprocessor on the full dataset leaks val/test statistics
into the scaler — the model would effectively "see" those rows before evaluation.
"""
from __future__ import annotations

import json
import pickle
import time
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.ml.eda.utils.signal_logger import SignalLogger


class StratifiedSplitter(SignalLogger):
    """
    Splits the raw DataFrame into stratified train / val / test folds.

    Input:  raw DataFrame from TrainingDataLoader (includes churn column)
    Output: three DataFrames (train, val, test) — all include churn column

    Why stratified: dataset has 16.84% churn rate. Random splitting could
    produce folds with very different rates (e.g. 10% vs 22%). Stratified
    splitting ensures each fold mirrors the 16.84% overall rate.

    Sizes (from STAGE3_ML_README §7.2):
        train: 70%   (3,941 rows) — used for fitting + CV during tuning
        val:   15%   (844 rows)   — used for threshold selection only
        test:  15%   (845 rows)   — LOCKED, touched ONCE at final evaluation
    """

    TRAIN_SIZE: float = 0.70
    VAL_SIZE: float   = 0.15
    TEST_SIZE: float  = 0.15
    RANDOM_STATE: int = 42   # frozen — never change between runs

    def __init__(self, target_col: str = "churn") -> None:
        super().__init__()
        self.target_col = target_col

    def split(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Split df into (train, val, test).

        Input:  df — full raw DataFrame including churn column
        Output: (df_train, df_val, df_test) — all include churn column
        """
        from sklearn.model_selection import train_test_split

        self._signal(
            "Splitting", f"{len(df):,} rows",
            f"stratified on {self.target_col} | seed={self.RANDOM_STATE}",
        )
        t0 = time.perf_counter()

        y = df[self.target_col]

        # First split: pull off the test set
        df_trainval, df_test = train_test_split(
            df,
            test_size=self.TEST_SIZE,
            stratify=y,
            random_state=self.RANDOM_STATE,
        )

        # Second split: split remaining into train and val
        # val proportion relative to the remaining 85% → 0.15/0.85 ≈ 0.1765
        val_proportion = self.VAL_SIZE / (self.TRAIN_SIZE + self.VAL_SIZE)
        df_train, df_val = train_test_split(
            df_trainval,
            test_size=val_proportion,
            stratify=df_trainval[self.target_col],
            random_state=self.RANDOM_STATE,
        )

        elapsed = time.perf_counter() - t0
        self._signal(
            "DONE",
            f"train={len(df_train):,} | val={len(df_val):,} | test={len(df_test):,}",
            f"churn rates: train={df_train[self.target_col].mean():.3f} "
            f"val={df_val[self.target_col].mean():.3f} "
            f"test={df_test[self.target_col].mean():.3f} | {elapsed:.2f}s",
        )

        return df_train, df_val, df_test

    def save_split_summary(
        self,
        df_train: pd.DataFrame,
        df_val: pd.DataFrame,
        df_test: pd.DataFrame,
        output_path: str | Path,
    ) -> Dict:
        """
        Save a JSON summary of the split sizes and churn rates.
        Input:  three DataFrames + output path
        Output: summary dict (also written to JSON)
        """
        tc = self.target_col
        summary = {
            "random_state": self.RANDOM_STATE,
            "train": {
                "rows": len(df_train),
                "churned": int(df_train[tc].sum()),
                "churn_rate": float(df_train[tc].mean()),
            },
            "val": {
                "rows": len(df_val),
                "churned": int(df_val[tc].sum()),
                "churn_rate": float(df_val[tc].mean()),
            },
            "test": {
                "rows": len(df_test),
                "churned": int(df_test[tc].sum()),
                "churn_rate": float(df_test[tc].mean()),
            },
            "total": len(df_train) + len(df_val) + len(df_test),
            "note": (
                "Test set is LOCKED — only touched once at final evaluation. "
                "Val set used only for threshold selection after training. "
                "Preprocessor fitted on train only."
            ),
        }
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        self._signal("Writing to", str(output_path), "split_summary.json")
        return summary


class PreprocessorBuilder(SignalLogger):
    """
    Builds and fits the sklearn ColumnTransformer Pipeline.

    Fits ONLY on X_train (engineered features, no churn column).
    Transforms X_train, X_val, X_test separately to avoid leakage.

    Pipeline structure:
        numeric_pipeline:
            SimpleImputer(median)  — defensive against any remaining nulls
            StandardScaler()       — required for MLP; harmless for XGBoost
        one_hot_pipeline:
            SimpleImputer(most_frequent)
            OneHotEncoder(handle_unknown='ignore')  — graceful unseen categories

    StandardScaler is applied to ALL numeric features because:
      - We plan to compare XGBoost with MLP (Stage 3.6)
      - XGBoost is scale-invariant (scaling doesn't hurt it)
      - MLP requires scaling (without it, gradients explode on large-value features)
      - One scaler for all models = one preprocessor.pkl = cleaner deployment

    Input:  X_train (engineered DataFrame, no churn column)
    Output: fitted sklearn Pipeline + saved preprocessor.pkl + feature_names.json
    """

    def __init__(self, artifacts_dir: str | Path = "models/artifacts") -> None:
        super().__init__()
        self.artifacts_dir = Path(artifacts_dir)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self._pipeline: Pipeline | None = None
        self._feature_names_out: List[str] = []

    def fit(self, X_train: pd.DataFrame) -> "PreprocessorBuilder":
        """
        Fit the sklearn ColumnTransformer on the training fold ONLY.

        Input:  X_train — engineered feature DataFrame (no churn column)
        Output: self (fluent interface)
        """
        self._signal("Fitting preprocessor on", f"X_train {X_train.shape}")
        t0 = time.perf_counter()

        numeric_cols, ohe_cols = self._detect_column_types(X_train)

        self._signal(
            "Columns detected",
            f"numeric={len(numeric_cols)} | one_hot={len(ohe_cols)}",
        )

        numeric_pipeline = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ])

        ohe_pipeline = Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(
                handle_unknown="ignore",   # unseen categories → zero vector
                sparse_output=False,
            )),
        ])

        transformers = [
            ("numeric", numeric_pipeline, numeric_cols),
        ]
        if ohe_cols:
            transformers.append(("one_hot", ohe_pipeline, ohe_cols))

        ct = ColumnTransformer(
            transformers=transformers,
            remainder="drop",   # drop any columns not explicitly listed
        )

        self._pipeline = Pipeline([("column_transformer", ct)])
        self._pipeline.fit(X_train)

        # Capture output feature names for feature_names.json
        self._feature_names_out = self._get_feature_names(numeric_cols, ohe_cols, X_train)

        elapsed = time.perf_counter() - t0
        self._signal(
            "DONE",
            f"preprocessor fitted | {len(self._feature_names_out)} output features",
            f"{elapsed:.2f}s",
        )
        return self

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        """
        Apply the fitted preprocessor to a DataFrame.

        Input:  X — engineered feature DataFrame (train, val, or test)
        Output: np.ndarray of shape (n_rows, n_features)
        """
        if self._pipeline is None:
            raise RuntimeError("Call fit() before transform().")
        return self._pipeline.transform(X)

    def fit_transform(self, X_train: pd.DataFrame) -> np.ndarray:
        """Convenience — fit then transform X_train."""
        return self.fit(X_train).transform(X_train)

    def save(self) -> Tuple[Path, Path]:
        """
        Save preprocessor.pkl and feature_names.json to artifacts_dir.

        Output: (preprocessor_path, feature_names_path)

        Both files are uploaded to HF Hub during model registration (Stage 3.11)
        so the API can reconstruct the exact same preprocessing on every inference.
        """
        if self._pipeline is None:
            raise RuntimeError("Call fit() before save().")

        pp_path = self.artifacts_dir / "preprocessor.pkl"
        fn_path = self.artifacts_dir / "feature_names.json"

        with open(pp_path, "wb") as f:
            pickle.dump(self._pipeline, f)
        self._signal("Writing to", str(pp_path), "preprocessor.pkl")

        with open(fn_path, "w", encoding="utf-8") as f:
            json.dump(
                {"feature_names": self._feature_names_out,
                 "n_features": len(self._feature_names_out)},
                f, indent=2,
            )
        self._signal("Writing to", str(fn_path), f"{len(self._feature_names_out)} features")

        return pp_path, fn_path

    @property
    def feature_names(self) -> List[str]:
        """Ordered output feature names after transformation."""
        return self._feature_names_out

    # ── private helpers ──────────────────────────────────────────────────────

    def _detect_column_types(
        self, X: pd.DataFrame
    ) -> Tuple[List[str], List[str]]:
        """
        Separate columns into numeric and categorical (object dtype).

        Input:  X — engineered DataFrame
        Output: (numeric_cols, ohe_cols)
        """
        numeric_cols = X.select_dtypes(include=[np.number]).columns.tolist()
        ohe_cols = X.select_dtypes(include=["object", "category"]).columns.tolist()
        return numeric_cols, ohe_cols

    def _get_feature_names(
        self,
        numeric_cols: List[str],
        ohe_cols: List[str],
        X_train: pd.DataFrame,
    ) -> List[str]:
        """
        Retrieve output feature names from the fitted ColumnTransformer.

        Input:  numeric_cols, ohe_cols, X_train (for OHE category extraction)
        Output: ordered list of feature names matching the transform() output
        """
        names = list(numeric_cols)  # numeric names pass through unchanged

        if ohe_cols:
            ct = self._pipeline.named_steps["column_transformer"]
            ohe = ct.named_transformers_.get("one_hot")
            if ohe is not None:
                encoder = ohe.named_steps["encoder"]
                for col, cats in zip(ohe_cols, encoder.categories_):
                    names.extend([f"{col}_{c}" for c in cats])

        return names
