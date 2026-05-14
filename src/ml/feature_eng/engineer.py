"""
engineer.py — Stage 3.2: Feature Engineering

Class: FeatureEngineer

All feature decisions are data-driven from Stage 3.1 EDA results.
Every WHY is documented inline with the exact EDA numbers that justified it.

EDA results used in this file:
  - Cramér's V thresholds (categorical encoding decisions)
  - Cohen's d values (numeric feature importance)
  - Cohort churn rates and lift values (binary flag decisions)
  - category_churn_rates.json values (target encoding)

Pattern:
  engineer.fit(df_train)       ← computes parameters from training data ONLY
  X = engineer.transform(df)   ← applies transformations, returns feature matrix
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

from src.ml.eda.utils.signal_logger import SignalLogger


# ── Column names as constants — single source of truth for the whole module ──
TARGET_COL = "churn"

# Raw columns from Supabase (from TrainingDataLoader)
RAW_CATEGORICAL = ["gender", "preferred_payment_mode", "preferred_login_device"]
RAW_ORDINAL_ENCODED = ["complain", "preferred_order_cat", "marital_status"]
RAW_NUMERIC = [
    "tenure_months", "order_count", "day_since_last_order", "hour_spend_on_app",
    "number_of_address", "number_of_device_registered", "coupon_used",
    "cashback_amount", "order_amount_hike_from_last_year", "warehouse_to_home",
    "satisfaction_score",
]
PASSTHROUGH_NUMERIC = ["city_tier"]   # already ordinal 1/2/3, no encoding needed

# Features created by this class
ENGINEERED_FEATURES = [
    # Target-encoded replacements
    "complain_risk", "order_cat_risk", "marital_risk",
    # Group 1 — Financial Stress
    "discount_dependency_ratio", "cashback_per_order", "order_value_declining_flag",
    # Group 2 — Service Dependency
    "platform_embeddedness_score", "is_cod_user", "address_per_tenure_ratio",
    # Group 3 — Engagement Decay
    "recency_risk_score", "order_frequency_normalized", "app_engagement_tier",
    # Group 4 — Contract Risk
    "tenure_segment", "is_new_customer", "silent_dissatisfaction_flag",
    "complaint_satisfaction_interaction", "warehouse_distance_risk",
]


class FeatureEngineer(SignalLogger):
    """
    Applies all 5 feature groups from Stage 3.2 design.

    Input:  raw DataFrame from TrainingDataLoader (includes churn column during fit)
    Output: engineered DataFrame WITHOUT churn column (features only)

    Usage:
        engineer = FeatureEngineer(artifacts_dir="models/artifacts")
        engineer.fit(df_train)            # learns rates + thresholds from train
        X_train = engineer.transform(df_train)
        X_val   = engineer.transform(df_val)
        X_test  = engineer.transform(df_test)
    """

    # ── EDA-driven thresholds — frozen constants ─────────────────────────────

    # Cramér's V threshold for engineer_ordinal vs one-hot decision.
    # Features with V >= 0.15 get target-encoded (engineer_ordinal).
    # Features with V <  0.15 get one-hot encoded (handled by PreprocessorBuilder).
    ORDINAL_CRAMERS_V_THRESHOLD: float = 0.15

    # tenure_segment cutoffs — from cohort analysis:
    #   new     (0–6mo)  : churn rate 32.4%, lift 1.93× baseline  ← highest risk
    #   growing (6–24mo) : churn rate  8.2%, lift 0.49×
    #   loyal   (24+mo)  : churn rate  0.0%, lift 0.0×
    TENURE_NEW_CUTOFF: int = 6
    TENURE_GROWING_CUTOFF: int = 24

    # is_new_customer binary flag — tenure <= 6 months.
    # Justification: cohort shows 32.4% churn rate (1.93× baseline of 16.84%).
    # Even though HOTSPOT_LIFT=2.0 threshold wasn't cleared (lift=1.93), the
    # business signal is clear enough to engineer a flag.
    IS_NEW_CUTOFF: int = 6

    # App engagement tier bins — from main_README design.
    # 0 = low engagement (<= 1 hr), 1 = medium (1–3 hrs), 2 = high (>3 hrs)
    APP_BINS = [-0.001, 1.0, 3.0, float("inf")]
    APP_LABELS = [0, 1, 2]

    # satisfaction_score threshold for silent_dissatisfaction_flag.
    # Score <= 3 = below midpoint on a 1–5 scale = dissatisfied.
    # Note from EDA: satisfaction_score has Cohen's d = +0.28 (churners score HIGHER).
    # This is a known Kaggle dataset quirk — scale may be inverted.
    # We still use <= 3 as "dissatisfied" based on face validity.
    SATISFACTION_DISSATISFIED_THRESHOLD: int = 3

    # Cramér's V values from EDA that drove encoding decisions (for documentation):
    #   complain              V=0.250 >= 0.15 → engineer_ordinal
    #   preferred_order_cat   V=0.226 >= 0.15 → engineer_ordinal
    #   marital_status        V=0.183 >= 0.15 → engineer_ordinal
    #   preferred_payment_mode V=0.096 < 0.15 → one-hot
    #   city_tier             V=0.085 < 0.15 → passthrough (natural ordinal)
    #   preferred_login_device V=0.051 < 0.15 → one-hot
    #   gender                V=0.029 < 0.15 → one-hot

    def __init__(self, artifacts_dir: str | Path = "models/artifacts") -> None:
        super().__init__()
        self.artifacts_dir = Path(artifacts_dir)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

        # Set during fit() — empty until then
        self._target_rates: Dict[str, Dict] = {}
        self._warehouse_log_median: Optional[float] = None
        self._address_iqr_cap: Optional[float] = None
        self._is_fitted: bool = False

    # ────────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ────────────────────────────────────────────────────────────────────────

    def fit(self, df_train: pd.DataFrame) -> "FeatureEngineer":
        """
        Learn all parameters from the training fold ONLY.
        Never call fit() with val or test data — that would be data leakage.

        Input:  df_train — raw training DataFrame including churn column
        Output: self (fluent interface, enables fit_transform chaining)

        Computes:
          - Target encoding rates (churn rate per category) for 3 columns
          - Warehouse log-median threshold for warehouse_distance_risk
          - Address IQR cap for platform_embeddedness_score
        """
        self._signal("Fitting on", "training fold", f"{len(df_train):,} rows")
        t0 = time.perf_counter()

        # ── Target encoding rates ────────────────────────────────────────────
        # Compute churn rate per category FROM TRAINING DATA ONLY.
        # Using the full dataset would cause leakage (the EDA rates in
        # category_churn_rates.json were approximate design-time estimates;
        # these training-fold rates are the authoritative production values).

        # complain: Cramér's V=0.250 → engineer_ordinal
        # Values in df are int 0/1 (loader converts bool→int)
        self._target_rates["complain"] = (
            df_train.groupby("complain")[TARGET_COL].mean().to_dict()
        )

        # preferred_order_cat: Cramér's V=0.226 → engineer_ordinal
        # Churn rates range from 4.9% (Grocery) to 27.4% (Mobile) — strong spread
        self._target_rates["preferred_order_cat"] = (
            df_train.groupby("preferred_order_cat")[TARGET_COL].mean().to_dict()
        )

        # marital_status: Cramér's V=0.183 → engineer_ordinal
        # Single at 26.7% vs Married at 11.5% — clear business signal
        self._target_rates["marital_status"] = (
            df_train.groupby("marital_status")[TARGET_COL].mean().to_dict()
        )

        # ── Warehouse log-median (for warehouse_distance_risk flag) ──────────
        # EDA: warehouse_to_home Cohen's d=+0.19, recommended log1p transform.
        # We log-transform and threshold at the median: customers above median
        # distance are flagged as higher risk (longer wait → higher churn).
        log_w = np.log1p(df_train["warehouse_to_home"])
        self._warehouse_log_median = float(log_w.median())

        # ── Address IQR cap (for platform_embeddedness_score) ────────────────
        # Stage 1 already clipped to (1, 100). Here we apply a tighter
        # statistical cap at Q3 + 1.5×IQR to reduce influence of outliers
        # on the embeddedness score formula.
        q1 = df_train["number_of_address"].quantile(0.25)
        q3 = df_train["number_of_address"].quantile(0.75)
        self._address_iqr_cap = float(q3 + 1.5 * (q3 - q1))

        self._is_fitted = True
        elapsed = time.perf_counter() - t0
        self._signal(
            "DONE", "FeatureEngineer fitted",
            f"3 target-rate maps | warehouse_median={self._warehouse_log_median:.3f} | "
            f"address_cap={self._address_iqr_cap:.1f} | {elapsed:.2f}s",
        )
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all feature engineering transformations.
        Can be called on train, val, or test — uses only parameters fitted on train.

        Input:  df — raw DataFrame (churn column optional — dropped if present)
        Output: X — engineered feature DataFrame without churn column
        """
        if not self._is_fitted:
            raise RuntimeError("Call fit() before transform().")

        self._signal("Transforming", f"{len(df):,} rows")
        t0 = time.perf_counter()

        # Work on a copy — never mutate the input DataFrame
        X = df.copy()

        # Drop the target column if present — it must never be a feature
        if TARGET_COL in X.columns:
            X = X.drop(columns=[TARGET_COL])

        # Apply transformations in order
        X = self._apply_target_encoding(X)
        X = self._group1_financial_stress(X)
        X = self._group2_service_dependency(X)
        X = self._group3_engagement_decay(X)
        X = self._group4_contract_risk(X)

        # Drop original columns that have been replaced by engineered versions
        # (ordinal-encoded columns replace their source categorical columns)
        cols_to_drop = ["preferred_order_cat", "marital_status"]
        # Note: "complain" is kept for now because it's used as int 0/1
        # in complaint_satisfaction_interaction and silent_dissatisfaction_flag.
        # After those features are created, we drop the raw complain.
        X = X.drop(columns=[c for c in cols_to_drop if c in X.columns])

        elapsed = time.perf_counter() - t0
        self._signal(
            "DONE", f"transformed → {X.shape[1]} features",
            f"{elapsed:.2f}s",
        )
        return X

    def fit_transform(self, df_train: pd.DataFrame) -> pd.DataFrame:
        """Convenience method — fit then transform on the training fold."""
        return self.fit(df_train).transform(df_train)

    def save_config(self, output_path: str | Path) -> Dict:
        """
        Save all fitted parameters to JSON.
        This JSON is loaded by the production API to reproduce exact transformations.
        Without this, training-serving skew is inevitable.

        Input:  output_path — path to write feature_engineering_config.json
        Output: the config dict (also written to disk)
        """
        if not self._is_fitted:
            raise RuntimeError("Call fit() before save_config().")

        config = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "encoding_decisions": {
                "ordinal_cramers_v_threshold": self.ORDINAL_CRAMERS_V_THRESHOLD,
                "ordinal_encoded": {
                    "complain": {
                        "cramers_v_from_eda": 0.250,
                        "decision": "engineer_ordinal — V=0.250 >= threshold 0.15",
                        "churn_rate_per_category": {
                            str(k): round(v, 4)
                            for k, v in self._target_rates["complain"].items()
                        },
                    },
                    "preferred_order_cat": {
                        "cramers_v_from_eda": 0.226,
                        "decision": "engineer_ordinal — V=0.226 >= threshold 0.15. "
                                    "Mobile churn 27.4% vs Grocery 4.9% — strong spread.",
                        "churn_rate_per_category": {
                            str(k): round(v, 4)
                            for k, v in self._target_rates["preferred_order_cat"].items()
                        },
                    },
                    "marital_status": {
                        "cramers_v_from_eda": 0.183,
                        "decision": "engineer_ordinal — V=0.183 >= threshold 0.15. "
                                    "Single churn 26.7% vs Married 11.5%.",
                        "churn_rate_per_category": {
                            str(k): round(v, 4)
                            for k, v in self._target_rates["marital_status"].items()
                        },
                    },
                },
                "one_hot_encoded": {
                    "gender":                  "V=0.029 < threshold 0.15 → one-hot",
                    "preferred_payment_mode":  "V=0.096 < threshold 0.15 → one-hot",
                    "preferred_login_device":  "V=0.051 < threshold 0.15 → one-hot",
                },
                "passthrough_numeric": {
                    "city_tier": "V=0.085, already ordinal (1<2<3), passthrough",
                },
            },
            "fitted_thresholds": {
                "warehouse_log_median":   round(self._warehouse_log_median, 4),
                "address_iqr_cap":        round(self._address_iqr_cap, 2),
                "tenure_new_cutoff":      self.TENURE_NEW_CUTOFF,
                "tenure_growing_cutoff":  self.TENURE_GROWING_CUTOFF,
                "is_new_customer_cutoff": self.IS_NEW_CUTOFF,
                "app_engagement_bins":    self.APP_BINS[:3] + [999.0],
                "satisfaction_dissatisfied_threshold": self.SATISFACTION_DISSATISFIED_THRESHOLD,
            },
            "feature_groups": {
                "group1_financial_stress": [
                    "discount_dependency_ratio",
                    "cashback_per_order",
                    "order_value_declining_flag",
                ],
                "group2_service_dependency": [
                    "platform_embeddedness_score",
                    "is_cod_user",
                    "address_per_tenure_ratio",
                ],
                "group3_engagement_decay": [
                    "recency_risk_score",
                    "order_frequency_normalized",
                    "app_engagement_tier",
                ],
                "group4_contract_risk": [
                    "tenure_segment",
                    "is_new_customer",
                    "silent_dissatisfaction_flag",
                    "complaint_satisfaction_interaction",
                    "warehouse_distance_risk",
                ],
                "target_encoded_replacements": [
                    "complain_risk",
                    "order_cat_risk",
                    "marital_risk",
                ],
            },
        }

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)

        self._signal("Writing to", str(output_path), "feature_engineering_config.json")
        return config

    # ────────────────────────────────────────────────────────────────────────
    # PRIVATE — target encoding
    # ────────────────────────────────────────────────────────────────────────

    def _apply_target_encoding(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Replace ordinal-encoded categorical columns with their training churn rates.

        WHY target encoding over one-hot:
          Cramér's V >= 0.15 → the churn rates differ substantially across
          categories → replacing with the actual rate gives the model a
          continuous risk signal instead of sparse binary columns.

        Fallback: unseen categories (e.g. new payment method added post-training)
        are mapped to the overall training churn rate (0.1684) so the model
        never receives NaN.
        """
        overall_rate = float(np.mean(list(self._target_rates["complain"].values())))

        # ── complain → complain_risk ─────────────────────────────────────────
        # EDA: Cramér's V=0.250, churn 10.9% (no complaint) vs 31.7% (complaint)
        # The 3× difference justifies replacing the binary flag with actual rates.
        X["complain_risk"] = (
            X["complain"].map(self._target_rates["complain"])
            .fillna(overall_rate)
        )

        # ── preferred_order_cat → order_cat_risk ─────────────────────────────
        # EDA: Cramér's V=0.226, rates span 4.9% (Grocery) to 27.4% (Mobile).
        # This also serves as the "category_volatility_score" from the README.
        X["order_cat_risk"] = (
            X["preferred_order_cat"].map(self._target_rates["preferred_order_cat"])
            .fillna(overall_rate)
        )

        # ── marital_status → marital_risk ────────────────────────────────────
        # EDA: Cramér's V=0.183, Single churn 26.7% vs Married 11.5%.
        X["marital_risk"] = (
            X["marital_status"].map(self._target_rates["marital_status"])
            .fillna(overall_rate)
        )

        return X

    # ────────────────────────────────────────────────────────────────────────
    # PRIVATE — 5 feature groups
    # ────────────────────────────────────────────────────────────────────────

    def _group1_financial_stress(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Group 1 — Financial Stress features.

        Captures customers who are financially strained or showing spend decline.
        Rationale: financial stress correlates with churn in e-commerce (customers
        reduce discretionary spending before leaving).
        """
        eps = 1e-6  # zero-division guard on all ratio features

        # discount_dependency_ratio = coupon_used / order_count
        # WHY: order_count and coupon_used have r=0.62 (moderate correlation from EDA).
        # Their ratio captures how dependent a customer is on discounts relative
        # to their order activity — a loyalty signal independent of order volume.
        X["discount_dependency_ratio"] = (
            X["coupon_used"] / (X["order_count"] + eps)
        )

        # cashback_per_order = cashback_amount / order_count
        # WHY: cashback_amount Cohen's d=-0.42 (churners receive LESS cashback).
        # Normalising by order count removes the confound of order volume.
        X["cashback_per_order"] = (
            X["cashback_amount"] / (X["order_count"] + eps)
        )

        # order_value_declining_flag = 1 if YoY order hike < 0
        # WHY: negative hike means customer is spending less than last year.
        # EDA: Cohen's d=-0.019 (weak individual signal) but combined with other
        # financial stress features it adds interpretable business context.
        # Negatives are intentionally preserved (not clipped) per Stage 3.1 design.
        X["order_value_declining_flag"] = (
            (X["order_amount_hike_from_last_year"] < 0).astype(int)
        )

        return X

    def _group2_service_dependency(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Group 2 — Service Dependency features.

        Captures how embedded a customer is in the platform ecosystem.
        High embeddedness = high switching cost = lower churn risk.
        """
        eps = 1e-6

        # platform_embeddedness_score = (addresses × 0.4) + (devices × 0.6)
        # WHY: number_of_device_registered Cohen's d=+0.29, number_of_address
        # Cohen's d=+0.12 — both have opposite-direction relationships with churn
        # (more devices/addresses → retained customers). Combining them into a
        # single score reduces dimensionality.
        # Devices weighted 0.6 (stronger signal) vs addresses 0.4 (weaker signal).
        # Address values are capped at IQR upper bound (fitted on train) to
        # prevent outliers from dominating the score.
        capped_addresses = X["number_of_address"].clip(upper=self._address_iqr_cap)
        X["platform_embeddedness_score"] = (
            (capped_addresses * 0.4) + (X["number_of_device_registered"] * 0.6)
        )

        # is_cod_user = 1 if preferred_payment_mode == COD
        # WHY: COD churn rate 24.9% vs 16.84% overall (1.48× lift from cohort analysis).
        # COD users never committed a saved payment method → lower switching cost.
        # Even though preferred_payment_mode gets one-hot encoded separately,
        # this explicit flag highlights the COD risk signal for interpretability.
        X["is_cod_user"] = (
            (X["preferred_payment_mode"] == "COD").astype(int)
        )

        # address_per_tenure_ratio = number_of_address / (tenure + 1)
        # WHY: normalises address count by how long the customer has been active.
        # A new customer with 5 addresses is more embedded than a loyal customer
        # with 5 addresses who hasn't grown their account.
        X["address_per_tenure_ratio"] = (
            X["number_of_address"] / (X["tenure_months"] + 1)
        )

        return X

    def _group3_engagement_decay(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Group 3 — Engagement Decay features.

        Captures declining activity relative to the customer's tenure.
        Raw activity counts confound new vs old customers — these features
        normalise by tenure to isolate the decay signal.
        """
        eps = 1e-6

        # recency_risk_score = days_since_last_order / (tenure + 1)
        # WHY: day_since_last_order Cohen's d=-0.42 (churners have MORE days since
        # last order). Dividing by tenure normalises: a 30-day gap means more for
        # a 2-month-old customer than a 2-year-old customer.
        X["recency_risk_score"] = (
            X["day_since_last_order"] / (X["tenure_months"] + 1)
        )

        # order_frequency_normalized = order_count / (tenure + 1)
        # WHY: order_count alone (Cohen's d=-0.064, negligible) is dominated by
        # tenure length. Normalising by tenure isolates the frequency signal.
        X["order_frequency_normalized"] = (
            X["order_count"] / (X["tenure_months"] + 1)
        )

        # app_engagement_tier = binned hour_spend_on_app
        # WHY: hour_spend_on_app Cohen's d=+0.050 (negligible linear signal).
        # However, binning captures non-linear structure:
        #   0 = low  (0–1 hr)  : at-risk segment
        #   1 = med  (1–3 hrs) : engaged segment
        #   2 = high (>3 hrs)  : power users
        # Cross-check: Mobile-only login users (preferred_login_device) may show
        # different app time patterns — the model will learn this interaction.
        tier = pd.cut(
            X["hour_spend_on_app"],
            bins=self.APP_BINS,
            labels=self.APP_LABELS,
            include_lowest=True,
        )
        X["app_engagement_tier"] = tier.astype(int)

        return X

    def _group4_contract_risk(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Group 4 — Contract Risk features.

        Captures lifecycle stage and latent dissatisfaction signals.
        These features identify customers who are at high structural churn risk
        regardless of their recent behaviour.
        """
        # tenure_segment = 0 (new), 1 (growing), 2 (loyal)
        # WHY: cohort analysis shows extreme non-linearity:
        #   new     (0–6mo)  → 32.4% churn rate (1.93× baseline)
        #   growing (6–24mo) → 8.2% churn rate
        #   loyal   (24+mo)  → 0.0% churn rate
        # Raw tenure_months is also kept for the model to use the continuous signal.
        # This ordinal captures the non-linear lifecycle structure.
        X["tenure_segment"] = pd.cut(
            X["tenure_months"],
            bins=[-0.1, self.TENURE_NEW_CUTOFF, self.TENURE_GROWING_CUTOFF, float("inf")],
            labels=[0, 1, 2],
            include_lowest=True,
        ).astype(int)

        # is_new_customer = 1 if tenure <= 6 months
        # WHY: cohort lift=1.93× (32.4% churn vs 16.84% baseline). The binary flag
        # makes this the most explicit feature for the model to identify new customers.
        # Separate from tenure_segment to allow the model to use both signals.
        X["is_new_customer"] = (
            (X["tenure_months"] <= self.IS_NEW_CUTOFF).astype(int)
        )

        # silent_dissatisfaction_flag = 1 if satisfaction <= 3 AND no complaint
        # WHY: the "silent churner" pattern. A customer who is dissatisfied but
        # never filed a complaint is hard to identify from individual features alone.
        # EDA note: satisfaction_score Cohen's d=+0.28 (churners score higher —
        # possible scale inversion in dataset). We use <= 3 as "below midpoint"
        # based on face validity despite this anomaly.
        # complain values are int 0/1 (converted from bool in loader).
        X["silent_dissatisfaction_flag"] = (
            ((X["satisfaction_score"] <= self.SATISFACTION_DISSATISFIED_THRESHOLD)
             & (X["complain"] == 0)).astype(int)
        )

        # complaint_satisfaction_interaction = complain × (6 - satisfaction_score)
        # WHY: a customer who complained AND has low satisfaction is the highest-risk
        # profile. Multiplying combines both signals into a single 0–5 risk score:
        #   complain=0 → score=0 regardless of satisfaction
        #   complain=1 + score=1 → 5 (maximum risk)
        #   complain=1 + score=5 → 1 (complained but satisfied — moderate risk)
        X["complaint_satisfaction_interaction"] = (
            X["complain"] * (6 - X["satisfaction_score"])
        )

        # warehouse_distance_risk = 1 if log(warehouse_to_home) > training median
        # WHY: warehouse_to_home Cohen's d=+0.19 (churners live farther).
        # EDA recommended log1p transform (right-skewed distribution).
        # We log-transform and threshold at the training median:
        # customers above-median distance are flagged as potentially at risk
        # from delivery frustration.
        log_w = np.log1p(X["warehouse_to_home"])
        X["warehouse_distance_risk"] = (
            (log_w > self._warehouse_log_median).astype(int)
        )

        # Drop the original complain column now that all derived features are created.
        # The raw 0/1 value is no longer needed — complain_risk, silent_dissatisfaction_flag,
        # and complaint_satisfaction_interaction carry all the information.
        if "complain" in X.columns:
            X = X.drop(columns=["complain"])

        return X
