"""
loader.py — single SQL JOIN that pulls the ML training set out of Supabase.

Implements the data contract from STAGE3_ML_README.md §3.2. The churn label is
sourced from predictions.risk_label where model_version='kaggle_baseline' —
that row represents the original Kaggle Churn label seeded in Stage 2.

No ad-hoc merging. No cleaning. No filtering beyond the model_version filter.
Stage 1 already cleaned the data; this module trusts that.
"""
from __future__ import annotations

import time
from typing import List

import pandas as pd

from database.connection import DatabaseConnection
from src.ml.eda.utils.signal_logger import SignalLogger


class TrainingDataLoader(SignalLogger):
    """Load the joined customers + customer_features + predictions DataFrame.

    OUTPUT shape (indexed by customer_id):
        - 6 categorical feature columns
        - 1 binary flag column (complaint_flag — treated as categorical for EDA)
        - 11 numeric feature columns
        - 1 target column ('churn', int {0, 1})
        = 19 columns + index

    Example:
        db = DatabaseConnection()
        loader = TrainingDataLoader(db)
        df = loader.load()
    """

    # Source-of-truth SQL. No params: training set is defined as
    # "every customer with a Kaggle ground-truth label" — a static filter.
    _SQL: str = """
        SELECT
            c.id                                  AS customer_id,
            c.gender,
            c.city_tier,
            c.marital_status,
            c.preferred_payment_mode,
            c.preferred_login_device,
            c.preferred_order_cat,
            cf.tenure_months,
            cf.order_count,
            cf.day_since_last_order,
            cf.hour_spend_on_app,
            cf.number_of_address,
            cf.number_of_device_registered,
            cf.coupon_used,
            cf.cashback_amount,
            cf.order_amount_hike_from_last_year,
            cf.warehouse_to_home,
            cf.satisfaction_score,
            cf.complain,
            c.kaggle_churn_label                  AS churn
        FROM customers c
        JOIN customer_features cf ON cf.customer_id = c.id
        WHERE c.kaggle_churn_label IS NOT NULL
        ;
    """

    # Feature taxonomy — consumed by downstream analyzers to pick the right tool.
    CATEGORICAL_COLS: List[str] = [
        "gender",
        "city_tier",                     # smallint 1/2/3 — treat as categorical for EDA clarity
        "marital_status",
        "preferred_payment_mode",
        "preferred_login_device",
        "preferred_order_cat",
        "complain",                      # BOOLEAN in Postgres — categorical analyzer handles it
    ]

    NUMERIC_COLS: List[str] = [
        "tenure_months",
        "order_count",
        "day_since_last_order",
        "hour_spend_on_app",
        "number_of_address",
        "number_of_device_registered",
        "coupon_used",
        "cashback_amount",
        "order_amount_hike_from_last_year",
        "warehouse_to_home",
        "satisfaction_score",
    ]

    TARGET_COL: str = "churn"
    ID_COL: str = "customer_id"

    def __init__(
        self,
        db: DatabaseConnection,
        model_version: str = "kaggle_baseline",
    ) -> None:
        super().__init__()
        self.db = db
        # Retained for backwards compat with the orchestrator + CLI. Unused by the
        # current Stage 3.1 SQL because the training set is defined by
        # customers.kaggle_churn_label IS NOT NULL, not by any prediction version.
        self.model_version = model_version

    def load(self) -> pd.DataFrame:
        """Execute the SQL, return a DataFrame indexed by customer_id."""
        self._signal(
            "Reading from",
            "Supabase.customers ⋈ customer_features",
            "filter: kaggle_churn_label IS NOT NULL",
        )
        t0 = time.perf_counter()

        # DatabaseConnection.execute_query returns a list of dict-rows
        # (psycopg2 RealDictCursor). pandas builds the DataFrame from that directly.
        rows = self.db.execute_query(self._SQL)
        df = pd.DataFrame(rows)
        if df.empty:
            raise RuntimeError(
                "Query returned zero rows. "
                "Expected ~5,630 customers with kaggle_churn_label populated."
            )
        df = df.set_index(self.ID_COL)

        # Postgres type normalisation. psycopg2 maps:
        #   - SMALLINT (churn, city_tier, etc.) → Python int       — fine as-is
        #   - NUMERIC (tenure_months, etc.)     → Python Decimal   — breaks numpy
        #   - BOOLEAN (complain)                 → Python bool      — display as 0/1
        # Convert to float/int so the rest of the pipeline works cleanly.
        df[self.TARGET_COL] = df[self.TARGET_COL].astype(int)
        for col in self.NUMERIC_COLS:
            if col in df.columns:
                df[col] = df[col].astype(float)
        if "complain" in df.columns:
            df["complain"] = df["complain"].astype(int)

        elapsed = time.perf_counter() - t0
        self._signal(
            "DONE",
            f"{len(df):,} rows × {len(df.columns)} cols",
            f"{elapsed:.2f}s",
        )
        return df
