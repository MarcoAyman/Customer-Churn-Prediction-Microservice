"""
src/api/services/prediction_service.py — Stage 3.4: Inference Pipeline

Class: PredictionService

Runs the full inference pipeline for a single customer:
    1. Fetch raw customer data from Supabase
    2. Convert Decimal → float (psycopg2 returns numeric as decimal.Decimal)
    3. Impute nulls (new customers have no order history)
    4. Run through FeatureEngineer.transform()
    5. Run through preprocessor.transform() (preprocessor.pkl)
    6. model.predict_proba() → churn probability
    7. Apply threshold → risk_tier (HIGH / MEDIUM / LOW)
    8. SHAP top-3 reason codes
    9. Write prediction row to predictions table (plain INSERT, full audit trail)
    10. Return structured response

Schema notes (predictions table):
    - No UNIQUE constraint on customer_id → plain INSERT, not upsert
    - Each call creates a new row → full audit trail
    - batch_run_id = NULL for realtime predictions
    - features_snapshot = JSONB of exact feature values used (required)
    - latency_ms = inference time in milliseconds
"""
from __future__ import annotations

import decimal
import json as _json
import logging
import time as _time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from database.connection import DatabaseConnection
from src.api.services.model_service import ModelLoader

logger = logging.getLogger(__name__)


class PredictionService:
    """
    Runs the full single-customer inference pipeline.

    Input:  customer_id (UUID) + connected db + loaded ModelLoader
    Output: dict with probability, risk_tier, shap_reasons, model_version
    """

    # SQL to fetch one customer's full feature row.
    # Mirrors the TrainingDataLoader SQL from Stage 3.1 EDA.
    # complain stored as boolean in Supabase → CASE cast to int for the model.
    CUSTOMER_QUERY = """
        SELECT
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
            CASE WHEN cf.complain THEN 1 ELSE 0 END AS complain
        FROM customers c
        JOIN customer_features cf ON cf.customer_id = c.id
        WHERE c.id = %(customer_id)s
          AND c.is_active = TRUE;
    """

    # Defaults for NULL fields — new customers have no order history yet.
    # Values come from the training distribution medians.
    NULL_DEFAULTS = {
        "day_since_last_order":             4.0,   # training median ~4 days
        "hour_spend_on_app":                3.0,   # training median ~3 hours
        "warehouse_to_home":                13.0,  # training median ~13 km
        "satisfaction_score":               3.0,   # midpoint of 1–5 scale
        "order_amount_hike_from_last_year": 15.0,  # training median ~15%
    }

    def __init__(self, model_service: ModelLoader) -> None:
        """
        Args:
            model_service: loaded ModelLoader from app.state
        """
        self.ms = model_service

    def predict(
        self,
        customer_id: str,
        db: DatabaseConnection,
        write_to_db: bool = True,
    ) -> Dict:
        """
        Full inference pipeline for one customer.

        Input:
            customer_id: UUID string of the customer to score
            db:          connected DatabaseConnection
            write_to_db: if True, writes result to predictions table

        Output dict:
            customer_id:   str
            probability:   float (0.0 – 1.0)
            risk_tier:     'HIGH' / 'MEDIUM' / 'LOW'
            will_churn:    bool (probability >= threshold)
            threshold:     float (from thresholds.yaml)
            shap_reasons:  list of 3 dicts [{feature, shap_value, direction}]
            model_version: str (e.g. 'v1.0.0')
            predicted_at:  ISO timestamp
            latency_ms:    int
        """
        t_start = _time.perf_counter()
        logger.info(f"[PredictionService] Predicting for customer_id={customer_id}")

        # ── 1. Fetch customer data from Supabase ──────────────────────────────
        row = self._fetch_customer(customer_id, db)

        # ── 2. Impute nulls ───────────────────────────────────────────────────
        row = self._impute_nulls(row)

        # ── 3. Capture features_snapshot before engineering ───────────────────
        # Store the raw (imputed) feature values used for this prediction.
        # Used for debugging and drift analysis — stored as JSONB in predictions.
        features_snapshot = {
            k: v for k, v in row.items()
            if k not in ("gender", "city_tier", "marital_status",
                         "preferred_payment_mode", "preferred_login_device",
                         "preferred_order_cat")
        }

        # ── 4. Feature engineering ────────────────────────────────────────────
        df   = pd.DataFrame([row])
        X_fe = self.ms.engineer.transform(df)

        # ── 5. Preprocessing (scale + OHE) ────────────────────────────────────
        X = self.ms.preprocessor.transform(X_fe)   # (1, 38) numpy array

        # ── 6. Predict probability ────────────────────────────────────────────
        probability = float(self.ms.predict_proba(X)[0])

        # ── 7. Apply threshold → risk tier ────────────────────────────────────
        will_churn = probability >= self.ms.threshold
        risk_tier  = self.ms.apply_threshold(probability)

        # ── 8. SHAP top-3 reasons ─────────────────────────────────────────────
        shap_reasons = self.ms.shap_top3(X)

        # ── 9. Compute latency ────────────────────────────────────────────────
        latency_ms   = int(((_time.perf_counter() - t_start) * 1000))
        predicted_at = datetime.now(timezone.utc)

        # ── 10. Write to predictions table ────────────────────────────────────
        if write_to_db:
            self._write_prediction(
                customer_id       = customer_id,
                probability       = probability,
                risk_tier         = risk_tier,
                will_churn        = will_churn,
                shap_reasons      = shap_reasons,
                features_snapshot = features_snapshot,
                predicted_at      = predicted_at,
                latency_ms        = latency_ms,
                db                = db,
            )

        result = {
            "customer_id":   customer_id,
            "probability":   round(probability, 4),
            "risk_tier":     risk_tier,
            "will_churn":    will_churn,
            "threshold":     self.ms.threshold,
            "shap_reasons":  shap_reasons,
            "model_version": self.ms.model_version,
            "predicted_at":  predicted_at.isoformat(),
            "latency_ms":    latency_ms,
        }

        logger.info(
            f"[PredictionService] DONE  customer={customer_id}"
            f"  prob={probability:.4f}  tier={risk_tier}"
            f"  churn={will_churn}  latency={latency_ms}ms"
        )
        return result

    # ── private methods ───────────────────────────────────────────────────────

    def _fetch_customer(self, customer_id: str, db: DatabaseConnection) -> Dict:
        """
        Fetch customer + features row from Supabase.

        Input:  customer_id UUID string
        Output: dict with all raw feature columns (may contain None values)

        Raises ValueError if customer not found or inactive.
        """
        logger.info(
            f"[PredictionService] Reading from → Supabase customers ⋈ customer_features"
            f"  WHERE customer_id={customer_id}"
        )
        rows = db.execute_query(self.CUSTOMER_QUERY, {"customer_id": customer_id})

        if not rows:
            raise ValueError(
                f"Customer {customer_id} not found or is inactive. "
                "Ensure the customer is registered and active."
            )

        row = dict(rows[0])

        # Convert Decimal → float — psycopg2 returns numeric columns as
        # decimal.Decimal which causes TypeError in feature engineering math:
        # "unsupported operand type(s) for /: 'decimal.Decimal' and 'float'"
        row = {
            k: float(v) if isinstance(v, decimal.Decimal) else v
            for k, v in row.items()
        }

        logger.info(
            f"[PredictionService] Fetched     → gender={row.get('gender')}"
            f"  tenure={row.get('tenure_months')}mo"
            f"  complain={row.get('complain')}"
        )
        return row

    def _impute_nulls(self, row: Dict) -> Dict:
        """
        Fill NULL values with training-distribution defaults.

        WHY: New customers have NULL for order-related features because
        they have no interaction history yet. pd.cut() and ratio formulas
        in FeatureEngineer will fail on None values.

        The defaults come from training distribution medians — a new customer
        is treated as "average" for these features until real data is available.

        Input:  row dict (may have None values)
        Output: row dict with None replaced by sensible defaults
        """
        imputed = {}
        for key, value in row.items():
            if value is None and key in self.NULL_DEFAULTS:
                imputed[key] = self.NULL_DEFAULTS[key]
                logger.debug(f"[PredictionService] Imputed → {key}=None → {self.NULL_DEFAULTS[key]}")
            else:
                imputed[key] = value
        return imputed

    def _write_prediction(
        self,
        customer_id:       str,
        probability:       float,
        risk_tier:         str,
        will_churn:        bool,
        shap_reasons:      List[Dict],
        features_snapshot: Dict,
        predicted_at:      datetime,
        latency_ms:        int,
        db:                DatabaseConnection,
    ) -> None:
        """
        Write prediction row to predictions table.

        Plain INSERT — no ON CONFLICT because predictions table has no UNIQUE
        constraint on customer_id. Each prediction call creates a new row,
        giving a full audit trail. The dashboard reads the latest row per
        customer using ORDER BY predicted_at DESC.

        Uses get_connection() + conn.commit() — execute_query() is SELECT-only.
        batch_run_id is NULL for realtime predictions (only set by batch scorer).
        """
        logger.info(
            f"[PredictionService] Writing to  → Supabase.predictions"
            f"  customer={customer_id}  tier={risk_tier}  prob={probability:.4f}"
        )

        sql = """
            INSERT INTO predictions (
                customer_id, churn_probability, churn_label, risk_tier,
                threshold_used, model_version, shap_top_reasons,
                features_snapshot, prediction_type, latency_ms, predicted_at
            ) VALUES (
                %(customer_id)s, %(churn_probability)s, %(churn_label)s,
                %(risk_tier)s, %(threshold_used)s, %(model_version)s,
                %(shap_top_reasons)s, %(features_snapshot)s,
                %(prediction_type)s, %(latency_ms)s, %(predicted_at)s
            );
        """

        params = {
            "customer_id":       customer_id,
            "churn_probability": probability,
            "churn_label":       will_churn,
            "risk_tier":         risk_tier,
            "threshold_used":    self.ms.threshold,
            "model_version":     self.ms.model_version,
            "shap_top_reasons":  _json.dumps(shap_reasons),
            "features_snapshot": _json.dumps(features_snapshot),
            "prediction_type":   "realtime",
            "latency_ms":        latency_ms,
            "predicted_at":      predicted_at.isoformat(),
        }

        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()

        logger.info("[PredictionService] Written  → predictions row inserted (audit trail)")
