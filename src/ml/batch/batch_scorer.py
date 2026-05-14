"""
src/ml/batch/batch_scorer.py — Stage 3.5: Batch Scoring + Drift Monitoring

Classes:
    BatchScorer   — scores eligible customers in Supabase in chunks of 500
    DriftMonitor  — computes PSI between current batch and reference distribution

Execution order (called by scripts/run_batch_scoring.py):
    1. BatchScorer.load_artifacts()    → loads model, preprocessor, engineer from HF Hub
    2. BatchScorer.load_customers()    → fetches eligible customers from Supabase
    3. BatchScorer.score_all()         → scores in chunks, returns predictions + X_batch
    4. BatchScorer.write_predictions() → writes to predictions table
    5. DriftMonitor.check()            → PSI per feature vs reference_distribution.pkl
    6. BatchScorer.write_batch_run()   → summary row to batch_runs table

Scoring gate (§5.6):
    tenure_months >= 1   AND order_count >= 1
    AND (predicted_at IS NULL OR predicted_at < NOW() - INTERVAL '18 days')

Files read:
    Supabase.model_versions              → artifact_path of production model
    HF Hub {version}/model.pkl           → XGBoost classifier
    HF Hub {version}/preprocessor.pkl    → sklearn Pipeline
    HF Hub {version}/feature_engineering_config.json
    HF Hub {version}/reference_distribution.pkl → PSI reference

Files written:
    Supabase.predictions   → one row per customer (plain INSERT, full audit trail)
    Supabase.batch_runs    → one summary row per batch run
    Supabase.drift_reports → one row if PSI > threshold (optional table)
"""
from __future__ import annotations

import decimal
import json
import logging
import math
import pickle
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from database.connection import DatabaseConnection
from src.ml.eda.utils.signal_logger import SignalLogger

logger = logging.getLogger(__name__)


class BatchScorer(SignalLogger):
    """
    Scores all eligible customers in Supabase using the production model.

    Eligibility gate (§5.6):
        tenure_months >= 1        — at least one month on platform
        order_count >= 1          — at least one purchase
        not scored in last 18 days — avoids redundant scoring

    Input:  DatabaseConnection
    Output: predictions rows in Supabase + batch_runs summary row
    """

    CHUNK_SIZE: int = 500

    # Scoring gate SQL — enforces §5.6 new customer gate + 18-day cooldown.
    # COALESCE handles NULL feature values — new customers missing order data
    # get the training distribution medians so feature engineering doesn't crash.
    ALL_CUSTOMERS_SQL = """
        SELECT
            c.id::text                                              AS customer_id,
            c.gender,
            c.city_tier,
            c.marital_status,
            c.preferred_payment_mode,
            c.preferred_login_device,
            c.preferred_order_cat,
            cf.tenure_months,
            cf.order_count,
            COALESCE(cf.day_since_last_order, 4.0)                 AS day_since_last_order,
            COALESCE(cf.hour_spend_on_app, 3.0)                    AS hour_spend_on_app,
            cf.number_of_address,
            cf.number_of_device_registered,
            cf.coupon_used,
            cf.cashback_amount,
            COALESCE(cf.order_amount_hike_from_last_year, 15.0)    AS order_amount_hike_from_last_year,
            COALESCE(cf.warehouse_to_home, 13.0)                   AS warehouse_to_home,
            COALESCE(cf.satisfaction_score, 3.0)                   AS satisfaction_score,
            CASE WHEN cf.complain THEN 1 ELSE 0 END                AS complain
        FROM customers c
        JOIN customer_features cf ON cf.customer_id = c.id
        LEFT JOIN predictions p
            ON  p.customer_id = c.id
            AND p.predicted_at = (
                SELECT MAX(p2.predicted_at)
                FROM predictions p2
                WHERE p2.customer_id = c.id
            )
        WHERE c.is_active = TRUE
          AND cf.tenure_months >= 1
          AND cf.order_count >= 1
          AND (
              p.predicted_at IS NULL
              OR p.predicted_at < NOW() - INTERVAL '18 days'
          )
        ORDER BY c.id;
    """

    def __init__(self, db: DatabaseConnection) -> None:
        super().__init__()
        self.db              = db
        self.model:          Optional[Any]  = None
        self.preprocessor:   Optional[Any]  = None
        self.engineer:       Optional[Any]  = None
        self.threshold:      float           = 0.50
        self.model_version:  str             = "unknown"
        self._ref_dist:      Optional[Dict]  = None
        self._started_at:    str             = datetime.now(timezone.utc).isoformat()

    def load_artifacts(self) -> None:
        """
        Query Supabase for the production model → download all artifacts from HF Hub.

        Reading from:
            Supabase.model_versions WHERE status='production'
            → version, artifact_path, decision_threshold

        Downloads from HF Hub:
            {version}/model.pkl                       → XGBoost classifier
            {version}/preprocessor.pkl                → sklearn Pipeline
            {version}/feature_engineering_config.json → FeatureEngineer.from_config()
            {version}/reference_distribution.pkl      → PSI reference stats
        """
        import yaml
        from huggingface_hub import hf_hub_download
        from src.ml.feature_eng.engineer import FeatureEngineer

        self._signal(
            "Reading from",
            "Supabase.model_versions WHERE status='production'",
            "purpose: get artifact_path to download from HF Hub",
        )
        rows = self.db.execute_query(
            "SELECT version, artifact_path, decision_threshold "
            "FROM model_versions WHERE status='production' "
            "ORDER BY created_at DESC LIMIT 1;"
        )
        if not rows:
            raise RuntimeError("No production model found in model_versions table.")

        self.model_version = rows[0]["version"]
        artifact_path      = rows[0]["artifact_path"]
        self.threshold     = float(rows[0]["decision_threshold"])

        self._signal(
            "Production model",
            f"version={self.model_version}",
            f"artifact_path={artifact_path} | threshold={self.threshold:.4f}",
        )

        parts   = artifact_path.split("/")
        repo_id = "/".join(parts[:2])
        version = parts[2]
        tmp_dir = Path(tempfile.mkdtemp(prefix="churnguard_batch_"))

        def _dl(filename: str) -> Path:
            self._signal(
                "Reading from",
                f"HF Hub: {repo_id}/{version}/{filename}",
                f"purpose: {filename.split('.')[0]} artifact for batch scoring",
            )
            return Path(hf_hub_download(
                repo_id=repo_id, repo_type="model",
                filename=f"{version}/{filename}",
                local_dir=str(tmp_dir),
            ))

        with open(_dl("model.pkl"), "rb") as f:
            self.model = pickle.load(f)

        with open(_dl("preprocessor.pkl"), "rb") as f:
            self.preprocessor = pickle.load(f)

        config_path   = _dl("feature_engineering_config.json")
        self.engineer = FeatureEngineer.from_config(config_path)

        ref_path = _dl("reference_distribution.pkl")
        with open(ref_path, "rb") as f:
            self._ref_dist = pickle.load(f)
        self._signal("Loaded", "reference_distribution.pkl", "used by DriftMonitor for PSI")

    def load_customers(self) -> pd.DataFrame:
        """
        Fetch all scoring-eligible customers from Supabase.

        Gate enforced in SQL (§5.6):
            tenure_months >= 1      — must have at least 1 month of history
            order_count >= 1        — must have made at least one purchase
            not scored in 18 days   — avoids redundant predictions

        Converts decimal.Decimal → float after fetch because psycopg2 returns
        numeric columns as Decimal objects, which causes TypeError in pandas
        arithmetic operations inside feature engineering.

        Output: DataFrame with customer_id + all raw feature columns
        """
        self._signal(
            "Reading from",
            "Supabase customers ⋈ customer_features (gate: tenure≥1, orders≥1, not scored 18d)",
            "purpose: eligible customers → batch prediction",
        )
        rows = self.db.execute_query(self.ALL_CUSTOMERS_SQL)
        df   = pd.DataFrame(rows)

        # Convert Decimal → float — psycopg2 returns numeric columns as
        # decimal.Decimal. pandas arithmetic (coupon/order_count etc.) raises:
        # TypeError: unsupported operand type(s) for /: 'decimal.Decimal' and 'float'
        df = df.apply(lambda col: col.map(
            lambda v: float(v) if isinstance(v, decimal.Decimal) else v
        ))

        self._signal(
            "Loaded",
            f"{len(df):,} eligible customers",
            f"columns={list(df.columns[:5])}...",
        )
        return df

    def score_all(self, df: pd.DataFrame) -> Tuple[List[Dict], np.ndarray]:
        """
        Apply feature engineering + preprocessing + predict on all eligible customers.

        Engineering and preprocessing run on ALL rows at once (not chunked) for
        speed. Chunking only applies to the predict_proba() call and logging.

        Input:  df — full eligible customer DataFrame from load_customers()
        Output: (predictions list, X_batch numpy array for drift monitoring)
        """
        customer_ids = df["customer_id"].tolist()
        df_features  = df.drop(columns=["customer_id"])

        self._signal("Transforming", f"{len(df):,} customers → feature engineering")
        X_fe    = self.engineer.transform(df_features)
        X_batch = self.preprocessor.transform(X_fe)
        self._signal("Preprocessing DONE", f"X_batch={X_batch.shape}")

        predictions  = []
        total_chunks = math.ceil(len(df) / self.CHUNK_SIZE)
        self._signal(
            "Scoring",
            f"{len(df):,} customers in {total_chunks} chunks of {self.CHUNK_SIZE}",
        )

        for chunk_idx in range(total_chunks):
            start = chunk_idx * self.CHUNK_SIZE
            end   = min(start + self.CHUNK_SIZE, len(df))

            X_chunk      = X_batch[start:end]
            ids_chunk    = customer_ids[start:end]
            probas_chunk = self.model.predict_proba(X_chunk)[:, 1]

            for cust_id, prob in zip(ids_chunk, probas_chunk):
                risk_tier  = self._apply_threshold(float(prob))
                will_churn = float(prob) >= self.threshold
                predictions.append({
                    "customer_id":  cust_id,
                    "probability":  float(prob),
                    "risk_tier":    risk_tier,
                    "will_churn":   will_churn,
                })

            n_high = sum(1 for p in predictions[start:end] if p["risk_tier"] == "HIGH")
            self._signal(
                f"Chunk {chunk_idx+1}/{total_chunks}",
                f"rows {start}–{end}",
                f"HIGH risk in chunk: {n_high}",
            )

        high_count = sum(1 for p in predictions if p["risk_tier"] == "HIGH")
        med_count  = sum(1 for p in predictions if p["risk_tier"] == "MEDIUM")
        low_count  = sum(1 for p in predictions if p["risk_tier"] == "LOW")
        self._signal(
            "Scoring DONE",
            f"{len(predictions):,} predictions",
            f"HIGH={high_count} | MEDIUM={med_count} | LOW={low_count}",
        )
        return predictions, X_batch

    def write_predictions(self, predictions: List[Dict], batch_run_id: str) -> int:
        """
        Write all predictions to Supabase.predictions table.

        Plain INSERT — no ON CONFLICT because predictions table has no UNIQUE
        constraint on customer_id. Design intent = full audit trail, one row
        per batch run per customer. Dashboard reads latest row per customer.

        Columns match actual predictions table schema:
            churn_label      (not will_churn)
            threshold_used   (not decision_threshold)
            prediction_type  (not prediction_source) = 'batch'
            features_snapshot = NULL for batch (too large to store per row)

        Uses get_connection() + conn.commit() — execute_query() is SELECT-only.
        All rows committed in one transaction after all chunks are written.
        """
        self._signal(
            "Writing to",
            "Supabase.predictions",
            f"{len(predictions):,} rows (plain INSERT — full audit trail)",
        )

        sql = """
            INSERT INTO predictions (
                customer_id, churn_probability, churn_label, risk_tier,
                threshold_used, model_version, batch_run_id,
                prediction_type, predicted_at
            ) VALUES (
                %(customer_id)s, %(churn_probability)s, %(churn_label)s,
                %(risk_tier)s, %(threshold_used)s, %(model_version)s,
                %(batch_run_id)s, %(prediction_type)s, %(predicted_at)s
            );
        """

        now          = datetime.now(timezone.utc).isoformat()
        written      = 0
        total_chunks = math.ceil(len(predictions) / self.CHUNK_SIZE)

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for chunk_idx in range(total_chunks):
                    start = chunk_idx * self.CHUNK_SIZE
                    chunk = predictions[start:start + self.CHUNK_SIZE]
                    for pred in chunk:
                        cur.execute(sql, {
                            "customer_id":      pred["customer_id"],
                            "churn_probability": pred["probability"],
                            "churn_label":       pred["will_churn"],
                            "risk_tier":         pred["risk_tier"],
                            "threshold_used":    self.threshold,
                            "model_version":     self.model_version,
                            "batch_run_id":      batch_run_id,
                            "prediction_type":   "batch",
                            "predicted_at":      now,
                        })
                    written += len(chunk)
            conn.commit()

        self._signal("DONE", f"{written:,} predictions written to Supabase")
        return written

    def create_batch_run(
        self,
        batch_run_id: str,
        triggered_by: str = "manual",
    ) -> None:
        """
        INSERT a batch_runs row with status='running' at the START of the batch.

        WHY before predictions:
            predictions.batch_run_id is a FK to batch_runs.id.
            The batch_runs row must exist before any predictions can reference it.
            If we insert predictions first, we get a ForeignKeyViolation.

        Input:  batch_run_id (uuid), triggered_by
        Output: one row in batch_runs with status='running'
        """
        self._signal(
            "Writing to",
            "Supabase.batch_runs",
            f"batch_run_id={batch_run_id} | status=running (placeholder before predictions)",
        )
        sql = """
            INSERT INTO batch_runs (
                id, model_version, triggered_by, status,
                started_at, drift_checked, drift_alert_fired
            ) VALUES (
                %(id)s, %(model_version)s, %(triggered_by)s, %(status)s,
                %(started_at)s, %(drift_checked)s, %(drift_alert_fired)s
            );
        """
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "id":              batch_run_id,
                    "model_version":   self.model_version,
                    "triggered_by":    triggered_by,
                    "status":          "running",
                    "started_at":      self._started_at,
                    "drift_checked":   False,
                    "drift_alert_fired": False,
                })
            conn.commit()
        self._signal("DONE", "batch_runs placeholder row inserted (status=running)")

    def complete_batch_run(
        self,
        batch_run_id:    str,
        n_eligible:      int,
        n_scored:        int,
        n_high:          int,
        n_medium:        int,
        n_low:           int,
        drift_checked:   bool,
        drift_alert:     bool,
        max_psi_feature: Optional[str],
        max_psi_value:   Optional[float],
        elapsed_s:       float,
    ) -> None:
        """
        UPDATE the batch_runs row to status='completed' at the END of the batch.

        Called after all predictions are written and drift is checked.
        Updates all count columns and sets status='completed'.
        duration_seconds is a computed column — Postgres calculates it automatically.

        Input:  final batch statistics
        Output: batch_runs row updated
        """
        self._signal(
            "Updating",
            "Supabase.batch_runs",
            f"batch_run_id={batch_run_id} | n_scored={n_scored} | drift={drift_alert}",
        )
        sql = """
            UPDATE batch_runs SET
                status               = 'completed',
                completed_at         = %(completed_at)s,
                customers_eligible   = %(customers_eligible)s,
                customers_scored     = %(customers_scored)s,
                customers_skipped    = %(customers_skipped)s,
                high_risk_count      = %(high_risk_count)s,
                medium_risk_count    = %(medium_risk_count)s,
                low_risk_count       = %(low_risk_count)s,
                drift_checked        = %(drift_checked)s,
                drift_alert_fired    = %(drift_alert_fired)s,
                max_psi_feature      = %(max_psi_feature)s,
                max_psi_value        = %(max_psi_value)s
            WHERE id = %(id)s;
        """
        now = datetime.now(timezone.utc).isoformat()
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "id":                  batch_run_id,
                    "completed_at":        now,
                    "customers_eligible":  n_eligible,
                    "customers_scored":    n_scored,
                    "customers_skipped":   max(0, n_eligible - n_scored),
                    "high_risk_count":     n_high,
                    "medium_risk_count":   n_medium,
                    "low_risk_count":      n_low,
                    "drift_checked":       drift_checked,
                    "drift_alert_fired":   drift_alert,
                    "max_psi_feature":     max_psi_feature,
                    "max_psi_value":       max_psi_value,
                })
            conn.commit()
        self._signal("DONE", "batch_runs row updated to status=completed")

    def _apply_threshold(self, probability: float) -> str:
        """Convert probability to risk tier string."""
        high_thr   = self.threshold + 0.05
        medium_thr = max(self.threshold - 0.15, 0.05)
        if probability >= high_thr:
            return "HIGH"
        elif probability >= medium_thr:
            return "MEDIUM"
        else:
            return "LOW"

    @property
    def reference_distribution(self) -> Optional[Dict]:
        return self._ref_dist


class DriftMonitor(SignalLogger):
    """
    Computes PSI (Population Stability Index) between batch feature distributions
    and the reference distribution saved during training (reference_distribution.pkl).

    PSI interpretation:
        < 0.10       — no significant change (green)
        0.10 – 0.20  — moderate change, monitor (yellow)
        > 0.20       — significant shift, retrain candidate (red)

    PSI formula per feature:
        PSI = Σ (actual_pct - expected_pct) × ln(actual_pct / expected_pct)

    Bins each feature into N_BINS equal-width bins using the reference p5/p95
    as the bin range — avoids extreme outliers dominating the binning.
    """

    PSI_ALERT_THRESHOLD: float = 0.20
    N_BINS:              int   = 10
    MIN_BIN_PCT:         float = 0.0001   # avoid log(0)

    def __init__(self) -> None:
        super().__init__()

    def check(
        self,
        X_batch:       np.ndarray,
        ref_dist:      Dict,
        feature_names: List[str],
        db:            DatabaseConnection,
        batch_run_id:  str,
    ) -> Tuple[bool, Dict]:
        """
        Compute PSI for each feature and detect drift.

        Input:
            X_batch:       (n_samples, 38) preprocessed batch feature matrix
            ref_dist:      dict from reference_distribution.pkl — contains
                           mean, std, percentiles (p5/p25/p50/p75/p95) per feature
            feature_names: 38 ordered feature names
            db:            DatabaseConnection
            batch_run_id:  links drift report to the batch run

        Output:
            (drift_detected: bool, results: dict with overall_psi + per-feature scores)
        """
        self._signal(
            "Running PSI drift check",
            f"{X_batch.shape[0]:,} batch rows vs reference ({ref_dist['n_samples']:,} train rows)",
            f"alert threshold: PSI > {self.PSI_ALERT_THRESHOLD}",
        )

        psi_scores    = {}
        drifted_feats = []

        for i, feat_name in enumerate(feature_names):
            p5  = float(np.array(ref_dist["percentiles"]["p5"])[i])
            p95 = float(np.array(ref_dist["percentiles"]["p95"])[i])
            psi = self._psi_for_feature(X_batch[:, i], p5, p95, ref_dist, i)
            psi_scores[feat_name] = round(float(psi), 4)
            if psi > self.PSI_ALERT_THRESHOLD:
                drifted_feats.append(feat_name)

        overall_psi    = float(np.mean(list(psi_scores.values())))
        drift_detected = overall_psi > self.PSI_ALERT_THRESHOLD or len(drifted_feats) > 0

        top5 = sorted(psi_scores.items(), key=lambda x: -x[1])[:5]
        self._signal(
            "PSI results",
            f"overall_psi={overall_psi:.4f}",
            f"drifted_features={len(drifted_feats)} | alert={drift_detected}",
        )
        for feat, psi in top5:
            flag = " ← DRIFT" if psi > self.PSI_ALERT_THRESHOLD else ""
            self._signal(f"  PSI {feat}", f"{psi:.4f}{flag}")

        if drift_detected:
            self._warn(
                f"DRIFT DETECTED — overall PSI={overall_psi:.4f} > {self.PSI_ALERT_THRESHOLD}\n"
                f"Top drifted features: {drifted_feats[:5]}\n"
                "Consider retraining the model."
            )

        # max PSI feature for batch_runs table
        max_feat, max_val = top5[0] if top5 else (None, None)

        return drift_detected, {
            "overall_psi":      overall_psi,
            "drifted_features": drifted_feats,
            "psi_scores":       psi_scores,
            "drift_detected":   drift_detected,
            "max_psi_feature":  max_feat,
            "max_psi_value":    max_val,
        }

    def _psi_for_feature(
        self,
        batch_col: np.ndarray,
        p5:        float,
        p95:       float,
        ref_dist:  Dict,
        feat_idx:  int,
    ) -> float:
        """Compute PSI for one feature column."""
        lo = p5 - 1e-6
        hi = p95 + 1e-6
        if lo >= hi:
            return 0.0

        bins    = np.linspace(lo, hi, self.N_BINS + 1)
        n_ref   = ref_dist["n_samples"]
        ref_p25 = float(np.array(ref_dist["percentiles"]["p25"])[feat_idx])
        ref_p50 = float(np.array(ref_dist["percentiles"]["p50"])[feat_idx])
        ref_p75 = float(np.array(ref_dist["percentiles"]["p75"])[feat_idx])

        ref_approx = np.array(
            [ref_p25] * (n_ref // 4)
            + [ref_p50] * (n_ref // 2)
            + [ref_p75] * (n_ref // 4)
        )

        ref_counts, _ = np.histogram(ref_approx, bins=bins)
        bat_counts, _ = np.histogram(batch_col,  bins=bins)

        ref_pct = np.clip(ref_counts / max(ref_counts.sum(), 1), self.MIN_BIN_PCT, None)
        bat_pct = np.clip(bat_counts / max(bat_counts.sum(), 1), self.MIN_BIN_PCT, None)

        return abs(float(np.sum((bat_pct - ref_pct) * np.log(bat_pct / ref_pct))))
