"""
register.py — Stage 3.3: Model Registration

Classes:
    HFHubPublisher       — pushes all model artifacts to HuggingFace Hub
    SupabaseModelRegistry — writes model_versions row + manages production status
    ModelRegistrar        — orchestrates both: one call handles HF Hub + Supabase

WHY two separate registries:
    HuggingFace Hub   = artifact STORAGE (the pkl, json, yaml files live here)
    Supabase          = production REGISTRY (the API reads this to know what to load)

The FastAPI on startup does:
    1. Query Supabase: SELECT * FROM model_versions WHERE status='production' LIMIT 1
    2. Read artifact_path from that row (e.g. 'marcohanna90/churnguard-model-registry/v1.0.0')
    3. Download all files from HF Hub
    4. Load into memory

HF Hub repo: marcohanna90/churnguard-model-registry
Requires:    HUGGINGFACE_TOKEN env variable

Files pushed to HF Hub per model version:
    {version}/model.pkl
    {version}/preprocessor.pkl
    {version}/feature_names.json
    {version}/feature_engineering_config.json
    {version}/thresholds.yaml
    {version}/metrics.json
    {version}/shap_summary.json
    {version}/run_manifest.json
    {version}/reference_distribution.pkl  (XGBoost only — shared across versions)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from src.ml.eda.utils.signal_logger import SignalLogger


class HFHubPublisher(SignalLogger):
    """
    Pushes all model artifacts to HuggingFace Hub.

    Input files read + pushed (from staging dir + artifacts dir):
        models/staging/{version}/model.pkl
        models/staging/{version}/metrics.json
        models/staging/{version}/thresholds.yaml
        models/staging/{version}/shap_summary.json
        models/staging/{version}/run_manifest.json
        models/artifacts/preprocessor.pkl
        models/artifacts/feature_names.json
        models/artifacts/feature_engineering_config.json
        models/artifacts/reference_distribution.pkl   (if exists)

    Output: all files uploaded to HF Hub under {repo_id}/{version}/

    Requires: HUGGINGFACE_TOKEN environment variable
              (set in .env or export HUGGINGFACE_TOKEN=hf_...)
    """

    REPO_ID   = "marcohanna90/churnguard-model-registry"
    REPO_TYPE = "model"

    def __init__(
        self,
        staging_dir:   str | Path,
        artifacts_dir: str | Path = "models/artifacts",
    ) -> None:
        """
        Args:
            staging_dir:   models/staging/{version}/ — version-specific artifacts
            artifacts_dir: models/artifacts/ — shared artifacts (preprocessor, etc.)
        """
        super().__init__()
        self.staging_dir   = Path(staging_dir)
        self.artifacts_dir = Path(artifacts_dir)
        self.version       = self.staging_dir.name   # e.g. 'v1.0.0'

    def publish(self) -> str:
        """
        Upload all artifacts to HF Hub. Returns the HF Hub artifact path.

        Output: artifact_path string, e.g.
                'marcohanna90/churnguard-model-registry/v1.0.0'
                — stored in Supabase model_versions.artifact_path
                — used by FastAPI startup to download the model
        """
        from huggingface_hub import HfApi, login

        # Authenticate using HUGGINGFACE_TOKEN from environment/.env
        token = os.environ.get("HUGGINGFACE_TOKEN") or self._load_token_from_env()
        if not token:
            raise RuntimeError(
                "HUGGINGFACE_TOKEN not set. Add it to .env or export it:\n"
                "  export HUGGINGFACE_TOKEN=hf_..."
            )

        login(token=token, add_to_git_credential=False)
        api = HfApi()

        self._signal(
            "Publishing to",
            f"HF Hub: {self.REPO_ID}/{self.version}/",
            "authenticating...",
        )

        # Define which files to push: (local_path, path_in_repo)
        files_to_push = self._collect_artifacts()

        pushed = 0
        for local_path, repo_path in files_to_push:
            local_path = Path(local_path)
            if not local_path.exists():
                self._warn(f"Skipping missing artifact: {local_path}")
                continue

            size_kb = local_path.stat().st_size / 1024
            self._signal(
                "Pushing",
                f"{local_path.name}",
                f"→ HF Hub: {self.REPO_ID}/{repo_path}   ({size_kb:.1f} KB)",
            )

            api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=repo_path,
                repo_id=self.REPO_ID,
                repo_type=self.REPO_TYPE,
            )
            pushed += 1

        artifact_path = f"{self.REPO_ID}/{self.version}"
        self._signal(
            "DONE",
            f"{pushed} files pushed to HF Hub",
            f"artifact_path={artifact_path}",
        )
        return artifact_path

    def _collect_artifacts(self) -> List[tuple]:
        """
        Build the list of (local_path, path_in_repo) tuples.

        Version-specific files go under {version}/ in the repo.
        Shared artifacts (preprocessor, feature_names) also versioned
        so each version is self-contained.
        """
        version = self.version
        files = [
            # ── Version-specific (from staging dir) ──
            (self.staging_dir / "model.pkl",            f"{version}/model.pkl"),
            (self.staging_dir / "metrics.json",         f"{version}/metrics.json"),
            (self.staging_dir / "thresholds.yaml",      f"{version}/thresholds.yaml"),
            (self.staging_dir / "shap_summary.json",    f"{version}/shap_summary.json"),
            (self.staging_dir / "run_manifest.json",    f"{version}/run_manifest.json"),
            # ── Shared artifacts (from artifacts dir) ──
            # These are the same for all versions trained on the same data split.
            # Copying per-version ensures each version is self-contained on HF Hub.
            (self.artifacts_dir / "preprocessor.pkl",
             f"{version}/preprocessor.pkl"),
            (self.artifacts_dir / "feature_names.json",
             f"{version}/feature_names.json"),
            (self.artifacts_dir / "feature_engineering_config.json",
             f"{version}/feature_engineering_config.json"),
            (self.artifacts_dir / "reference_distribution.pkl",
             f"{version}/reference_distribution.pkl"),
        ]
        return files

    def _load_token_from_env(self) -> Optional[str]:
        """Try to load HUGGINGFACE_TOKEN from .env file."""
        env_file = Path(".env")
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("HUGGINGFACE_TOKEN="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
        return None


class SupabaseModelRegistry(SignalLogger):
    """
    Manages the model_versions table in Supabase.

    WHY this table: The FastAPI on Render reads this table on startup to discover
    which model version is 'production' and download it from HF Hub. MLflow is
    local only — Supabase is the production source of truth.

    Rules:
        - Only ONE row should have status='production' at any time
        - When a new model is promoted to production, the previous one is archived
        - Staging models can coexist with the production model
        - status enum values: 'production', 'staging', 'archived'

    Methods:
        register()  — INSERT a new row for the trained model
        promote()   — set status='production', archive the previous production model
    """

    def __init__(self, db) -> None:
        """
        Args:
            db: DatabaseConnection instance (already connected)
        """
        super().__init__()
        self.db = db

    def register(
        self,
        version:       str,
        metrics:       Dict,
        threshold:     float,
        algorithm:     str,
        artifact_path: str,
        mlflow_run_id: str,
        status:        str,
        feature_names: List[str],
        split_info:    Dict,
    ) -> str:
        """
        INSERT a new row into model_versions.

        Input:
            version:       e.g. 'v1.0.0'
            metrics:       from ModelEvaluator — AUC, recall, precision etc.
            threshold:     from ThresholdSelector
            algorithm:     'XGBoost' or 'LightGBM'
            artifact_path: HF Hub path from HFHubPublisher.publish()
            mlflow_run_id: from MLflowRunLogger.run_id
            status:        'production' or 'staging'
            feature_names: list of 38 feature names (hashed for feature_set_hash)
            split_info:    train/val/test row counts from split_summary.json

        Output:
            inserted row id (uuid string)
        """
        import hashlib

        # Compute a hash of the feature set to detect if features changed between versions
        feature_hash = hashlib.md5(
            json.dumps(sorted(feature_names)).encode()
        ).hexdigest()[:12]

        test_m = metrics.get("test", {})

        self._signal(
            "Writing to",
            "Supabase.model_versions",
            f"version={version} | status={status} | algorithm={algorithm}",
        )
        self._signal(
            "Row contents",
            f"auc_roc={test_m.get('auc_roc', 0):.4f} "
            f"recall={test_m.get('recall', 0):.4f} "
            f"precision={test_m.get('precision', 0):.4f}",
            f"threshold={threshold:.4f} | artifact_path={artifact_path}",
        )

        sql = """
            INSERT INTO model_versions (
                version, status, auc_roc, pr_auc, recall_score, precision_score,
                f1_score, brier_score, business_cost_score, decision_threshold,
                algorithm, feature_set_hash, train_rows, val_rows, test_rows,
                train_date, mlflow_run_id, artifact_path,
                promoted_by, promotion_notes, created_at
            ) VALUES (
                %(version)s, %(status)s, %(auc_roc)s, %(pr_auc)s,
                %(recall_score)s, %(precision_score)s, %(f1_score)s,
                %(brier_score)s, %(business_cost_score)s, %(decision_threshold)s,
                %(algorithm)s, %(feature_set_hash)s, %(train_rows)s,
                %(val_rows)s, %(test_rows)s, %(train_date)s,
                %(mlflow_run_id)s, %(artifact_path)s,
                %(promoted_by)s, %(promotion_notes)s, NOW()
            )
            RETURNING id::text;
        """

        params = {
            "version":              version,
            "status":               status,
            "auc_roc":              test_m.get("auc_roc"),
            "pr_auc":               test_m.get("pr_auc"),
            "recall_score":         test_m.get("recall"),
            "precision_score":      test_m.get("precision"),
            "f1_score":             test_m.get("f1"),
            "brier_score":          test_m.get("brier_score"),
            "business_cost_score":  test_m.get("business_cost_score"),
            "decision_threshold":   threshold,
            "algorithm":            algorithm,
            "feature_set_hash":     feature_hash,
            "train_rows":           split_info.get("train", {}).get("rows"),
            "val_rows":             split_info.get("val", {}).get("rows"),
            "test_rows":            split_info.get("test", {}).get("rows"),
            "train_date":           datetime.now(timezone.utc).date().isoformat(),
            "mlflow_run_id":        mlflow_run_id,
            "artifact_path":        artifact_path,
            "promoted_by":          "run_training.py",
            "promotion_notes":      f"Automated registration · algorithm={algorithm}",
        }

        # USE get_connection() NOT execute_query() for write operations.
        # execute_query() never calls conn.commit() — it is for SELECT only.
        # INSERT/UPDATE without commit are silently rolled back when the
        # connection returns to the pool.
        row_id = "unknown"
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # psycopg2 named-dict params use %(name)s syntax
                cur.execute(sql, params)
                result = cur.fetchone()
                row_id = result[0] if result else "unknown"
            conn.commit()   # ← commit the INSERT so it persists

        self._signal(
            "DONE",
            "model_versions row inserted",
            f"id={row_id} | version={version} | status={status}",
        )
        return row_id

    def promote_to_production(self, version: str) -> None:
        """
        Set version to 'production' and archive all other production rows.

        WHY: Only one row should have status='production' at any time.
        The API reads status='production' on startup — two production rows
        would cause non-deterministic model loading.

        Input:  version string to promote (e.g. 'v1.0.0')
        Effect: row with version=version gets status='production'
                all other rows with status='production' get status='archived'
        """
        self._signal(
            "Promoting to production",
            f"version={version}",
            "archiving previous production model first",
        )

        # USE get_connection() for UPDATE — execute_query() is SELECT-only.
        # Both statements run in ONE transaction with a single commit.
        # If the promote UPDATE fails, the archive UPDATE is also rolled back
        # — preventing a state where the old model is archived but nothing
        # is promoted to production.
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # Step A: archive any existing production row
                cur.execute(
                    """UPDATE model_versions
                       SET    status = 'archived', archived_at = NOW()
                       WHERE  status = 'production' AND version != %(version)s""",
                    {"version": version},
                )
                archived_count = cur.rowcount
                self._signal(
                    "Archived",
                    f"{archived_count} previous production row(s) → status=archived",
                )

                # Step B: promote the specified version
                cur.execute(
                    """UPDATE model_versions
                       SET    status = 'production', promoted_at = NOW()
                       WHERE  version = %(version)s""",
                    {"version": version},
                )
                promoted_count = cur.rowcount

            conn.commit()   # ← both UPDATEs committed atomically

        self._signal(
            "DONE",
            f"version={version} is now status=production",
            f"promoted={promoted_count} row | Restart Render to load the new model",
        )

    def get_production_version(self) -> Optional[Dict]:
        """Return the current production model_versions row, or None."""
        rows = self.db.execute_query(
            "SELECT * FROM model_versions WHERE status='production' ORDER BY created_at DESC LIMIT 1;"
        )
        return dict(rows[0]) if rows else None


class ModelRegistrar(SignalLogger):
    """
    Orchestrates HFHubPublisher + SupabaseModelRegistry in one call.

    Usage:
        registrar = ModelRegistrar(db=db, staging_dir=..., version='v1.0.0')
        registrar.register(
            model=model, metrics=metrics, threshold=threshold,
            algorithm='XGBoost', mlflow_run_id=run_id,
            status='production', feature_names=feature_names,
        )

    Reads to verify before pushing:
        models/staging/{version}/run_manifest.json  — confirms model was trained
        src/ml/feature_eng/reports/split_summary.json — train/val/test row counts
    """

    def __init__(
        self,
        db,
        staging_dir:   str | Path,
        artifacts_dir: str | Path = "models/artifacts",
        split_report:  str | Path = "src/ml/feature_eng/reports/split_summary.json",
    ) -> None:
        """
        Args:
            db:            DatabaseConnection (already connected)
            staging_dir:   models/staging/{version}/
            artifacts_dir: models/artifacts/
            split_report:  path to split_summary.json from Stage 3.2
        """
        super().__init__()
        self.db            = db
        self.staging_dir   = Path(staging_dir)
        self.artifacts_dir = Path(artifacts_dir)
        self.split_report  = Path(split_report)
        self.version       = self.staging_dir.name

    def register(
        self,
        model,
        metrics:       Dict,
        threshold:     float,
        algorithm:     str,
        mlflow_run_id: str,
        status:        str,
        feature_names: List[str],
    ) -> Dict:
        """
        Full registration: verify → push to HF Hub → write to Supabase.

        Input:
            model:         fitted classifier (XGBoost or LightGBM)
            metrics:       from ModelEvaluator
            threshold:     from ThresholdSelector
            algorithm:     'XGBoost' or 'LightGBM'
            mlflow_run_id: from MLflowRunLogger.run_id
            status:        'production' or 'staging'
            feature_names: list of 38 feature names

        Output:
            dict with artifact_path + supabase_row_id
        """
        self._signal(
            "Starting registration",
            f"version={self.version} | algorithm={algorithm} | status={status}",
        )

        # Verify run_manifest.json exists — ensures model was actually trained
        self._signal(
            "Reading from",
            str(self.staging_dir / "run_manifest.json"),
            "purpose: verify model was trained correctly before pushing to HF Hub",
        )
        manifest_path = self.staging_dir / "run_manifest.json"
        if not manifest_path.exists():
            raise RuntimeError(
                f"run_manifest.json not found at {manifest_path}. "
                "Was the trainer run successfully?"
            )
        with open(manifest_path) as f:
            manifest = json.load(f)
        self._signal(
            "Verified",
            "run_manifest.json",
            f"algorithm={manifest.get('algorithm')} | git_sha={manifest.get('git_sha', 'unknown')[:8]}",
        )

        # Load split_summary.json for train/val/test row counts (for model_versions table)
        self._signal(
            "Reading from",
            str(self.split_report),
            "purpose: extract train/val/test row counts → model_versions.train_rows etc.",
        )
        if self.split_report.exists():
            with open(self.split_report) as f:
                split_info = json.load(f)
        else:
            self._warn(f"split_summary.json not found at {self.split_report}. Row counts will be null.")
            split_info = {}

        # ── Step 1: Push to HF Hub ────────────────────────────────────────────
        self._signal("Step 1/3", "Pushing artifacts to HuggingFace Hub")
        publisher     = HFHubPublisher(self.staging_dir, self.artifacts_dir)
        artifact_path = publisher.publish()

        # ── Step 2: Write to Supabase ────────────────────────────────────────
        self._signal("Step 2/3", "Writing to Supabase model_versions table")
        registry = SupabaseModelRegistry(self.db)
        row_id   = registry.register(
            version       = self.version,
            metrics       = metrics,
            threshold     = threshold,
            algorithm     = algorithm,
            artifact_path = artifact_path,
            mlflow_run_id = mlflow_run_id,
            status        = status,
            feature_names = feature_names,
            split_info    = split_info,
        )

        # ── Step 3: Promote to production if applicable ──────────────────────
        if status == "production":
            self._signal("Step 3/3", "Promoting to production in Supabase")
            registry.promote_to_production(self.version)
        else:
            self._signal(
                "Step 3/3",
                f"status='{status}' — no promotion needed",
                "to promote later: registry.promote_to_production(version)",
            )

        result = {
            "version":       self.version,
            "algorithm":     algorithm,
            "status":        status,
            "artifact_path": artifact_path,
            "supabase_row":  row_id,
            "mlflow_run_id": mlflow_run_id,
        }

        self._signal(
            "DONE",
            f"version={self.version} registered",
            f"HF Hub={artifact_path} | Supabase row={row_id}",
        )
        return result
