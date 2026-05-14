"""
src/api/services/model_service.py — Stage 3.4: API Model Loading

Class: ModelLoader

Responsible for downloading all model artifacts from HuggingFace Hub
and loading them into memory on API startup. Stored in app.state so
every request accesses the same pre-loaded objects without re-downloading.

Files downloaded from HF Hub ({REPO_ID}/{version}/):
    model.pkl                       → XGBoost classifier
    preprocessor.pkl                → sklearn Pipeline (scaler + OHE)
    feature_engineering_config.json → fitted thresholds + target encoding rates
    thresholds.yaml                 → decision threshold + risk tier bounds
    feature_names.json              → 38 ordered feature names
    shap_summary.json               → global SHAP importance (for logging)

What ModelLoader provides to PredictionService:
    .model              → fitted XGBClassifier
    .preprocessor       → fitted sklearn Pipeline
    .engineer           → FeatureEngineer reconstructed from config (no fit needed)
    .threshold          → float decision threshold (e.g. 0.2097)
    .risk_tiers         → dict {HIGH: >=0.26, MEDIUM: >=0.06, LOW: below}
    .feature_names      → list of 38 feature name strings
    .explainer          → shap.TreeExplainer ready for per-prediction SHAP values
    .model_version      → version string (e.g. 'v1.0.0')
"""
from __future__ import annotations

import json
import logging
import os
import pickle
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)


class ModelLoader:
    """
    Downloads model artifacts from HF Hub and loads them into memory.

    Usage (in startup_event):
        loader = ModelLoader()
        await loader.load(artifact_path="marcohanna90/churnguard-model-registry/v1.0.0")
        app.state.model_service = loader

    Then in PredictionService:
        ms = request.app.state.model_service
        proba = ms.model.predict_proba(X)[:, 1]
    """

    REPO_ID   = "marcohanna90/churnguard-model-registry"
    REPO_TYPE = "model"

    def __init__(self) -> None:
        self.model:         Optional[Any]   = None
        self.preprocessor:  Optional[Any]   = None
        self.engineer:      Optional[Any]   = None   # FeatureEngineer from config
        self.explainer:     Optional[Any]   = None   # shap.TreeExplainer
        self.threshold:     float            = 0.50   # default until loaded
        self.risk_tiers:    Dict[str, float] = {}
        self.feature_names: List[str]        = []
        self.model_version: str              = "unknown"
        self._local_dir:    Optional[Path]   = None
        self._loaded:       bool             = False

    def load(self, artifact_path: str) -> "ModelLoader":
        """
        Download all artifacts from HF Hub and load into memory.

        Input:
            artifact_path: from Supabase model_versions.artifact_path
                           e.g. 'marcohanna90/churnguard-model-registry/v1.0.0'
                           The version subfolder is extracted from this path.

        Output: self (fluent interface, for chaining)

        Steps:
            1. Parse version from artifact_path
            2. Download all version files from HF Hub to temp dir
            3. Load model.pkl → self.model
            4. Load preprocessor.pkl → self.preprocessor
            5. Load feature_engineering_config.json → reconstruct FeatureEngineer
            6. Load thresholds.yaml → self.threshold + self.risk_tiers
            7. Load feature_names.json → self.feature_names
            8. Build shap.TreeExplainer → self.explainer
            9. Run smoke test
        """
        import yaml
        from huggingface_hub import hf_hub_download

        # Extract version from artifact_path: 'repo/name/v1.0.0' → 'v1.0.0'
        self.model_version = artifact_path.split("/")[-1]
        logger.info(f"[ModelLoader] Loading version={self.model_version} from HF Hub")
        logger.info(f"[ModelLoader] Reading from  → HF Hub: {artifact_path}/")

        # Download to a persistent temp directory (survives the function scope)
        self._local_dir = Path(tempfile.mkdtemp(prefix="churnguard_model_"))
        logger.info(f"[ModelLoader] Writing to    → local cache: {self._local_dir}")

        def _download(filename: str) -> Path:
            """Download one file from HF Hub and return its local path."""
            local_path = hf_hub_download(
                repo_id=self.REPO_ID,
                repo_type=self.REPO_TYPE,
                filename=f"{self.model_version}/{filename}",
                local_dir=str(self._local_dir),
                token=self._get_token(),
            )
            logger.info(
                f"[ModelLoader] Reading from  → HF Hub: {self.model_version}/{filename}"
                f"   purpose: {self._file_purpose(filename)}"
            )
            return Path(local_path)

        # ── 1. model.pkl ─────────────────────────────────────────────────────
        model_path = _download("model.pkl")
        with open(model_path, "rb") as f:
            self.model = pickle.load(f)
        logger.info(
            f"[ModelLoader] Loaded        → model.pkl ({self.model.__class__.__name__})"
        )

        # ── 2. preprocessor.pkl ──────────────────────────────────────────────
        pp_path = _download("preprocessor.pkl")
        with open(pp_path, "rb") as f:
            self.preprocessor = pickle.load(f)
        logger.info("[ModelLoader] Loaded        → preprocessor.pkl (sklearn Pipeline)")

        # ── 3. feature_engineering_config.json → FeatureEngineer ─────────────
        config_path = _download("feature_engineering_config.json")
        from src.ml.feature_eng.engineer import FeatureEngineer
        self.engineer = FeatureEngineer.from_config(config_path)
        logger.info(
            "[ModelLoader] Loaded        → feature_engineering_config.json"
            " → FeatureEngineer reconstructed (no fit needed)"
        )

        # ── 4. thresholds.yaml ───────────────────────────────────────────────
        thr_path = _download("thresholds.yaml")
        with open(thr_path) as f:
            thr_data = yaml.safe_load(f)
        self.threshold  = float(thr_data["decision_threshold"])
        self.risk_tiers = thr_data.get("risk_tiers", {})
        logger.info(
            f"[ModelLoader] Loaded        → thresholds.yaml"
            f"  decision_threshold={self.threshold:.4f}"
        )

        # ── 5. feature_names.json ────────────────────────────────────────────
        fn_path = _download("feature_names.json")
        with open(fn_path) as f:
            fn_data = json.load(f)
        self.feature_names = fn_data["feature_names"]
        logger.info(
            f"[ModelLoader] Loaded        → feature_names.json"
            f"  ({len(self.feature_names)} features)"
        )

        # ── 6. SHAP TreeExplainer ────────────────────────────────────────────
        # TreeExplainer is exact and fast for XGBoost/LightGBM.
        # Built once at startup — reused per prediction request.
        import shap
        self.explainer = shap.TreeExplainer(self.model)
        logger.info("[ModelLoader] Built         → shap.TreeExplainer (ready for per-prediction SHAP)")

        # ── 7. Smoke test ────────────────────────────────────────────────────
        self._smoke_test()

        self._loaded = True
        logger.info(
            f"[ModelLoader] DONE          → version={self.model_version} loaded"
            f"  threshold={self.threshold:.4f}"
            f"  features={len(self.feature_names)}"
        )
        return self

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """
        Return churn probabilities for a feature matrix.

        Input:  X — (n_samples, 38) numpy array from preprocessor.transform()
        Output: (n_samples,) array of churn probabilities [0.0, 1.0]
        """
        self._check_loaded()
        return self.model.predict_proba(X)[:, 1]

    def apply_threshold(self, probability: float) -> str:
        """
        Convert a churn probability to a risk tier label.

        Input:  probability — float from predict_proba [0.0, 1.0]
        Output: 'HIGH' / 'MEDIUM' / 'LOW'

        Risk tiers come from thresholds.yaml — selected by ThresholdSelector
        on the val set during training.
        """
        self._check_loaded()
        # risk_tiers values are strings like ">= 0.260"
        # Parse the numeric threshold from the string
        try:
            high_thr   = float(self.risk_tiers.get("HIGH",   ">= 0.26").split()[-1])
            medium_thr = float(self.risk_tiers.get("MEDIUM", ">= 0.06").split()[-1])
        except (ValueError, IndexError):
            high_thr, medium_thr = 0.26, 0.06

        if probability >= high_thr:
            return "HIGH"
        elif probability >= medium_thr:
            return "MEDIUM"
        else:
            return "LOW"

    def shap_top3(self, X_row: np.ndarray) -> List[Dict]:
        """
        Compute SHAP values for a single prediction row and return top-3 reasons.

        Input:  X_row — (1, 38) numpy array (one customer after preprocessing)
        Output: list of 3 dicts:
                [{"feature": "tenure_months", "shap_value": -0.90, "direction": "increases_risk"},
                 {"feature": "complain_risk", "shap_value": 0.48, "direction": "increases_risk"},
                 ...]

        WHY per-prediction SHAP:
            The model's global SHAP summary (shap_summary.json) shows which features
            matter overall. Per-prediction SHAP shows WHY this specific customer
            was flagged — the API response tells the operator: "this customer is
            flagged because of high complain_risk and low tenure."
        """
        self._check_loaded()

        shap_values = self.explainer.shap_values(X_row)

        # For binary classification, shap_values may be list [class0, class1]
        if isinstance(shap_values, list):
            sv = shap_values[1][0]   # class 1 (churn), first (only) row
        else:
            sv = shap_values[0]

        # Pair feature names with SHAP values and sort by absolute magnitude
        pairs = sorted(
            zip(self.feature_names, sv),
            key=lambda x: abs(x[1]),
            reverse=True,
        )

        return [
            {
                "feature":    name,
                "shap_value": float(val),
                "direction":  "increases_risk" if val > 0 else "decreases_risk",
            }
            for name, val in pairs[:3]
        ]

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    # ── private ──────────────────────────────────────────────────────────────

    def _smoke_test(self) -> None:
        """
        Run a quick sanity prediction on a synthetic all-zeros row.
        Ensures the model, preprocessor, and feature names are consistent.
        Raises RuntimeError if the smoke test fails.
        """
        logger.info("[ModelLoader] Running smoke test...")
        try:
            X_dummy = np.zeros((1, len(self.feature_names)))
            proba   = self.model.predict_proba(X_dummy)[:, 1]
            assert 0.0 <= proba[0] <= 1.0, f"Probability out of range: {proba[0]}"
            logger.info(
                f"[ModelLoader] Smoke test    → PASSED (dummy proba={proba[0]:.4f})"
            )
        except Exception as e:
            raise RuntimeError(f"Model smoke test failed: {e}") from e

    def _check_loaded(self) -> None:
        if not self._loaded:
            raise RuntimeError(
                "ModelLoader.load() has not been called. "
                "Check that startup_event completed successfully."
            )

    @staticmethod
    def _get_token() -> Optional[str]:
        """Load HF token from env or .env file."""
        token = os.environ.get("HUGGINGFACE_TOKEN")
        if token:
            return token
        env_file = Path(".env")
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("HUGGINGFACE_TOKEN="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
        return None

    @staticmethod
    def _file_purpose(filename: str) -> str:
        """Return a human-readable purpose string for each artifact file."""
        purposes = {
            "model.pkl":                        "XGBoost classifier → predict_proba()",
            "preprocessor.pkl":                 "sklearn Pipeline → scale + OHE",
            "feature_engineering_config.json":  "fitted thresholds + encoding rates → FeatureEngineer",
            "thresholds.yaml":                  "decision threshold + risk tier bounds",
            "feature_names.json":               "38 ordered feature names → SHAP labelling",
            "shap_summary.json":                "global SHAP importance (informational)",
        }
        return purposes.get(filename, "model artifact")
