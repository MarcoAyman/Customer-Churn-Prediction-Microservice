"""
Stage 3.3 — Model Training module.

Trains XGBoost (v1.0.0 → production) and LightGBM (v1.1.0 → staging),
tracks experiments in MLflow, pushes artifacts to HuggingFace Hub,
and writes model_versions rows to Supabase.

Run via: python scripts/run_training.py
"""
from src.ml.training.trainer import OptunaTuner, XGBoostTrainer, LightGBMTrainer
from src.ml.training.evaluator import ModelEvaluator, ThresholdSelector, SHAPExplainer
from src.ml.training.mlflow_tracker import MLflowExperiment, MLflowRunLogger
from src.ml.training.register import HFHubPublisher, SupabaseModelRegistry, ModelRegistrar

__all__ = [
    "OptunaTuner", "XGBoostTrainer", "LightGBMTrainer",
    "ModelEvaluator", "ThresholdSelector", "SHAPExplainer",
    "MLflowExperiment", "MLflowRunLogger",
    "HFHubPublisher", "SupabaseModelRegistry", "ModelRegistrar",
]
