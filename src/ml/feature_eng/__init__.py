"""
Stage 3.2 — Feature Engineering module.

Public entry point: FeatureEngPipeline (in orchestrator.py).
Run via: python scripts/run_feature_engineering.py
"""
from src.ml.feature_eng.engineer import FeatureEngineer
from src.ml.feature_eng.pipeline import StratifiedSplitter, PreprocessorBuilder
from src.ml.feature_eng.orchestrator import FeatureEngPipeline

__all__ = ["FeatureEngineer", "StratifiedSplitter", "PreprocessorBuilder", "FeatureEngPipeline"]
