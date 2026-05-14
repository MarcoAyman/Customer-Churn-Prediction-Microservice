"""ChurnGuard — Stage 3.1: Supabase EDA module.

Public entry point: SupabaseEDA. Call .run() to execute the full EDA pipeline.
"""
from src.ml.eda.eda_orchestrator import SupabaseEDA
__all__ = ["SupabaseEDA"]
