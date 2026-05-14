"""
src/api/routes/predictions.py — Stage 3.4: Prediction Endpoints

Routes:
    POST /api/v1/predict/single    — score one customer by customer_id
    GET  /api/v1/predict/health    — model health check (version, threshold)
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from database.connection import DatabaseConnection
from src.api.dependencies import get_db
from src.api.services.prediction_service import PredictionService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/predict", tags=["Predictions"])


# ── Request / Response models ─────────────────────────────────────────────────

class PredictRequest(BaseModel):
    """Request body for single prediction."""
    customer_id: str = Field(
        ...,
        description="UUID of the customer to score",
        example="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    )
    write_to_db: bool = Field(
        default=True,
        description="If True, writes prediction to the predictions table in Supabase.",
    )


class ShapReason(BaseModel):
    """One SHAP reason code."""
    feature:    str   = Field(..., description="Feature name (e.g. 'tenure_months')")
    shap_value: float = Field(..., description="SHAP contribution to churn probability")
    direction:  str   = Field(..., description="'increases_risk' or 'decreases_risk'")


class PredictResponse(BaseModel):
    """Response from the single prediction endpoint."""
    customer_id:   str             = Field(..., description="Customer UUID")
    probability:   float           = Field(..., description="Churn probability [0.0-1.0]")
    risk_tier:     str             = Field(..., description="HIGH / MEDIUM / LOW")
    will_churn:    bool            = Field(..., description="probability >= threshold")
    threshold:     float           = Field(..., description="Decision threshold used")
    shap_reasons:  List[ShapReason] = Field(..., description="Top-3 SHAP reason codes")
    model_version: str             = Field(..., description="e.g. 'v1.0.0'")
    predicted_at:  str             = Field(..., description="ISO timestamp")


class ModelHealthResponse(BaseModel):
    """Model health check response."""
    status:        str   = Field(..., description="'ready' or 'not_loaded'")
    model_version: str   = Field(..., description="Loaded model version")
    threshold:     float = Field(..., description="Current decision threshold")
    n_features:    int   = Field(..., description="Number of input features")


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get(
    "/health",
    response_model=ModelHealthResponse,
    summary="Model health — version, threshold, feature count",
)
def model_health(request: Request) -> ModelHealthResponse:
    """
    Check that the model is loaded and return its metadata.
    Used by the dashboard to display the active model version.
    No authentication required.
    """
    ms = getattr(request.app.state, "model_service", None)

    if ms is None or not ms.is_loaded:
        return ModelHealthResponse(
            status="not_loaded",
            model_version="none",
            threshold=0.0,
            n_features=0,
        )

    return ModelHealthResponse(
        status="ready",
        model_version=ms.model_version,
        threshold=ms.threshold,
        n_features=len(ms.feature_names),
    )


@router.post(
    "/single",
    response_model=PredictResponse,
    summary="Score one customer — returns probability, risk tier, and SHAP reasons",
    description="""
    Runs the full inference pipeline for a single customer:

    1. Fetches raw features from Supabase (customers ⋈ customer_features)
    2. Applies feature engineering (same transformations as training)
    3. Applies preprocessing (StandardScaler + OneHotEncoder)
    4. XGBoost predict_proba() → churn probability
    5. Applies decision threshold → risk tier (HIGH / MEDIUM / LOW)
    6. Computes SHAP top-3 reason codes
    7. Writes result to predictions table (if write_to_db=True)

    Returns the probability, risk tier, and the top 3 features that
    drove the prediction for this specific customer.
    """,
)
def predict_single(
    body: PredictRequest,
    request: Request,
    db: DatabaseConnection = Depends(get_db),
) -> PredictResponse:
    """
    Score one customer by customer_id.

    The model must be loaded (startup_event completed) before this
    endpoint is called. Returns 503 if the model is not ready.
    """
    logger.info(
        f"POST /api/v1/predict/single"
        f"  customer_id={body.customer_id}  write_to_db={body.write_to_db}"
    )

    # Check model is loaded
    ms = getattr(request.app.state, "model_service", None)
    if ms is None or not ms.is_loaded:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model not loaded yet. API is still initialising — try again in a few seconds.",
        )

    # Run prediction
    try:
        svc    = PredictionService(model_service=ms)
        result = svc.predict(
            customer_id=body.customer_id,
            db=db,
            write_to_db=body.write_to_db,
        )
    except ValueError as e:
        # Customer not found or invalid input
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        logger.error(f"Prediction failed for {body.customer_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Prediction pipeline error: {str(e)}",
        )

    return PredictResponse(**result)
