"""
src/api/models/responses.py
══════════════════════════════════════════════════════════════════════════════
STANDARD RESPONSE WRAPPERS — consistent API response structure.

WHY STANDARD RESPONSES?
  Without a standard structure, different endpoints return data in different
  shapes. The frontend has to handle each one differently.

  With a standard wrapper, every response looks like:
    { "success": true,  "data": {...},    "message": "..." }
    { "success": false, "data": null,     "message": "Error detail" }

  The frontend always checks response.success first, then reads response.data.
  Simple and consistent.
══════════════════════════════════════════════════════════════════════════════
"""

from typing import Any, Optional
from pydantic import BaseModel


class APIResponse(BaseModel):
    """
    Standard wrapper for all successful API responses.
    Every endpoint returns this shape.
    """
    success: bool = True
    data:    Any  = None
    message: str  = "OK"


class ErrorResponse(BaseModel):
    """
    Standard wrapper for error responses.
    Returned by the global exception handler in main.py.
    """
    success: bool   = False
    data:    None   = None
    message: str    = "An error occurred"
    # Optional field list for validation errors (422 responses)
    errors: Optional[list] = None


class HealthResponse(BaseModel):
    """Response shape for GET /api/v1/health"""
    status:      str   = "healthy"
    environment: str   = "development"
    version:     str   = "1.0.0"
    db_connected: bool = False
