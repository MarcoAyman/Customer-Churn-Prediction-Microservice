"""
src/api/models/customer.py
══════════════════════════════════════════════════════════════════════════════
PYDANTIC MODELS — Customer request and response schemas.

WHY PYDANTIC MODELS?
  Pydantic models are the contract between the frontend and the API.
  They define:
    1. What fields the API expects in the request body
    2. What types those fields must be
    3. What valid values are allowed (ENUM validation)
    4. What the API sends back in the response

  If the frontend sends city_tier: "banana", Pydantic rejects it
  before the request even reaches the route handler — with a clear
  error message explaining what was wrong.

  This is the first line of data integrity defense. The database ENUMs
  are the second line. The data_integrity.py validator is the third.

ENUMS DEFINED HERE:
  These must match the PostgreSQL ENUMs in schema.sql exactly.
  If you change one here, change the corresponding ENUM in schema.sql too.
══════════════════════════════════════════════════════════════════════════════
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# ─────────────────────────────────────────────────────────────────────────────
# ENUMS — must match schema.sql ENUMs exactly
# ─────────────────────────────────────────────────────────────────────────────

class GenderEnum(str, Enum):
    """Matches gender_enum in schema.sql"""
    male   = "Male"
    female = "Female"


class MaritalStatusEnum(str, Enum):
    """Matches marital_status_enum in schema.sql"""
    single   = "Single"
    married  = "Married"
    divorced = "Divorced"


class PaymentModeEnum(str, Enum):
    """Matches payment_mode_enum in schema.sql — only canonical values"""
    cod         = "COD"
    credit_card = "Credit Card"
    debit_card  = "Debit Card"
    e_wallet    = "E wallet"
    upi         = "UPI"


class LoginDeviceEnum(str, Enum):
    """Matches login_device_enum in schema.sql"""
    mobile_phone = "Mobile Phone"
    computer     = "Computer"


class OrderCatEnum(str, Enum):
    """Matches order_cat_enum in schema.sql — only canonical values"""
    grocery            = "Grocery"
    fashion            = "Fashion"
    mobile             = "Mobile"
    laptop_accessory   = "Laptop & Accessory"
    others             = "Others"


# ─────────────────────────────────────────────────────────────────────────────
# REQUEST MODEL — what the entry form sends to POST /customers/register
# ─────────────────────────────────────────────────────────────────────────────

class CustomerRegisterRequest(BaseModel):
    """
    Validated request body for customer registration.

    All ENUM fields will be validated against their allowed values automatically.
    If the frontend sends an invalid value, Pydantic returns a 422 error with
    a clear message before the request reaches the route handler.

    Field examples are shown in the OpenAPI docs (/docs).
    """

    # Optional text — Kaggle rows have no names; live registrations may
    full_name: Optional[str] = Field(
        default=None,
        max_length=255,
        examples=["Sarah Mitchell"],
        description="Customer full name. Optional.",
    )

    # Required ENUM fields — must be one of the allowed values
    gender: GenderEnum = Field(
        ...,   # ... means required (no default)
        examples=["Male"],
        description="Customer gender. Must be 'Male' or 'Female'.",
    )

    marital_status: MaritalStatusEnum = Field(
        ...,
        examples=["Single"],
        description="Marital status. Must be 'Single', 'Married', or 'Divorced'.",
    )

    city_tier: int = Field(
        ...,
        ge=1,    # ge = greater than or equal to
        le=3,    # le = less than or equal to
        examples=[2],
        description="City tier: 1=metro, 2=mid-size, 3=smaller city.",
    )

    preferred_payment_mode: PaymentModeEnum = Field(
        ...,
        examples=["Credit Card"],
        description="Preferred payment method.",
    )

    preferred_login_device: LoginDeviceEnum = Field(
        ...,
        examples=["Mobile Phone"],
        description="Device the customer primarily uses to log in.",
    )

    preferred_order_cat: OrderCatEnum = Field(
        ...,
        examples=["Grocery"],
        description="Customer's preferred product category.",
    )

    # ── Validators ────────────────────────────────────────────────────────────

    @field_validator("full_name")
    @classmethod
    def clean_full_name(cls, v: Optional[str]) -> Optional[str]:
        """
        Strip leading/trailing whitespace from the name.
        Convert empty string to None — empty string and None are equivalent
        in meaning (no name provided) but None is cleaner in the database.
        """
        if v is None:
            return None
        cleaned = v.strip()
        return cleaned if cleaned else None

    model_config = {
        # Show example values in OpenAPI docs
        "json_schema_extra": {
            "example": {
                "full_name":               "Sarah Mitchell",
                "gender":                  "Female",
                "marital_status":          "Single",
                "city_tier":               2,
                "preferred_payment_mode":  "Credit Card",
                "preferred_login_device":  "Mobile Phone",
                "preferred_order_cat":     "Grocery",
            }
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# RESPONSE MODEL — what the API sends back after successful registration
# ─────────────────────────────────────────────────────────────────────────────

class CustomerRegisterResponse(BaseModel):
    """
    Response body returned after successful customer registration.

    This is what the entry form's useCustomerForm hook receives and
    passes to SuccessCard for display.
    """

    # The UUID generated by PostgreSQL at INSERT time
    customer_id: UUID = Field(
        description="UUID assigned by PostgreSQL. Unique, permanent identifier."
    )

    # Server timestamp at moment of INSERT
    registered_at: datetime = Field(
        description="UTC timestamp of when the customer was inserted into the database."
    )

    # How many days until the customer passes the scoring eligibility gate
    # (tenure_months >= 1 AND order_count >= 1)
    days_until_scoreable: int = Field(
        default=30,
        description="Days until this customer is eligible for churn scoring."
    )

    # Confirmation that both rows were created
    status: str = Field(
        default="created",
        description="Always 'created' on success."
    )

    # The initial feature values that were inserted into customer_features
    # Shown in SuccessCard so the operator can verify what was created
    initial_features: dict = Field(
        default_factory=dict,
        description="Initial customer_features row values at registration time."
    )

    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL DTO — data transfer between service layers (not exposed to API)
# ─────────────────────────────────────────────────────────────────────────────

class CustomerInsertData(BaseModel):
    """
    Internal model used by customer_service.py to pass cleaned data
    to the database insert operation.

    NOT returned in any API response — internal only.
    The 'DTO' suffix stands for Data Transfer Object.
    """
    full_name:               Optional[str]
    gender:                  str       # stored as ENUM string in DB
    marital_status:          str
    city_tier:               int
    preferred_payment_mode:  str
    preferred_login_device:  str
    preferred_order_cat:     str
    is_active:               bool = True
    role:                    str  = "customer"
