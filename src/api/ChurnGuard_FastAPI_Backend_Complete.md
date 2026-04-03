# ChurnGuard — FastAPI Backend Architecture
### Complete Technical Reference
> **Version:** 1.0.0 · **Stack:** Python 3.11 · FastAPI · Pydantic v2 · psycopg2 · Supabase · Render · React · Vercel

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Project Structure](#2-project-structure)
3. [Configuration Layer](#3-configuration-layer)
4. [Database Connection Module](#4-database-connection-module)
5. [Data Models & Pydantic Schemas](#5-data-models--pydantic-schemas)
6. [Data Integrity & Validation](#6-data-integrity--validation)
7. [Services Layer](#7-services-layer)
8. [Routes Layer](#8-routes-layer)
9. [Server-Sent Events (SSE)](#9-server-sent-events-sse)
10. [Dependency Injection](#10-dependency-injection)
11. [Application Factory — main.py](#11-application-factory--mainpy)
12. [Full Request Lifecycle](#12-full-request-lifecycle)
13. [Error Handling Strategy](#13-error-handling-strategy)
14. [Deployment on Render](#14-deployment-on-render)
15. [Environment Variables Reference](#15-environment-variables-reference)
16. [API Endpoints Reference](#16-api-endpoints-reference)
17. [Design Principles](#17-design-principles)
18. [Layer Communication Map](#18-layer-communication-map)
19. [Quick Reference Cheatsheet](#19-quick-reference-cheatsheet)
20. [Extending This Backend](#20-extending-this-backend)

---

## 1. System Overview

ChurnGuard's backend serves three distinct responsibilities handled by one FastAPI application:

```
┌─────────────────────────────────────────────────────────────────┐
│                        FASTAPI APPLICATION                       │
│                      (Render · Docker · Port $PORT)              │
│                                                                  │
│   ┌──────────────────┐  ┌─────────────────┐  ┌──────────────┐  │
│   │  CUSTOMER ENTRY  │  │  DASHBOARD DATA  │  │  SSE STREAM  │  │
│   │                  │  │                  │  │              │  │
│   │ POST /register   │  │ GET /overview    │  │ GET /events  │  │
│   │                  │  │ GET /at-risk     │  │              │  │
│   │ Validates input  │  │ GET /churn-trend │  │ Pushes live  │  │
│   │ Inserts 2 rows   │  │ GET /drift       │  │ events to    │  │
│   │ Fires SSE event  │  │ GET /last-batch  │  │ dashboard    │  │
│   └────────┬─────────┘  └────────┬────────┘  └──────┬───────┘  │
│            │                     │                   │           │
│            └─────────────────────┼───────────────────┘           │
│                                  │                               │
│                    ┌─────────────▼──────────────┐               │
│                    │     DatabaseConnection       │               │
│                    │     (psycopg2 pool)          │               │
│                    └─────────────┬──────────────┘               │
└──────────────────────────────────┼──────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   Supabase PostgreSQL        │
                    │   Port 6543 (pgBouncer)      │
                    │   7 Tables · 4 Views         │
                    └─────────────────────────────┘
```

### Consumers of the API

| Consumer | Deployed On | What It Calls |
|---|---|---|
| Entry Form (`src/entry_form`) | Vercel | `POST /api/v1/customers/register` |
| Ops Dashboard (`src/operational_dashboard`) | Vercel | `GET /api/v1/admin/*` + SSE stream |
| GitHub Actions (daily cron) | GitHub | `POST /api/v1/admin/refresh-tenure` |
| GitHub Actions (batch cron) | GitHub | `POST /api/v1/predict/batch` (Stage 3) |

---

## 2. Project Structure

```
churn_prediction/
│
├── src/api/                         ← FastAPI application
│   ├── __init__.py
│   ├── config.py                    ← All settings from .env (pydantic-settings)
│   ├── dependencies.py              ← Shared FastAPI dependencies
│   ├── main.py                      ← App factory, CORS, routers, startup
│   │
│   ├── models/                      ← Pydantic data schemas
│   │   ├── customer.py              ← Request/response models + ENUMs
│   │   └── responses.py             ← Standard APIResponse wrapper
│   │
│   ├── validators/
│   │   └── data_integrity.py        ← Business rule checks (3rd defense layer)
│   │
│   ├── services/                    ← All business logic
│   │   ├── customer_service.py      ← Registration orchestrator
│   │   ├── feature_service.py       ← customer_features row creation + tenure
│   │   └── sse_service.py           ← In-memory event queue (asyncio.Queue)
│   │
│   └── routes/                      ← HTTP handlers (thin — call services only)
│       ├── customers.py             ← POST /api/v1/customers/register
│       ├── admin.py                 ← GET  /api/v1/admin/*
│       └── events.py                ← GET  /api/v1/admin/events (SSE)
│
├── database/
│   └── connection.py                ← Reusable DatabaseConnection class
│
├── config/
│   ├── cleaning_config.py           ← Stage 1 cleaning rules (used by API too)
│   └── db_connection_config.py      ← Pool settings constants
│
├── Dockerfile                       ← Container definition for Render
├── render.yaml                      ← Render infrastructure-as-code
├── requirements.txt                 ← Python dependencies
└── scripts/
    └── run_api.py                   ← CLI: python scripts/run_api.py
```

### Why This Structure

```
RULE: Every file has ONE job it can be described in one sentence.

config.py          → "reads all settings from the environment"
models/            → "defines what data looks like"
validators/        → "enforces business rules that Pydantic cannot"
services/          → "contains all business logic"
routes/            → "receives HTTP requests and calls services"
dependencies.py    → "provides shared resources to routes"
main.py            → "wires everything together"
```

> If you cannot describe a file's job in one sentence, it is doing too much.

---

## 3. Configuration Layer

**File:** `src/api/config.py`

### How It Works

`pydantic-settings` (`BaseSettings`) reads every configurable value from the environment. The app **refuses to start** if required fields are missing or the wrong type. Silent misconfiguration is harder to debug than a loud startup failure.

**Priority order (first match wins):**
```
1. Environment variable set in shell / Render dashboard
2. .env file (loaded by BaseSettings automatically)
3. Default value defined on the field
```

```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    database_url:    str          # required — no default → app crashes if missing
    admin_api_key:   str = "..."  # has default — optional in dev
    cors_origins:    str = "..."  # comma-separated string → parsed to list
    environment:     str = "development"

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,   # DATABASE_URL and database_url both work
        extra="ignore",         # extra env vars don't cause errors
    )
```

### `@lru_cache` Singleton Pattern

```python
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

settings = get_settings()   # module-level convenience alias
```

> **Why `@lru_cache`?** `Settings()` reads from the environment and validates on every call. The cache ensures this happens **once per process**, not once per request. All imports share the same instance.

### Settings Reference

| Variable | Type | Required | Default | Description |
|---|---|---|---|---|
| `DATABASE_URL` | `str` | ✓ | — | Supabase pooler URL (port 6543) |
| `ADMIN_API_KEY` | `str` | — | dev key | Header auth for admin endpoints |
| `CORS_ORIGINS` | `str` | — | localhost | Comma-separated allowed origins |
| `ENVIRONMENT` | `str` | — | `development` | `development` or `production` |
| `SSE_PING_INTERVAL_SECONDS` | `int` | — | `30` | Keepalive ping to prevent Render sleep closing SSE |
| `POOL_MIN_CONNECTIONS` | `int` | — | `1` | Warm connections always ready |
| `POOL_MAX_CONNECTIONS` | `int` | — | `5` | Max simultaneous DB connections |
| `HUGGINGFACE_REPO` | `str` | — | `""` | ML artifact repo path |
| `HF_TOKEN` | `str` | — | `""` | HuggingFace download token |

### Derived Properties

```python
@property
def cors_origins_list(self) -> List[str]:
    # "http://a.com,https://b.com" → ["http://a.com", "https://b.com"]
    return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

@property
def is_production(self) -> bool:
    return self.environment.lower() == "production"

@property
def is_development(self) -> bool:
    return self.environment.lower() == "development"
```

### Built-in Validators

```python
@field_validator("database_url")
@classmethod
def validate_database_url(cls, v: str) -> str:
    if not v.startswith("postgresql://"):
        raise ValueError("DATABASE_URL must start with 'postgresql://'")
    if ":5432" in v:
        logger.warning("Use port 6543 (pooler), not 5432 (direct)")
    return v

@field_validator("environment")
@classmethod
def validate_environment(cls, v: str) -> str:
    if v.lower() not in {"development", "production", "test"}:
        raise ValueError(f"environment must be development/production/test")
    return v.lower()
```

---

## 4. Database Connection Module

**File:** `database/connection.py`

### Architecture

A single reusable class wrapping `psycopg2.pool.SimpleConnectionPool`. Every part of the system that needs a database connection imports this class — **one source of truth, no psycopg2 calls anywhere else**.

```
┌────────────────────────────────────────────────┐
│             DatabaseConnection                  │
│                                                │
│  connect()         → opens the pool             │
│  disconnect()      → closes all connections     │
│  health_check()    → runs SELECT 1              │
│  execute_query()   → SELECT → returns list[dict]│
│  get_connection()  → context manager for writes │
│  is_connected      → bool property              │
│  get_pool_status() → diagnostic dict            │
└────────────────────────────────────────────────┘
```

### Connection Pool

```python
self._pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,      # 1 warm connection always ready — zero cold-start latency
    maxconn=5,      # max 5 simultaneous queries — conservative for free tier
    dsn=db_url,
    connect_timeout=10,                           # fail fast if unreachable
    options=f"-c statement_timeout={timeout_ms}"  # cancels runaway queries
)
```

> **Why pooling?** Opening a raw PostgreSQL connection takes 50–200ms (TCP handshake + TLS negotiation + PostgreSQL auth). The pool reuses open connections. Requests borrow a connection, use it, and return it — zero cold-start overhead after first request.

### The Two Usage Patterns

**Pattern A — SELECT queries (read-only)**
```python
# Returns list[dict] — column names as keys, Python-native types
rows = db.execute_query(
    "SELECT * FROM customers WHERE city_tier = %s",
    params=(2,),       # ALWAYS parameterised — never string format (SQL injection)
    as_dict=True       # RealDictCursor — row["column_name"] not row[0]
)
```

**Pattern B — INSERT / UPDATE / DELETE (writes)**
```python
with db.get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO customers (...) VALUES (%s)", values)
    conn.commit()   # explicit commit — YOU control the transaction boundary
# connection is automatically returned to the pool here (via finally block)
# if an exception occurs → rollback() is called automatically
```

### Retry Logic

```
connect() called
    │
    ├── Attempt 1 → OperationalError (Supabase paused / network blip)
    │   └── wait 2s (RETRY_DELAY_SECONDS from config)
    │
    ├── Attempt 2 → OperationalError
    │   └── wait 2s
    │
    └── Attempt 3 → OperationalError
        └── raise ConnectionError("Failed after 3 attempts")
```

### Password Masking

All database URL logging uses `_mask_password()`. Passwords never appear in any log file.

```
postgresql://postgres.abc:mysecret@host:6543/postgres
                          ↓
postgresql://postgres.abc:****@host:6543/postgres
```

### Port Validation

| Port | Type | Behaviour |
|---|---|---|
| `6543` | pgBouncer pooler | ✓ Correct — multiplexes connections, no limit issues on Render |
| `5432` | Direct connection | ⚠ Warning logged — works locally, exhausts Supabase connection limit in production |

### Context Manager Support

```python
# Recommended — auto-connects on enter, auto-disconnects on exit
with DatabaseConnection() as db:
    rows = db.execute_query("SELECT * FROM v_current_risk_summary")
# db.disconnect() called here automatically

# FastAPI route usage via Depends(get_db)
def my_route(db: DatabaseConnection = Depends(get_db)):
    # db is already connected — get_db() handles lifecycle
    return db.execute_query("SELECT 1")
```

---

## 5. Data Models & Pydantic Schemas

**File:** `src/api/models/customer.py`

### Three Lines of Defense

```
HTTP Request arrives at FastAPI
          │
          ▼
┌─────────────────────────────────────────────────────┐
│  LINE 1: Pydantic Model Validation (automatic)      │
│                                                     │
│  Checks: field types, ENUM membership, ge/le ranges │
│  On failure: HTTP 422 returned BEFORE route runs    │
│  Raised by: FastAPI automatically                   │
└──────────────────────┬──────────────────────────────┘
                       │  all fields valid
                       ▼
┌─────────────────────────────────────────────────────┐
│  LINE 2: data_integrity.py (business rules)         │
│                                                     │
│  Checks: name length, suspicious values,            │
│          cross-field rules, belt-and-suspenders     │
│  On failure: ValueError raised → HTTP 422           │
│  Raised by: customer_service.py                     │
└──────────────────────┬──────────────────────────────┘
                       │  business rules passed
                       ▼
┌─────────────────────────────────────────────────────┐
│  LINE 3: PostgreSQL ENUMs + CHECK constraints       │
│                                                     │
│  Checks: ENUM values, NOT NULL, UNIQUE, ranges      │
│  On failure: psycopg2.Error (caught by route)       │
│  Enforced by: Supabase PostgreSQL                   │
└──────────────────────┬──────────────────────────────┘
                       │  data is clean
                       ▼
                  Two DB rows inserted
```

### ENUMs — Must Match `schema.sql` Exactly

```python
class PaymentModeEnum(str, Enum):
    cod         = "COD"          # matches payment_mode_enum in schema.sql
    credit_card = "Credit Card"
    debit_card  = "Debit Card"
    e_wallet    = "E wallet"
    upi         = "UPI"
```

> **`str, Enum` inheritance** — values serialise as plain strings in JSON: `"COD"` not `"PaymentModeEnum.cod"`. Pydantic handles this automatically.

### All ENUMs and Their Allowed Values

| Enum Class | Allowed Values | PostgreSQL ENUM |
|---|---|---|
| `GenderEnum` | `Male`, `Female` | `gender_enum` |
| `MaritalStatusEnum` | `Single`, `Married`, `Divorced` | `marital_status_enum` |
| `PaymentModeEnum` | `COD`, `Credit Card`, `Debit Card`, `E wallet`, `UPI` | `payment_mode_enum` |
| `LoginDeviceEnum` | `Mobile Phone`, `Computer` | `login_device_enum` |
| `OrderCatEnum` | `Grocery`, `Fashion`, `Mobile`, `Laptop & Accessory`, `Others` | `order_cat_enum` |

### Request Model — `CustomerRegisterRequest`

```python
class CustomerRegisterRequest(BaseModel):
    full_name:              Optional[str]         # optional, max 255 chars
    gender:                 GenderEnum            # "Male" | "Female"
    marital_status:         MaritalStatusEnum     # "Single" | "Married" | "Divorced"
    city_tier:              int = Field(ge=1, le=3)  # 1, 2, or 3 only
    preferred_payment_mode: PaymentModeEnum
    preferred_login_device: LoginDeviceEnum
    preferred_order_cat:    OrderCatEnum

    @field_validator("full_name")
    @classmethod
    def clean_full_name(cls, v):
        # Strip whitespace; convert empty string → None
        if v is None: return None
        cleaned = v.strip()
        return cleaned if cleaned else None
```

### Response Model — `CustomerRegisterResponse`

```python
class CustomerRegisterResponse(BaseModel):
    customer_id:          UUID      # generated by PostgreSQL RETURNING id
    registered_at:        datetime  # server UTC timestamp at INSERT moment
    days_until_scoreable: int       # always 30 (tenure >= 1 month gate)
    status:               str       # always "created"
    initial_features:     dict      # shows operator what customer_features contains
```

### Standard API Envelope — `APIResponse`

Every single endpoint returns this same shape. The frontend always checks `success` first.

```python
class APIResponse(BaseModel):
    success: bool = True
    data:    Any  = None
    message: str  = "OK"
```

**Success:**
```json
{
  "success": true,
  "data": { "customer_id": "e5fdab1d-...", "registered_at": "..." },
  "message": "Customer registered successfully"
}
```

**Failure:**
```json
{
  "success": false,
  "data": null,
  "message": "city_tier must be between 1 and 3, got 7"
}
```

---

## 6. Data Integrity & Validation

**File:** `src/api/validators/data_integrity.py`

### What Pydantic Cannot Check

Pydantic validates **types** and **ENUM membership**. It cannot validate **business rules**:

| Check | Pydantic | `data_integrity.py` |
|---|---|---|
| `city_tier` is an integer | ✓ | — |
| `city_tier` is between 1 and 3 | ✓ `(ge=1, le=3)` | ✓ belt-and-suspenders |
| `full_name` is under 255 chars | ✓ `(max_length)` | ✓ |
| `full_name` is NOT `"test"` or `"asdf"` | ✗ | ✓ suspicious pattern check |
| `full_name` is at least 2 chars | ✗ | ✓ |
| Cross-field rules | ✗ | ✓ extensible placeholder |

### Usage Pattern

```python
# In customer_service.py — always call before DB insert
issues = validate_customer_request(request)
if issues:
    raise ValueError(f"Data integrity checks failed: {'; '.join(issues)}")
```

### Validation Flow

```
validate_customer_request(request)
    │
    ├── Check 1: city_tier in [1, 2, 3]
    │   Fail → append "city_tier must be between 1 and 3, got X"
    │
    ├── Check 2: full_name length >= 2 chars (if provided)
    │   Fail → append "full_name must be at least 2 characters"
    │
    ├── Check 3: full_name length <= 255 chars
    │   Fail → append "full_name must be under 255 characters"
    │
    ├── Check 4: full_name not exact-match suspicious word
    │   "test", "admin", "null", "undefined" → WARNING log only (not rejected)
    │
    └── Return issues[]
            ├── Empty  → all checks passed, proceed with registration
            └── Non-empty → raise ValueError → HTTP 422
```

### Observability — Everything Is Logged

```python
def log_validation_summary(request: CustomerRegisterRequest) -> None:
    logger.info("  Registration request received:")
    logger.info(f"    full_name:              {request.full_name or '(not provided)'}")
    logger.info(f"    gender:                 {request.gender.value}")
    logger.info(f"    marital_status:         {request.marital_status.value}")
    logger.info(f"    city_tier:              {request.city_tier}")
    logger.info(f"    preferred_payment_mode: {request.preferred_payment_mode.value}")
    logger.info(f"    preferred_login_device: {request.preferred_login_device.value}")
    logger.info(f"    preferred_order_cat:    {request.preferred_order_cat.value}")
```

Every registration attempt is logged in full. You can debug any issue from the Render log dashboard without reproducing locally.

---

## 7. Services Layer

Services contain **all business logic**. Routes never contain logic. This is the most important architectural rule in the entire codebase.

```
Route receives HTTP request
          │
          └── calls service_function(request, db)
                        │
                        ├── validate()
                        ├── transform()
                        ├── write to DB
                        ├── fire side effects (SSE events)
                        └── return domain object (Pydantic model)
                                   │
          Route wraps in APIResponse and returns HTTP response
```

---

### 7.1 `customer_service.py` — Registration Orchestrator

The core of the entire system. Eight sequential steps, every one logged.

```
register_customer(request, db)
        │
        ├── Step 1:  log_validation_summary()
        │            → Logs all 7 fields to stdout/Render logs
        │
        ├── Step 2:  validate_customer_request()
        │            → Business rules check (Section 6)
        │            → Raises ValueError on failure → HTTP 422
        │
        ├── Step 3:  _apply_cleaning_rules()
        │            → Calls clean_single_record() from Stage 1 pipeline
        │            → Same rules used for Kaggle CSV seeding
        │
        ├── Step 4:  Build insert_data dict
        │            → Sets: registered_at=NOW(), is_active=True, role="customer"
        │            → Sets: email=None, password_hash=None (not collected)
        │
        ├── Step 5:  _insert_customer_row()
        │            → INSERT INTO customers (...) VALUES (%s) RETURNING id
        │            → Gets UUID from PostgreSQL in the SAME query
        │
        ├── Step 6:  insert_initial_features(db, customer_id)
        │            → INSERT INTO customer_features (tenure=0, orders=0, ...)
        │            → MUST be same request — cannot be deferred (see below)
        │
        ├── Step 7:  sse_service.publish("new_customer", {...})
        │            → in-memory queue (fast, <1ms)
        │            → sse_events table (persistent, survives Render sleep)
        │
        └── Step 8:  return CustomerRegisterResponse(customer_id=..., ...)
```

**Why call `clean_single_record()` from Stage 1?**

The Kaggle CSV seeding pipeline and the live API both feed data into the same database. Using the same cleaning function for both guarantees consistent canonical values regardless of source:

```
Data Source         →  Cleaning Function         →  Database
────────────────────────────────────────────────────────────
Kaggle CSV          →  clean_dataframe()          →  Supabase
Frontend Form       →  clean_single_record()      →  Supabase
                             ↑
              Both imported from src/pipeline/stage1_clean.py
              One set of rules. Zero inconsistency possible.
```

**`RETURNING id` pattern** — gets the generated UUID without a second database round-trip:

```sql
INSERT INTO customers (gender, city_tier, ...) VALUES (%s, %s, ...)
RETURNING id;
-- PostgreSQL returns the auto-generated UUID immediately
-- Python: customer_id = cur.fetchone()[0]
```

---

### 7.2 `feature_service.py` — Feature Row Creation + Tenure Refresh

**Inserts the `customer_features` row immediately after customer INSERT.**

**Why synchronous, not deferred?**

```
If we defer customer_features insert to a background job:

  Customer inserted → job queued → job fails / Render sleeps
                                         ↓
              customer row exists with NO customer_features row
                                         ↓
  v_customer_ml_features LEFT JOIN produces all-NULL feature row
                                         ↓
              Batch scoring tries to score a NULL vector → crash
```

Inserting in the same HTTP request guarantees consistency. One request = both rows = no gaps.

**Initial feature values at registration:**

| Feature | Initial Value | Why |
|---|---|---|
| `tenure_months` | `0.0` | Just registered — zero time elapsed |
| `satisfaction_score` | `NULL` | Not yet rated — genuinely unknown |
| `complain` | `FALSE` | No complaints filed — valid starting state |
| `warehouse_to_home` | `NULL` | No delivery address yet |
| `number_of_address` | `1` | Minimum per DB CHECK constraint |
| `number_of_device_registered` | `1` | Device used to register |
| `day_since_last_order` | `NULL` | No orders exist yet |
| `order_count` | `0` | No orders yet |
| `coupon_used` | `0` | Zero is valid — not unknown |
| `cashback_amount` | `0.0` | Zero is valid — not unknown |
| `features_source` | `"system"` | Inserted by FastAPI, not CSV seeding |

**Daily tenure recomputation** — called by GitHub Actions cron via `POST /admin/refresh-tenure`:

```sql
UPDATE customer_features cf
SET
    tenure_months = ROUND(
        EXTRACT(EPOCH FROM (NOW() - c.registered_at)) / 2592000.0,
        1
    ),
    -- 2592000 = seconds in 30 days exactly
    -- ROUND to 1 decimal: 9.033 months → 9.0
    features_computed_at = NOW()
FROM customers c
WHERE cf.customer_id = c.id
  AND c.is_active = TRUE;
```

> **Why recompute from source, not increment?** Incrementing `tenure_months += (1/30)` daily accumulates floating-point drift. After 1 year: `12.000000000000004`. Computing from `registered_at` always gives the exact correct value.

---

### 7.3 `sse_service.py` — Event Queue

See full SSE section → [Section 9](#9-server-sent-events-sse).

---

## 8. Routes Layer

Routes are **thin**. Each route handler does exactly three things:

1. Receive the validated request
2. Call one service function
3. Wrap the result in `APIResponse` and return

**The thinnest possible route:**

```python
@router.post("/register", response_model=APIResponse, status_code=201)
def register_new_customer(
    request: CustomerRegisterRequest,          # Pydantic validates automatically
    db: DatabaseConnection = Depends(get_db),  # injected, lifecycle managed
) -> APIResponse:
    try:
        result = register_customer(request=request, db=db)
        return APIResponse(success=True, data=result.model_dump())
    except ValueError as e:           # business rule violation
        raise HTTPException(422, detail=str(e))
    except Exception as e:            # unexpected error
        logger.error(f"Unexpected: {e}", exc_info=True)
        raise HTTPException(500, detail="Internal error")
```

No validation logic. No database calls. No business decisions. Just wire the request to the service.

### Router Organisation

| File | Prefix | Auth | Endpoints |
|---|---|---|---|
| `customers.py` | `/customers` | None | `POST /register` |
| `admin.py` | `/admin` | `X-Admin-Key` | `GET /overview`, `/risk-distribution`, `/churn-trend`, `/at-risk`, `/drift`, `/last-batch`, `/sse-status` · `POST /refresh-tenure` |
| `events.py` | `/admin` | `X-Admin-Key` | `GET /events` (SSE stream) |

All three routers mounted in `main.py` at prefix `/api/v1`:

```python
app.include_router(customers_router, prefix="/api/v1")
app.include_router(admin_router,     prefix="/api/v1")
app.include_router(events_router,    prefix="/api/v1")
```

### Admin Router — Auth Applied at Router Level

```python
router = APIRouter(
    prefix="/admin",
    tags=["Admin Dashboard"],
    dependencies=[Depends(verify_admin)],  # ← applied to EVERY route in this router
)
```

One line protects all 8 admin endpoints simultaneously.

---

## 9. Server-Sent Events (SSE)

### What Is SSE?

SSE is a **one-directional, server-to-client** streaming protocol over a standard HTTP connection. The browser opens a `GET` request and keeps it open — the server pushes text chunks as events occur.

```
Browser                                  FastAPI
   │                                        │
   │  GET /api/v1/admin/events              │
   │────────────────────────────────────>   │
   │                                        │  connection stays open
   │  ← data: {"event_type": "ping"}        │  (25s keepalive)
   │                                        │
   │  ← data: {"event_type": "new_customer",│  customer registers
   │            "payload": {...}}           │
   │                                        │
   │  ← data: {"event_type":                │  batch job completes
   │            "batch_completed", ...}     │
   │                                        │
   │  ← data: {"event_type": "ping"}        │  (25s keepalive)
   │                                        │
```

### SSE vs WebSocket — Why SSE Wins Here

| Criteria | SSE | WebSocket |
|---|---|---|
| **Direction** | Server → Client only | Bidirectional |
| **Protocol** | Standard HTTP/1.1 | Upgrade handshake |
| **Auto-reconnect** | ✓ Built into browser `EventSource` | ✗ Must implement manually |
| **Works through proxies** | ✓ Always | ✗ Sometimes blocked |
| **Render compatibility** | ✓ | Issues with sleep cycles |
| **Dashboard needs** | Receive events only | Overkill |

> The dashboard only **receives** events. It never **sends** them. SSE is the right tool.

### The `asyncio.Queue` — Core of SSEService

```python
class SSEService:
    def __init__(self, max_queue_size: int = 100):
        self._queue: asyncio.Queue = asyncio.Queue()
        # asyncio.Queue is thread-safe for FastAPI's async context

    def publish(self, event_type: str, payload: dict, db=None):
        # Called SYNCHRONOUSLY from any service
        event = {
            "id":         str(uuid4()),
            "event_type": event_type,
            "payload":    payload,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._queue.put_nowait(event)    # non-blocking — instant
        if db:
            self._persist_event(db, event)  # also writes to sse_events table

    async def listen(self):
        # Called by the SSE route — async generator (runs forever)
        while True:
            try:
                event = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=25.0    # blocks without CPU until event arrives
                )
                yield event         # hands event to the SSE route
            except asyncio.TimeoutError:
                yield {             # 25s elapsed with no event → send keepalive
                    "event_type": "ping",
                    "payload": {},
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            except asyncio.CancelledError:
                return              # client disconnected — stop cleanly
```

### The SSE Route

```python
@router.get("/events")
async def stream_sse_events() -> StreamingResponse:

    async def event_generator():
        async for event in sse_service.listen():
            # SSE wire format: "data: " + JSON string + "\n\n" (double newline)
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(
        content=event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",       # never cache SSE
            "Connection":       "keep-alive",     # keep the HTTP connection open
            "X-Accel-Buffering":"no",             # disable nginx/Render buffering
        },
    )
```

### Publish + Persist — Two-Track Architecture

```
sse_service.publish("new_customer", payload, db=db)
                │
                ├── Track 1: in-memory asyncio.Queue
                │   → Dashboard sees event in < 100ms
                │   → Lost if Render sleeps
                │
                └── Track 2: sse_events table in Supabase
                    → Survives Render sleep/wake cycles
                    → Reconnecting clients can catch up
                    → Deleted after 24 hours by cleanup job
```

### Event Types Reference

| Event Type | Triggered By | Payload Fields |
|---|---|---|
| `new_customer` | `customer_service.register_customer()` | `customer_id`, `full_name`, `city_tier`, `payment`, `device` |
| `batch_completed` | Batch scoring endpoint | `customers_scored`, `high_risk_count`, `duration_seconds`, `drift_alert` |
| `high_churn_alert` | Batch scoring (per customer) | `customer_id`, `score`, `risk_tier`, `top_reason` |
| `drift_alert` | Drift detector (PSI > 0.20) | `feature`, `psi`, `batch_run_id` |
| `model_promoted` | Model registry | `version`, `auc_roc` |
| `ping` | SSEService (every 25s) | `{}` (empty) |

### How the Dashboard Reacts to SSE Events

`Dashboard.jsx` watches `events[0]` (newest event). On `batch_completed` it invalidates all React Query caches:

```javascript
// In Dashboard.jsx useEffect — runs when events[0] changes
useEffect(() => {
    const latest = events[0];
    if (!latest) return;

    if (latest.event_type === 'batch_completed') {
        // Force ALL polling queries to refetch immediately
        // instead of waiting for their next 60s/120s tick
        queryClient.invalidateQueries({ queryKey: ['kpi-summary'] });
        queryClient.invalidateQueries({ queryKey: ['risk-distribution'] });
        queryClient.invalidateQueries({ queryKey: ['churn-trend'] });
        queryClient.invalidateQueries({ queryKey: ['top-at-risk'] });
        queryClient.invalidateQueries({ queryKey: ['drift-monitor'] });
    }

    if (latest.event_type === 'drift_alert') {
        queryClient.invalidateQueries({ queryKey: ['drift-monitor'] });
        queryClient.invalidateQueries({ queryKey: ['last-batch'] });
    }
}, [events[0]?.id, queryClient]);
```

> **SSE is the trigger. React Query is the data fetcher.** SSE says "something changed". React Query goes and gets the fresh data.

---

## 10. Dependency Injection

**File:** `src/api/dependencies.py`

FastAPI's dependency injection system provides shared resources to route handlers automatically. Declare the dependency once — inject it anywhere.

### `get_db()` — Database Connection Dependency

```python
def get_db() -> Generator[DatabaseConnection, None, None]:
    db = DatabaseConnection()
    try:
        db.connect()       # opens psycopg2 pool
        yield db           # ← route handler executes here with db available
    except ConnectionError as e:
        raise HTTPException(503, "Database connection unavailable")
    finally:
        db.disconnect()    # ← ALWAYS runs — even if route raised an exception
```

**Usage in any route:**
```python
@router.get("/overview")
def get_overview(db: DatabaseConnection = Depends(get_db)):
    # db is connected and ready
    # db.disconnect() called automatically when function returns
    rows = db.execute_query("SELECT * FROM v_current_risk_summary")
    return APIResponse(success=True, data=rows[0])
```

### `verify_admin()` — Authentication Dependency

```python
def verify_admin(
    x_admin_key: str = Header(alias="X-Admin-Key", default=None)
) -> str:
    if x_admin_key is None:
        raise HTTPException(403, "X-Admin-Key header is required")

    # secrets.compare_digest = constant-time string comparison
    # Prevents timing attacks: naive == leaks key length via response time
    if not secrets.compare_digest(x_admin_key, settings.admin_api_key):
        raise HTTPException(403, "Invalid admin key")

    return x_admin_key
```

### Dependency Graph

```
POST /api/v1/customers/register
    ├── CustomerRegisterRequest  ← Pydantic (automatic, no Depends needed)
    └── Depends(get_db)          ← DatabaseConnection (connected, auto-closes)

GET /api/v1/admin/overview
    ├── Depends(verify_admin)    ← checks X-Admin-Key header → 403 if wrong
    └── Depends(get_db)          ← DatabaseConnection (connected, auto-closes)

GET /api/v1/admin/events  (SSE)
    └── Depends(verify_admin)    ← checks X-Admin-Key header
        (no get_db — SSEService has its own in-memory queue)

GET /api/v1/health
    (no dependencies — no auth, opens its own DB connection internally)
```

---

## 11. Application Factory — `main.py`

### Startup Sequence

```
python scripts/run_api.py
    │
    uvicorn.run("src.api.main:app", ...)
    │
    FastAPI imports src/api/main.py
    │
    ├── 1. Logging configured (stdout, INFO level, timestamp format)
    │
    ├── 2. settings = get_settings()
    │       → Reads .env file
    │       → Validates all fields
    │       → Raises on missing DATABASE_URL
    │
    ├── 3. FastAPI app = FastAPI(title="ChurnGuard API", ...)
    │
    ├── 4. CORSMiddleware added
    │       → allow_origins from settings.cors_origins_list
    │       → allow_headers includes "X-Admin-Key" (required for preflight)
    │
    ├── 5. Global exception handlers registered
    │       → Exception → 500
    │       → ValueError → 422
    │
    ├── 6. Routers mounted at /api/v1
    │       → customers_router
    │       → admin_router
    │       → events_router
    │
    └── 7. @app.on_event("startup") runs:
            ├── Logs: environment, API prefix, CORS, admin key status
            └── Tests DB connection:
                ├── ✓ healthy → "API ready. Docs at /docs"
                └── ✗ failed  → warning logged, API starts in degraded mode
                                 (DB endpoints will return 503)
```

### CORS Configuration

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,  # ["https://app.vercel.app", ...]
    allow_credentials=False,    # no cookies — X-Admin-Key header auth only
    allow_methods=["GET", "POST", "PUT", "OPTIONS"],
    allow_headers=["Content-Type", "X-Admin-Key"],
    #              ↑ X-Admin-Key MUST be listed here
    #                or the browser blocks the preflight OPTIONS request
)
```

> **CORS preflight:** Browsers send `OPTIONS` before any cross-origin POST or request with custom headers. If `X-Admin-Key` is not in `allow_headers`, the browser silently blocks the request — the API never even sees it.

### Global Exception Handlers

```python
@app.exception_handler(Exception)
async def global_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error(f"Unhandled: {exc}", exc_info=True)  # full stack trace in logs
    return JSONResponse(500, content=ErrorResponse(message="Internal error").model_dump())

@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    # ValueError from services = business rule violation
    return JSONResponse(422, content=ErrorResponse(message=str(exc)).model_dump())
```

---

## 12. Full Request Lifecycle

### Customer Registration — Complete End-to-End

```
1. BROWSER (entry form, Vercel)
   ──────────────────────────────────────────────────────────────
   POST https://churnguard-api.onrender.com/api/v1/customers/register
   Headers: Content-Type: application/json
   Body: {
     "gender": "Female", "marital_status": "Single",
     "city_tier": 2, "preferred_payment_mode": "Credit Card",
     "preferred_login_device": "Mobile Phone",
     "preferred_order_cat": "Grocery"
   }

2. RENDER (Uvicorn receives the request)
   ──────────────────────────────────────────────────────────────
   ├── CORS check: is Origin in CORS_ORIGINS? ✓

   ├── Route match: POST /api/v1/customers/register → customers.py

   ├── Dependency resolution:
   │   └── get_db() called → DatabaseConnection created, pool opened

   ├── Pydantic validation (automatic, before route runs):
   │   ├── city_tier: int, ge=1, le=3? ✓
   │   ├── gender: GenderEnum member? ✓
   │   └── preferred_payment_mode: PaymentModeEnum member? ✓
   │   If any fail → HTTP 422 returned here, route never runs

   └── register_new_customer() route handler:
       └── register_customer(request, db) service:
           ├── Step 1: log_validation_summary() → logs all 7 fields
           ├── Step 2: validate_customer_request() → business rules ✓
           ├── Step 3: _apply_cleaning_rules() → clean_single_record() ✓
           ├── Step 4: build insert_data dict
           ├── Step 5: _insert_customer_row()
           │           INSERT INTO customers (...) VALUES (%s) RETURNING id
           │           customer_id = "e5fdab1d-6c26-4e78-a132-8dee49e9b929"
           ├── Step 6: insert_initial_features(db, customer_id)
           │           INSERT INTO customer_features
           │           (customer_id, tenure_months=0.0, order_count=0, ...)
           ├── Step 7: sse_service.publish("new_customer", {...})
           │           → asyncio.Queue (in-memory, immediate)
           │           → sse_events table (persistent)
           └── Step 8: return CustomerRegisterResponse(...)

   Route wraps in APIResponse → HTTP 201 returned
   Dependency cleanup: db.disconnect() (via finally block)

3. BROWSER receives:
   ──────────────────────────────────────────────────────────────
   {
     "success": true,
     "data": {
       "customer_id":          "e5fdab1d-6c26-4e78-a132-8dee49e9b929",
       "registered_at":        "2026-03-22T14:35:09.123Z",
       "days_until_scoreable": 30,
       "status":               "created",
       "initial_features":     { "tenure_months": 0.0, "order_count": 0 }
     },
     "message": "Customer registered successfully"
   }

   SuccessCard.jsx renders → UUID, timestamp, feature values displayed
   5 seconds later → form auto-resets

4. MEANWHILE — Dashboard browser (Vercel)
   ──────────────────────────────────────────────────────────────
   EventSource open on GET /api/v1/admin/events
   ← receives: data: {"event_type": "new_customer", "payload": {...}}\n\n

   EventFeed.jsx slides new event card in from the right
```

---

## 13. Error Handling Strategy

### HTTP Status Code Map

| Scenario | Status | Raised By |
|---|---|---|
| Wrong field type (`city_tier: "banana"`) | `422` | Pydantic (automatic) |
| Invalid ENUM value | `422` | Pydantic (automatic) |
| Business rule violation | `422` | `customer_service` raises `ValueError` |
| Missing `X-Admin-Key` header | `403` | `verify_admin` dependency |
| Wrong `X-Admin-Key` value | `403` | `verify_admin` dependency |
| Database unavailable | `503` | `get_db` dependency |
| Unexpected server error | `500` | Global exception handler |
| Successful creation | `201` | Route handler (`status_code=201`) |
| Successful retrieval | `200` | Route handler (default) |

### Consistent Error Response Shape

Every error returns this structure. The frontend checks `success: false` and reads `message`:

```json
{
  "success": false,
  "data": null,
  "message": "Descriptive error message here",
  "errors": null
}
```

### `exc_info=True` Pattern

Every `logger.error()` call in route handlers uses `exc_info=True`:

```python
logger.error(f"Failed to register customer: {e}", exc_info=True)
#                                                  ↑
#     Includes full Python stack trace in the log output
#     Visible in Render's log dashboard
#     Critical for debugging production errors without local reproduction
```

### Exception Hierarchy

```
Exception caught at:
    │
    ├── Pydantic validation failure
    │   → FastAPI handles automatically → HTTP 422
    │
    ├── ValueError raised by a service
    │   → global ValueError handler → HTTP 422
    │   → also caught by route try/except → HTTPException 422
    │
    ├── psycopg2.OperationalError (DB unreachable)
    │   → caught by get_db() → HTTPException 503
    │
    ├── psycopg2.Error (query failed, constraint violation)
    │   → caught by route except Exception → HTTPException 500
    │   → logged with exc_info=True
    │
    └── Any other Exception
        → global exception handler → HTTP 500
        → logged with full stack trace
```

---

## 14. Deployment on Render

### Dockerfile Strategy

```dockerfile
FROM python:3.11-slim           # slim = no docs/tests/build tools → smaller image
ENV PYTHONDONTWRITEBYTECODE=1   # no .pyc files → cleaner container
ENV PYTHONUNBUFFERED=1          # stdout/stderr not buffered → real-time Render logs

WORKDIR /app

# Install libpq-dev (psycopg2 dependency) and gcc
RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# COPY requirements FIRST — Docker layer cache optimisation
# If only source code changes → this layer is reused → pip install not re-run
# Rebuilds are 3-5× faster
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .    # copy source code AFTER dependencies

CMD uvicorn src.api.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1
```

### Render Service Configuration

```
Service Type:   Web Service
Runtime:        Docker (detects Dockerfile automatically)
Health Check:   GET /api/v1/health  (must return 200 within 30s)
Auto-Deploy:    Yes — rebuilds on every push to main branch
Free Tier:      Sleeps after 15 min inactivity, wakes on first request (~30-60s)
```

### Render Free Tier — Cold Start Mitigation

Render free tier sleeps after 15 minutes of inactivity. GitHub Actions batch workflow wakes it first:

```yaml
- name: Wake Render (3 retry warm-up)
  run: |
    for i in 1 2 3; do
      STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
               $RENDER_API_URL/api/v1/health)
      if [ "$STATUS" = "200" ]; then
        echo "API is awake"
        exit 0
      fi
      echo "Attempt $i failed (status $STATUS), waiting 15s..."
      sleep 15
    done
    echo "API did not wake after 3 attempts"
    exit 1   # fail the workflow — no point running batch against a dead API
```

### Deployment Checklist

```
Before deploying:
  ☐ Dockerfile in project root (Render detects it automatically)
  ☐ render.yaml committed to repo (optional but recommended)
  ☐ requirements.txt includes all production dependencies
  ☐ All secrets removed from code (in .env, not committed)

In Render dashboard → Environment:
  ☐ DATABASE_URL  = postgresql://postgres.[ref]:[pwd]@...pooler...:6543/postgres
  ☐ ADMIN_API_KEY = (generate: openssl rand -hex 32)
  ☐ CORS_ORIGINS  = https://dashboard.vercel.app,https://entry.vercel.app
  ☐ ENVIRONMENT   = production

After deploying:
  ☐ GET /api/v1/health → {"status": "healthy", "db_connected": true}
  ☐ POST /api/v1/customers/register → 201 with UUID
  ☐ GET /api/v1/admin/overview (with X-Admin-Key) → KPI data
  ☐ GET /api/v1/admin/events → stream stays open, pings every 25s
```

---

## 15. Environment Variables Reference

| Variable | Required | Example Value | Description |
|---|---|---|---|
| `DATABASE_URL` | ✓ | `postgresql://postgres.abc:pwd@host:6543/postgres` | Supabase pooler. Port 6543, not 5432. |
| `ADMIN_API_KEY` | ✓ | `a3f9d2c8...` (32 hex) | Sent in `X-Admin-Key` header for all admin endpoints |
| `CORS_ORIGINS` | ✓ | `https://app.vercel.app,https://entry.vercel.app` | Comma-separated. No spaces. |
| `ENVIRONMENT` | — | `production` | `development` enables hot-reload |
| `POOL_MIN_CONNECTIONS` | — | `1` | Warm connections always open |
| `POOL_MAX_CONNECTIONS` | — | `5` | Max simultaneous DB queries |
| `SSE_PING_INTERVAL_SECONDS` | — | `30` | Keepalive to prevent Render closing idle SSE |
| `HUGGINGFACE_REPO` | — | `username/churnguard-model` | Set when model is trained |
| `HF_TOKEN` | — | `hf_abc123...` | HuggingFace private repo access token |

> **Never put real secrets in `.env.example`, `render.yaml`, or any committed file.** Set secrets directly in the Render dashboard.

---

## 16. API Endpoints Reference

### Base URLs

```
Development:  http://localhost:8000
Production:   https://your-churnguard-api.onrender.com
Docs (Swagger): /docs
Docs (ReDoc):   /redoc
```

### Authentication

Admin endpoints require header:
```
X-Admin-Key: your-admin-api-key
```

---

### `POST /api/v1/customers/register`
Register a new customer. **No auth required.**

**Request body:**
```json
{
  "full_name":              "Sarah Mitchell",
  "gender":                 "Female",
  "marital_status":         "Single",
  "city_tier":              2,
  "preferred_payment_mode": "Credit Card",
  "preferred_login_device": "Mobile Phone",
  "preferred_order_cat":    "Grocery"
}
```

**Response `201`:**
```json
{
  "success": true,
  "data": {
    "customer_id":          "e5fdab1d-6c26-4e78-a132-8dee49e9b929",
    "registered_at":        "2026-03-22T14:35:09.123Z",
    "days_until_scoreable": 30,
    "status":               "created",
    "initial_features":     { "tenure_months": 0.0, "order_count": 0, "complain": false }
  },
  "message": "Customer registered successfully"
}
```

**Side effect:** Publishes `new_customer` SSE event to dashboard feed.

---

### `GET /api/v1/admin/overview` 🔐
KPI summary for dashboard top cards.

**Response data:**
```json
{
  "total_customers": 5630,
  "high_risk_count": 948,
  "medium_risk_count": 0,
  "low_risk_count": 4682,
  "onboarding_count": 0,
  "high_risk_pct": 16.8,
  "last_scored_at": "2026-03-15T23:47:57Z"
}
```

**Source:** `v_current_risk_summary` view (pre-built SQL query in schema.sql)

---

### `GET /api/v1/admin/risk-distribution` 🔐
Risk tier counts for the horizontal bar chart.

**Response data:**
```json
[
  { "tier": "HIGH",       "count": 948,  "color": "#ef4444" },
  { "tier": "MEDIUM",     "count": 0,    "color": "#f59e0b" },
  { "tier": "LOW",        "count": 4682, "color": "#10b981" },
  { "tier": "ONBOARDING", "count": 0,    "color": "#3b82f6" }
]
```

---

### `GET /api/v1/admin/churn-trend` 🔐
Churn rate per batch cycle for the line chart.

**Response data (array, last 10 completed batch runs):**
```json
[
  { "batch_date": "2026-03-15", "high_risk_pct": 16.8, "customers_scored": 5630, "drift_alert_fired": false }
]
```

**Source:** `v_churn_trend` view

---

### `GET /api/v1/admin/at-risk` 🔐
Top 20 customers by churn probability for the table.

**Response data (array of 20):**
```json
[
  {
    "customer_id":      "e5fdab1d-...",
    "display_id":       "#e5fdab1d",
    "churn_probability": 1.0,
    "risk_tier":        "HIGH",
    "tenure_months":    4,
    "day_since_last_order": 5,
    "satisfaction_score": 2,
    "complain":         true,
    "top_reason":       null
  }
]
```

**Source:** `v_top_at_risk` view

---

### `GET /api/v1/admin/drift` 🔐
PSI drift values per feature for the drift monitor table.

**Response data (array, one per tracked feature):**
```json
[
  { "feature_name": "DaySinceLastOrder", "psi_value": null, "drift_level": "pending", "reference_mean": 4.54 }
]
```

If no batch run has completed: returns `"drift_level": "pending"` placeholder rows.

---

### `GET /api/v1/admin/last-batch` 🔐
Most recent batch run record for the health bar.

**Response data:**
```json
{
  "model_version": "kaggle_baseline",
  "status": "completed",
  "started_at": "2026-03-15T23:47:50Z",
  "completed_at": "2026-03-15T23:47:57Z",
  "duration_seconds": 7,
  "customers_scored": 5630,
  "high_risk_count": 948,
  "drift_alert_fired": false
}
```

---

### `POST /api/v1/admin/refresh-tenure` 🔐
Recompute `tenure_months` for all active customers. Called by daily GitHub Actions cron.

**Response data:**
```json
{ "customers_updated": 5630 }
```

---

### `GET /api/v1/admin/events` 🔐 — SSE Stream

Persistent streaming endpoint. Response never closes until client disconnects.

**Wire format:**
```
data: {"id":"uuid","event_type":"new_customer","payload":{...},"created_at":"..."}\n\n
data: {"id":"uuid","event_type":"ping","payload":{},"created_at":"..."}\n\n
```

**Headers required:**
```
Accept: text/event-stream
X-Admin-Key: your-admin-api-key
```

---

### `GET /api/v1/health` — No Auth
Used by Render health checks and GitHub Actions warm-up ping.

**Response:**
```json
{ "status": "healthy", "environment": "production", "version": "1.0.0", "db_connected": true }
```

---

## 17. Design Principles

### 1 — Separation of Concerns

```
Each layer has one job it can be described in one sentence:

main.py        → "wires components together and starts the app"
config.py      → "reads all settings from the environment"
models/        → "defines what data looks like"
validators/    → "enforces business rules Pydantic cannot"
services/      → "contains all business logic"
routes/        → "handles HTTP layer — calls services, wraps responses"
dependencies/  → "provides shared resources to routes"
```

### 2 — One Source of Truth

| What | Where | Why |
|---|---|---|
| All settings | `config.py` | One place to change any setting |
| DB connection | `database/connection.py` | One place for all pool/retry/health logic |
| Cleaning rules | `config/cleaning_config.py` | Used by BOTH pipeline AND API |
| ENUM values | `models/customer.py` | Must match `schema.sql` — one place to update both |
| SSE singleton | `services/sse_service.py` | One queue shared across all publishers |
| API response shape | `models/responses.py` | Consistent envelope everywhere |

### 3 — Fail Loudly at Startup

The app validates configuration, tests the database, and logs a full status report on startup. A misconfigured app that starts silently creates hard-to-debug production bugs at request time. Loud startup failures are always preferable.

### 4 — Parameterised Queries Always

```python
# ✗ NEVER — SQL injection vulnerability
cur.execute(f"SELECT * FROM customers WHERE city_tier = {city_tier}")

# ✓ ALWAYS — parameterised
cur.execute("SELECT * FROM customers WHERE city_tier = %s", (city_tier,))
```

### 5 — Log Everything, Mask Secrets

Every service function logs:
- What it received (inputs)
- What it did (step-by-step)
- What it returned (outputs)

All log output is visible in Render's dashboard. Passwords and API keys are always masked before logging.

### 6 — Routes Are Thin

A route handler that is more than ~20 lines is doing too much. Extract logic into a service. Routes exist only to translate between HTTP and domain objects.

---

## 18. Layer Communication Map

How information flows between every file in the system:

```
HTTP Request
     │
     ▼
┌────────────────────────────────────────────────────────────────┐
│  main.py                                                        │
│  CORSMiddleware → route matching → dependency resolution        │
└─────────────────────────────┬──────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                    │
         ▼                    ▼                    ▼
  ┌─────────────┐    ┌─────────────────┐   ┌─────────────┐
  │customers.py │    │admin.py         │   │events.py    │
  │(route)      │    │(route)          │   │(route)      │
  └──────┬──────┘    └────────┬────────┘   └──────┬──────┘
         │                    │                    │
         │ calls              │ calls              │ reads
         ▼                    ▼                    ▼
  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
  │customer_service │  │DB queries via   │  │sse_service      │
  │.py (service)    │  │execute_query()  │  │.listen()        │
  └────────┬────────┘  └────────┬────────┘  └─────────────────┘
           │                    │
    calls  │                    │ reads from
           ▼                    ▼
  ┌─────────────────┐  ┌─────────────────────────────────────┐
  │feature_service  │  │ Supabase Views                      │
  │.py (service)    │  │ v_current_risk_summary              │
  └────────┬────────┘  │ v_top_at_risk                       │
           │           │ v_churn_trend                       │
    writes │           │ v_customer_ml_features              │
           │           └──────────────┬──────────────────────┘
           ▼                          │
  ┌─────────────────────────────────────────────────────────┐
  │ database/connection.py (DatabaseConnection)             │
  │ psycopg2 pool → Supabase PostgreSQL :6543               │
  └─────────────────────────────────────────────────────────┘
           │
    also   │
    reads  ▼
  ┌─────────────────┐
  │stage1_clean.py  │
  │clean_single_    │   ← Both CSV pipeline and API
  │record()         │     use the same cleaning rules
  └─────────────────┘
```

---

## 19. Quick Reference Cheatsheet

### Add a New Endpoint — Checklist

```
1. Define request/response models in src/api/models/
2. Add validation rules in src/api/validators/data_integrity.py (if needed)
3. Write service function in src/api/services/
4. Add route in src/api/routes/ (thin — calls service only)
5. Mount router in main.py (if new router file)
6. Add endpoint to Section 16 of this document
```

### Common Patterns

**Pattern: Read from a database view**
```python
@router.get("/my-endpoint")
def get_data(db: DatabaseConnection = Depends(get_db)) -> APIResponse:
    rows = db.execute_query("SELECT * FROM v_my_view;")
    return APIResponse(success=True, data=rows)
```

**Pattern: Insert a row and get the generated ID**
```python
with db.get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO my_table (...) VALUES (%s) RETURNING id", values)
        new_id = cur.fetchone()[0]
    conn.commit()
```

**Pattern: Publish an SSE event from a service**
```python
from src.api.services.sse_service import sse_service

sse_service.publish(
    event_type="my_event",
    payload={"key": "value"},
    db=db,   # pass db to also persist to sse_events table
)
```

**Pattern: Protect a single route (not whole router)**
```python
@router.get("/sensitive", dependencies=[Depends(verify_admin)])
def sensitive_endpoint() -> APIResponse:
    ...
```

**Pattern: Return an error from a service**
```python
# In a service function — never raise HTTPException (services don't know HTTP)
if some_condition_fails:
    raise ValueError("Clear description of what failed and why")
# Route catches ValueError and converts to HTTP 422
```

### Import Map — What to Import From Where

```python
# Settings
from src.api.config import settings, get_settings

# DB connection
from database.connection import DatabaseConnection

# Models
from src.api.models.customer import CustomerRegisterRequest, CustomerRegisterResponse
from src.api.models.responses import APIResponse, ErrorResponse

# Dependencies (in route files only)
from src.api.dependencies import get_db, verify_admin

# SSE
from src.api.services.sse_service import sse_service

# Stage 1 cleaning (used by customer_service)
from src.pipeline.stage1_clean import clean_single_record
```

---

## 20. Extending This Backend

### Adding a New Feature (e.g. ML Batch Scoring Endpoint)

Following the exact same pattern used for customer registration:

```
Step 1 — Model (src/api/models/batch.py)
  BatchScoreRequest:  triggered_by: str
  BatchScoreResponse: batch_run_id: UUID, customers_scored: int, ...

Step 2 — Service (src/api/services/batch_service.py)
  run_batch_scoring(db):
    1. Load model from HuggingFace (hf_hub_download)
    2. Read eligible customers from v_customer_ml_features
    3. Apply preprocessor.pkl → feature matrix
    4. model.predict_proba() → scores
    5. Write predictions to predictions table
    6. Write batch_runs record
    7. sse_service.publish("batch_completed", {...})
    8. return BatchScoreResponse(...)

Step 3 — Route (src/api/routes/admin.py — add to existing router)
  @router.post("/predict/batch")
  def trigger_batch(db = Depends(get_db)) -> APIResponse:
      result = run_batch_scoring(db)
      return APIResponse(success=True, data=result.model_dump())
```

No changes needed in `main.py`, `config.py`, or `dependencies.py`. The architecture absorbs new features without modification.

### Adding a New SSE Event Type

```
Step 1 — Add to schema.sql
  ALTER TYPE sse_event_type_enum ADD VALUE 'my_new_event';

Step 2 — Publish from any service
  sse_service.publish("my_new_event", {"key": "value"}, db=db)

Step 3 — Handle in Dashboard.jsx
  case 'my_new_event':
      return `New event: ${payload.key}`

Step 4 — Add to Section 9 event types table in this document
```

---

*End of ChurnGuard FastAPI Backend Technical Reference — v1.0.0*
