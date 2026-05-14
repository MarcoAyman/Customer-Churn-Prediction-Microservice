# 🛒 churn_prediction.md
## Customer Churn Prediction Microservice
### Production-Grade ML System | E-Commerce / Retail SaaS

> **Version:** 1.1.0 | **Created:** 2026-03-04 | **Last Updated:** 2026-03-06 | **Author:** [Your Name]
> **Status:** Phase 1 — Data Research & Understanding
> **Deployment Decision:** LOCKED — Free tier, fully automated batch inference
>
> **Elevator Pitch:** A production-grade Customer Churn Prediction microservice
> that ingests raw e-commerce behavioral data, engineers domain-driven signals,
> trains and tracks multi-algorithm models via MLflow, serves predictions through
> a Dockerized FastAPI microservice on Render, automates 20-day batch scoring
> via GitHub Actions cron, and surfaces actionable churn risk scores to business
> stakeholders via a React dashboard on Vercel — all on zero-cost infrastructure.

---

## TABLE OF CONTENTS

1. [Project Identity](#1-project-identity)
2. [Project Overview](#2-project-overview)
3. [AI/ML Problem Framing](#3-aiml-problem-framing)
4. [Tech Stack](#4-tech-stack)
5. [System Architecture](#5-system-architecture)
6. [Phase 1 — Data Research & Understanding](#6-phase-1--data-research--understanding)
7. [Phase 2 — ML Pipeline](#7-phase-2--ml-pipeline)
8. [MLOps & Model Lifecycle](#8-mlops--model-lifecycle)
9. [Phase 3 — Application Layer](#9-phase-3--application-layer)
10. [Deployment Architecture — Free Tier, Fully Automated](#10-deployment-architecture--free-tier-fully-automated)
11. [Environment & Configuration](#11-environment--configuration)
12. [Current State & Progress](#12-current-state--progress)
13. [Task Backlog](#13-task-backlog)
14. [Active Task — What I Need Right Now](#14-active-task--what-i-need-right-now)
15. [Code Style & Conventions](#15-code-style--conventions)
16. [Constraints & Hard Rules](#16-constraints--hard-rules)
17. [LLM Session Prompts](#17-llm-session-prompts)
18. [Error Log & Debugging History](#18-error-log--debugging-history)
19. [Directory Map](#19-directory-map)
20. [Glossary](#20-glossary)
21. [Changelog](#21-changelog)

---

## 1. PROJECT IDENTITY

| Field               | Value                                                                    |
|---------------------|--------------------------------------------------------------------------|
| **Project Name**    | `customer-churn-prediction-microservice`                                 |
| **Codename**        | `CHURNGUARD`                                                             |
| **AI Project Type** | Predictive / Binary Classification + MLOps Pipeline                     |
| **Scope**           | ML + MLOps + REST API + Full Stack Web Application                       |
| **Business Domain** | E-Commerce / Retail SaaS                                                 |
| **Target Company**  | Mid-size online retailer or Shopify-tier merchant platform               |
| **Stage**           | Phase 1 — Data Research & Understanding                                  |
| **Repo URL**        | `https://github.com/[your-username]/churn-prediction-microservice`       |
| **Live URL**        | TBD (Deployment: Phase 3)                                                |
| **Started**         | 2026-03-04                                                               |
| **Target Deadline** | [FILL IN]                                                                |
| **License**         | MIT                                                                      |
| **Portfolio Role**  | Flagship MLOps project demonstrating full production ML lifecycle        |

---

## 2. PROJECT OVERVIEW

### 2.1 One-Line Summary
```
A production-grade Customer Churn Prediction microservice with a complete ML 
lifecycle: Kaggle data ingestion → domain-driven feature engineering → 
multi-algorithm training tracked in MLflow → FastAPI prediction service → 
drift monitoring with automated retraining → React stakeholder dashboard.
```

### 2.2 Business Problem
```
TARGET COMPANY: E-commerce / Retail SaaS (Shopify merchant, mid-size retailer)

PAIN POINT:
- Retailers have no systematic way to identify customers drifting toward 
  disengagement before they churn.
- Current approach is fully reactive: discounts and outreach only happen 
  after a customer has already gone silent or cancelled.
- Industry benchmark: acquiring a new customer costs 5–7x more than retaining 
  an existing one. A 5% improvement in retention can increase profit by 25–95%.

COST OF ERRORS:
- False Negative (missed churner):   HIGH cost — lost customer, zero retention 
                                     opportunity, potential bad review.
- False Positive (wrong prediction): MEDIUM cost — unnecessary discount or 
                                     outreach campaign, but customer stays.
- Decision: Optimize for Recall (catch as many churners as possible).
            Acceptable precision floor: 60% (intervention cost is low).

WITHOUT THIS SYSTEM:
- Marketing acts on gut feel or lagging indicators (already churned)
- No systematic batch scoring of the customer base
- No alert mechanism for the retention team
```

### 2.3 ML Solution
```
An automated ML pipeline that:
1. Engineers behavioral and financial signals from raw transaction/activity data
2. Trains a champion model (XGBoost/LightGBM) and challenger models with 
   full MLflow experiment tracking
3. Scores the entire active customer base every 20 days via batch inference
4. Provides a real-time single-customer prediction API for on-demand queries
5. Surfaces churn probability, risk tier, and top contributing factors 
   (SHAP explanations) to a business-readable React dashboard
6. Monitors production data and model drift, triggers automated retraining
   when statistical thresholds are breached
```

### 2.4 Success Criteria
```
TECHNICAL:
  - Model AUC-ROC >= 0.85 on held-out temporal test set
  - Recall >= 0.80 at operating threshold (catch 4 in 5 true churners)
  - Precision >= 0.60 at operating threshold
  - Batch scoring pipeline: <10 min runtime for full customer base
  - Single prediction API latency: <200ms p99
  - End-to-end pipeline runs without manual intervention

BUSINESS:
  - Dashboard surfaces top-20 at-risk customers with SHAP reason codes
  - Batch churn report delivered every 20 days to stakeholder view
  - Monitoring alerts fire within 24h of drift threshold breach
  - System explains WHY a customer is predicted to churn 
    (not just yes/no — reason codes matter for retention actions)
```

### 2.5 Out of Scope (This Version)
```
- Does NOT integrate with a live CRM or e-commerce platform in real-time
  (Kaggle CSV is the data source for this portfolio version)
- Does NOT predict revenue impact of individual churners
- Does NOT support real-time streaming event ingestion (Kafka/Kinesis)
- Does NOT include A/B testing of retention interventions
- Does NOT handle multi-tenant SaaS (single merchant context only)
- Does NOT build a mobile app frontend
```

---

## 3. AI/ML PROBLEM FRAMING

### 3.1 Problem Type
```
Task Type:          Binary Classification

Prediction Target:  Churn (1 = customer churned or will churn within 
                    the prediction window, 0 = retained)

Input:              Engineered feature vector per customer — behavioral 
                    signals, financial patterns, service usage, contract data

Output:             churn_probability: float (0.0–1.0)
                    churn_label:       int   (1 if prob >= threshold else 0)
                    risk_tier:         str   ("HIGH" | "MEDIUM" | "LOW")
                    top_reasons:       list  (SHAP top-3 contributing features)

Prediction Horizon: Retroactive on historical data (Phase 1–2);
                    20-day batch scoring cycle in production (Phase 3)

Granularity:        Per customer_id — one row, one prediction
```

### 3.2 Target Variable Definition
```
CHURN LABEL (from Kaggle dataset):
  Churn = 1: Customer has churned (left, cancelled, or gone inactive 
              beyond the dataset's defined inactivity window)
  Churn = 0: Customer is retained / active

IMPORTANT NOTES:
  - The Kaggle label is retrospective (historical data). In production,
    we re-frame this as: "will this customer churn in the NEXT N days?"
    using behavioral signals as leading indicators.
    
  - Customers with tenure < [N days — determine after EDA] may be excluded 
    from training as they lack sufficient behavioral history.

  - The churn definition must be locked before training and never changed 
    mid-experiment — doing so invalidates all prior comparisons.

  - After Phase 1 EDA, document the exact Kaggle label definition here.
    [UPDATE AFTER PHASE 1 EDA]
```

### 3.3 Feature Engineering Strategy
```
This is the most intellectually valuable part of the project. Every engineered
feature below maps to ACTUAL columns in the Kaggle E-Commerce dataset.
The PDF reference document (Phase1_Feature_Reference.pdf) is the authoritative
source for business meaning. Engineering actions below are final decisions.

═══════════════════════════════════════════════════════════════════
GROUP 1: FINANCIAL STRESS SIGNALS
Sources: OrderAmountHikeFromlastYear, CouponUsed, CashbackAmount, OrderCount
═══════════════════════════════════════════════════════════════════

  discount_dependency_ratio
    = CouponUsed / OrderCount
    WHY: High ratio = customer only stays because of discounts.
         Loyalty that evaporates when promotions stop.
    WATCH-OUT: OrderCount=0 causes division by zero → clip to 0.

  cashback_per_order
    = CashbackAmount / OrderCount
    WHY: High cashback-per-order with declining OrderCount = incentive-only
         engagement masking disengagement. Dangerous profile.
    WATCH-OUT: Same zero-division guard needed.

  order_value_declining_flag
    = 1 if OrderAmountHikeFromlastYear < 0 else 0
    WHY: Negative year-over-year order value = slow-burn churn signal.
         Keep negatives — do NOT clip this feature.
    WATCH-OUT: Missing values → impute with 0 (no change assumed).

═══════════════════════════════════════════════════════════════════
GROUP 2: SERVICE DEPENDENCY SCORE
Sources: NumberOfAddress, NumberOfDeviceRegistered, PreferredPaymentMode
═══════════════════════════════════════════════════════════════════

  platform_embeddedness_score
    = (NumberOfAddress × 0.4) + (NumberOfDeviceRegistered × 0.6)
    WHY: Customers with more addresses and devices have higher switching cost.
         Weighted: devices carry more lock-in than addresses.
    WATCH-OUT: Cap outliers on NumberOfAddress (IQR method) first.

  is_cod_user
    = 1 if PreferredPaymentMode == 'Cash on Delivery' else 0
    WHY: COD users systematically show higher churn rates.
         Signals low digital trust and financial caution.
    NOTE: One-hot encode PreferredPaymentMode AND keep this binary flag.

  address_per_tenure_ratio
    = NumberOfAddress / (Tenure + 1)
    WHY: A customer who added many addresses early on is more embedded
         than one who added the same count after 3 years.

═══════════════════════════════════════════════════════════════════
GROUP 3: ENGAGEMENT DECAY TRAJECTORY
Sources: HourSpendOnApp, DaySinceLastOrder, OrderCount, Tenure,
         NumberOfDeviceRegistered, PreferredLoginDevice
═══════════════════════════════════════════════════════════════════

  recency_risk_score
    = DaySinceLastOrder / (Tenure + 1)
    WHY: A 30-day gap for a 3-month customer is alarming.
         The same gap for a 3-year customer is vacation.
         Normalising by tenure makes recency meaningful.
    CRITICAL: Large DaySinceLastOrder + low Tenure ≈ certain churner.

  order_frequency_normalized
    = OrderCount / (Tenure + 1)
    WHY: Order count raw is misleading — a new customer can't have many orders.
         Normalised by tenure gives a true engagement rate.

  app_engagement_tier
    = pd.cut(HourSpendOnApp, bins=[0, 1, 3, 999], labels=[0, 1, 2])
    WHY: Low app time precedes churn by weeks — bucketing it into
         Low / Medium / High makes the signal more robust.
    WATCH-OUT: Zero could be website-only user. Cross-check with
               PreferredLoginDevice (Computer users may not use app).

  is_mobile_only
    = 1 if PreferredLoginDevice == 'Mobile Phone' else 0
    WHY: Mobile-only users have lower friction to switch platforms.
    NOTE: Encode PreferredLoginDevice one-hot AND keep this binary flag.

═══════════════════════════════════════════════════════════════════
GROUP 4: CONTRACT RISK PROFILE
Sources: Tenure, SatisfactionScore, Complain, CityTier, WarehouseToHome
═══════════════════════════════════════════════════════════════════

  tenure_segment
    = 0 if Tenure <= 6 else (1 if Tenure <= 24 else 2)
    Labels: 0=New (0-6mo), 1=Growing (6-24mo), 2=Loyal (24+mo)
    WHY: Churn hazard is highest for new customers. Encoding as ordinal
         lets the model learn the lifecycle curve directly.

  silent_dissatisfaction_flag
    = 1 if (SatisfactionScore <= 3) AND (Complain == 0) else 0
    WHY: The most dangerous churn profile — unhappy but not complaining.
         A customer who complained AND was resolved (Complain=1, Score≥4) is OK.
         A customer who is silent AND unsatisfied is about to leave.
    NOTE: This is a COMPOSITE feature — neither column alone catches this.

  complaint_satisfaction_interaction
    = Complain × (6 - SatisfactionScore)
    WHY: A complaint by someone with score=1 is far more serious than
         a complaint by someone with score=4. This interaction captures severity.
    RANGE: 0 (no complaint) to 5 (complained + worst score).

  warehouse_distance_risk
    = 1 if WarehouseToHome > median(WarehouseToHome) else 0
    WHY: Above-median distance = consistently slower delivery = churn risk.
    WATCH-OUT: Log-transform WarehouseToHome BEFORE computing median
               to handle outliers (very remote addresses).

═══════════════════════════════════════════════════════════════════
GROUP 5: CATEGORY VOLATILITY SIGNAL
Source: PreferedOrderCat
═══════════════════════════════════════════════════════════════════

  category_volatility_score
    Mapping (by historical churn rate — update after Phase 1 EDA):
    'Grocery'     → 0   (necessity buyer — lowest churn)
    'Mobile'      → 1
    'Fashion'     → 2
    'Electronics' → 2
    'Others'      → 1
    WHY: Necessity category buyers churn less — their purchase is habitual.
         Discretionary buyers (fashion, electronics) respond to better deals.
    NOTE: Compute actual churn rates per category in Phase 1 EDA,
          then update this mapping before training.
          [UPDATE AFTER EDA: category_churn_rates = {}]

═══════════════════════════════════════════════════════════════════
PIPELINE ARTIFACTS (saved with every training run)
═══════════════════════════════════════════════════════════════════
  models/artifacts/preprocessor.pkl        — fitted sklearn Pipeline
  models/artifacts/feature_names.json      — ordered feature list
  models/artifacts/feature_importance_pre.csv — pre-model RF importances
  models/artifacts/category_churn_rates.json — PreferedOrderCat mapping
  models/artifacts/reference_distribution.pkl — Evidently reference dataset
                                               (training feature distributions
                                                used for future drift checks)

NOTE: preprocessor is fit ONLY on training data. Never on val or test.
      feature_names.json is the CONTRACT between training and inference.
      Column order must match exactly at prediction time — enforced in predict.py.
```

### 3.4 Class Imbalance
```
EXPECTED IMBALANCE: ~80% retained (0) vs ~20% churned (1)
  [Verify exact ratio after Phase 1 EDA — update this section]

STRATEGY (applied in this priority order):
  1. scale_pos_weight in XGBoost (neg_samples / pos_samples)
     — handles imbalance during training without data modification
  2. class_weight='balanced' in Logistic Regression
  3. Threshold tuning post-training — do NOT default to 0.5
     Use precision-recall curve to select threshold maximizing business metric
  4. SMOTE as a fallback option if above strategies are insufficient
     — only applied to TRAINING data, never test data

DO NOT evaluate on accuracy — it is misleading with imbalanced data.
Primary evaluation metric: AUC-ROC + Recall at chosen threshold.
```

### 3.5 Evaluation Metrics
| Metric                      | Target     | Rationale                                           |
|-----------------------------|------------|-----------------------------------------------------|
| AUC-ROC                     | >= 0.85    | Primary — threshold-independent overall performance |
| Recall @ operating threshold| >= 0.80    | Business priority: catch churners, not miss them    |
| Precision @ threshold       | >= 0.60    | Prevents wasted outreach budget                     |
| F1 Score                    | >= 0.70    | Harmonic balance for reporting                      |
| PR-AUC                      | >= 0.70    | Better than ROC-AUC under class imbalance           |
| Business Cost Score         | Minimize   | See Section 7.4 — custom cost-weighted metric       |
| Brier Score                 | < 0.15     | Calibration — probabilities must be trustworthy     |

### 3.6 Business Cost Metric (Custom)
```python
# The most important metric that rarely appears in Kaggle notebooks
# Assign real costs to FP and FN based on business reality

# Estimated costs (tune after stakeholder input):
COST_FN = 150  # USD — missed churner: lost LTV, acquisition cost to replace
COST_FP = 15   # USD — false alarm: discount/outreach campaign cost

def business_cost_score(y_true, y_pred, threshold=0.65):
    """Lower is better. This is what the business actually cares about."""
    labels = (y_pred >= threshold).astype(int)
    fn = ((y_true == 1) & (labels == 0)).sum()
    fp = ((y_true == 0) & (labels == 1)).sum()
    return (fn * COST_FN) + (fp * COST_FP)

# Log this metric in every MLflow run. It bridges ML and business language.
```

---

## 4. TECH STACK

> **LLM Instruction:** Use ONLY these versions. No alternatives unless I ask.

### 4.1 Data Science & ML
| Component           | Technology                        | Version   |
|---------------------|-----------------------------------|-----------|
| Language            | Python                            | 3.11.x    |
| Data Processing     | Pandas / NumPy                    | 2.x / 1.x |
| Visualization (EDA) | Matplotlib / Seaborn / Plotly     | latest    |
| ML — Tree Models    | XGBoost / LightGBM                | 2.x / 4.x |
| ML — Classical      | Scikit-learn (LogReg, MLP, RF)    | 1.4.x     |
| Hyperparameter Opt  | Optuna (Bayesian optimization)    | 3.x       |
| Explainability      | SHAP                              | 0.44.x    |
| Class Imbalance     | imbalanced-learn (SMOTE fallback) | 0.12.x    |
| Model Serialization | Joblib + MLflow Model format      | latest    |

### 4.2 MLOps & Experiment Tracking
| Component             | Technology                          | Where It Runs        | Version   |
|-----------------------|-------------------------------------|----------------------|-----------|
| Experiment Tracking   | MLflow (local server)               | LOCAL only           | 2.x       |
| Model Artifact Store  | HuggingFace Hub (free private repo) | CLOUD                | hf_hub    |
| Model Registry        | model_versions table in PostgreSQL  | CLOUD (Supabase)     | —         |
| Data/Model Drift      | Evidently AI                        | LOCAL + report in DB | 0.4.x     |
| Batch Scheduler       | GitHub Actions (cron workflow)      | CLOUD — free tier    | —         |
| Pipeline Trigger      | GitHub Actions workflow_dispatch    | CLOUD — free         | —         |

> **Decision:** Airflow and Prefect are dropped. They require always-on servers.
> GitHub Actions cron is free, auditable, and sufficient for a 20-day batch cycle.
> MLflow runs locally during training — no cloud MLflow server needed.

### 4.3 Backend / Serving
| Component           | Technology                        | Where It Runs        | Version   |
|---------------------|-----------------------------------|----------------------|-----------|
| Prediction API      | FastAPI                           | Render (free tier)   | 0.110.x   |
| ASGI Server         | Uvicorn                           | Inside Docker        | 0.27.x    |
| Data Validation     | Pydantic v2                       | —                    | 2.x       |
| Background Tasks    | FastAPI BackgroundTasks            | —                    | bundled   |
| Containerization    | Docker + Docker Compose (local)   | Docker Hub (deploy)  | latest    |

> **Decision:** Celery dropped — requires Redis (another server, cost, complexity).
> FastAPI BackgroundTasks handles async batch scoring within the free-tier container.

### 4.4 Frontend
| Component           | Technology                        | Where It Runs        | Version   |
|---------------------|-----------------------------------|----------------------|-----------|
| Framework           | React.js (Vite)                   | Vercel (free tier)   | 18.x      |
| Language            | TypeScript                        | —                    | 5.x       |
| Styling             | Tailwind CSS                      | —                    | 3.x       |
| Data Fetching       | React Query (TanStack Query)      | —                    | 5.x       |
| Charts              | Recharts                          | —                    | 2.x       |
| Routing             | React Router v6                   | —                    | 6.x       |
| Dashboard Refresh   | React Query polling (30s interval)| —                    | —         |

> **Decision on "live stream":** NOT WebSockets — overkill for churn cadence.
> NOT SSE for MVP — adds server complexity.
> DECISION: React Query polling every 30 seconds on admin dashboard.
> Displays "Last updated: X min ago" timestamp. Looks live, costs nothing.
> SSE upgrade path exists if needed later (see Section 5.5).

### 4.5 Database
| Component           | Technology                        | Where It Runs        | Version   |
|---------------------|-----------------------------------|----------------------|-----------|
| Primary Database    | PostgreSQL                        | Supabase (free tier) | 15.x      |
| ORM                 | SQLAlchemy (async) + Alembic      | —                    | 2.x       |
| Local Dev DB        | PostgreSQL in Docker              | Local only           | —         |
| DB Init / Seeding   | Python seed script from CSV       | Run once, local      | —         |

> **Decision:** Supabase free tier gives 500MB PostgreSQL, REST API auto-generated,
> and a connection pooler (pgBouncer) — all free. This is the right call.
> Render's free PostgreSQL deletes after 90 days — do not use it for the DB.

### 4.6 Infrastructure — LOCKED (Free Tier)
| Component              | Tool                       | Free Tier Limit              | Notes                              |
|------------------------|----------------------------|------------------------------|------------------------------------|
| API Hosting            | Render                     | 750 hrs/mo, sleeps after 15m | ⚠️ Cold start ~30s — see §5.5      |
| Frontend Hosting       | Vercel                     | Unlimited (hobby)            | Auto-deploy on git push to main    |
| Database               | Supabase                   | 500MB, 2 projects            | pgBouncer pooling included         |
| Model Artifact Storage | HuggingFace Hub            | Unlimited (public repo)      | Private repo with free account     |
| Batch Scheduler        | GitHub Actions cron        | 2,000 min/month              | Calls /predict/batch endpoint      |
| Container Registry     | Docker Hub                 | 1 free private repo          | Used by Render for deployment      |
| CI/CD                  | GitHub Actions             | 2,000 min/month              | test + lint + deploy on push       |
| Secrets Management     | GitHub Secrets             | Free                         | Injected into Actions workflows    |
| MLflow (local)         | Self-hosted local server   | Free                         | Training only — not cloud-deployed |

---

## 5. SYSTEM ARCHITECTURE

### 5.1 Deployment Topology (Free Tier — LOCKED)
```
═══════════════════════════════════════════════════════════════════════
  LOCAL MACHINE (your laptop — training only, not always-on)
═══════════════════════════════════════════════════════════════════════
  ┌─────────────────────────────────────────────────────────────────┐
  │  python train.py → MLflow (local, http://localhost:5000)        │
  │  python tune.py  → Optuna → best params → MLflow run            │
  │  python register.py → uploads model artifacts to HuggingFace Hub│
  └─────────────────────────────────────────────────────────────────┘
           │ push artifacts (model.pkl, preprocessor.pkl,
           │ feature_names.json, versions.json)
           ▼
═══════════════════════════════════════════════════════════════════════
  CLOUD — always on, free tier
═══════════════════════════════════════════════════════════════════════

  [HuggingFace Hub — free]          [Supabase — free PostgreSQL]
   churnguard/model-registry          ├── customers
   ├── v1.0.0/model.pkl               ├── predictions (audit trail)
   ├── v1.0.0/preprocessor.pkl        ├── batch_runs
   ├── v1.0.0/feature_names.json      ├── model_versions
   └── versions.json                  └── drift_reports
        {"production": "v1.0.0"}
           │                                    │
           │ downloaded at startup              │ read/write
           ▼                                    ▼
  [Render — FastAPI Docker container]◄──────────┘
   ├── loads champion model from HF Hub on startup
   ├── POST /api/v1/predict/single   (real-time)
   ├── POST /api/v1/predict/batch    (called by GitHub Actions)
   ├── GET  /api/v1/admin/overview   (dashboard data)
   ├── GET  /api/v1/health           (liveness + readiness)
   └── POST /api/v1/admin/monitoring (drift report)
           │
           │ REST + JWT
           ▼
  [Vercel — React frontend]
   ├── /register  — new customer onboarding form
   ├── /login
   ├── /admin/*   — KPI dashboard (React Query polling 30s)
   └── /profile   — customer self-view

  [GitHub Actions — free cron]
   ├── EVERY 20 DAYS (cron: '0 2 */20 * *'):
   │     → POST https://churnguard.onrender.com/api/v1/predict/batch
   │     → API runs batch scoring internally as BackgroundTask
   │     → Results written to Supabase predictions table
   │     → Dashboard auto-reflects on next 30s poll
   │
   └── ON PUSH TO MAIN:
         → pytest (unit + integration)
         → docker build + push to Docker Hub
         → Render auto-deploys new image (webhook)

⚠️  RENDER FREE TIER COLD START WARNING:
    Render free tier sleeps after 15 minutes of inactivity.
    Wake-up takes ~30 seconds on first request.
    Mitigation: GitHub Actions pings /api/v1/health 2 min before
    batch job fires (warm-up step in workflow — see Section 8.2).
    For the dashboard: React frontend shows a "Connecting..." spinner
    on first load while Render wakes up.
```

### 5.2 Model Artifact Delivery — Exact Flow
```
TRAINING (LOCAL):
  1. Train model locally with full MLflow tracking
  2. Best run identified by highest val_auc_roc in MLflow UI
  3. Run: python src/ml/register.py --version v1.2.0
  
register.py does the following:
  a. Loads model.pkl + preprocessor.pkl + feature_names.json
     from local mlruns/<run_id>/artifacts/
  b. Pushes to HuggingFace Hub private repo "churnguard/model-registry":
       hf_api.upload_folder(
           folder_path="./models/staging/v1.2.0/",
           repo_id="[your-hf-username]/churnguard-model-registry",
           path_in_repo="v1.2.0/"
       )
  c. Updates versions.json in the Hub repo:
       { "production": "v1.2.0", "staging": "v1.1.0", "archived": ["v1.0.0"] }
  d. Inserts record into Supabase model_versions table
  e. Prints: "Model v1.2.0 registered. Restart Render service to load."

SERVING (CLOUD — Render):
  On FastAPI startup (src/api/startup.py):
    1. Fetch versions.json from HF Hub
    2. Read production version: "v1.2.0"
    3. Download: model.pkl, preprocessor.pkl, feature_names.json
       via hf_hub_download() → cached in /tmp/ on Render
    4. Load model into memory
    5. Initialize SHAP TreeExplainer
    6. Mark service as ready (GET /health/ready returns 200)
  
  ⚠️ Render free tier /tmp/ is NOT persistent across deploys.
     Every redeploy re-downloads from HF Hub (~2-5 seconds for small models).
     This is acceptable — model.pkl for 5,600 rows will be < 20MB.
```

### 5.3 Batch Inference Flow (Every 20 Days — GitHub Actions)
```
GitHub Actions: .github/workflows/batch_score.yml

  TRIGGER: cron '0 2 */20 * *'  (2am UTC, every 20 days)
           workflow_dispatch     (manual trigger from GitHub UI)

  Step 1 — WARM UP (2 min before batch):
    curl https://churnguard.onrender.com/api/v1/health
    (wakes Render container from sleep)

  Step 2 — TRIGGER BATCH:
    curl -X POST \
      -H "X-Admin-Key: ${{ secrets.ADMIN_API_KEY }}" \
      https://churnguard.onrender.com/api/v1/predict/batch
    → Returns: {"batch_run_id": "uuid", "status": "running"}

  INSIDE FastAPI (BackgroundTask running on Render):
    Step 3: Query Supabase → fetch all customers where tenure_months >= 1
            AND NOT scored in last 18 days
            (excludes new customers with insufficient data — see §5.6)
    Step 4: Chunk customers into batches of 500 (memory safety)
    Step 5: For each chunk:
              → assemble feature vector per customer
              → apply preprocessor.pkl transform
              → model.predict_proba()
              → compute SHAP top-3 reasons (TreeExplainer)
              → determine risk_tier
    Step 6: Write all predictions to Supabase predictions table
    Step 7: Update batch_runs table: status='completed', counts per tier
    Step 8: Run Evidently drift check:
              → compare this batch's feature distributions
                vs reference distribution (saved during training)
              → compute PSI per feature
              → if any PSI > 0.20: set drift_alert=True in drift_reports table
              → GitHub Actions reads response and creates a GitHub Issue
                if drift detected (visible in repo, no Slack needed)
    Step 9: React dashboard reflects new data on next 30s poll

  GitHub Actions reads batch response and:
    - Logs summary to Actions run (visible in GitHub)
    - Creates GitHub Issue if drift alert fires
    - Marks workflow FAILED if batch status != 'completed' (alerts you)
```

**Exact GitHub Actions YAML — `.github/workflows/batch_score.yml`:**
```yaml
name: Batch Churn Scoring — Every 20 Days

on:
  schedule:
    - cron: '0 2 */20 * *'   # 2am UTC, every 20 days
  workflow_dispatch:          # allow manual trigger from GitHub UI
    inputs:
      force_all_customers:
        description: 'Score all customers regardless of last scored date'
        required: false
        default: 'false'

env:
  API_BASE_URL: ${{ secrets.RENDER_API_URL }}   # e.g. https://churnguard.onrender.com

jobs:
  batch-score:
    name: Run Batch Churn Prediction
    runs-on: ubuntu-latest
    timeout-minutes: 30          # fail safe — batch should never exceed 20min

    steps:
      - name: Wake Render service (cold start prevention)
        run: |
          echo "Pinging API to wake from sleep..."
          for i in 1 2 3; do
            STATUS=$(curl -s -o /dev/null -w "%{http_code}" $API_BASE_URL/api/v1/health)
            echo "Attempt $i: HTTP $STATUS"
            if [ "$STATUS" = "200" ]; then break; fi
            sleep 20
          done
          echo "API is awake."

      - name: Trigger batch scoring
        id: trigger
        run: |
          RESPONSE=$(curl -s -X POST \
            -H "X-Admin-Key: ${{ secrets.ADMIN_API_KEY }}" \
            -H "Content-Type: application/json" \
            -d '{"force_all": ${{ github.event.inputs.force_all_customers || 'false' }}}' \
            $API_BASE_URL/api/v1/predict/batch)

          echo "Response: $RESPONSE"
          BATCH_ID=$(echo $RESPONSE | python3 -c "import sys,json; print(json.load(sys.stdin)['batch_run_id'])")
          echo "batch_run_id=$BATCH_ID" >> $GITHUB_OUTPUT

      - name: Poll batch completion (max 20 min)
        run: |
          BATCH_ID=${{ steps.trigger.outputs.batch_run_id }}
          for i in $(seq 1 24); do
            sleep 50
            STATUS=$(curl -s \
              -H "X-Admin-Key: ${{ secrets.ADMIN_API_KEY }}" \
              $API_BASE_URL/api/v1/predict/batch/status/$BATCH_ID \
              | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
            echo "Poll $i: $STATUS"
            if [ "$STATUS" = "completed" ]; then
              echo "Batch completed successfully."
              exit 0
            fi
            if [ "$STATUS" = "failed" ]; then
              echo "Batch FAILED."
              exit 1
            fi
          done
          echo "Batch timed out after 20 minutes."
          exit 1

      - name: Fetch batch summary
        id: summary
        if: success()
        run: |
          RESULT=$(curl -s \
            -H "X-Admin-Key: ${{ secrets.ADMIN_API_KEY }}" \
            $API_BASE_URL/api/v1/predict/batch/status/${{ steps.trigger.outputs.batch_run_id }})
          echo "=== BATCH SUMMARY ==="
          echo $RESULT | python3 -c "
          import sys, json
          d = json.load(sys.stdin)
          print(f\"Scored: {d['customers_scored']} customers\")
          print(f\"HIGH risk: {d['high_risk_count']} ({d.get('high_risk_pct',0):.1f}%)\")
          print(f\"MEDIUM risk: {d['medium_risk_count']}\")
          print(f\"LOW risk: {d['low_risk_count']}\")
          print(f\"Drift alert: {d.get('drift_alert', False)}\")
          "
          DRIFT=$(echo $RESULT | python3 -c "import sys,json; print(json.load(sys.stdin).get('drift_alert', False))")
          echo "drift_detected=$DRIFT" >> $GITHUB_OUTPUT

      - name: Create GitHub Issue if drift detected
        if: steps.summary.outputs.drift_detected == 'True'
        uses: actions/github-script@v7
        with:
          script: |
            await github.rest.issues.create({
              owner: context.repo.owner,
              repo: context.repo.repo,
              title: '🚨 DATA DRIFT ALERT — Batch Run ' + new Date().toISOString().split('T')[0],
              body: `## Drift Detected in Batch Scoring Run\n\n` +
                    `**Date:** ${new Date().toISOString()}\n` +
                    `**Batch Run ID:** ${{ steps.trigger.outputs.batch_run_id }}\n\n` +
                    `PSI threshold exceeded (> 0.20) on one or more top features.\n\n` +
                    `**Actions Required:**\n` +
                    `- [ ] Review drift report in ChurnGuard admin dashboard\n` +
                    `- [ ] Check which features drifted (see /admin/monitoring)\n` +
                    `- [ ] Evaluate whether retraining is needed\n` +
                    `- [ ] Run \`python src/ml/train.py\` if retraining approved\n\n` +
                    `cc @[your-github-username]`,
              labels: ['drift-alert', 'mlops', 'needs-review']
            })
```

### 5.4 Real-Time Single Prediction Flow
```
POST /api/v1/predict/single
  │
  ├─ Validate JWT token (customer or admin)
  ├─ Validate request body (Pydantic)
  ├─ If customer_id provided:
  │    → Fetch customer record from Supabase
  │    → Recompute system features (tenure, recency, etc.)
  │  Else if features dict provided:
  │    → Use features directly (admin/testing mode)
  │
  ├─ Check: tenure_months >= 1?
  │    NO  → Return: {"scoreable": false, "reason": "Insufficient tenure",
  │                   "days_until_scoreable": N}
  │    YES → Continue
  │
  ├─ Apply preprocessor.pkl → feature vector
  ├─ model.predict_proba() → churn_probability
  ├─ SHAP TreeExplainer → top 3 contributing features
  ├─ Determine risk_tier (HIGH/MEDIUM/LOW thresholds from config)
  ├─ Write to predictions table (audit trail)
  └─ Return full prediction response (< 200ms target)
```

### 5.5 Dashboard Refresh Strategy (React Query Polling)
```
CHOSEN APPROACH: React Query polling — NOT WebSockets, NOT SSE (for MVP)

WHY:
  - WebSockets: requires persistent connection, incompatible with Render
    free tier sleep behavior, overkill for churn cadence
  - SSE: good option but adds FastAPI endpoint complexity for Phase 3 MVP
  - Polling: dead simple, works with Render sleep/wake, looks live to users

IMPLEMENTATION:
  useQuery({
    queryKey: ['admin-overview'],
    queryFn: () => api.get('/admin/overview'),
    refetchInterval: 30_000,            // poll every 30 seconds
    staleTime: 25_000,                  // treat data fresh for 25s
  })
  
  Dashboard header shows: "Last updated: 2 minutes ago  🟢"
  If API is sleeping (Render cold start): "Connecting...  🟡"

UPGRADE PATH (Phase 3+ stretch goal):
  Replace polling with SSE for these specific events:
  - New customer registered
  - Batch run completed
  - Drift alert triggered
  Events pushed via FastAPI EventSourceResponse to a notification bell.
```

### 5.6 New Customer Scoring Gate
```
PROBLEM: A customer who just registered has Tenure=0, OrderCount=0,
         DaySinceLastOrder=NULL. The model was never trained on this profile.
         Predicting churn at registration is meaningless and potentially harmful.

RULE:
  A customer is only eligible for batch scoring when:
    tenure_months >= 1   (at least one full month on platform)
    order_count >= 1     (at least one purchase made)

WHAT HAPPENS UNTIL THEN:
  - Dashboard shows "Onboarding" badge instead of risk tier
  - days_until_scoreable counter shown to admin
  - Customer excluded from all batch runs and drift checks
  - Single prediction endpoint returns 200 with scoreable=false (not an error)

WHERE THIS IS ENFORCED:
  - batch_score.py: WHERE clause filters tenure_months >= 1 AND order_count >= 1
  - predict.py: explicit check before feature assembly, returns early
  - Frontend: RiskBadge component shows "Onboarding" state
```

### 5.7 Full Architecture Mermaid Diagram
```mermaid
graph TD
    subgraph LOCAL["🖥️ Local Machine (Training Only)"]
        TR[train.py + tune.py] --> ML[MLflow local server]
        ML --> REG[register.py]
    end

    subgraph CLOUD["☁️ Cloud — Free Tier"]
        REG -->|upload artifacts| HF[HuggingFace Hub\nmodel-registry repo]
        HF -->|download on startup| API

        subgraph RENDER["Render — FastAPI Container"]
            API[FastAPI\n/predict /admin /auth]
        end

        subgraph SUPABASE["Supabase — PostgreSQL"]
            DB[(customers\npredictions\nbatch_runs\nmodel_versions)]
        end

        API <-->|read/write| DB

        subgraph VERCEL["Vercel — React Frontend"]
            FE[Dashboard\npolling 30s]
        end

        FE -->|REST + JWT| API

        subgraph GHA["GitHub Actions"]
            CRON[cron every 20 days]
            CI[CI — test + lint + deploy]
        end

        CRON -->|POST /predict/batch| API
        CI -->|Docker build + push| RENDER
    end

    subgraph MONITOR["Monitoring"]
        API -->|PSI check| EV[Evidently Report\nstored in DB]
        EV -->|PSI > 0.20| ISSUE[GitHub Issue\nDrift Alert]
    end
```

---

## 6. PHASE 1 — DATA RESEARCH & UNDERSTANDING

### 6.1 Kaggle Dataset Strategy
```
PRIMARY DATASET:
  Recommended: "E-Commerce Dataset" by Ankur Sinha (Kaggle)
  URL: https://www.kaggle.com/datasets/ankitverma2010/ecommerce-customer-churn-analysis-and-prediction
  Features: ~20 columns including Tenure, CityTier, WarehouseToHome, 
            HourSpendOnApp, NumberOfDeviceRegistered, SatisfactionScore,
            NumberOfAddress, Complain, OrderAmountHikeFromlastYear,
            CouponUsed, OrderCount, DaySinceLastOrder, CashbackAmount
  Target:   Churn (0/1)
  Rows:     ~5,600

  ALTERNATIVE / SUPPLEMENTARY:
  Telco Customer Churn (IBM Watson)
  URL: https://www.kaggle.com/datasets/blastchar/telco-customer-churn
  Features: ~21 columns, different behavioral signals
  Consideration: Can be used to enrich the feature vocabulary conceptually
                 (Phase 1 brainstorm: merging 2 CSVs if compatible)

MERGING STRATEGY NOTE (from your Phase 1 diagram):
  Merging two CSVs increases feature count but requires:
  - Shared key (customer_id / email) — may not exist between Kaggle datasets
  - Same time period / cohort definition
  - Recommendation: Use ONE primary dataset for MVP. 
    If features are insufficient, engineer new ones from existing columns 
    rather than merging mismatched datasets. Document this decision here 
    after Phase 1 EDA.
    [DECISION: _______________ — fill after EDA]
```

### 6.2 EDA Checklist
```
UNDERSTAND THE DATA:
  [ ] Load CSV, inspect shape, dtypes, head/tail
  [ ] Check all column names and their business meaning
  [ ] Map every column to: raw feature / derived / ID / target
  [ ] Identify the churn label column and its value distribution
  
NULL & QUALITY ANALYSIS:
  [ ] Missing value count and % per column
  [ ] Decide imputation strategy per column (median / mode / drop / flag)
  [ ] Duplicate rows check (drop exact duplicates)
  [ ] Outlier detection (IQR method) on numerical features
  [ ] Identify any obviously leaky features 
      (e.g., "days since cancellation" — future info, drop it)

DISTRIBUTION ANALYSIS:
  [ ] Class balance: Churn=1 vs Churn=0 (expected ~20–30% churn)
  [ ] Distribution plots for all numerical features (histograms + KDE)
  [ ] Bar charts for all categorical features
  [ ] Correlation heatmap (Pearson for numeric)
  [ ] Box plots: feature distributions split by Churn=0 vs Churn=1
      This directly shows which features separate the two classes.

CHURN DRIVER ANALYSIS (the business question):
  [ ] Mean feature values for churned vs retained customers (side-by-side)
  [ ] Top features correlated with churn target
  [ ] Key question: What makes a customer churn in this dataset?
      Document findings here after EDA. [UPDATE AFTER EDA]
  
OUTPUTS FROM PHASE 1:
  notebooks/01_eda.ipynb               — full EDA notebook
  reports/eda_report.html              — Ydata-profiling or Sweetviz auto-report
  data/raw/ecommerce_churn.csv         — original dataset (committed to repo)
  docs/eda_findings.md                 — written summary of key findings
```

### 6.3 Key EDA Findings (Fill After Phase 1)
```
Churn Rate:             [e.g., 16.8% — update after EDA]
Dataset Shape:          [rows x cols — update after EDA]
Missing Values:         [Top 3 columns with nulls — update after EDA]
Top Churn Predictors:   [e.g., Tenure, Complain, SatisfactionScore — update]
Class Imbalance Ratio:  [e.g., 83.2% retained : 16.8% churned — update]
Leaky Features Found:   [Any columns dropped for leakage — update]
Decision on Merging:    [Keep single dataset / merge — with reason — update]
Surprise Findings:      [Anything unexpected discovered — update]
```

---

## 7. PHASE 2 — ML PIPELINE

### 7.1 Feature Engineering Pipeline
```
INPUT:  data/raw/ecommerce_churn.csv  (or cleaned version from Phase 1)
OUTPUT: data/processed/features_train.parquet
        data/processed/features_val.parquet
        data/processed/features_test.parquet
        models/artifacts/preprocessor.pkl
        models/artifacts/feature_names.json
        models/artifacts/feature_importance_pre.csv

SCRIPT: src/pipeline/features.py

STEPS (in order):
  1. Drop ID columns (CustomerID) — not a feature, it's a key
  2. Drop leaky features identified in Phase 1
  3. Handle missing values (MedianImputer for numeric, ModeImputer for categorical)
  4. Engineer new domain signals (see Section 3.3):
       - Financial stress signals
       - Service dependency score
       - Engagement decay trajectory
       - Contract risk profile
  5. Encode categoricals (OrdinalEncoder for ordinal, 
     OneHotEncoder for nominal — inside sklearn Pipeline)
  6. Scale numerics (StandardScaler inside Pipeline)
  7. Fit Pipeline ONLY on training fold — transform val/test separately
  8. Save: preprocessor.pkl (fitted Pipeline)
  9. Save: feature_names.json (ordered list post-transform)
  10. Compute and save pre-model feature importances via 
      RandomForest.feature_importances_ to feature_importance_pre.csv
```

### 7.2 Train / Validation / Test Split
```
CRITICAL: MUST use stratified split (not random) to preserve class balance.
For Kaggle static dataset (no time dimension), use stratified k-fold.

If dataset has a time column (e.g., order dates):
  -> Use temporal split to prevent future leakage.
  [Determine after Phase 1 EDA — update here]

SPLIT RATIOS:
  Training:   70%  — used for model fitting and hyperparameter tuning
  Validation: 15%  — used for early stopping and threshold selection
  Test:       15%  — LOCKED, never touched until final evaluation

  stratify=True on Churn column in all splits.
  random_state=42 for reproducibility.

ANTI-PATTERNS TO AVOID (hard rules):
  - Never fit the preprocessor on anything other than the training split
  - Never use the test set for ANY decision during training
  - Never re-use test set across experiments — it becomes contaminated
```

### 7.3 Model Training — Multi-Algorithm Comparison
```
ALGORITHM 1: XGBoost Classifier (primary candidate)
  Why: State-of-the-art on tabular data, handles imbalance natively
       via scale_pos_weight, built-in feature importance.
  Key hyperparams to tune: n_estimators, max_depth, learning_rate,
                           subsample, colsample_bytree, scale_pos_weight

ALGORITHM 2: LightGBM Classifier (challenger)
  Why: Faster training than XGBoost, good on moderate-size tabular data,
       excellent with categorical features.
  Key hyperparams to tune: n_estimators, num_leaves, learning_rate,
                           min_child_samples, class_weight

ALGORITHM 3: Logistic Regression (interpretable baseline)
  Why: Establishes a minimum bar. If tree models don't beat it significantly,
       the feature engineering needs work.
  Key hyperparams: C (regularization strength), class_weight='balanced'

ALGORITHM 4: MLP Classifier (neural baseline)
  Why: Tests if non-linear deep patterns exist beyond tree ensembles.
       Often NOT the best on small tabular datasets but worth benchmarking.
  Key hyperparams: hidden_layer_sizes, learning_rate_init, alpha, dropout

HYPERPARAMETER OPTIMIZATION:
  Tool: Optuna (Bayesian optimization — smarter than GridSearch)
  Objective: Maximize AUC-ROC on validation set
  Trials: 50–100 per algorithm
  Pruning: MedianPruner (stops bad trials early)
  Script: src/ml/tune.py
```

### 7.4 MLflow Experiment Tracking
```
EXPERIMENT NAME: "churn-prediction-ecommerce"

LOGGED PER RUN:
  Parameters:
    - algorithm name
    - all hyperparameters
    - feature_set_version (hash of feature_names.json)
    - train_rows, val_rows, test_rows
    - class_imbalance_ratio
    - random_seed
    - preprocessing strategy

  Metrics:
    - val_auc_roc
    - val_pr_auc
    - val_recall @ threshold
    - val_precision @ threshold
    - val_f1
    - val_brier_score
    - business_cost_score (Section 3.6)
    - train_auc_roc (to detect overfitting gap)
    - inference_latency_ms (single prediction)

  Artifacts:
    - model (MLflow native format)
    - preprocessor.pkl
    - feature_names.json
    - confusion_matrix.png
    - roc_curve.png
    - pr_curve.png
    - shap_summary_plot.png
    - shap_feature_importance.csv
    - eval_report.html

  Tags:
    - phase: "training"
    - algorithm: "xgboost"
    - dataset_version: "v1.0"
    - status: "candidate" | "champion" | "archived"

MLFLOW UI: http://localhost:5000 (local dev)
SCRIPT: src/ml/train.py
```

### 7.5 Model Evaluation & Selection
```
EVALUATION PROCEDURE:
  1. All trained models evaluated on the SAME held-out test set
  2. Primary metric: AUC-ROC (threshold-independent)
  3. Secondary: Business Cost Score at selected threshold
  4. Threshold selection: plot precision-recall curve, 
     choose threshold that meets Recall >= 0.80 constraint
     while maximizing precision above floor (0.60)

CHAMPION SELECTION RULE:
  Champion = model with highest AUC-ROC on test set
  Condition: must also meet all minimum metric targets (Section 3.5)
  Tie-breaker: lower business_cost_score wins

WHAT "MODEL READY FOR DEPLOYMENT" MEANS:
  [ ] AUC-ROC >= 0.85 on test set
  [ ] Recall >= 0.80 at chosen threshold
  [ ] Precision >= 0.60 at chosen threshold
  [ ] No significant overfitting (train AUC - test AUC < 0.05)
  [ ] SHAP values computed and reasonable (no single feature dominates > 40%)
  [ ] Business cost score computed and logged
  [ ] Model artifact registered in MLflow with tag: "production_candidate"
  [ ] Evaluation report (HTML) generated and reviewed
  [ ] Prediction on sample inputs manually spot-checked

DIAGNOSING "IS IT DATA, MODEL, OR PREPROCESSING?" (from your diagram):
  Data issues:    Low val AUC despite tuning — revisit feature engineering
  Model issues:   High train AUC, low val AUC — overfitting (regularize)
  Preprocessing:  Warnings about feature names, unstable scaling — check pipeline
```

### 7.6 FastAPI Prediction Microservice
```
SERVICE: src/api/main.py
ENDPOINTS:
  POST /api/v1/predict/single  — Real-time single customer prediction
  POST /api/v1/predict/batch   — Trigger batch scoring of all active customers
  GET  /api/v1/model/info      — Current champion model metadata
  GET  /api/v1/health          — Liveness check
  GET  /api/v1/health/ready    — Readiness (model loaded check)

STARTUP BEHAVIOR (Render cloud):
  - Download versions.json from HuggingFace Hub
  - Read production version tag
  - Download model.pkl + preprocessor.pkl + feature_names.json from HF Hub
    (cached to /tmp/ — re-downloaded on each Render deploy, ~2-5 seconds)
  - Initialize SHAP TreeExplainer
  - Run synthetic test prediction to verify pipeline end-to-end
  - Mark ready: GET /health/ready returns 200

STARTUP BEHAVIOR (local dev):
  - Load model artifacts from local models/production/ directory
  - Same pipeline verification as above

DOCKER:
  Dockerfile:          docker/Dockerfile.api
  docker-compose.yml:  runs API + PostgreSQL + MLflow locally
  Port mapping:        API: 8000, MLflow: 5000, DB: 5432

CI/CD (GitHub Actions):
  On push to main:
    -> Run pytest (unit + integration tests)
    -> Build Docker image
    -> Push to Docker Hub / GHCR
    -> Deploy to cloud (Phase 3)
```

---

## 8. MLOPS & MODEL LIFECYCLE

### 8.1 Model Versioning
```
Format: MAJOR.MINOR.PATCH

  MAJOR: New feature set, changed target definition, new algorithm family
  MINOR: Retraining with new data window, hyperparameter re-tuning
  PATCH: Bug fix in preprocessing or inference code, no model re-fit

Registry (source of truth): HuggingFace Hub — versions.json
  {"production": "v1.0.0", "staging": null, "archived": []}

Mirror (for dashboard): Supabase model_versions table
MLflow: LOCAL only — all training experiments tracked locally, not cloud deployed

Current Production Model: v[TBD — fill after first training run]
Current Staging Model:    v[TBD]
```

### 8.2 GitHub Actions Batch Scoring Workflow — LOCKED
```yaml
# .github/workflows/batch_score.yml
# The ENTIRE batch scheduling infrastructure. No Airflow. No always-on server.

name: Batch Churn Scoring

on:
  schedule:
    - cron: '0 2 */20 * *'     # 2am UTC every 20 days — fully automated
  workflow_dispatch:            # manual trigger from GitHub UI (admin button)

jobs:
  batch-score:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - name: Wake up Render service (cold start mitigation)
        run: |
          for i in 1 2 3 4 5; do
            STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
              https://churnguard.onrender.com/api/v1/health)
            if [ "$STATUS" = "200" ]; then echo "API ready."; break; fi
            echo "Attempt $i — sleeping 15s..."
            sleep 15
          done

      - name: Trigger batch scoring
        id: batch
        run: |
          RESPONSE=$(curl -s -X POST \
            -H "X-Admin-Key: ${{ secrets.ADMIN_API_KEY }}" \
            https://churnguard.onrender.com/api/v1/predict/batch)
          echo "response=$RESPONSE" >> $GITHUB_OUTPUT

      - name: Poll until complete (max 20 min)
        run: |
          BATCH_ID=$(echo '${{ steps.batch.outputs.response }}' | \
            python3 -c "import sys,json; print(json.load(sys.stdin)['batch_run_id'])")
          for i in $(seq 1 40); do
            STATUS=$(curl -s \
              -H "X-Admin-Key: ${{ secrets.ADMIN_API_KEY }}" \
              "https://churnguard.onrender.com/api/v1/predict/batch/$BATCH_ID" | \
              python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
            echo "Status: $STATUS"
            if [ "$STATUS" = "completed" ]; then exit 0; fi
            if [ "$STATUS" = "failed" ]; then exit 1; fi
            sleep 30
          done
          exit 1

      - name: Create GitHub Issue if drift alert fired
        run: |
          DRIFT=$(curl -s \
            -H "X-Admin-Key: ${{ secrets.ADMIN_API_KEY }}" \
            https://churnguard.onrender.com/api/v1/admin/monitoring/latest | \
            python3 -c "import sys,json; print(json.load(sys.stdin).get('drift_alert',False))")
          if [ "$DRIFT" = "True" ]; then
            gh issue create \
              --title "DATA DRIFT ALERT — $(date '+%Y-%m-%d')" \
              --body "PSI threshold breached. Review /admin/monitoring dashboard." \
              --label "drift-alert"
          fi
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

### 8.3 CI/CD Workflow (Push to Main)
```yaml
# .github/workflows/ci_cd.yml
name: CI/CD Pipeline

on:
  push:       { branches: [main] }
  pull_request: { branches: [main] }

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install -r requirements.txt
      - run: ruff check src/
      - run: pytest tests/ -v --tb=short

  deploy:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/build-push-action@v5
        with:
          push: true
          tags: ${{ secrets.DOCKERHUB_USERNAME }}/churnguard-api:latest
          file: docker/Dockerfile.api
      - name: Trigger Render redeploy
        run: curl -X POST "${{ secrets.RENDER_DEPLOY_HOOK_URL }}"
```

### 8.4 Retraining Policy
```
RETRAINING IS ALWAYS LOCAL. Never automated to cloud (no budget for always-on).
Human reviews drift alert, decides to retrain.

SIGNAL TO RETRAIN:
  Automatic: GitHub Issue "DATA DRIFT ALERT" created by batch workflow
  Scheduled: Consider after every 3rd batch cycle (~60 days) even without drift

LOCAL RETRAINING STEPS:
  [ ] Pull latest customer export from Supabase
  [ ] python src/pipeline/features.py
  [ ] python src/ml/train.py --config config/model_config.yaml
  [ ] Compare new run vs champion in local MLflow UI
  [ ] If new AUC >= prod_AUC - 0.01: python src/ml/register.py --version vX.X.X
  [ ] Trigger Render redeploy (new model downloads on startup)

SAFETY GATES (never retrain if):
  - New labeled data < 200 samples since last train
  - Data quality check fails
  - Less than 7 days since last training run
```

### 8.5 Drift Monitoring
```
TOOL: Evidently AI — runs inside FastAPI as part of batch scoring job

WHAT IS MONITORED (per batch run):
  Feature drift (input drift):
    - Method: Population Stability Index (PSI) per numeric feature
    - Method: Chi-squared test per categorical feature
    - Reference: feature distributions from training set
                 (saved as reference_stats.json during training, loaded by API)
    - Alert threshold: PSI > 0.20 on any top-10 SHAP feature
    - Output: drift_alert flag + per-feature PSI scores
              stored in drift_reports table in Supabase

  Prediction drift (output drift):
    - Track mean churn_probability across batch run
    - Compare to rolling 3-run average
    - Alert if mean shifts > 0.10

WHERE RESULTS APPEAR:
  /admin/monitoring page in React dashboard
  GitHub Issue created by Actions if alert fires (permanent visible record)
  drift_reports table in Supabase (queryable for trend analysis)
```

### 8.6 Rollback Procedure
```
TRIGGER: Observed AUC drop on ground truth labels, API errors > 2%,
         or obviously bad predictions noticed in dashboard

STEPS:
  1. python src/ml/register.py --rollback --version vX.X.X
     (updates versions.json on HF Hub to previous production version)
  2. Trigger Render redeploy (downloads previous model on startup)
  3. Update Supabase model_versions: set old to 'archived', previous to 'production'
  4. Log ERR entry in Section 17
  5. Do not retrain until root cause is identified

Rollback time: ~3-5 minutes (Render restart + HF Hub download)
```

### 8.7 Free Tier Operational Constraints & Mitigations
```
RENDER COLD START (~30s after 15min idle):
  Mitigation 1: Batch workflow warm-up step (Section 8.2) — automated
  Mitigation 2: React frontend "Connecting..." spinner
  Mitigation 3 (optional): cron-job.org free plan pings /health every 14 min
                            → keeps Render awake at all times, zero cost

SUPABASE 500MB LIMIT:
  5,600 customers × 20 batch runs × ~500 bytes/row = ~56MB — well within limit
  Drop features_snapshot JSONB for batch runs; keep only shap_top_reasons

GITHUB ACTIONS 2,000 MIN/MONTH:
  Batch scoring: ~15 min × 1.5 runs/month = ~23 min
  CI/CD: ~5 min × 10 pushes/month = ~50 min
  Total estimated: ~73 min/month — well within free tier

HUGGINGFACE MODEL SIZE:
  XGBoost on 5,600 rows: typically 1–15MB — downloads in < 2 seconds on Render
```

---

## 9. PHASE 3 — APPLICATION LAYER

### 9.1 User Authentication Flow
```
USERS: Two roles — Customer and Admin (Analyst / Business Stakeholder)

CUSTOMER ROLE:
  - Can register and log in
  - Inputs their own data via onboarding form
  - Can view their own churn risk score (optional — for demo purposes)

ADMIN / ANALYST ROLE:
  - Can view the full KPI dashboard
  - Can see all customer churn risk scores and reasons
  - Can trigger manual batch scoring
  - Can view monitoring reports

AUTH IMPLEMENTATION:
  - JWT tokens (access: 24h, refresh: 7 days)
  - Stored in httpOnly cookies (NOT localStorage — security)
  - FastAPI dependency injection for route protection
  - Password hashed with bcrypt (cost factor >= 12)
```

### 9.2 Customer Registration — Feature Split
```
CRITICAL DESIGN DECISION (from your Phase 3 diagram):
When a new customer registers, they input SOME fields.
The system COMPUTES the remaining fields required by the ML model.

USER-INPUTTED AT REGISTRATION:
  - Full name, email, password
  - City / location (maps to CityTier)
  - Preferred device (maps to NumberOfDeviceRegistered proxy)
  - Satisfaction rating (if collected via onboarding survey)
  - Payment preference (maps to PreferredPaymentMode)
  - Product category interest (maps to PreferredOrderCat)

SYSTEM-COMPUTED (derived, not user-input):
  - Tenure:                 computed as (today - registration_date) in months
  - DaySinceLastOrder:      computed from order history in DB
  - OrderCount:             computed from orders table
  - CouponUsed:             computed from coupon redemption table
  - OrderAmountHikeFromlastYear: computed from order history
  - CashbackAmount:         computed from cashback transaction table
  - Complain:               flag set by customer support system

KEY RULE: The ML model's feature engineering pipeline must receive
a COMPLETE feature vector. Backend computes missing fields before 
passing to the prediction service. Frontend never sends raw ML features.
```

### 9.3 Frontend Pages
```
PUBLIC:
  /login               — Login form
  /register            — Customer registration + onboarding

PROTECTED (Customer):
  /dashboard           — Personal churn risk indicator (optional feature)
  /profile             — View/update their information

PROTECTED (Admin):
  /admin/overview      — KPI dashboard
                         - Overall churn rate %
                         - Customer count by risk tier (HIGH/MEDIUM/LOW)
                         - Trend chart: churn risk over last 3 batch cycles
                         - Top 20 at-risk customers table with reason codes
  /admin/customers     — Full customer list with churn scores + filters
  /admin/model         — Current model version, metrics, last trained date
  /admin/monitoring    — Latest Evidently drift report viewer
  /admin/settings      — Trigger manual batch scoring, set alert thresholds
```

### 9.4 Database Schema
```sql
-- Users / Customers
CREATE TABLE customers (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email                VARCHAR(255) UNIQUE NOT NULL,
  password_hash        VARCHAR(255) NOT NULL,
  role                 VARCHAR(20) DEFAULT 'customer',  -- customer | admin
  
  -- User-inputted at registration
  full_name            VARCHAR(255),
  city_tier            INT,                    -- 1, 2, or 3
  preferred_payment    VARCHAR(50),
  preferred_order_cat  VARCHAR(50),
  
  -- System-computed (updated by background jobs)
  tenure_months        INT DEFAULT 0,
  day_since_last_order INT,
  order_count          INT DEFAULT 0,
  complain_flag        BOOLEAN DEFAULT FALSE,
  satisfaction_score   FLOAT,
  
  -- Metadata
  registered_at        TIMESTAMP DEFAULT NOW(),
  last_updated         TIMESTAMP DEFAULT NOW()
);

-- Prediction audit trail
CREATE TABLE predictions (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id      UUID REFERENCES customers(id),
  run_type         VARCHAR(20),              -- 'batch' | 'realtime'
  churn_probability FLOAT NOT NULL,
  churn_label      BOOLEAN NOT NULL,
  risk_tier        VARCHAR(10),             -- 'HIGH' | 'MEDIUM' | 'LOW'
  threshold_used   FLOAT NOT NULL,
  model_version    VARCHAR(50) NOT NULL,
  shap_top_reasons JSONB,                  -- [{"feature": "Tenure", "impact": 0.22}]
  features_snapshot JSONB,                 -- full feature vector used (for audit)
  ground_truth     BOOLEAN,               -- filled in later if known
  batch_run_id     UUID,
  predicted_at     TIMESTAMP DEFAULT NOW()
);

-- Batch run log
CREATE TABLE batch_runs (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  triggered_by     VARCHAR(50),           -- 'scheduler' | 'manual' | 'drift_alert'
  customers_scored INT,
  high_risk_count  INT,
  medium_risk_count INT,
  low_risk_count   INT,
  model_version    VARCHAR(50),
  drift_report_path VARCHAR(500),
  status           VARCHAR(20),           -- 'running' | 'completed' | 'failed'
  started_at       TIMESTAMP DEFAULT NOW(),
  completed_at     TIMESTAMP
);

-- Model version registry (mirrors MLflow)
CREATE TABLE model_versions (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  version          VARCHAR(50) UNIQUE NOT NULL,
  status           VARCHAR(20) DEFAULT 'staging',  -- staging | production | archived
  auc_roc          FLOAT,
  recall_score     FLOAT,
  precision_score  FLOAT,
  business_cost    FLOAT,
  threshold_used   FLOAT,
  train_date       DATE,
  mlflow_run_id    VARCHAR(255),
  artifact_path    VARCHAR(500),
  notes            TEXT,
  promoted_at      TIMESTAMP,
  created_at       TIMESTAMP DEFAULT NOW()
);
```

### 9.5 API Contract — Key Endpoints
```python
# POST /api/v1/predict/single
# REQUEST:
{
  "customer_id": "uuid-string"    # System fetches features from DB
  # OR provide features directly:
  "features": {
    "Tenure": 5,
    "CityTier": 2,
    "SatisfactionScore": 2,
    ...
  }
}

# RESPONSE:
{
  "customer_id": "uuid-string",
  "churn_probability": 0.78,
  "churn_label": true,
  "risk_tier": "HIGH",
  "threshold": 0.65,
  "top_reasons": [
    {"feature": "Tenure", "impact": 0.22, "direction": "increases_churn"},
    {"feature": "Complain", "impact": 0.18, "direction": "increases_churn"},
    {"feature": "SatisfactionScore", "impact": -0.12, "direction": "reduces_churn"}
  ],
  "model_version": "1.2.0",
  "prediction_id": "pred_abc123",
  "predicted_at": "2026-03-04T14:30:00Z",
  "latency_ms": 87
}

# POST /api/v1/predict/batch
# Triggers async scoring of all active customers
# Returns: { "batch_run_id": "uuid", "status": "running", "customers_queued": 5200 }

# GET /api/v1/admin/overview
# Returns: KPI summary for dashboard
{
  "as_of": "2026-03-04",
  "total_customers": 5200,
  "high_risk_count": 312,
  "high_risk_pct": 6.0,
  "medium_risk_count": 780,
  "churn_rate_trend": [...],
  "top_at_risk": [...],       # top 20 customers
  "last_batch_run": "2026-02-13"
}
```

---

## 10. DEPLOYMENT ARCHITECTURE — DECISION LOG

> This section records WHAT was decided, WHY, and what was explicitly rejected.
> Future LLM sessions must read this before suggesting infrastructure changes.

### 10.1 Decisions Locked (Do Not Revisit Without Strong Reason)

```
DECISION 1: Training stays local. Cloud training rejected.
  CHOSEN:   Local machine (laptop/workstation)
  REJECTED: AWS SageMaker, Google Vertex AI, Colab
  REASON:   Dataset is 5,600 rows. Training takes < 2 minutes on any modern
            laptop. Cloud training adds cost, credential complexity, and 
            obscures your engineering work behind a managed service. The 
            portfolio value is in the pipeline code, not the training hardware.

DECISION 2: HuggingFace Hub as model registry. S3/GCS rejected.
  CHOSEN:   HuggingFace Hub (free private repo)
  REJECTED: AWS S3, Google Cloud Storage, MLflow cloud
  REASON:   Free tier, no credentials setup beyond HF_TOKEN, native versioning
            via git-lfs, hf_hub_download() works identically in local dev and 
            on Render. Model files for this dataset will be < 20MB — well within
            Hub limits. Added portfolio bonus: visible on your HF profile.

DECISION 3: GitHub Actions cron for batch scheduling. Airflow/Prefect rejected.
  CHOSEN:   GitHub Actions scheduled workflow
  REJECTED: Apache Airflow, Prefect, Celery Beat, cron on cloud VM
  REASON:   Airflow/Prefect require always-on infrastructure — not free.
            GitHub Actions is already in the repo, free at 2,000 min/month,
            provides a visible audit log of every batch run in the repo UI,
            supports manual trigger (workflow_dispatch), and sends email alerts
            on failure automatically. For one job every 20 days, it is the 
            optimal tool. The workflow YAML IS the scheduler — no extra service.

DECISION 4: Supabase for database. Managed PostgreSQL on Render rejected.
  CHOSEN:   Supabase (free tier: 500MB, pgBouncer pooling included)
  REJECTED: Render PostgreSQL (free tier deleted after 90 days),
            PlanetScale (MySQL, not Postgres), Railway (limited free tier)
  REASON:   Supabase free tier is persistent (not 90-day expiry), includes
            pgBouncer connection pooling (critical for Render serverless 
            behavior), has a built-in dashboard for data inspection, and 
            provides a REST API as a fallback. DATABASE_URL port 6543 
            (pooler) must be used — not 5432 — for Render compatibility.

DECISION 5: Render for API hosting. Railway/Fly.io/AWS rejected.
  CHOSEN:   Render (free tier: 750h/mo, Docker deploy)
  REJECTED: Railway (limited free tier hours), Fly.io (requires credit card),
            AWS EC2 (cost), Heroku (no longer free)
  REASON:   Render auto-deploys from Docker Hub on webhook, no credit card
            required, native Docker support, environment variable UI.
            Cold start (~30s after 15min idle) is mitigated by the warm-up
            step in the GitHub Actions batch workflow.
  CRITICAL: Use port 10000 in Dockerfile (Render default). Set 
            RENDER_EXTERNAL_URL env var automatically provided by Render.

DECISION 6: React Query polling over WebSockets/SSE (for MVP).
  CHOSEN:   React Query refetchInterval: 30s
  REJECTED: WebSockets (incompatible with Render free sleep behavior),
            SSE (adds FastAPI complexity, save for stretch goal)
  REASON:   Churn is a slow-moving phenomenon — 30-second data freshness
            is more than sufficient. Polling is simple, debuggable, and 
            works perfectly with Render's sleep/wake cycle. If the API is
            sleeping, React Query retries gracefully. WebSockets would fail.

DECISION 7: GitHub Issues for drift alerts. Slack/email rejected for MVP.
  CHOSEN:   GitHub Issues created by Actions script on drift detection
  REJECTED: Slack webhook (requires Slack workspace), SendGrid (setup overhead)
  REASON:   GitHub Issues are visible in the repo, require zero external setup,
            are linked to the specific workflow run that triggered them, and
            can be labeled and assigned. For a portfolio project, they are
            more impressive than a Slack message because an interviewer can
            see the alert history in the public repo.
```

### 10.2 Free Tier Limits — Monitor These
```
SERVICE         LIMIT                 USAGE ESTIMATE          ACTION IF EXCEEDED
──────────────────────────────────────────────────────────────────────────────
Render          750h/mo (~31 days)    ~750h (always on)       Will sleep when idle
                                                               Acceptable — batch wakes it
Supabase        500MB storage         ~50MB for 5,600 rows    Safe margin
                500MB bandwidth/day   Minimal (internal API)  Safe
HuggingFace     —                     ~20MB model artifacts   No limit concern
GitHub Actions  2,000 min/mo free     ~5 min per batch run    18 runs = 90min — very safe
                                      ~2 min per CI run       50 CI runs = 100min
Vercel          100 deploys/day       —                       No limit concern
```

### 10.3 GitHub Secrets Required
```
Set these in: GitHub repo → Settings → Secrets and variables → Actions

Secret Name             Value Source                    Used In
───────────────────────────────────────────────────────────────────────
RENDER_API_URL          Render service URL              batch_score.yml
ADMIN_API_KEY           Generate: openssl rand -hex 32  batch_score.yml + API
DOCKERHUB_USERNAME      Your Docker Hub username        ci_cd.yml
DOCKERHUB_TOKEN         Docker Hub → Account → Security ci_cd.yml
RENDER_DEPLOY_HOOK_URL  Render → Service → Settings     ci_cd.yml
HF_TOKEN                HuggingFace → Settings → Tokens register.py (local)
                        (also set in Render env vars)   API startup (cloud)
```

---

## 11. ENVIRONMENT & CONFIGURATION

### 10.1 Environment Variables
```bash
# ── ML / Model ─────────────────────────────────────────────────────────────
MODEL_VERSION=1.0.0                # loaded from HF Hub on startup
PREDICTION_THRESHOLD=0.65
RISK_TIER_HIGH=0.70
RISK_TIER_MEDIUM=0.45
BATCH_CHUNK_SIZE=500               # rows per scoring chunk — memory safety
MIN_TENURE_TO_SCORE=1              # months — exclude brand-new customers

# ── HuggingFace Hub (model artifact registry) ──────────────────────────────
HF_TOKEN=hf_xxxxxxxxxxxxxxxxxxxx   # write token for register.py, read for API
HF_REPO_ID=your-hf-username/churnguard-model-registry

# ── MLflow (local training only) ───────────────────────────────────────────
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_EXPERIMENT_NAME=churn-prediction-ecommerce
# NOTE: MLflow is NEVER deployed to cloud — local tracking server only

# ── Database (Supabase) ────────────────────────────────────────────────────
DATABASE_URL=postgresql://postgres.[project-ref]:[password]@aws-0-[region].pooler.supabase.com:6543/postgres
# Use pooler connection string (port 6543) for Render compatibility

# ── API ────────────────────────────────────────────────────────────────────
API_HOST=0.0.0.0
API_PORT=8000
JWT_SECRET=your-secret-min-32-chars-never-commit-this
JWT_ALGORITHM=HS256
JWT_EXPIRY_HOURS=24
ADMIN_API_KEY=long-random-string-for-batch-workflow-auth

# ── Batch Scoring ──────────────────────────────────────────────────────────
BATCH_CHUNK_SIZE=500
# Schedule is in GitHub Actions cron — not in .env

# ── Monitoring ─────────────────────────────────────────────────────────────
DRIFT_PSI_THRESHOLD=0.20
DRIFT_PSI_CRITICAL=0.25
# NOTE: Alerts go to GitHub Issues (via Actions) — no Slack/email needed

# ── Frontend (Vercel) ──────────────────────────────────────────────────────
VITE_API_URL=https://churnguard.onrender.com
VITE_APP_NAME=ChurnGuard

# ── Render deployment ──────────────────────────────────────────────────────
# Set in Render dashboard environment variables (not in .env file):
# DATABASE_URL, HF_TOKEN, HF_REPO_ID, JWT_SECRET, ADMIN_API_KEY,
# MODEL_VERSION, PREDICTION_THRESHOLD, all RISK_TIER_* vars

# GitHub Secrets required:
# ADMIN_API_KEY — used by batch_score.yml to authenticate
# DOCKERHUB_USERNAME + DOCKERHUB_TOKEN — for image push
# RENDER_DEPLOY_HOOK_URL — webhook from Render service settings
```

### 10.2 Local Dev Setup
```bash
# 1. Clone
git clone https://github.com/[username]/churn-prediction-microservice.git
cd churn-prediction-microservice

# 2. Python environment
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 3. Frontend
cd frontend && npm install && cd ..

# 4. Environment
cp .env.example .env
# Fill in: HF_TOKEN, DATABASE_URL (Supabase), JWT_SECRET, ADMIN_API_KEY

# 5. Start LOCAL infrastructure (PostgreSQL for dev + MLflow)
docker-compose up -d
# Starts: postgres:5432 (local dev DB) + mlflow:5000 (local tracking)
# Note: Supabase is used in production; local Postgres mirrors the schema

# 6. Initialize DB schema + seed CSV data
python scripts/init_db.py          # creates all tables
python scripts/seed_customers.py --csv data/raw/ecommerce_churn.csv

# 7. Phase 1 — EDA
jupyter notebook notebooks/01_eda.ipynb

# 8. Phase 2 — Run full ML pipeline
make pipeline                       # features → train → evaluate → register

# 9. Phase 2 — Start serving API locally
make serve                          # uvicorn on :8000

# 10. Phase 3 — Start frontend
cd frontend && npm run dev          # Vite on :5173

# Individual commands:
# Features:     python src/pipeline/features.py
# Train:        python src/ml/train.py --config config/model_config.yaml
# Tune:         python src/ml/tune.py --algorithm xgboost --trials 50
# Evaluate:     python src/ml/evaluate.py --run-id [mlflow-run-id]
# Register:     python src/ml/register.py --version v1.0.0
#               (pushes artifacts to HuggingFace Hub)
# Rollback:     python src/ml/register.py --rollback --version v0.9.0
# Batch score:  python src/pipeline/batch_score.py  (local test run)
# MLflow UI:    http://localhost:5000
# API docs:     http://localhost:8000/docs  (FastAPI Swagger)
```

---

## 12. CURRENT STATE & PROGRESS

> **Last Updated:** 2026-03-04 | **Current Phase:** Phase 1

### Completed
```
[+] Project brainstorming and architecture design
[+] Code_Prompt_Proj.md master template created
[+] churn_prediction.md project document created (this file)
[+] Kaggle dataset identified and selected
```

### In Progress
```
[~] Phase 1: Data download and initial EDA
[~] Project repository setup and directory structure
```

### Not Started
```
[ ] Phase 1: Full EDA notebook (01_eda.ipynb)
[ ] Phase 1: Auto-profiling report (Ydata/Sweetviz)
[ ] Phase 1: EDA findings documentation
[ ] Phase 2: Feature engineering pipeline (features.py)
[ ] Phase 2: Model training and MLflow integration (train.py)
[ ] Phase 2: Optuna hyperparameter tuning (tune.py)
[ ] Phase 2: Model evaluation and champion selection (evaluate.py)
[ ] Phase 2: FastAPI prediction microservice
[ ] Phase 2: Docker containerization
[ ] Phase 2: Evidently monitoring integration
[ ] Phase 3: PostgreSQL schema setup and migrations
[ ] Phase 3: FastAPI auth endpoints (register, login, JWT)
[ ] Phase 3: React frontend (Vite + Tailwind)
[ ] Phase 3: KPI admin dashboard
[ ] Phase 3: Batch scoring scheduler (20-day cycle)
[ ] Phase 3: GitHub Actions CI/CD pipeline
[ ] Phase 3: Cloud deployment
```

---

## 13. TASK BACKLOG

| Priority  | ID    | Task                                                         | Phase | Effort | Status      |
|-----------|-------|--------------------------------------------------------------|-------|--------|-------------|
| P0 CRIT  | T-001 | Set up repo, directory structure, virtual env, .env.example  | All   | 1h     | Not Started |
| P0 CRIT  | T-002 | Download Kaggle dataset, initial load + shape check           | 1     | 0.5h   | Not Started |
| P0 CRIT  | T-003 | Full EDA notebook: nulls, distributions, churn drivers        | 1     | 4h     | Not Started |
| P0 CRIT  | T-004 | Document EDA findings, update Section 6.3 of this file        | 1     | 1h     | Not Started |
| P0 CRIT  | T-005 | Feature engineering pipeline (src/pipeline/features.py)       | 2     | 5h     | Not Started |
| P0 CRIT  | T-006 | Model training with MLflow tracking (src/ml/train.py)         | 2     | 4h     | Not Started |
| P0 CRIT  | T-007 | Optuna hyperparameter tuning (src/ml/tune.py)                 | 2     | 3h     | Not Started |
| P0 CRIT  | T-008 | Model evaluation, champion selection, SHAP plots              | 2     | 3h     | Not Started |
| P1 HIGH  | T-009 | FastAPI prediction microservice + Docker                       | 2     | 5h     | Not Started |
| P1 HIGH  | T-010 | Evidently drift monitoring integration                        | 2     | 3h     | Not Started |
| P1 HIGH  | T-011 | PostgreSQL schema + Alembic migrations                        | 3     | 2h     | Not Started |
| P1 HIGH  | T-012 | FastAPI auth: register, login, JWT (src/api/routes/auth.py)   | 3     | 3h     | Not Started |
| P1 HIGH  | T-013 | Batch scoring scheduler + 20-day cron                         | 3     | 4h     | Not Started |
| P1 HIGH  | T-014 | React frontend scaffold (Vite + Tailwind + React Query)        | 3     | 3h     | Not Started |
| P1 HIGH  | T-015 | Admin KPI dashboard (churn %, risk tiers, top-20 table)       | 3     | 5h     | Not Started |
| P2 MED  | T-016 | Customer registration form + system-computed feature logic     | 3     | 3h     | Not Started |
| P2 MED  | T-017 | GitHub Actions CI/CD pipeline                                 | 3     | 3h     | Not Started |
| P2 MED  | T-018 | Unit + integration test suite (pytest)                        | All   | 5h     | Not Started |
| P2 MED  | T-019 | Cloud deployment (Render / Fly.io / AWS)                      | 3     | 4h     | Not Started |
| P3 LOW  | T-020 | Monitoring dashboard (Grafana or Streamlit)                   | 3     | 4h     | Not Started |
| P3 LOW  | T-021 | Automated retraining trigger (Airflow/Prefect DAG)            | 3     | 6h     | Not Started |
| P3 LOW  | T-022 | README.md + architecture diagrams for portfolio               | All   | 2h     | Not Started |

---

## 14. ACTIVE TASK — WHAT I NEED RIGHT NOW

> UPDATE THIS SECTION BEFORE EVERY LLM SESSION. This is what the model acts on.

### 13.1 Current Task
```
Task ID:     T-001 (starting here)
Task Title:  Project repository scaffolding

Definition of Done:
  Git repo initialized with full directory structure from Section 18.
  Virtual environment configured. requirements.txt with all dependencies
  from Section 4. .env.example with all variables from Section 10.
  docker-compose.yml starting PostgreSQL + MLflow.
  Makefile with shortcuts: pipeline, train, serve, test.
  Empty placeholder files in every src/ subdirectory.
```

### 13.2 Relevant Files
```
This is setup — create from scratch following Section 18 (Directory Map)
and Section 10 (Configuration).
```

### 13.3 Exact Request
```
[UPDATE THIS BEFORE EVERY SESSION — see template below]

"I am starting Task T-[ID]: [task title].

Context: [1–3 sentences of relevant context]

I need you to:
1. [Specific deliverable 1]
2. [Specific deliverable 2]
3. [Specific deliverable 3]

Current blocker: [describe any issue if applicable]
Reference sections: [e.g., Section 7.1 for feature engineering details]"
```

### 13.4 Task-Specific Constraints
```
[FILL IN per task from Section 15 + any task-specific additions]
```

### 13.5 Desired Output
```
[ ] Complete file(s) — full working code
[ ] Diff / patch only
[ ] Plan first, then code on approval
[ ] Code with detailed inline comments
[ ] Architecture decision only
```

---

## 15. CODE STYLE & CONVENTIONS

### 14.1 Python
```python
# Formatter:  Black, line length: 88
# Linter:     Ruff
# Type hints: Required on ALL function signatures
# Docstrings: Google-style
# Imports:    isort (profile=black)
# Tests:      pytest

def engineer_financial_features(
    df: pd.DataFrame,
    tenure_col: str = "Tenure",
    charge_col: str = "MonthlyCharges",
) -> pd.DataFrame:
    """Derive financial stress signal features from raw charge and tenure data.

    Args:
        df: Input DataFrame with at minimum tenure and charge columns.
        tenure_col: Column name for customer tenure (months).
        charge_col: Column name for monthly charges (USD).

    Returns:
        DataFrame with additional financial signal columns appended.
        Original columns are preserved.

    Raises:
        ValueError: If required columns are missing from the DataFrame.
    """
    required = [tenure_col, charge_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    ...
```

### 14.2 TypeScript / React
```typescript
// Formatter:  Prettier (tabWidth: 2, singleQuote: true)
// Linter:     ESLint + TypeScript strict mode
// Components: Functional only, no class components
// State:      useState / React Query — no Redux for this project size

interface CustomerRiskCard {
  customerId: string;
  fullName: string;
  churnProbability: number;
  riskTier: 'HIGH' | 'MEDIUM' | 'LOW';
  topReasons: Array<{ feature: string; impact: number; direction: string }>;
  predictedAt: string;
}
```

### 14.3 General Conventions
```
Git commits:    Conventional Commits — feat:, fix:, chore:, refactor:, docs:, ml:
Branch naming:  feature/T-001-repo-scaffold | fix/ERR-001-description | ml/T-006-training
Config:         ALL thresholds, paths, hyperparameters in config/ YAML or .env
Logging:        Python logging module with structured logs — no bare print() in src/
Notebooks:      Exploratory ONLY — no business logic in notebooks
                (notebooks can call src/ modules but not the other way around)
```

---

## 16. CONSTRAINTS & HARD RULES

> LLM Instruction: These are absolute. Never violate them. Never suggest a workaround
> that would break one of these unless I explicitly ask to revisit it.

```
DATA INTEGRITY:
[NO]  NEVER use random splits — use stratified splits preserving churn class balance
[NO]  NEVER fit any preprocessor (scaler, encoder, imputer) on validation or test data
[NO]  NEVER include the CustomerID column as a model feature
[NO]  NEVER include features derived from future information (leakage)
[NO]  NEVER compare two models evaluated on different test sets
[YES] ALWAYS lock the test set before any model training begins

ML CORRECTNESS:
[NO]  NEVER use accuracy as the primary metric (class imbalance makes it misleading)
[NO]  NEVER default to threshold 0.5 — always tune threshold on the PR curve
[NO]  NEVER skip class imbalance handling (scale_pos_weight or class_weight minimum)
[YES] ALWAYS log every experiment in MLflow — no untracked training runs
[YES] ALWAYS save preprocessor.pkl AND feature_names.json together with every model
[YES] ALWAYS enforce feature column order at inference time (ERR-001 prevention)

CODE QUALITY:
[NO]  NEVER hardcode the prediction threshold, file paths, or model versions in code
[NO]  NEVER use bare except: — catch specific exceptions
[NO]  NEVER commit the .env file — it is gitignored
[NO]  NEVER load the full customer table into memory — chunk batch scoring

PRIVACY & SECURITY:
[NO]  NEVER log raw customer PII in application logs — use customer_id only
[NO]  NEVER store passwords in plaintext — always bcrypt with cost >= 12
[NO]  NEVER store JWT tokens in localStorage — use httpOnly cookies

DEPLOYMENT:
[YES] ALWAYS validate new model before promoting to production (Section 8.2)
[YES] ALWAYS maintain previous model version for rollback (Section 8.4)
[YES] ALWAYS log every prediction to the predictions table (audit + ground truth)
[YES] ALWAYS version model artifacts — never overwrite production model files in place
```

---

## 17. LLM SESSION PROMPTS

> Copy the relevant block, fill the placeholder, paste at the top of your session.
> Append the full churn_prediction.md below it.

### Claude (Opus / Sonnet)
```
You are a senior ML engineer and MLOps architect.
Project: Customer Churn Prediction Microservice ("CHURNGUARD")
Domain:  E-Commerce / Retail SaaS — binary churn classification
Current Phase: [FILL: Phase 1 / Phase 2 / Phase 3]
Active task: [FILL: one line from Section 13]

Read the full project document below before responding.
Focus ONLY on Section 13 (Active Task) unless I redirect you.

Working style:
- Skip preamble — deliver the solution directly.
- Complete, runnable code unless I ask for design first.
- If my approach has a flaw, say so briefly then deliver the corrected version.
- Ask ONE clarifying question if needed before writing — don't assume.
- Section 15 hard rules are non-negotiable.

[PASTE FULL churn_prediction.md BELOW]
```

### GPT-4o / o3
```
You are a senior ML engineer specializing in MLOps and tabular classification.
Project: Customer Churn Prediction Microservice for an E-Commerce SaaS company.
Full specification: churn_prediction.md (follows below).

Rules:
- Implement ONLY what Section 13 requests.
- Use exact library versions from Section 4.
- Apply Python conventions from Section 14.
- Obey ALL constraints in Section 15 — especially ML correctness rules.
- For any data task: check Section 15 ML correctness rules before writing code.
- Return complete, runnable code.

[PASTE FULL churn_prediction.md BELOW]
```

### LLaMA / Mistral / Local
```
### SYSTEM:
Expert ML engineer. E-Commerce churn prediction project. 
Read the full document below. Execute ONLY Section 13.
Libraries: Section 4. Style: Section 14. Rules: Section 15 — absolute.
Output complete Python code. No prose between code blocks.

### DOCUMENT:
[PASTE FULL churn_prediction.md BELOW]

### REQUEST:
[1-3 sentence plain-language summary of the specific ask]
```

---

## 18. ERROR LOG & DEBUGGING HISTORY

> Log EVERY bug. Review before each session. This is your project's long-term memory.

```
TEMPLATE — copy per error:
=========================================================
ID:           ERR-XXX
Date:         YYYY-MM-DD
Phase:        [1 / 2 / 3]
Component:    [EDA / Feature Eng / Training / API / Frontend / DB / Infra]
Severity:     [Critical / High / Medium / Low]
Status:       [Open / Resolved / Won't Fix]

Error:        [Exact error message or stack trace]
Context:      [What were you doing? What input caused it?]
Root Cause:   [WHY it happened — fill even after resolving]
Fix:          [What changed and in which file]
LLM Help:     [Claude / GPT / None]
=========================================================
```

---

## 19. DIRECTORY MAP

```
churn-prediction-microservice/
|
+-- src/
|   +-- pipeline/
|   |   +-- ingest.py              # Load CSV, validate schema, write to raw
|   |   +-- validate.py            # Data quality checks, go/no-go gate
|   |   +-- features.py            # Feature engineering pipeline (THE key file)
|   |   +-- batch_score.py         # 20-day batch scoring runner
|   |
|   +-- ml/
|   |   +-- train.py               # Multi-algorithm training + MLflow logging
|   |   +-- tune.py                # Optuna Bayesian hyperparameter optimization
|   |   +-- evaluate.py            # Metrics, SHAP plots, eval report generation
|   |   +-- validate_model.py      # Champion selection gate (pass/fail)
|   |   +-- predict.py             # Core prediction logic (API + batch share this)
|   |   +-- register.py            # Promote candidate to MLflow Production tag
|   |
|   +-- api/
|   |   +-- main.py                # FastAPI app + router registration + startup
|   |   +-- dependencies.py        # JWT auth dependency, DB session, model loader
|   |   +-- routes/
|   |   |   +-- auth.py            # /register, /login, /refresh
|   |   |   +-- customers.py       # CRUD customer endpoints
|   |   |   +-- predict.py         # /predict/single, /predict/batch
|   |   |   +-- admin.py           # /overview, /monitoring, /retrain
|   |   +-- schemas/
|   |   |   +-- customer.py        # Pydantic Customer models
|   |   |   +-- prediction.py      # Request/Response models for prediction
|   |   |   +-- auth.py            # Login/Register schemas
|   |   +-- middleware/
|   |       +-- logging.py         # Request logging middleware
|   |       +-- rate_limit.py      # Basic rate limiting
|   |
|   +-- monitoring/
|   |   +-- drift_detector.py      # Evidently PSI checks per feature
|   |   +-- performance_tracker.py # Ground truth labeling + AUC tracking
|   |   +-- generate_report.py     # HTML report generation (Evidently)
|   |   +-- alerts.py              # Slack / email alert dispatch
|   |
|   +-- db/
|       +-- models.py              # SQLAlchemy ORM table definitions
|       +-- session.py             # Async DB session factory
|       +-- crud/
|       |   +-- customers.py       # Customer DB operations
|       |   +-- predictions.py     # Prediction record operations
|       +-- migrations/            # Alembic migration files
|
+-- frontend/                      # React Vite app
|   +-- src/
|   |   +-- pages/
|   |   |   +-- Login.tsx
|   |   |   +-- Register.tsx
|   |   |   +-- AdminDashboard.tsx  # KPI overview (main stakeholder view)
|   |   |   +-- CustomerList.tsx    # Full list with risk scores
|   |   |   +-- ModelHealth.tsx     # Model version + metrics
|   |   |   +-- Monitoring.tsx      # Drift report viewer
|   |   +-- components/
|   |   |   +-- RiskBadge.tsx       # HIGH/MEDIUM/LOW tier visual
|   |   |   +-- ChurnScoreCard.tsx  # Per-customer card with reasons
|   |   |   +-- TrendChart.tsx      # Churn rate over time
|   |   |   +-- TopRiskTable.tsx    # Top-20 at-risk customers table
|   |   +-- hooks/
|   |   |   +-- useAuth.ts
|   |   |   +-- usePredictions.ts
|   |   +-- lib/
|   |   |   +-- api.ts              # Axios instance + interceptors
|   |   |   +-- auth.ts             # Token management
|   |   +-- types/
|   |       +-- prediction.ts
|   |       +-- customer.ts
|   +-- package.json
|   +-- tailwind.config.ts
|   +-- vite.config.ts
|
+-- notebooks/
|   +-- 01_eda.ipynb                # Phase 1 EDA (exploration only)
|   +-- 02_feature_experiments.ipynb # Feature engineering experiments
|   +-- 03_model_comparison.ipynb   # Visualize MLflow run comparisons
|
+-- data/
|   +-- raw/
|   |   +-- ecommerce_churn.csv     # Original Kaggle dataset (committed)
|   +-- processed/                  # gitignored — generated by pipeline
|   +-- samples/
|       +-- sample_100.csv          # 100 rows for local testing
|
+-- models/                         # gitignored
|   +-- production/                 # champion model artifacts
|   +-- staging/                    # candidate model artifacts
|   +-- artifacts/                  # preprocessor.pkl, feature_names.json
|
+-- config/
|   +-- model_config.yaml           # Hyperparameters + feature config
|   +-- pipeline_config.yaml        # Scheduling + batch settings
|   +-- feature_config.yaml         # Feature list, types, imputation rules
|   +-- thresholds.yaml             # Decision thresholds per model version
|
+-- reports/
|   +-- eda/                        # Phase 1 EDA HTML reports
|   +-- evaluation/                 # Model eval HTML reports per run
|   +-- monitoring/                 # Evidently drift reports by date
|
+-- tests/
|   +-- unit/
|   |   +-- test_features.py        # Feature engineering unit tests
|   |   +-- test_predict.py         # Prediction logic unit tests
|   |   +-- test_schemas.py         # Pydantic schema validation tests
|   +-- integration/
|       +-- test_api.py             # FastAPI endpoint integration tests
|       +-- test_pipeline.py        # End-to-end pipeline tests on sample data
|
+-- scripts/
|   +-- init_db.py                  # Create all tables (run once)
|   +-- seed_customers.py           # Load CSV data into PostgreSQL
|   +-- download_data.py            # Kaggle API download script
|
+-- docker/
|   +-- Dockerfile.api              # Production API image
|   +-- Dockerfile.mlflow           # MLflow tracking server image
|
+-- .github/workflows/
|   +-- ci.yml                      # Test + lint on every PR
|   +-- cd.yml                      # Build + deploy on merge to main
|
+-- .env.example                    # Template — safe to commit
+-- .env                            # GITIGNORED
+-- .gitignore
+-- docker-compose.yml              # Local dev: PostgreSQL + MLflow + API
+-- Makefile                        # Shortcuts: make train, serve, test, lint
+-- requirements.txt                # Pinned Python dependencies
+-- pyproject.toml                  # Black + Ruff + isort config
+-- README.md                       # Portfolio README with architecture diagram
+-- churn_prediction.md             # <- THIS FILE
```

---

## 20. GLOSSARY

| Term                 | Definition                                                                         |
|----------------------|------------------------------------------------------------------------------------|
| Churn                | A customer who has stopped purchasing / cancelled / gone inactive beyond the window |
| Churn Probability    | Model output: float 0–1 representing likelihood of churn                            |
| Risk Tier            | Bucketed label: HIGH (>=0.70) / MEDIUM (0.45–0.69) / LOW (<0.45)                   |
| Champion Model       | The model currently tagged "Production" in MLflow Registry and serving predictions  |
| Candidate Model      | A newly trained model undergoing evaluation vs. champion                            |
| SHAP Reason Codes    | Top-N features contributing to a specific customer's churn prediction               |
| PSI                  | Population Stability Index — quantifies feature distribution shift (drift detector) |
| Temporal Split       | Train/test split ordered by time — required for time-dependent features             |
| Stratified Split     | Split that preserves the class balance ratio across train/val/test folds            |
| Preprocessor.pkl     | Fitted sklearn Pipeline (imputer + encoder + scaler) — must travel with model      |
| Feature Leakage      | When training data contains information from the future — inflates metrics falsely  |
| Business Cost Score  | Custom metric: (FN * $150) + (FP * $15) — see Section 3.6                         |
| Batch Scoring Cycle  | Every 20 days: score ALL active customers and generate stakeholder report           |
| Ground Truth         | Confirmed actual churn outcome — available after observation window closes          |
| Operating Threshold  | The probability cutoff selected from PR curve (NOT defaulted to 0.5)               |
| System-Computed      | Features calculated by the backend (tenure, recency) — NOT user-inputted            |

---

## 21. CHANGELOG

```
## [1.1.0] — 2026-03-06  ← CURRENT
### Added
- Section 10: Deployment Architecture Decision Log (7 decisions, full rationale)
- Free tier limits monitoring table with usage estimates per service
- GitHub Secrets reference table (what each secret is, where to get it)
- Full GitHub Actions batch_score.yml YAML — warm-up loop, polling,
  completion check, drift-triggered GitHub Issue creation
- New customer scoring gate enforced (tenure_months >= 1, order_count >= 1)
- SSE upgrade path documented as Phase 3 stretch goal
- reference_distribution.pkl added to training artifacts
  (required for Evidently drift checks in production)
- category_churn_rates.json added to pipeline artifacts
  (PreferedOrderCat → churn risk ordinal mapping, computed in EDA)

### Changed
- Feature engineering (Section 3.3) fully rewritten — now maps to ACTUAL
  dataset columns from Phase1_Feature_Reference.pdf. All 5 groups defined
  with exact formulas, rationale, and watch-outs:
    Group 1 (Financial Stress):   discount_dependency_ratio,
                                   cashback_per_order,
                                   order_value_declining_flag
    Group 2 (Service Dependency): platform_embeddedness_score,
                                   is_cod_user, address_per_tenure_ratio
    Group 3 (Engagement Decay):   recency_risk_score,
                                   order_frequency_normalized,
                                   app_engagement_tier, is_mobile_only
    Group 4 (Contract Risk):      tenure_segment,
                                   silent_dissatisfaction_flag,
                                   complaint_satisfaction_interaction,
                                   warehouse_distance_risk
    Group 5 (Category Volatility):category_volatility_score

## [1.0.0] — 2026-03-04
### Added
- Initial project specification across 3 phases
- ML problem fully framed: features, metrics, business cost function
- Tech stack locked and documented
- Database schema: customers, predictions, batch_runs, model_versions
- API contract defined for all endpoints
- Directory structure and task backlog T-001 through T-022
- Dataset confirmed: Kaggle E-Commerce Churn (single source, no merging)

### Architecture Decisions Locked
- API hosting:          Render (free tier, Docker-based)
- Database:             Supabase PostgreSQL (free, 500MB, port 6543 pooler)
- Frontend:             Vercel (free)
- Model artifact store: HuggingFace Hub (free private repo)
- Batch scheduling:     GitHub Actions cron (not Airflow/Prefect/Celery)
- MLflow:               Local only — never deployed to cloud
- Dashboard refresh:    React Query polling 30s — NOT WebSockets
- Cold start mitigation:warm-up step in batch_score.yml (3 retries)
- New customer gate:    No scoring until tenure_months >= 1, order_count >= 1
- Drift alerts:         GitHub Issues (not Slack) — zero setup, public audit trail
- CI/CD:                GitHub Actions — test + Docker build + Render deploy

## [Unreleased — update this section after every work session]
### Added
-
### Changed
-
### Fixed
-
### Decisions Made
-
```

---

*This document is the single source of truth for the CHURNGUARD project.*  
*Update Sections 11, 13, and 20 after every work session.*  
*Section 6.3 must be updated after Phase 1 EDA is complete.*  
*This file travels with the project — paste it into any LLM to restore full context instantly.*
