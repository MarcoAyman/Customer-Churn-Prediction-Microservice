# ChurnGuard — Customer Churn Prediction Microservice

**Production-grade MLOps system for e-commerce customer churn prediction.**

> Kaggle data → domain-driven feature engineering → XGBoost training with Optuna → FastAPI prediction service → automated 20-day batch scoring → PSI drift monitoring → React stakeholder dashboard.
> All on zero-cost infrastructure.

---

## Live Links

| Service | URL |
|---------|-----|
| **ML Dashboard** (Netlify) | https://earnest-wisp-d60961.netlify.app |
| **Ops Dashboard** (Vercel) | *(React frontend — customer entry + risk dashboard)* |
| **API Docs** (Render) | *(FastAPI /docs — Swagger UI)* |
| **HF Hub** (Model Registry) | https://huggingface.co/marcohanna90/churnguard-model-registry |
| **Reports Repo** (GitHub) | https://github.com/MarcoAyman/Customer-Churn-Prediction-Microservice-Reports |

---

## Results

| Metric | Value |
|--------|-------|
| **AUC-ROC** (test set) | 0.9901 |
| **Recall** (test set) | 0.9930 |
| **Precision** (test set) | 0.5975 |
| **F1 Score** | 0.7460 |
| **Decision threshold** | 0.2097 (from PR curve on val set) |
| **Customers scored** | 5,121 in 218s |
| **Business cost reduction** | 93% vs baseline (predict all retained) |

XGBoost wins on every metric vs LightGBM challenger (AUC: 0.9901 vs 0.9890, Precision: 0.5975 vs 0.5640).

Top 3 SHAP features: `tenure_months` · `address_per_tenure_ratio` · `complain_risk`

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  LOCAL (training only)                                          │
│                                                                 │
│  run_training.py                                                │
│    ├── OptunaTuner (20 trials · 5-fold CV)                      │
│    ├── XGBoostTrainer + LightGBMTrainer                         │
│    ├── ThresholdSelector (PR curve on val)                      │
│    ├── SHAPExplainer                                            │
│    ├── MLflowRunLogger → mlruns/ (local)                        │
│    └── ModelRegistrar                                           │
│          ├── HFHubPublisher → HuggingFace Hub                   │
│          └── SupabaseModelRegistry → model_versions table       │
└─────────────────────────────────────────────────────────────────┘
                              │
                    9 artifacts per version
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  CLOUD (production)                                             │
│                                                                 │
│  ┌─────────────────┐     ┌──────────────────────────────────┐  │
│  │  Render          │     │  Supabase PostgreSQL             │  │
│  │  FastAPI Docker  │────▶│  customers                       │  │
│  │                  │     │  customer_features               │  │
│  │  /predict/single │     │  predictions                     │  │
│  │  /admin/*        │     │  batch_runs                      │  │
│  │  /events (SSE)   │     │  model_versions                  │  │
│  └─────────────────┘     │  drift_reports                   │  │
│          ▲               └──────────────────────────────────┘  │
│          │                                                      │
│  ┌───────────────┐   ┌──────────────────────────────────────┐  │
│  │ GitHub Actions│   │  Vercel (React)                      │  │
│  │ cron 20 days  │   │  Customer entry form                 │  │
│  │               │   │  Ops dashboard (SSE live feed)       │  │
│  │ batch scoring │   └──────────────────────────────────────┘  │
│  └───────────────┘                                             │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Netlify (static)                                         │  │
│  │  ML Dashboard — 5 tabs:                                   │  │
│  │  Pipeline Flowchart · System Architecture · EDA ·         │  │
│  │  Feature Engineering · Model Registry                     │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

**ML / Data**

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![XGBoost](https://img.shields.io/badge/XGBoost-2.0-orange)
![LightGBM](https://img.shields.io/badge/LightGBM-staging-green)
![Optuna](https://img.shields.io/badge/Optuna-hyperparameter_tuning-purple)
![SHAP](https://img.shields.io/badge/SHAP-explainability-red)
![MLflow](https://img.shields.io/badge/MLflow-experiment_tracking-blue)
![scikit-learn](https://img.shields.io/badge/scikit--learn-preprocessing-orange)

**API / Backend**

![FastAPI](https://img.shields.io/badge/FastAPI-0.111-green?logo=fastapi)
![Docker](https://img.shields.io/badge/Docker-containerized-blue?logo=docker)
![Render](https://img.shields.io/badge/Render-hosted-purple)

**Data / Storage**

![Supabase](https://img.shields.io/badge/Supabase-PostgreSQL-green?logo=supabase)
![HuggingFace](https://img.shields.io/badge/HuggingFace-model_registry-yellow)

**Frontend / Dashboard**

![React](https://img.shields.io/badge/React-ops_dashboard-blue?logo=react)
![Vercel](https://img.shields.io/badge/Vercel-hosted-black?logo=vercel)
![Netlify](https://img.shields.io/badge/Netlify-ML_dashboard-teal?logo=netlify)

**Automation**

![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-batch_cron-blue?logo=github)

---

## Project Structure

```
Customer-Churn-Prediction-Microservice/
│
├── src/
│   ├── api/                    ← FastAPI application
│   │   ├── main.py             ← app factory, startup model loading
│   │   ├── routes/             ← customers, admin, predictions, SSE
│   │   ├── services/           ← model_service, prediction_service, ...
│   │   └── models/             ← Pydantic request/response schemas
│   │
│   └── ml/
│       ├── eda/                ← Stage 3.1: EDA + report generation
│       ├── feature_eng/        ← Stage 3.2: feature engineering pipeline
│       ├── training/           ← Stage 3.3: trainer, evaluator, MLflow, registry
│       └── batch/              ← Stage 3.5: batch scorer + PSI drift monitor
│
├── scripts/                    ← CLI entry points
│   ├── run_eda.py
│   ├── run_feature_engineering.py
│   ├── run_training.py
│   ├── run_batch_scoring.py
│   └── run_api.py
│
├── database/
│   ├── connection.py           ← psycopg2 connection pool (Supabase)
│   └── schema.sql              ← full Supabase schema
│
├── data/
│   ├── raw/                    ← original Kaggle dataset
│   ├── cleaned/                ← after cleaning pipeline
│   └── splits/                 ← X_train/val/test + y_train/val/test (.npy)
│
├── models/
│   ├── artifacts/              ← preprocessor.pkl, feature_names.json, config
│   └── staging/
│       ├── v1.0.0/             ← XGBoost (production): model + metrics + SHAP
│       └── v1.1.0/             ← LightGBM (staging): challenger model
│
├── mlruns/                     ← MLflow local experiment tracking
├── Dockerfile                  ← production image for Render
├── render.yaml                 ← Render deployment config
└── requirements.txt
```

> Full file-by-file documentation: [`PROJECT_STRUCTURE.md`](./PROJECT_STRUCTURE.md)

---

## ML Pipeline

### Data
- **Source:** Kaggle E-Commerce Customer Churn dataset
- **Size:** 5,630 customers · 20 raw features · 16.84% churn rate · 4.94:1 imbalance
- **Split:** Stratified 70/15/15 → train=3,940 / val=845 / test=845

### Feature Engineering (38 features)
Five domain-driven feature groups engineered from raw behavioral signals:

| Group | Features | Signal |
|-------|----------|--------|
| **Financial Stress** | `discount_dependency_ratio`, `cashback_per_order`, `order_value_declining_flag` | Over-reliance on discounts |
| **Service Dependency** | `platform_embeddedness_score`, `address_per_tenure_ratio`, `is_cod_user` | How embedded in the platform |
| **Engagement Decay** | `recency_risk_score`, `order_frequency_normalized`, `app_engagement_tier` | Signs of disengagement |
| **Contract Risk** | `tenure_segment`, `silent_dissatisfaction_flag`, `complaint_satisfaction_interaction` | Churn hazard over time |
| **Category Volatility** | `category_volatility_score` | Shopping pattern instability |

Encoding decisions driven by Cramér's V from EDA:
- **V ≥ 0.15** → target encoding (ordinal risk rates)
- **V < 0.15** → one-hot encoding

### Training
- **Algorithm:** XGBoost (production) + LightGBM (staging/challenger)
- **Tuning:** Optuna — 20 trials · 5-fold CV · objective: maximise recall at precision ≥ 0.60
- **Imbalance:** `scale_pos_weight=4.94` (no SMOTE — safer for tree models)
- **Threshold:** 0.2097 — selected from PR curve on val set, not defaulted to 0.50
- **Tracking:** MLflow local experiment tracking (`mlflow ui --port 5000`)
- **Registry:** HuggingFace Hub (artifacts) + Supabase `model_versions` (status control)

### Inference
- **Single prediction:** POST `/api/v1/predict/single` → full pipeline in <200ms → SHAP top-3 reason codes
- **Batch scoring:** All eligible customers (tenure ≥ 1mo, orders ≥ 1) → 5,121 customers in ~3.5 min
- **Scoring gate:** New customers excluded (§5.6) until they have interaction history
- **Drift monitoring:** PSI per feature after each batch · alert if PSI > 0.20

---

## API Endpoints

```
POST   /api/v1/customers/register      Register a new customer
POST   /api/v1/predict/single          Score one customer (SHAP reasons included)
GET    /api/v1/predict/health          Model version, threshold, feature count
GET    /api/v1/admin/overview          Dashboard stats (requires X-Admin-Key)
GET    /api/v1/admin/at-risk           Top at-risk customers (requires X-Admin-Key)
GET    /api/v1/admin/events            SSE stream for real-time dashboard feed
GET    /api/v1/health                  System health check (DB + model status)
```

**Example prediction response:**
```json
{
  "customer_id": "e5fdab1d-6c26-4e78-a132-8dee49e9b929",
  "probability": 0.8183,
  "risk_tier": "HIGH",
  "will_churn": true,
  "threshold": 0.2097,
  "shap_reasons": [
    {"feature": "address_per_tenure_ratio", "shap_value": 1.15, "direction": "increases_risk"},
    {"feature": "complain_risk",            "shap_value": 1.14, "direction": "increases_risk"},
    {"feature": "marital_risk",             "shap_value": 0.49, "direction": "increases_risk"}
  ],
  "model_version": "v1.0.0",
  "predicted_at": "2026-05-14T08:59:35+00:00",
  "latency_ms": 187
}
```

---

## Running Locally

**Prerequisites:** Python 3.11, Supabase project, HuggingFace account

```bash
# 1. Clone and install
git clone https://github.com/MarcoAyman/Customer-Churn-Prediction-Microservice.git
cd Customer-Churn-Prediction-Microservice
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Fill in: DATABASE_URL, ADMIN_API_KEY, HUGGINGFACE_TOKEN, CORS_ORIGINS

# 3. Seed database
python scripts/run_cleaning.py
python scripts/run_seeding.py

# 4. Run ML pipeline
python scripts/run_eda.py
python scripts/run_feature_engineering.py
python scripts/run_training.py

# 5. Start API
python scripts/run_api.py
# → http://localhost:8000/docs

# 6. Score all customers
python scripts/run_batch_scoring.py
```

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Imbalance handling | `scale_pos_weight` | Safer than SMOTE for tree models — no synthetic samples |
| Threshold selection | PR curve on val set | Business goal is recall maximisation, not accuracy |
| Test set policy | Touched once at final eval | Prevents metric inflation from repeated evaluation |
| Model registry | HF Hub + Supabase | HF Hub for artifact storage · Supabase for status control (production/staging/archived) |
| Prediction storage | Plain INSERT, no UPSERT | No UNIQUE on customer_id — full audit trail per prediction |
| DB write pattern | `get_connection()` + `conn.commit()` | `execute_query()` is SELECT-only, never commits |
| Batch run order | `batch_runs` INSERT before `predictions` | FK constraint: predictions reference batch_run_id |
| Scoring gate | tenure ≥ 1mo AND orders ≥ 1 | Model never trained on zero-interaction profiles |

---

## Database Schema

Six production tables in Supabase:

```
customers          ← registration data + demographics
customer_features  ← behavioral features (updated by background jobs)
predictions        ← full audit trail of every prediction
batch_runs         ← one row per batch scoring execution
model_versions     ← model registry (production/staging/archived)
drift_reports      ← PSI drift alerts when feature distribution shifts
```

Views: `v_current_risk_summary` · `v_top_at_risk`

---

## Author

**Marco Hanna** · ML/AI Engineer
- GitHub: [@MarcoAyman](https://github.com/MarcoAyman)
- Portfolio: [marco-hanna-portfolio.vercel.app](https://marco-hanna-portfolio.vercel.app)
- MSc Data Analytics | ECE Degree | AWS Certified

---

## License

MIT — see [LICENSE](./LICENSE)
