# ChurnGuard — Complete Project Structure

> **Version:** 1.1.0 | **Author:** Marco Hanna | **Stack:** Python · FastAPI · XGBoost · Supabase · HuggingFace Hub · Render · Netlify

---

## Overview

ChurnGuard is a production-grade customer churn prediction microservice with a complete ML lifecycle:
raw data → feature engineering → model training → API serving → batch scoring → drift monitoring → stakeholder dashboard.

---

## Directory Tree

```
Customer-Churn-Prediction-Microservice/
│
├── src/                                   ← all application source code
│   ├── api/                               ← FastAPI application
│   │   ├── main.py
│   │   ├── config.py
│   │   ├── dependencies.py
│   │   ├── models/
│   │   │   ├── customer.py
│   │   │   └── responses.py
│   │   ├── routes/
│   │   │   ├── admin.py
│   │   │   ├── customers.py
│   │   │   ├── events.py
│   │   │   └── predictions.py
│   │   ├── services/
│   │   │   ├── customer_service.py
│   │   │   ├── feature_service.py
│   │   │   ├── model_service.py
│   │   │   ├── prediction_service.py
│   │   │   └── sse_service.py
│   │   └── validators/
│   │       └── data_integrity.py
│   │
│   ├── ml/                                ← ML pipeline
│   │   ├── eda/                           ← Stage 3.1: Exploratory Data Analysis
│   │   │   ├── eda_orchestrator.py
│   │   │   ├── analyzers/
│   │   │   │   ├── base.py
│   │   │   │   ├── categorical.py
│   │   │   │   └── numeric.py
│   │   │   ├── dataset/
│   │   │   │   ├── loader.py
│   │   │   │   └── sanity.py
│   │   │   ├── global_analysis/
│   │   │   │   ├── class_balance.py
│   │   │   │   ├── cohorts.py
│   │   │   │   └── correlation.py
│   │   │   ├── reporting/
│   │   │   │   ├── manifest_writer.py
│   │   │   │   ├── narrator.py
│   │   │   │   └── plot_saver.py
│   │   │   ├── utils/
│   │   │   │   └── signal_logger.py
│   │   │   └── reports/                   ← generated EDA outputs
│   │   │       ├── summary.json
│   │   │       ├── sanity.json
│   │   │       ├── class_balance.json
│   │   │       ├── correlation.json
│   │   │       ├── cohorts.json
│   │   │       ├── category_churn_rates.json
│   │   │       ├── features/              ← 18 per-feature JSON reports
│   │   │       └── figures/               ← 41 PNG plots
│   │   │
│   │   ├── feature_eng/                   ← Stage 3.2: Feature Engineering
│   │   │   ├── engineer.py
│   │   │   ├── pipeline.py
│   │   │   ├── orchestrator.py
│   │   │   └── reports/                   ← generated feature eng outputs
│   │   │       ├── feature_engineering_config.json
│   │   │       ├── feature_summary.json
│   │   │       └── split_summary.json
│   │   │
│   │   ├── training/                      ← Stage 3.3: Model Training
│   │   │   ├── trainer.py
│   │   │   ├── evaluator.py
│   │   │   ├── mlflow_tracker.py
│   │   │   └── register.py
│   │   │
│   │   └── batch/                         ← Stage 3.5: Batch Scoring
│   │       └── batch_scorer.py
│   │
│   ├── pipeline/                          ← data ingestion pipeline
│   │   ├── stage1_clean.py
│   │   └── stage2_seed.py
│   │
│   └── monitoring/                        ← placeholder for future monitoring
│
├── scripts/                               ← CLI entry points (run these directly)
│   ├── run_api.py
│   ├── run_eda.py
│   ├── run_feature_engineering.py
│   ├── run_training.py
│   ├── run_batch_scoring.py
│   ├── run_cleaning.py
│   ├── run_seeding.py
│   ├── run_db_connection.py
│   ├── run_db_diagnostics.py
│   ├── run_diagnosis.py
│   ├── run_add_full_name.py
│   └── run_seed_batch_run.py
│
├── database/
│   ├── connection.py
│   └── schema.sql
│
├── data/
│   ├── raw/
│   │   └── E_Commerce_Dataset.xlsx        ← original Kaggle dataset
│   ├── cleaned/
│   │   └── ecommerce_churn_clean.csv      ← after stage1_clean.py
│   └── splits/                            ← after run_feature_engineering.py
│       ├── X_train.npy  (3,940 × 38)
│       ├── X_val.npy    (845 × 38)
│       ├── X_test.npy   (845 × 38)
│       ├── y_train.npy  (3,940,)
│       ├── y_val.npy    (845,)
│       └── y_test.npy   (845,)
│
├── models/
│   ├── artifacts/                         ← shared artifacts (preprocessor + config)
│   │   ├── preprocessor.pkl
│   │   ├── feature_names.json
│   │   ├── feature_engineering_config.json
│   │   └── reference_distribution.pkl
│   └── staging/                           ← per-version model artifacts
│       ├── v1.0.0/                        ← XGBoost (production)
│       │   ├── model.pkl
│       │   ├── metrics.json
│       │   ├── shap_summary.json
│       │   ├── thresholds.yaml
│       │   └── run_manifest.json
│       └── v1.1.0/                        ← LightGBM (staging)
│           ├── model.pkl
│           ├── metrics.json
│           ├── shap_summary.json
│           ├── thresholds.yaml
│           └── run_manifest.json
│
├── mlruns/                                ← MLflow local experiment tracking
│   └── 404204742456234020/                ← experiment ID
│       └── {run_id}/                      ← one folder per training run
│           ├── artifacts/
│           ├── metrics/
│           ├── params/
│           └── tags/
│
├── config/
│   ├── cleaning_config.py
│   ├── db_config.py
│   └── db_connection_config.py
│
├── logs/                                  ← auto-generated run logs
│   ├── eda_*.log
│   ├── feature_eng_*.log
│   ├── training_*.log
│   └── batch_*.log
│
├── tests/
│   └── test_db_connection.py
│
├── Dockerfile                             ← production Docker image for Render
├── render.yaml                            ← Render deployment config
├── requirements.txt                       ← pinned Python dependencies
├── .env                                   ← secrets (gitignored)
└── README.md
```

---

## File Purpose — Full Reference

### `src/api/`

| File | Purpose |
|------|---------|
| `main.py` | FastAPI application factory. Creates the app, mounts all routers, registers CORS and exception handlers. `startup_event` queries Supabase for the production model version, downloads 5 artifacts from HF Hub, and loads them into `app.state.model_service` — once, at startup. |
| `config.py` | Pydantic settings class. Reads `DATABASE_URL`, `ADMIN_API_KEY`, `CORS_ORIGINS`, `HUGGINGFACE_TOKEN` from `.env`. Cached with `@lru_cache`. |
| `dependencies.py` | FastAPI dependency injection. `get_db()` — yields a connected `DatabaseConnection`, closes after request. `verify_admin()` — checks `X-Admin-Key` header, raises HTTP 403 if wrong. |

#### `src/api/models/`

| File | Purpose |
|------|---------|
| `customer.py` | Pydantic request models for customer registration. Validates field types, value ranges, and required fields at the HTTP boundary before any DB write. |
| `responses.py` | Pydantic response models. `HealthResponse`, `ErrorResponse`, and other shared response shapes used across multiple routes. |

#### `src/api/routes/`

| File | Purpose |
|------|---------|
| `customers.py` | `POST /api/v1/customers/register` — accepts the registration form, validates with Pydantic, calls `customer_service` to insert into Supabase, calls `feature_service` to create the initial features row. Returns the created customer. |
| `admin.py` | Protected admin endpoints (`X-Admin-Key` required). Dashboard data: overview stats, at-risk customer list, churn trend by batch run, drift reports. Reads from Supabase views (`v_current_risk_summary`, `v_top_at_risk`). |
| `events.py` | `GET /api/v1/admin/events` — Server-Sent Events (SSE) stream. Pushes real-time events to the React ops dashboard. Keepalive ping every 30 seconds. |
| `predictions.py` | `POST /api/v1/predict/single` — scores one customer end-to-end: fetch → engineer → preprocess → predict → SHAP → write. `GET /api/v1/predict/health` — returns loaded model version, threshold, feature count. |

#### `src/api/services/`

| File | Purpose |
|------|---------|
| `customer_service.py` | Inserts a new customer row into Supabase. Handles duplicate detection and returns the created customer UUID. |
| `feature_service.py` | Creates the initial `customer_features` row immediately after customer registration. All behavioral features start at zero. Also has `recompute_all_tenures()` — called by the daily cron endpoint to update `tenure_months` for all active customers. |
| `model_service.py` | `ModelLoader` class. Downloads all model artifacts from HF Hub on API startup and holds them in memory. Provides `predict_proba()`, `apply_threshold()` (probability → HIGH/MEDIUM/LOW), and `shap_top3()` (per-prediction SHAP reason codes). Runs a smoke test on load. |
| `prediction_service.py` | `PredictionService` class. Full inference pipeline for one customer: fetch raw features from Supabase → convert `Decimal` → `float` → impute nulls → `FeatureEngineer.transform()` → `preprocessor.transform()` → `predict_proba()` → threshold → SHAP → INSERT to predictions table. |
| `sse_service.py` | Manages the SSE event queue. Pushes events (customer registered, batch complete, drift alert) to connected dashboard clients. |

#### `src/api/validators/`

| File | Purpose |
|------|---------|
| `data_integrity.py` | Cross-field business rule validation beyond Pydantic type checks. Example: `city_tier` must be 1, 2, or 3. Called by the customer registration route before DB insert. |

---

### `src/ml/eda/`

| File | Purpose |
|------|---------|
| `eda_orchestrator.py` | Wires all EDA components together in execution order: load → sanity → categorical → numeric → class balance → correlation → cohorts → reports. Called by `scripts/run_eda.py`. |
| `analyzers/base.py` | Abstract base class `BaseAnalyzer`. All analyzers inherit from this. Defines the `analyze()` interface and shared signal logging. |
| `analyzers/categorical.py` | `CategoricalAnalyzer`. For each categorical feature: frequency table, churn rate per category, Cramér's V association with churn label. Outputs per-feature JSON. |
| `analyzers/numeric.py` | `NumericAnalyzer`. For each numeric feature: distribution stats, Cohen's d effect size vs churn label, box plots, histograms. Outputs per-feature JSON. |
| `dataset/loader.py` | `TrainingDataLoader`. Queries Supabase (`customers ⋈ customer_features`) to load the full 5,630-row dataset for EDA. Returns a pandas DataFrame. |
| `dataset/sanity.py` | `SanityChecker`. Validates the loaded DataFrame: checks row count, null rates per column, expected column presence, churn rate plausibility. |
| `global_analysis/class_balance.py` | `ClassBalanceAnalyzer`. Computes imbalance ratio (4.94:1), churner/retained counts, percentage breakdown. Used to justify `scale_pos_weight` decision. |
| `global_analysis/cohorts.py` | `CohortAnalyzer`. Groups customers by tenure bucket, city tier, payment mode and computes churn rate per cohort. Validates feature group hypotheses. |
| `global_analysis/correlation.py` | `CorrelationAnalyzer`. Pearson correlation matrix across all numeric features. Identifies multicollinearity. |
| `reporting/manifest_writer.py` | `ManifestWriter`. Writes `figures/manifest.json` — a catalogue of all generated plots for the dashboard to read. |
| `reporting/narrator.py` | `StatisticalNarrator`. Converts raw analysis numbers into human-readable summary sentences for the dashboard. |
| `reporting/plot_saver.py` | `PlotSaver`. Saves matplotlib figures to `reports/figures/` with consistent naming convention. |
| `utils/signal_logger.py` | `SignalLogger` base class. All major classes inherit from this. Provides `_signal()` for structured `INFO` log lines and `_warn()` for `WARNING` lines — the consistent log format you see throughout the pipeline. |

#### `src/ml/eda/reports/`

| File/Folder | Purpose |
|-------------|---------|
| `summary.json` | Overall EDA summary: row count, churn rate, imbalance ratio, top predictors. |
| `sanity.json` | Sanity check results: null rates per column, row count, schema validation. |
| `class_balance.json` | Churned vs retained counts, imbalance ratio, percentage breakdown. |
| `correlation.json` | Pearson correlation matrix between all numeric features. |
| `cohorts.json` | Churn rate by tenure bucket, city tier, payment mode. |
| `category_churn_rates.json` | Churn rate per category for every categorical feature. Used to inform target encoding decisions. |
| `features/{feature}.json` | Per-feature analysis: stats, Cohen's d / Cramér's V, distribution data, churn rate by value. 18 files total. |
| `figures/` | 41 PNG plots: histograms, box plots, stacked bar charts, correlation heatmap, class balance donut, cohort charts. |

---

### `src/ml/feature_eng/`

| File | Purpose |
|------|---------|
| `engineer.py` | `FeatureEngineer` class. The core of feature engineering. `fit(df_train)` — learns parameters from training data only (target encoding rates, log-median, IQR cap). `transform(df)` — applies 5 feature groups to create 17 engineered features. `from_config(path)` — reconstructs a fitted engineer from `feature_engineering_config.json` for use at inference time without training data. `save_config(path)` — serialises all fitted parameters to JSON. |
| `pipeline.py` | `PreprocessorBuilder` class. Builds and fits the sklearn `Pipeline`: `StandardScaler` on numeric features, `OneHotEncoder` on low-V categorical features. Fitted on `X_train` only. Saves `preprocessor.pkl`. |
| `orchestrator.py` | `FeatureEngineeringOrchestrator`. Wires `FeatureEngineer` and `PreprocessorBuilder` together: load splits → fit engineer → transform → fit preprocessor → transform → save all artifacts + numpy splits. Called by `scripts/run_feature_engineering.py`. |

#### `src/ml/feature_eng/reports/`

| File | Purpose |
|------|---------|
| `feature_engineering_config.json` | All fitted parameters: target encoding rates per category, warehouse log-median, address IQR cap, ordinal Cramér's V threshold, one-hot column names. This is the training ↔ inference contract — the API downloads this from HF Hub to reconstruct `FeatureEngineer` without retraining. |
| `feature_summary.json` | Summary of all 38 features: which group they belong to, encoding method, source column. |
| `split_summary.json` | Train/val/test row counts, churn rates per split, random seed. |

---

### `src/ml/training/`

| File | Purpose |
|------|---------|
| `trainer.py` | Three classes. `OptunaTuner` — runs 20 Optuna trials with 5-fold CV inside the training fold to find optimal hyperparameters (objective: maximise recall while precision ≥ 0.60). `XGBoostTrainer` — trains XGBoost on the full training fold with the best params, saves `model.pkl` and `run_manifest.json`, computes and saves `reference_distribution.pkl` for PSI drift monitoring. `LightGBMTrainer` — same pattern for LightGBM. |
| `evaluator.py` | Three classes. `ThresholdSelector` — scans the PR curve on the val set to find the lowest threshold where recall ≥ 0.80. Saves `thresholds.yaml`. `ModelEvaluator` — computes AUC, recall, precision, F1, Brier, confusion matrix, business cost score on both val and test sets. Saves `metrics.json`. `SHAPExplainer` — computes global SHAP feature importance using `TreeExplainer` on 500 training samples. Saves `shap_summary.json`. |
| `mlflow_tracker.py` | Two classes. `MLflowExperiment` — creates or retrieves the local MLflow experiment. `MLflowRunLogger` — context manager that logs hyperparameters, metrics, and artifact files to one MLflow run. Stores `run_id` which is saved to Supabase `model_versions.mlflow_run_id`. |
| `register.py` | Three classes. `HFHubPublisher` — uploads 9 artifact files per model version to HuggingFace Hub. `SupabaseModelRegistry` — INSERTs a row to `model_versions` table and promotes a version to production (archive old → promote new in one transaction using `get_connection()` + `conn.commit()`). `ModelRegistrar` — orchestrates: verify manifest → HF Hub push → Supabase insert → promote if production. |

---

### `src/ml/batch/`

| File | Purpose |
|------|---------|
| `batch_scorer.py` | Two classes. `BatchScorer` — scores all eligible customers (tenure ≥ 1 month, order_count ≥ 1, not scored in 18 days). Downloads production model from HF Hub, runs `FeatureEngineer.from_config()` + `preprocessor.transform()` + `model.predict_proba()` on all eligible customers in chunks of 500. Writes predictions to Supabase. `create_batch_run()` inserts the `batch_runs` row BEFORE predictions (FK constraint). `complete_batch_run()` updates it to `status='completed'` after. `DriftMonitor` — computes PSI (Population Stability Index) per feature between the current batch and the reference distribution. PSI > 0.20 fires a drift alert and writes to `drift_reports`. |

---

### `src/pipeline/`

| File | Purpose |
|------|---------|
| `stage1_clean.py` | Cleans the raw Kaggle Excel file: standardises column names, handles missing values, removes duplicates, maps categorical values to consistent strings. Outputs `data/cleaned/ecommerce_churn_clean.csv`. |
| `stage2_seed.py` | Seeds the cleaned CSV into Supabase: inserts rows into `customers` and `customer_features` tables. Sets `kaggle_churn_label` (the ground truth). Sets `features_source='kaggle_seed'`. |

---

### `database/`

| File | Purpose |
|------|---------|
| `connection.py` | `DatabaseConnection` class. Manages a `psycopg2` connection pool to Supabase (port 6543 pgBouncer). `execute_query()` — SELECT only, always calls `fetchall()`. `get_connection()` — context manager for INSERT/UPDATE/DELETE; caller must call `conn.commit()`. The distinction between these two is critical — never use `execute_query()` for writes. |
| `schema.sql` | Full Supabase schema: all table definitions, enums (`risk_tier_enum`, `batch_status_enum`), foreign keys, indexes, computed columns (`duration_seconds` in `batch_runs`), and views (`v_current_risk_summary`, `v_top_at_risk`). |

---

### `scripts/`

| File | Purpose |
|------|---------|
| `run_api.py` | Starts the FastAPI server with Uvicorn. Development mode: `--reload`. Production: `--host 0.0.0.0 --port $PORT`. |
| `run_eda.py` | CLI entry for Stage 3.1. Runs the full EDA pipeline, saves all reports to `src/ml/eda/reports/`, then copies and git-pushes to the reports repo for Netlify. |
| `run_feature_engineering.py` | CLI entry for Stage 3.2. Runs feature engineering, saves 38-feature numpy splits, saves `preprocessor.pkl` and `feature_engineering_config.json`. |
| `run_training.py` | CLI entry for Stage 3.3. Trains XGBoost (production) then LightGBM (staging). Optuna tuning → full training → threshold selection → evaluation → SHAP → MLflow → HF Hub → Supabase. Ends with `MLReportsSyncer` pushing training artifacts to the reports repo. |
| `run_batch_scoring.py` | CLI entry for Stage 3.5. Creates `batch_runs` row → loads eligible customers → scores all → writes predictions → PSI drift check → updates `batch_runs` to completed. Exit code 2 if drift detected. |
| `run_cleaning.py` | CLI wrapper for `stage1_clean.py`. |
| `run_seeding.py` | CLI wrapper for `stage2_seed.py`. |
| `run_db_connection.py` | Quick connectivity test — connects to Supabase and runs `SELECT 1`. |
| `run_db_diagnostics.py` | Prints row counts for all tables. Used to verify seeding completed correctly. |
| `run_diagnosis.py` | Prints full environment info: Python version, installed packages, env vars (redacted). |
| `run_add_full_name.py` | One-time migration script — adds `full_name` column from first + last name fields. |
| `run_seed_batch_run.py` | Seeds a synthetic `batch_runs` row for dashboard testing before real batch scoring ran. |

---

### `models/`

| File/Folder | Purpose |
|-------------|---------|
| `artifacts/preprocessor.pkl` | Fitted sklearn Pipeline (StandardScaler + OneHotEncoder). Must travel with the model — the same object that transformed `X_train` must transform inference data. |
| `artifacts/feature_names.json` | Ordered list of all 38 feature names. The contract between training and inference — if this changes, the model must be retrained. |
| `artifacts/feature_engineering_config.json` | Fitted parameters for `FeatureEngineer.from_config()`: target encoding rates, log-median, IQR cap. Downloaded by the API at startup. |
| `artifacts/reference_distribution.pkl` | Statistics of the training feature distribution (mean, std, percentiles per feature). Used by `DriftMonitor` for PSI comparison against each batch. |
| `staging/v1.0.0/model.pkl` | Trained XGBClassifier. The production model. |
| `staging/v1.0.0/metrics.json` | Val + test evaluation metrics: AUC=0.9901, Recall=0.9930, Precision=0.5975, confusion matrix, business cost score. |
| `staging/v1.0.0/shap_summary.json` | Global SHAP feature importance: all 38 features ranked by mean absolute SHAP value. Top-3: tenure_months, address_per_tenure_ratio, complain_risk. |
| `staging/v1.0.0/thresholds.yaml` | Decision threshold (0.2097) and risk tier bounds (HIGH ≥ 0.260, MEDIUM ≥ 0.060). Selected from PR curve on val set. |
| `staging/v1.0.0/run_manifest.json` | Reproducibility record: algorithm, trained_at, git SHA, data hash, hyperparameters, seeds, environment versions. |
| `staging/v1.1.0/` | Same 5 files for LightGBM (staging/challenger). |

---

### `data/`

| File/Folder | Purpose |
|-------------|---------|
| `raw/E_Commerce_Dataset.xlsx` | Original Kaggle e-commerce churn dataset. 5,630 rows, 20 columns, 16.84% churn rate. Never modified. |
| `cleaned/ecommerce_churn_clean.csv` | After `stage1_clean.py`: standardised column names, nulls handled, categorical values normalised. |
| `splits/X_train.npy` | 3,940 × 38 training feature matrix. Generated by `run_feature_engineering.py`. All 38 features after engineering + preprocessing. |
| `splits/X_val.npy` | 845 × 38. Used for threshold selection and intermediate evaluation. |
| `splits/X_test.npy` | 845 × 38. Locked test set. Touched once at final evaluation only. |
| `splits/y_*.npy` | Binary churn labels (0=retained, 1=churned) for each split. |

---

### `mlruns/`

Local MLflow tracking server. One subfolder per experiment run. Each run stores:
- `params/` — hyperparameters logged by `MLflowRunLogger.log_params()`
- `metrics/` — evaluation metrics logged by `MLflowRunLogger.log_metrics()`
- `artifacts/` — copies of model.pkl, metrics.json, thresholds.yaml, shap_summary.json, run_manifest.json, best_hyperparameters.json
- `meta.yaml` — run metadata (start time, status, run name)

View all runs side by side: `mlflow ui --port 5000`

---

### Root files

| File | Purpose |
|------|---------|
| `Dockerfile` | Production Docker image for Render. Base: `python:3.11-slim`. Copies source, installs `requirements.txt`, runs `uvicorn src.api.main:app --host 0.0.0.0 --port $PORT`. |
| `render.yaml` | Render deployment config: service name, region, build command, start command, environment variable references. |
| `requirements.txt` | All Python dependencies with pinned versions: fastapi, uvicorn, psycopg2-binary, xgboost, lightgbm, optuna, mlflow, shap, scikit-learn, huggingface-hub, pandas, numpy, pyyaml, python-dotenv. |
| `.env` | Secrets (gitignored): `DATABASE_URL`, `ADMIN_API_KEY`, `HUGGINGFACE_TOKEN`, `CORS_ORIGINS`. |
| `README.md` | Project specification document (churn_prediction.md). Single source of truth for all architectural decisions. |

---

## External Services

| Service | Role | What lives there |
|---------|------|-----------------|
| **Supabase** | PostgreSQL database | customers, customer_features, predictions, batch_runs, model_versions, drift_reports, views |
| **HuggingFace Hub** | Model artifact storage | `marcohanna90/churnguard-model-registry/v1.0.0/` and `v1.1.0/` — 9 files each |
| **Render** | API hosting | FastAPI Docker container, auto-deploys on git push |
| **Vercel** | Ops dashboard hosting | React frontend — customer entry form + operations dashboard |
| **Netlify** | ML dashboard hosting | Static `index.html` — 5-tab portfolio dashboard reading from reports repo |
| **GitHub** | Reports repo | `Customer-Churn-Prediction-Microservice-Reports` — EDA plots, feature eng reports, ML training reports |
| **MLflow** | Experiment tracking | Local only — `mlruns/` folder, never deployed |

---

## Run Order (full pipeline from scratch)

```bash
# 1. Clean raw data
python scripts/run_cleaning.py

# 2. Seed into Supabase
python scripts/run_seeding.py

# 3. EDA + publish reports
python scripts/run_eda.py

# 4. Feature engineering
python scripts/run_feature_engineering.py

# 5. Train models + register
python scripts/run_training.py

# 6. Start API
python scripts/run_api.py

# 7. Batch scoring (first run)
python scripts/run_batch_scoring.py
```

---

## Key Architectural Decisions

| Decision | What | Why |
|----------|------|-----|
| Fit on train only | `preprocessor.fit()` and `engineer.fit()` called on `X_train` only | Prevents data leakage from val/test into scaling and encoding parameters |
| No SMOTE | `scale_pos_weight=4.94` instead | SMOTE creates synthetic minority samples; scale_pos_weight reweights the loss function — safer and simpler for tree models |
| Target encoding threshold | Cramér's V ≥ 0.15 → ordinal encoding | Data-driven decision from EDA; V < 0.15 means categories have weak churn signal → one-hot instead |
| Threshold from PR curve | `ThresholdSelector` scans val set | Business goal is recall maximisation; default 0.50 threshold misses too many churners |
| Test set policy | Touched once, at final evaluation | Prevents optimistic metric inflation from repeated evaluation |
| HF Hub for artifacts | Not local filesystem | Artifacts must be accessible by the Render API container at runtime; local disk is ephemeral on Render |
| Supabase as model registry | `model_versions` table with `status` enum | Simple, auditable, queryable — no MLflow server needed in production |
| Plain INSERT for predictions | No `ON CONFLICT` | No UNIQUE constraint on `customer_id` — full audit trail, every prediction is a new row |
| `get_connection()` for writes | Not `execute_query()` | `execute_query()` never calls `conn.commit()` — writes would silently roll back |
| `create_batch_run` before predictions | FK constraint | `predictions.batch_run_id → batch_runs.id` — batch_runs row must exist first |
| Scoring gate §5.6 | tenure ≥ 1 month AND order_count ≥ 1 | Model never trained on customers with zero interaction history — predictions would be meaningless |
