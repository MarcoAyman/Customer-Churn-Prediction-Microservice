# ChurnGuard — Stage 3.1 EDA Module Reading Guide

> **Audience:** Marco (the author) and anyone else who needs to understand this module.
> **Goal:** Read this doc top-to-bottom and you'll know exactly what every file does
> and how they fit together. Takes ~30 minutes.

---

## 1. The big picture (1 paragraph)

You have 5,630 customers in Supabase. This module reads them, analyses every feature from every angle, and writes the results as JSON + PNG files into `src/ml/eda/reports/`. Those files are the input for Stage 3.2 feature engineering and also what the developer dashboard renders. **No ML training happens here — only analysis.**

```
Supabase (customers + customer_features)
        │
        ▼
   SupabaseEDA.run()    ←── one method call does everything
        │
        ▼
src/ml/eda/reports/
   ├── summary.json              ← top-level overview
   ├── sanity.json               ← "is the data healthy?"
   ├── class_balance.json        ← churn vs retained ratio + recommendation
   ├── correlation.json          ← feature-vs-feature redundancy
   ├── cohorts.json              ← churn by tenure / city / payment cohort
   ├── category_churn_rates.json ← feeds Stage 3.2 feature engineering
   ├── features/
   │   └── <col>.json  ×18       ← one deep analysis per feature
   └── figures/
       ├── manifest.json         ← index of all plots
       └── *.png  ×41            ← the actual charts
```

---

## 2. Folder structure — what each folder means

```
src/ml/eda/
│
├── utils/                    ← Shared helpers (currently only SignalLogger)
│   └── signal_logger.py
│
├── dataset/                  ← Getting data OUT of Supabase and verifying it
│   ├── loader.py             ← runs the SQL, returns DataFrame
│   └── sanity.py             ← checks the DataFrame looks correct
│
├── reporting/                ← HOW things get rendered/saved (not WHAT)
│   ├── plot_saver.py         ← styles and saves matplotlib figures
│   ├── narrator.py           ← turns numbers into prose explanations
│   └── manifest_writer.py    ← builds the index of all plots
│
├── analyzers/                ← PER-FEATURE analysis (one feature at a time)
│   ├── base.py               ← abstract base class (the contract)
│   ├── categorical.py        ← for columns like gender, payment_mode
│   └── numeric.py            ← for columns like tenure, order_count
│
├── global_analysis/          ← CROSS-FEATURE / DATASET-level analysis
│   ├── class_balance.py      ← the churn vs retained imbalance
│   ├── correlation.py        ← feature-vs-feature relationships
│   └── cohorts.py            ← churn rate by customer segment
│
└── eda_orchestrator.py       ← THE ENTRY POINT — calls everything above
```

**The structure reflects responsibility, not execution order.** Folders
group classes by *what kind of thing they do*:
- `utils/` — foundational building blocks other classes inherit from
- `dataset/` — the data pipe (SQL → clean DataFrame)
- `reporting/` — tools that save things (plots, JSONs, manifest)
- `analyzers/` — one feature at a time
- `global_analysis/` — whole-dataset relationships
- Top-level `eda_orchestrator.py` — the conductor that uses everything

---

## 3. The reading order (follow this path)

Reading files in alphabetical order or in the order they execute will
confuse you. Read them in **dependency order** — simplest things first,
then the things that build on them.

```
  Phase 1: Foundations  (start here)
     1. utils/signal_logger.py
     2. reporting/narrator.py
     3. reporting/manifest_writer.py

  Phase 2: Plots
     4. reporting/plot_saver.py

  Phase 3: Getting data
     5. dataset/loader.py
     6. dataset/sanity.py

  Phase 4: Per-feature analysis
     7. analyzers/base.py
     8. analyzers/categorical.py
     9. analyzers/numeric.py

  Phase 5: Global analysis
    10. global_analysis/class_balance.py
    11. global_analysis/correlation.py
    12. global_analysis/cohorts.py

  Phase 6: The conductor
    13. eda_orchestrator.py
    14. scripts/run_eda.py    (outside src/ml/eda/)
```

Each file below is walked through in this order.

---

## 4. File-by-file walkthrough

### Phase 1 — Foundations

#### **1. `utils/signal_logger.py`** — the "[ClassName] action → target" logging convention

**What it is:** A tiny base class (~40 lines) that every other class inherits from.

**What it does:** Gives every class a `self._signal(action, target, detail)` method that prints things like:

```
[TrainingDataLoader] Reading from   → Supabase.customers ⋈ customer_features
[SanityChecker]      DONE           → sanity=pass  (churn_rate=16.84%)
```

**Why it exists:** Marco wanted a clear, grep-able log format that shows exactly what every component is doing to external resources (the DB, the filesystem). Instead of 10 different logging styles, one mixin enforces consistency.

**Read it first because:** every other file uses it. Once you understand `_signal()`, the log output from any part of the pipeline makes sense.

**Key API:**
- `self._signal(action, target, detail="")` — prints an info line
- `self._warn(message)` — prints a warning line
- `self.logger` — standard Python logger, in case you need it directly

---

#### **2. `reporting/narrator.py`** — turns numbers into sentences

**What it is:** A class of pure classmethods that take statistical results and return human-readable prose.

**What it does:** Converts raw numbers into explanations like:

Input: `cohens_d = -0.96`
Output: `"Cohen's d = -0.96 — large effect size. Values are higher in retained customers (negative shift)."`

Input: `p = 1e-181, u_stat = 12345`
Output: `"Mann-Whitney U = 12,345. Highly significant (p < 0.0001). This non-parametric test asks whether churn and retained customers have the same distribution for this feature."`

**Why it exists:** The narrator is Marco's learning tool. Every plot in the dashboard has text explaining what it means, all produced here. Keeping narration in ONE place means terminology is consistent — change "moderate" to "medium" in this one file and every feature updates.

**Read it second because:** it has zero dependencies, it's pure string formatting, and it teaches you what the statistical terms mean (bands for Cohen's d, Cramér's V, p-value thresholds).

**Key methods** (all classmethods, all pure):
- `cohens_d(d)` — effect size for numeric features
- `cramers_v(v)` — effect size for categorical features
- `mann_whitney(p, u)` — non-parametric test narration
- `chi_squared(p, stat, dof)` — categorical association test
- `distribution_shape(skew, mean, median)` — describes a histogram in words
- `outliers(count, total, p99, max)` — outlier narration with treatment advice
- `category_churn_rates(rates_dict, overall)` — narrates per-category churn rates
- `boxplot_by_churn(ret_median, chur_median, d)` — narrates the separation visible in a box plot

**The effect-size bands used everywhere:**
```
Cohen's d:   <0.2 negligible | <0.5 small | <0.8 medium | else large
Cramér's V:  <0.1 weak       | <0.3 moderate          | else strong
p-value:     <1e-4 highly sig | <0.01 sig | <0.05 marginal | else n.s.
```

---

#### **3. `reporting/manifest_writer.py`** — the figures index

**What it is:** A ~70-line class that accumulates figure metadata and writes `figures/manifest.json` at the end of the run.

**What it does:** Every time a plot is saved, the analyzer calls `manifest.add(path, caption, feature, plot_type, context)`. At the end, `manifest.write(path)` dumps all entries into a JSON file.

**Why it exists:** The dashboard needs to know what plots exist, their captions, and which feature each one belongs to — without hardcoding filenames. Rename a plot, delete a plot, add a new plot — the dashboard just rereads the manifest.

**Read it third because:** it's small, simple, and you'll see `manifest.add(...)` calls all over the analyzers.

**Key API:**
- `manifest.add(path, caption, feature=None, plot_type=None, context="feature")`
- `manifest.write(output_path)`
- `manifest.count` — how many figures registered

---

### Phase 2 — Plots

#### **4. `reporting/plot_saver.py`** — the matplotlib styling + save logic

**What it is:** A ~110-line class that centralises plot styling and file saving.

**What it does:** Two things:
1. In `__init__`, applies a dark-theme matplotlib rcParams config matching the ChurnGuard dashboard (JetBrains Mono font, #0d1117 background, etc.)
2. Provides `save(fig, name)` which saves a figure as PNG, closes it, and returns the relative path for JSON embedding.

**Why it exists:** Without this, every analyzer would reinvent the same styling code. With this, every plot in the module is visually consistent, and changing the theme means editing one file.

**Read it fourth because:** once you know this exists, every `self.plot_saver.save(fig, name)` call in the analyzers makes sense.

**Key API:**
- `plot_saver.COLORS` — dict of colour tokens (blue, red, green, orange, etc.)
- `plot_saver.churn_palette` — `[green, red]` for churn=0 and churn=1 in that order
- `plot_saver.save(fig, name)` — save + close + return relative path

**Colour convention:**
- Green `#3fb950` = retained customers (churn=0)
- Red `#f85149` = churned customers (churn=1)
- Orange `#e3b341` = reference / baseline lines
- Blue `#58a6ff` = neutral data, distributions
- Purple `#bc8cff` = secondary data (e.g., median line on histogram)

---

### Phase 3 — Getting data

#### **5. `dataset/loader.py`** — TrainingDataLoader

**What it is:** A ~100-line class that runs one SQL query and returns a pandas DataFrame.

**What it does:**
1. Runs a JOIN across `customers` + `customer_features` filtered on `kaggle_churn_label IS NOT NULL`
2. Sources the churn label from `customers.kaggle_churn_label` (the seeded Kaggle ground truth)
3. Normalises Postgres types for downstream use:
   - `NUMERIC` (Decimal) → `float`
   - `BOOLEAN` (complain) → `int (0/1)`
   - `SMALLINT` (churn) → `int`
4. Sets `customer_id` as the DataFrame index (so it's NOT treated as a feature)

**Why it exists:** The contract between "the database" and "the ML pipeline". Every feature the pipeline cares about comes through here in the right dtype. If Marco later renames a column in Supabase, THIS is the only file that needs updating.

**Read it fifth because:** every analyzer downstream consumes a DataFrame — this file shows you exactly what the DataFrame looks like (column names, dtypes).

**Key attributes** (also useful for the orchestrator):
- `CATEGORICAL_COLS` — the 7 columns treated as categorical
- `NUMERIC_COLS` — the 11 columns treated as numeric
- `TARGET_COL` — `"churn"`
- `ID_COL` — `"customer_id"` (used as index)
- `_SQL` — the full parametrised SQL query as a class attribute

**Key method:** `loader.load()` — returns a `pd.DataFrame` indexed by customer_id.

---

#### **6. `dataset/sanity.py`** — SanityChecker

**What it is:** A ~150-line class that inspects the DataFrame just loaded and writes a `sanity.json` report.

**What it does:** Defensive checks (not cleaning — Stage 1 already did that):
- Row count matches expected ~5,630
- No duplicate `customer_id` values
- Zero NULLs in any feature column
- Numeric columns have numeric dtypes
- Target column has exactly `[0, 1]` values
- Computes target distribution (churn rate, counts, imbalance ratio)

**Why it exists:** If Stage 1 ever breaks or the seeding gets out of sync, SanityChecker fails loudly on the first run. Anything downstream can trust that the data is well-formed.

**Read it sixth because:** it's the same pattern as most analyzers (`check()` → dict, `save()` → JSON), but smaller and easier to follow.

**Key methods:**
- `checker.check()` — returns the result dict
- `checker.save(path)` — runs check, writes JSON, logs warnings

**Output JSON structure:**
```json
{
  "rows": 5630,
  "expected_rows": 5630,
  "duplicate_ids": 0,
  "null_counts": {"gender": 0, "tenure_months": 0, ...},
  "target_values": [0, 1],
  "target_distribution": {"churned_count": 948, "churn_rate": 0.1684, ...},
  "status": "pass",
  "warnings": []
}
```

---

### Phase 4 — Per-feature analysis

#### **7. `analyzers/base.py`** — FeatureAnalyzer (the abstract base class)

**What it is:** A ~100-line abstract base class. All per-feature analyzers inherit from it.

**What it does:** Defines the COMMON CONTRACT every feature analyzer follows:
- Takes a feature name, DataFrame, target column, and the three shared services (plot_saver, narrator, manifest)
- Provides `analyze()` which calls the subclass's `_analyze()` and stamps metadata
- Provides `save(output_dir)` which analyzes + writes `<feature_name>.json`
- Provides `_churn_split()` helper that returns `(retained_values, churned_values)` — used by numeric analyzers

**Why it exists:** Without this base class, `CategoricalAnalyzer` and `NumericAnalyzer` would duplicate the save-JSON / stamp-metadata logic. With it, they only implement `_analyze()` and get everything else for free. Also, the orchestrator can loop over features polymorphically — it doesn't care whether each is categorical or numeric.

**Read it seventh because:** once you understand the base, both subclasses are just "the same shape, different statistics inside".

**Key pattern to internalise:**
```python
class FeatureAnalyzer:
    def analyze(self) -> Dict:           # public — common
        result = self._analyze()         # calls subclass
        result["feature_name"] = ...     # stamp metadata
        return result

    def save(self, output_dir) -> Dict:  # public — common
        result = self.analyze()
        # write to <output_dir>/<feature_name>.json
        return result

    def _analyze(self) -> Dict:          # abstract — each subclass implements
        raise NotImplementedError
```

---

#### **8. `analyzers/categorical.py`** — CategoricalAnalyzer

**What it is:** A ~290-line class that does deep analysis on ONE categorical column.

**What it produces for each categorical feature:**
1. **Distribution** — value counts + percentages (pie chart if 2 cats, bar chart if 3+)
2. **Per-category churn** — for every category, how many customers + churn rate inside that category
3. **100% stacked churn plot** — the visual hotspot detector (red bar extending past orange line = hotspot)
4. **Chi-squared test** — tests whether category proportions differ between churn and retained
5. **Cramér's V** — effect size for the association strength
6. **Recommendation** — one of:
   - `engineer_ordinal` (strong association → map each cat to its churn rate in Stage 3.2)
   - `keep_one_hot` (default)
   - Plus a list of hotspot categories (churn rate > HOTSPOT_LIFT × overall)

**Why it exists:** Categorical features need different tools than numeric ones. You can't compute a mean or a histogram on `preferred_payment_mode` — you need contingency tables and chi-squared. This class encapsulates "everything you'd do to understand a categorical column vs the target".

**Read it eighth because:** it's the simpler of the two analyzers (categorical stats are more intuitive than numeric ones).

**Key threshold constants:**
- `PIE_MAX_CATEGORIES = 2` — up to 2 cats → pie, 3+ → horizontal bar
- `HOTSPOT_LIFT = 1.5` — category flagged as hotspot if its churn rate > 1.5× overall
- `MIN_EXPECTED_FREQ = 5` — chi-squared reliability check

**The key plot to understand:** `_plot_stacked_churn()`. Each bar is one category, always reaches 100%, split into green (retained) + red (churned). The orange dashed line sits at the overall retention rate. If any red segment extends past the orange line, that category churns more than average — visible at a glance.

---

#### **9. `analyzers/numeric.py`** — NumericAnalyzer

**What it is:** A ~330-line class that does deep analysis on ONE numeric column.

**What it produces for each numeric feature:**
1. **Summary stats** — mean, median, std, p1/p5/p25/p75/p95/p99, min/max, skewness, kurtosis
2. **Split-by-churn stats** — same stats computed separately for retained vs churned customers
3. **Histogram** — distribution shape with mean + median reference lines
4. **Box plot split by churn** — visual separation diagnostic (median shift, IQR overlap)
5. **Mann-Whitney U test** — non-parametric significance test (used because our data is skewed — t-test would be unreliable)
6. **Cohen's d** — pooled-std effect size
7. **Outlier report** — IQR-based count (descriptive only — does NOT modify data)
8. **Recommendation** — signal strength + suggested transform (none / log1p / log1p optional) + outlier handling advice

**Why Mann-Whitney instead of t-test:** a t-test assumes normally distributed data. Most customer-behaviour features are heavily skewed (long tails). The Mann-Whitney U is non-parametric — it just asks "is the RANK distribution different between the two groups?" — works on any shape.

**Read it ninth because:** by now you understand the pattern (analyze → plots → narrate → recommend), you just need to learn the specific statistics.

**Key threshold constants:**
- `D_STRONG = 0.8, D_MODERATE = 0.5, D_WEAK = 0.2` — Cohen's d bands for signal strength
- `SKEW_HEAVY = 1.5, SKEW_MODERATE = 0.8` — skewness thresholds for transform recommendation

**The key plot to understand:** `_plot_box_by_churn()`. Two box plots side by side: green for retained, red for churned. If the medians are far apart and the boxes barely overlap, the feature is a strong separator. If the boxes sit on top of each other, the feature carries no churn signal ON ITS OWN (it might still contribute in combination with others — ensembles do this).

---

### Phase 5 — Global analysis

#### **10. `global_analysis/class_balance.py`** — ClassBalanceAnalyzer

**What it is:** A ~190-line class. Analyses the CHURN LABEL itself, not a feature.

**What it does:**
1. Counts churned vs retained, computes rate + imbalance ratio
2. Produces a donut chart (green + red, rate in the centre)
3. Classifies severity: `balanced` / `moderate` / `heavy` / `severe`
4. Recommends a Stage 3.5 handling strategy based on severity:
   - `balanced` (<2:1) → no action needed
   - `moderate` (2–5:1) → algorithmic weights are sufficient (`scale_pos_weight`)
   - `heavy` (5–10:1) → weights + explicit threshold tuning via PR curve
   - `severe` (>10:1) → stack weights + SMOTE + threshold + focal loss

**Why it exists:** SanityChecker already computes the ratio, but ClassBalanceAnalyzer does the modeling recommendation — different audience, different purpose. Sanity asks "is the data healthy?" and ClassBalance asks "how do I model around this?".

**Read it tenth because:** it's the simplest global analyzer — sets the pattern for the next two.

**Your real data result:** moderate (4.94:1) → `algorithmic_weights` (set `scale_pos_weight = n_retained / n_churned` in XGBoost).

---

#### **11. `global_analysis/correlation.py`** — CorrelationAnalyzer

**What it is:** A ~300-line class. Looks at numeric features AGAINST EACH OTHER (not against the target).

**What it does:**
1. **Pearson correlation matrix** — every numeric feature vs every other, saved as a heatmap
2. **Extracts pair list** — all upper-triangular pairs, ranked by |r|, labelled severity (high/moderate/low)
3. **Computes VIF (Variance Inflation Factor)** — VIF_i = diagonal of inv(correlation_matrix). Standard multicollinearity diagnostic
4. **Recommendation** — for each strongly correlated pair, flags the higher-VIF feature as the drop candidate

**Why both Pearson AND VIF:** Pearson catches pairwise redundancy (feature A ≈ feature B). VIF catches multivariate redundancy (feature A is predictable from a linear combination of the others, even if no single pair is strongly correlated). You need both.

**Read it eleventh because:** it introduces VIF, which is worth knowing, and the heatmap pattern.

**Key thresholds:**
- `STRONG_CORR = 0.85` — pairs above this → drop candidate
- `MODERATE_CORR = 0.50` — pairs above this → "worth knowing" but not dropped
- `HIGH_VIF = 10.0` — VIF above this → multicollinear warning

**Your real data result:** zero strong pairs, zero high-VIF features. All 11 numeric features carry independent signal.

---

#### **12. `global_analysis/cohorts.py`** — CohortAnalyzer

**What it is:** A ~300-line class. Slices churn rate by customer SEGMENT.

**What it does:** Takes a list of "cohort specs" and for each one produces:
1. Churn rate per bucket
2. A bar chart with overall rate baseline + hotspot threshold reference lines
3. Identifies hotspot buckets (churn rate > HOTSPOT_LIFT × overall)
4. Recommends binary flag features for Stage 3.2

**The key design choice — declarative specs:**
```python
DEFAULT_SPECS = [
    {"name": "tenure_bucket", "type": "numeric_bucket", "feature": "tenure_months",
     "bins": [-0.1, 6, 24, 10000], "labels": ["new", "growing", "loyal"]},
    {"name": "city_tier", "type": "categorical", "feature": "city_tier"},
    {"name": "preferred_payment_mode", "type": "categorical", "feature": "preferred_payment_mode"},
]
```

Adding a new cohort means adding a dict to the list — no code change. Each spec has either:
- `type: numeric_bucket` + bins + labels (for binning a continuous feature)
- `type: categorical` (for an already-discrete feature)

**Why it exists:** This formalises Marco's original idea: "look at each segment vs churn, flag the segments with elevated churn as candidate features". Hotspots become flag candidates in Stage 3.2.

**Read it twelfth because:** the declarative spec pattern is elegant and worth understanding — it's how you'd extend the module without rewriting anything.

**Key threshold:** `HOTSPOT_LIFT = 2.0` (bucket flagged if churn > 2× overall — currently too strict for real data; we've flagged this for adjustment).

---

### Phase 6 — The conductor

#### **13. `eda_orchestrator.py`** — SupabaseEDA

**What it is:** The ~300-line entry-point class. `SupabaseEDA(db).run()` executes the entire pipeline.

**What `run()` does, in order:**

```
  1. _prepare_reports_tree()       → mkdir reports/, reports/features/, reports/figures/
  2. TrainingDataLoader.load()     → DataFrame from Supabase
  3. SanityChecker.save()          → sanity.json
  4. Create shared services:
       - plot_saver  (PlotSaver)
       - narrator    (StatisticalNarrator)
       - manifest    (ManifestWriter)
  5. ClassBalanceAnalyzer.save()   → class_balance.json
  6. CorrelationAnalyzer.save()    → correlation.json
  7. CohortAnalyzer.save()         → cohorts.json
  8. For each categorical col:
       CategoricalAnalyzer.save()  → features/<col>.json
  9. For each numeric col:
       NumericAnalyzer.save()      → features/<col>.json
 10. manifest.write()               → figures/manifest.json
 11. _write_category_churn_rates() → category_churn_rates.json  (for Stage 3.2)
 12. _build_summary() + write       → summary.json  (for the dashboard)
```

**Why it exists:** Nobody wants to manually call 18 analyzers. The orchestrator is the single public face of the module. It also owns cross-cutting concerns — the shared services, the output layout, the final summary.

**Read it thirteenth because:** now you can finally see how all the pieces you've read about fit together. The logic is linear — no surprises.

**Key outputs for downstream:**
- `summary.json` — what the dashboard reads first (top-level KPIs, feature list, narratives)
- `category_churn_rates.json` — direct input to Stage 3.2 feature engineering (the `category_volatility_score`)

---

#### **14. `scripts/run_eda.py`** — the CLI wrapper

**What it is:** A ~120-line script that you run from the command line.

**What it does:**
1. Parses CLI args (`--reports-root`, `--model-version`, `--log-dir`)
2. Adds project root to `sys.path` so imports resolve
3. Sets up dual logging (console + timestamped file under `logs/`)
4. Opens a `DatabaseConnection` context manager
5. Calls `SupabaseEDA(db).run()`
6. Returns exit code 0 / 2 / 1 based on success / warn / crash

**Why it's separate:** Business logic lives in `src/ml/eda/`. Scripts are thin CLI wrappers. Any other entry point (e.g., pytest, a web handler) can import `SupabaseEDA` directly — no CLI setup required.

**Usage:**
```bash
python scripts/run_eda.py                                 # default
python scripts/run_eda.py --reports-root /tmp/custom      # alt output dir
```

**Exit codes:**
- `0` — success, all green
- `2` — completed but sanity check produced warnings
- `1` — unhandled exception

---

## 5. How data flows through the module (end-to-end trace)

Follow one category — let's say `preferred_payment_mode` — from Supabase to JSON file.

```
┌────────────────────────────────────────────────────────────────────────┐
│ 1. run_eda.py:                                                          │
│    DatabaseConnection() ─── with __enter__() → connects to Supabase     │
│                                                                         │
│ 2. SupabaseEDA(db).run():                                               │
│       ↓                                                                 │
│ 3. TrainingDataLoader(db).load():                                       │
│       SELECT c.preferred_payment_mode, ... FROM customers c JOIN ...    │
│       → returns DataFrame, col 'preferred_payment_mode' is type object  │
│         with values like "COD", "Credit Card", "UPI", ...               │
│       ↓                                                                 │
│ 4. SupabaseEDA creates shared services:                                 │
│       plot_saver = PlotSaver(reports_root)                              │
│       narrator   = StatisticalNarrator()                                │
│       manifest   = ManifestWriter()                                     │
│       ↓                                                                 │
│ 5. For preferred_payment_mode specifically:                             │
│       CategoricalAnalyzer(                                              │
│           feature_name='preferred_payment_mode',                        │
│           df=<DataFrame>,                                               │
│           target_col='churn',                                           │
│           plot_saver=plot_saver,                                        │
│           narrator=narrator,                                            │
│           manifest=manifest,                                            │
│       ).save(reports_root / 'features')                                 │
│       ↓                                                                 │
│ 6. Inside CategoricalAnalyzer._analyze():                               │
│       a. value_counts() → distribution dict                             │
│       b. groupby(feature)['churn'].mean() → per_category_dict           │
│       c. chi2_contingency() → chi² statistic + p-value                  │
│       d. compute Cramér's V                                             │
│       e. _plot_distribution() → 5-slice horizontal bar chart            │
│            ↓                                                            │
│            plot_saver.save(fig, 'cat__preferred_payment_mode__...')     │
│            manifest.add(path, caption, feature, plot_type, context)     │
│       f. _plot_stacked_churn() → 100% stacked bar chart                 │
│            ↓ same as above                                              │
│       g. narrator.chi_squared(p, chi2, dof) → narration string          │
│          narrator.cramers_v(v)                → narration string        │
│          narrator.category_churn_rates(...)   → narration string        │
│       h. _build_recommendation() → {"action": "keep_one_hot", ...}      │
│       ↓                                                                 │
│ 7. FeatureAnalyzer.save() (the base-class method):                      │
│       json.dump(result, features/preferred_payment_mode.json)           │
│       ↓                                                                 │
│ 8. SupabaseEDA then collects all categorical results and:               │
│       _write_category_churn_rates()                                     │
│          → extracts {"preferred_payment_mode": {"COD": 0.31, ...}}      │
│          → writes category_churn_rates.json                             │
│          → this file is read by Stage 3.2 feature engineering           │
└────────────────────────────────────────────────────────────────────────┘
```

At the end, you have: `features/preferred_payment_mode.json` with ~200 lines of analysis, 2 PNGs registered in the manifest, a narrative explaining what's there, and a recommendation for Stage 3.2. All 18 features go through the same path.

---

## 6. How to answer common questions about the module

**"Where are the reports written?"**
→ `src/ml/eda/reports/` (configurable via `--reports-root`).

**"Which file contains the SQL?"**
→ `src/ml/eda/dataset/loader.py`, class attribute `_SQL`.

**"Where do I change the churn label source?"**
→ Same place — the `_SQL` currently sources it from `c.kaggle_churn_label`.

**"Where do I change the dashboard colours?"**
→ `src/ml/eda/reporting/plot_saver.py`, the `COLORS` dict.

**"Where are the statistical test thresholds defined?"**
→ Class attributes at the top of each analyzer:
- `categorical.py`: `HOTSPOT_LIFT`, `MIN_EXPECTED_FREQ`
- `numeric.py`: `D_STRONG`, `D_MODERATE`, `D_WEAK`, `SKEW_HEAVY`, `SKEW_MODERATE`
- `cohorts.py`: `HOTSPOT_LIFT`
- `correlation.py`: `STRONG_CORR`, `MODERATE_CORR`, `HIGH_VIF`
- `class_balance.py`: `BALANCED_MAX`, `MODERATE_MAX`, `HEAVY_MAX`

**"Where does the explanation text come from?"**
→ `src/ml/eda/reporting/narrator.py`. Every narrative string in every JSON is generated here.

**"How do I add a new cohort slice?"**
→ Append a dict to `cohorts.py::DEFAULT_SPECS`. No code changes needed.

**"How do I add a new analyzer?"**
→ Subclass `FeatureAnalyzer` (in `analyzers/base.py`), implement `_analyze()`, then have the orchestrator call it in the appropriate loop.

**"How do I test one feature without running the whole EDA?"**
→ In a Python shell:
```python
from database.connection import DatabaseConnection
from src.ml.eda.dataset.loader import TrainingDataLoader
from src.ml.eda.analyzers.categorical import CategoricalAnalyzer
from src.ml.eda.reporting.plot_saver import PlotSaver
from src.ml.eda.reporting.narrator import StatisticalNarrator
from src.ml.eda.reporting.manifest_writer import ManifestWriter

with DatabaseConnection() as db:
    df = TrainingDataLoader(db).load()

ps, nr, mf = PlotSaver("/tmp/test"), StatisticalNarrator(), ManifestWriter()
result = CategoricalAnalyzer("complain", df, "churn", ps, nr, mf).analyze()
print(result["recommendation"])
```

---

## 7. What's NOT in this module (and why)

| Concern | Where it lives |
|---------|----------------|
| Data cleaning | `src/pipeline/stage1_clean.py` — done BEFORE Stage 3.1 |
| Seeding Supabase | `src/pipeline/stage2_seed.py` — done BEFORE Stage 3.1 |
| Feature engineering | `src/ml/feature_engineering/` — Stage 3.2, next |
| Model training | `src/ml/train.py` — Stage 3.6, later |
| Drift monitoring | `src/ml/drift/` — Stage 3.15, later |
| FastAPI endpoints | `src/api/routes/` — not affected by Stage 3 |

**Stage 3.1 is purely descriptive.** It does not modify data. It does not train anything. It does not decide what features to drop — only what to consider dropping. The final decisions happen in Stage 3.2 based on the `*.json` outputs produced here.

---

## 8. Suggested next action

Once you've finished reading this:

1. **Open `src/ml/eda/utils/signal_logger.py`** (10 lines, 1 minute) — understand the logging convention.
2. **Open `src/ml/eda/reporting/narrator.py`** (~240 lines, 10 minutes) — read through every method. This is where you learn the statistical vocabulary.
3. **Open `src/ml/eda/analyzers/categorical.py`** and trace `_analyze()` top-to-bottom, consulting the narrator methods it calls — that's 80% of the module understood.
4. **Open `src/ml/eda/reports/features/complain.json`** and cross-reference it with the analyzer code — every field in the JSON corresponds to something you just read.

After those four steps, you'll know exactly what every other file does without reading it line-by-line.

---

*Stop here, walk through the files in order, and ping me when you hit something that isn't clear. We'll move to Stage 3.2 only once you're comfortable with the full Stage 3.1 picture.*
