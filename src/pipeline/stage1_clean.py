"""
src/pipeline/stage1_clean.py
══════════════════════════════════════════════════════════════════════════════
STAGE 1 — DATA CLEANING

PURPOSE:
  This is the FIRST and ONLY place in the entire system where raw, messy
  data is touched and fixed. The output of this script is what goes into
  the database (Supabase PostgreSQL).

  Think of the database as a clean warehouse. Before goods enter the warehouse,
  they go through a quality control gate. This script IS that gate.

WHAT THIS SCRIPT DOES (in order):
  Step 1 → Load the raw Excel file
  Step 2 → Inspect the data and log what we found
  Step 3 → Remove duplicate rows
  Step 4 → Normalise categorical aliases  ('CC' → 'Credit Card', etc.)
  Step 5 → Impute (fill) missing values
  Step 6 → Clip out-of-range numeric values
  Step 7 → Reject rows that cannot be salvaged
  Step 8 → Final validation — confirm zero nulls and valid categories
  Step 9 → Save clean CSV to data/cleaned/

WHAT THIS SCRIPT DOES NOT DO:
  ✗ One-hot encoding        → that belongs in Stage 2 (Feature Engineering)
  ✗ Scaling / normalising   → that belongs in Stage 2
  ✗ Derived feature creation → that belongs in Stage 2
  ✗ Anything model-related  → that belongs in Stage 3

  The database stores HUMAN-READABLE values: gender='Male', city_tier=2.
  NOT: gender_Male=1, gender_Female=0.
  The database is for the business. The model gets its own transformed version.

USED BY:
  scripts/run_cleaning.py        → CLI entry point for Kaggle CSV seeding
  src/api/routes/customers.py    → inline for frontend registration payloads
                                    (uses clean_single_record() function)

══════════════════════════════════════════════════════════════════════════════
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

import logging          # Python's built-in logging — structured, levelled output
import sys              # sys.exit() to halt with an error code on critical failure
from dataclasses import dataclass, field   # clean data container for the report
from pathlib import Path                   # modern, OS-agnostic file path handling
from typing import Optional                # type hints for optional function arguments

import pandas as pd    # core data manipulation library

# Import all cleaning rules from the config file.
# WHY: rules are data, not logic. They live in config, not in this script.
# If a new alias appears in production, you update config.py — not this file.
from config.cleaning_config import (
    ALL_ALIAS_MAPS,
    CUSTOMER_ID_COLUMN,
    EXCEL_SHEET_NAME,
    IMPUTE_WITH_MEDIAN,
    IMPUTE_WITH_ZERO,
    NUMERIC_RANGE_CONSTRAINTS,
    TARGET_COLUMN,
    VALID_CATEGORICAL_VALUES,
    VALID_CHURN_VALUES,
    VALID_GENDER_VALUES,
)

# ─────────────────────────────────────────────────────────────────────────────
# LOGGER SETUP
#
# WHY NOT just print()?
#   logging gives us: timestamps, severity levels (INFO/WARNING/ERROR),
#   and the ability to write to both console and a file simultaneously.
#   print() gives us none of that.
#
# Logger name = module name → makes it easy to trace which module produced
# which log line when multiple modules are running together.
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)  # __name__ = 'src.pipeline.stage1_clean'


# ─────────────────────────────────────────────────────────────────────────────
# CLEANING REPORT — DATA CLASS
#
# WHY a dataclass?
#   After cleaning finishes, the caller (seed script, API, test) needs to know
#   exactly what happened: how many rows were removed, what was imputed, etc.
#   A dataclass gives us a structured, printable object instead of a raw dict.
#   It also makes testing easy — assert report.duplicates_removed == 0.
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CleaningReport:
    """
    A structured summary of everything the cleaning pipeline did.
    Returned alongside the cleaned DataFrame so the caller has full visibility.
    """
    input_rows: int = 0                              # rows before any cleaning
    output_rows: int = 0                             # rows after all cleaning
    duplicates_removed: int = 0                      # exact duplicate rows dropped
    rows_rejected: int = 0                           # rows deleted (unfixable)

    # Dict: column name → count of values that were remapped
    # e.g. {"PreferredPaymentMode": 422, "PreferredLoginDevice": 1231}
    aliases_normalised: dict = field(default_factory=dict)

    # Dict: column name → human-readable description of what was done
    # e.g. {"Tenure": "264 nulls → median(9.00)"}
    nulls_imputed: dict = field(default_factory=dict)

    # Dict: column name → count of values clipped to the allowed range
    values_clipped: dict = field(default_factory=dict)

    def print_summary(self) -> None:
        """Print a clean, readable summary to the logger."""
        logger.info("=" * 60)
        logger.info("  CLEANING REPORT — STAGE 1 COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Input rows:          {self.input_rows:>6,}")
        logger.info(f"  Duplicates removed:  {self.duplicates_removed:>6,}")
        logger.info(f"  Rows rejected:       {self.rows_rejected:>6,}")
        logger.info(f"  Output rows:         {self.output_rows:>6,}")
        logger.info(f"  Data loss:           {self.rows_rejected / max(self.input_rows, 1) * 100:.2f}%")

        if self.aliases_normalised:
            logger.info("")
            logger.info("  Aliases Normalised (dirty → canonical):")
            for col, count in self.aliases_normalised.items():
                logger.info(f"    {col}: {count:,} values remapped")

        if self.nulls_imputed:
            logger.info("")
            logger.info("  Null Values Imputed:")
            for col, description in self.nulls_imputed.items():
                logger.info(f"    {col}: {description}")

        if self.values_clipped:
            logger.info("")
            logger.info("  Out-of-Range Values Clipped:")
            for col, count in self.values_clipped.items():
                logger.info(f"    {col}: {count:,} values clipped")

        logger.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — LOAD RAW DATA
# ─────────────────────────────────────────────────────────────────────────────

def load_raw_data(filepath: str) -> pd.DataFrame:
    """
    Load the raw Excel file into a pandas DataFrame.

    WHY THIS IS A SEPARATE FUNCTION:
      Separation of concerns — loading data and processing data are different
      jobs. This function only loads. If we switch from Excel to CSV later,
      we change only this function — nothing else breaks.

    Args:
        filepath: path to the Excel file (relative or absolute)

    Returns:
        pd.DataFrame: the raw, unmodified data exactly as it is in the file

    Raises:
        FileNotFoundError: if the file does not exist at the given path
        SystemExit: if the file exists but cannot be parsed
    """
    path = Path(filepath)  # convert string path to Path object for safer handling

    # Check the file exists before attempting to read it.
    # Fail loudly with a clear message rather than a cryptic pandas error.
    if not path.exists():
        logger.error(f"File not found: {filepath}")
        logger.error("Make sure the Excel file is in data/raw/ before running this script.")
        sys.exit(1)  # exit code 1 = error (convention: 0 = success, non-zero = error)

    logger.info("=" * 60)
    logger.info("  STAGE 1 — DATA CLEANING")
    logger.info("=" * 60)
    logger.info(f"  Loading raw data from: {filepath}")

    try:
        # Read the specific sheet — the Excel file has two sheets:
        # 'Data Dict' (the data dictionary) and 'E Comm' (the actual data).
        # We only want the actual data sheet.
        df = pd.read_excel(
            path,
            sheet_name=EXCEL_SHEET_NAME,  # 'E Comm' — defined in config
        )
    except Exception as e:
        logger.error(f"Failed to read Excel file: {e}")
        sys.exit(1)

    logger.info(f"  ✓ Loaded {len(df):,} rows × {len(df.columns)} columns")
    logger.info(f"  Columns: {list(df.columns)}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — INSPECT AND LOG DATA QUALITY ISSUES
# ─────────────────────────────────────────────────────────────────────────────

def inspect_raw_data(df: pd.DataFrame) -> None:
    """
    Thoroughly inspect the raw data and log every issue found.
    This step does NOT modify the data — it only observes and reports.

    WHY RUN THIS BEFORE CLEANING?
      You need to know what you are fixing before you fix it.
      This log output is the evidence trail: 'here is what the raw data
      contained, here is what we did about it.'
      It also surfaces unexpected issues (e.g. new categories we haven't seen).

    Args:
        df: the raw DataFrame, unmodified
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 2 — RAW DATA INSPECTION")
    logger.info("─" * 60)

    # ── Dataset dimensions ───────────────────────────────────────────────────
    logger.info(f"  Dataset shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    logger.info(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")

    # ── Churn distribution ────────────────────────────────────────────────────
    # This tells us about class imbalance — critical for ML decisions later.
    logger.info("")
    logger.info("  Target column (Churn) distribution:")
    churn_counts = df[TARGET_COLUMN].value_counts()  # count 0s and 1s
    churn_rate = df[TARGET_COLUMN].mean() * 100       # percentage of churners
    logger.info(f"    Churn = 0 (Retained): {churn_counts.get(0, 0):,} customers")
    logger.info(f"    Churn = 1 (Churned):  {churn_counts.get(1, 0):,} customers")
    logger.info(f"    Churn rate:           {churn_rate:.1f}%")
    logger.info(f"    → Class imbalance ratio: {churn_counts.get(0,0)/churn_counts.get(1,1):.1f}:1")
    logger.info("    → IMBALANCE NOTED: will be handled in Stage 3 (ML) via scale_pos_weight")
    logger.info("    → This is NOT fixed at cleaning stage — imbalance is real signal, not an error")

    # ── Duplicates ────────────────────────────────────────────────────────────
    logger.info("")
    dup_count = df.duplicated(subset=[CUSTOMER_ID_COLUMN]).sum()  # duplicate CustomerID rows
    logger.info(f"  Duplicate CustomerID rows: {dup_count:,}")
    if dup_count == 0:
        logger.info("    → No duplicates found in this dataset")
    else:
        logger.info(f"    → {dup_count} rows will be dropped (keep first occurrence)")

    # ── Missing values ────────────────────────────────────────────────────────
    logger.info("")
    logger.info("  Missing values per column:")
    nulls = df.isnull().sum()    # count nulls in every column
    total = len(df)
    has_nulls = False
    for col in df.columns:
        null_count = nulls[col]
        if null_count > 0:
            has_nulls = True
            pct = null_count / total * 100   # percentage of rows affected
            logger.info(f"    {col:<35} {null_count:>4} nulls ({pct:.1f}%)")
    if not has_nulls:
        logger.info("    → No missing values found")

    # ── Categorical columns — alias inspection ────────────────────────────────
    logger.info("")
    logger.info("  Categorical columns — unique values found in raw data:")

    # PreferredPaymentMode — has aliases: 'CC' = 'Credit Card', 'Cash on Delivery' = 'COD'
    logger.info("")
    logger.info("  [PreferredPaymentMode]")
    logger.info("    Raw values found: " + str(sorted(df["PreferredPaymentMode"].unique())))
    logger.info("    PROBLEM: 'CC' and 'Credit Card' are the same payment method")
    logger.info("    PROBLEM: 'Cash on Delivery' and 'COD' are the same payment method")
    logger.info("    SOLUTION: Map 'CC' → 'Credit Card', 'Cash on Delivery' → 'COD'")
    cc_count = (df["PreferredPaymentMode"] == "CC").sum()
    cod_full_count = (df["PreferredPaymentMode"] == "Cash on Delivery").sum()
    logger.info(f"    Rows affected: 'CC'={cc_count:,}, 'Cash on Delivery'={cod_full_count:,}")

    # PreferredLoginDevice — has alias: 'Phone' = 'Mobile Phone'
    logger.info("")
    logger.info("  [PreferredLoginDevice]")
    logger.info("    Raw values found: " + str(sorted(df["PreferredLoginDevice"].unique())))
    logger.info("    PROBLEM: 'Phone' and 'Mobile Phone' refer to the same device type")
    logger.info("    SOLUTION: Map 'Phone' → 'Mobile Phone'")
    phone_count = (df["PreferredLoginDevice"] == "Phone").sum()
    logger.info(f"    Rows affected: 'Phone'={phone_count:,}")

    # PreferedOrderCat — has alias: 'Mobile Phone' = 'Mobile' (as a product category)
    logger.info("")
    logger.info("  [PreferedOrderCat]  ← note: intentional typo 'Prefered' is in original data")
    logger.info("    Raw values found: " + str(sorted(df["PreferedOrderCat"].unique())))
    logger.info("    PROBLEM: 'Mobile Phone' here means 'mobile phones as a product category'")
    logger.info("             NOT the login device. It is the same as category 'Mobile'.")
    logger.info("    SOLUTION: Map 'Mobile Phone' → 'Mobile'")
    mobile_phone_cat = (df["PreferedOrderCat"] == "Mobile Phone").sum()
    logger.info(f"    Rows affected: 'Mobile Phone'={mobile_phone_cat:,}")

    # Gender — clean, no issues
    logger.info("")
    logger.info("  [Gender]")
    logger.info("    Raw values found: " + str(sorted(df["Gender"].unique())))
    logger.info("    → No issues found. Values are clean.")

    # MaritalStatus — clean, no issues
    logger.info("")
    logger.info("  [MaritalStatus]")
    logger.info("    Raw values found: " + str(sorted(df["MaritalStatus"].unique())))
    logger.info("    → No issues found. Values are clean.")

    # ── Numeric columns — range inspection ────────────────────────────────────
    logger.info("")
    logger.info("  Numeric columns — ranges:")
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    for col in numeric_cols:
        if col == CUSTOMER_ID_COLUMN:
            continue  # CustomerID is just an identifier, ranges don't matter
        col_min = df[col].min()
        col_max = df[col].max()
        col_mean = df[col].mean()
        logger.info(f"    {col:<35} min={col_min:.1f}  max={col_max:.1f}  mean={col_mean:.2f}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — REMOVE DUPLICATES
# ─────────────────────────────────────────────────────────────────────────────

def remove_duplicates(df: pd.DataFrame, report: CleaningReport) -> pd.DataFrame:
    """
    Remove exact duplicate rows based on the CustomerID column.

    WHY CustomerID and not all columns?
      Two rows are considered the same customer if they share a CustomerID.
      Even if some other column differs slightly (data entry error), the same
      CustomerID should not appear twice.

    WHY keep='first'?
      We keep the first occurrence because it is typically the most original
      record. If the duplication was due to a system error, the first insert
      is usually the intended one.

    Args:
        df: DataFrame to deduplicate
        report: CleaningReport to update with counts

    Returns:
        pd.DataFrame: deduplicated DataFrame
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 3 — REMOVING DUPLICATES")
    logger.info("─" * 60)

    rows_before = len(df)  # record count before dropping

    df = df.drop_duplicates(
        subset=[CUSTOMER_ID_COLUMN],  # match on CustomerID only
        keep="first",                  # keep the first occurrence of each CustomerID
    )

    rows_after = len(df)                              # count after dropping
    dropped = rows_before - rows_after                # how many were removed
    report.duplicates_removed = dropped               # store in report

    if dropped == 0:
        logger.info("  ✓ No duplicate rows found — dataset is clean on this dimension")
    else:
        logger.info(f"  Removed {dropped:,} duplicate CustomerID rows")
        logger.info(f"  Remaining rows: {rows_after:,}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — NORMALISE CATEGORICAL ALIASES
# ─────────────────────────────────────────────────────────────────────────────

def normalise_categorical_aliases(
    df: pd.DataFrame,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Map dirty/aliased category values to their single canonical form.

    WHY THIS IS CRITICAL:
      If you skip this step and directly one-hot encode the raw data,
      'CC' and 'Credit Card' become TWO separate feature columns.
      The model treats them as two different payment behaviours.
      Feature importance gets split between them. Both signals are weakened.
      At inference, a new 'CC' record hits the wrong feature column.
      This is a silent bug — no error, wrong predictions.

      After normalisation:
        'CC' → 'Credit Card'          (merged into one column at OHE time)
        'Cash on Delivery' → 'COD'    (merged into one column at OHE time)
        'Phone' → 'Mobile Phone'      (merged into one column at OHE time)
        'Mobile Phone' (order cat) → 'Mobile'  (merged into one column)

    The alias maps are defined in config/cleaning_config.py.
    This function is just the engine that applies them.

    Args:
        df: DataFrame with raw categorical values
        report: CleaningReport to update

    Returns:
        pd.DataFrame: DataFrame with canonical categorical values
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 4 — NORMALISING CATEGORICAL ALIASES")
    logger.info("─" * 60)
    logger.info("  Goal: Map dirty aliases to canonical values for consistent DB storage")
    logger.info("  Rule source: config/cleaning_config.py → ALL_ALIAS_MAPS")

    df = df.copy()  # never modify the original DataFrame — always work on a copy

    # Iterate over every column that has an alias map defined in config
    for column_name, alias_map in ALL_ALIAS_MAPS.items():

        # Skip if this column doesn't exist in the DataFrame
        # (protects against future schema changes)
        if column_name not in df.columns:
            logger.warning(f"  Column '{column_name}' not found in DataFrame — skipping")
            continue

        # Record the values BEFORE mapping so we can count how many changed
        original_values = df[column_name].copy()

        # Apply the alias map: replace each value using the dictionary
        # Values NOT in the map become NaN — this is intentional.
        # An unexpected value (not in the map) means the map is incomplete
        # and we need to add the new alias. We detect this in Step 8.
        df[column_name] = df[column_name].map(alias_map)

        # Count how many values actually changed (were different from original)
        changed_count = (df[column_name] != original_values).sum()

        # Record in report for the summary
        report.aliases_normalised[column_name] = int(changed_count)

        # Log before → after unique values so you can see the merge happened
        unique_before = sorted(original_values.dropna().unique())
        unique_after = sorted(df[column_name].dropna().unique())

        logger.info("")
        logger.info(f"  [{column_name}]")
        logger.info(f"    Before: {unique_before}")
        logger.info(f"    After:  {unique_after}")
        logger.info(f"    Values remapped: {changed_count:,}")

        # Detect unmapped values (they become NaN after .map())
        # This should never happen if the config is complete.
        unmapped = df[column_name].isnull().sum() - original_values.isnull().sum()
        if unmapped > 0:
            # Find which values weren't in the map
            missing_vals = original_values[df[column_name].isnull()].unique()
            logger.warning(f"    ⚠ {unmapped} values were NOT in the alias map: {missing_vals}")
            logger.warning(f"    → These values became NaN. Add them to cleaning_config.py!")

    logger.info("")
    logger.info("  ✓ Alias normalisation complete")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — IMPUTE MISSING VALUES
# ─────────────────────────────────────────────────────────────────────────────

def impute_missing_values(
    df: pd.DataFrame,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Fill missing values (NaN) in each column using the documented strategy.

    STRATEGY DECISION (per column, from config/cleaning_config.py):
      MEDIAN → Tenure, WarehouseToHome, HourSpendOnApp,
               OrderAmountHikeFromlastYear, OrderCount, DaySinceLastOrder
      ZERO   → CouponUsed

    WHY THIS MATTERS:
      Null values crash sklearn at training time.
      Null values at inference time mean a customer cannot be scored.
      We must resolve ALL nulls before data enters the database.

    WHY NOT IMPUTE AFTER READING FROM THE DB (in Stage 2)?
      Because the database should store meaningful, resolved values.
      A NULL in the database means 'we don't know this.'
      After cleaning, we DO know — we've made a deliberate imputation choice.
      Storing that resolved value in the DB means every downstream consumer
      (API, ML pipeline, dashboard) gets clean data without re-imputing.

    Args:
        df: DataFrame after alias normalisation
        report: CleaningReport to update

    Returns:
        pd.DataFrame: DataFrame with all documented nulls filled
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 5 — IMPUTING MISSING VALUES")
    logger.info("─" * 60)
    logger.info("  Goal: Resolve all NaN values before database insertion")
    logger.info("  Strategy: Median for skewed numerics, Zero for count columns")

    df = df.copy()  # work on a copy — never mutate the input

    # ── MEDIAN IMPUTATION ─────────────────────────────────────────────────────
    logger.info("")
    logger.info("  Median Imputation columns:")
    logger.info("  (Median chosen over Mean because these columns have skewed distributions")
    logger.info("   and/or outliers. Median is the 50th percentile — unaffected by extremes.)")

    for col in IMPUTE_WITH_MEDIAN:
        if col not in df.columns:
            continue  # skip if column doesn't exist (future-proofing)

        null_count = int(df[col].isnull().sum())  # how many nulls exist

        if null_count == 0:
            logger.info(f"    {col:<35} No nulls — skipping")
            continue  # nothing to do for this column

        # Compute median on the non-null values only
        # IMPORTANT: compute BEFORE filling so the null rows don't affect it
        median_value = df[col].median()

        # Fill all NaN values in this column with the computed median
        df[col] = df[col].fillna(median_value)

        # Record in the report
        pct = null_count / len(df) * 100
        description = f"{null_count} nulls ({pct:.1f}%) → median={median_value:.2f}"
        report.nulls_imputed[col] = description
        logger.info(f"    {col:<35} {description}")

        # Special note for OrderAmountHikeFromlastYear
        if col == "OrderAmountHikeFromlastYear":
            logger.info(f"    ↳ NOTE: This column CAN be negative (customer spent less than last year).")
            logger.info(f"            Median is {median_value:.2f} (positive). Missing rows assumed 'average'.")
            logger.info(f"            Negative values in non-null rows are PRESERVED — not clipped.")

    # ── ZERO IMPUTATION ───────────────────────────────────────────────────────
    logger.info("")
    logger.info("  Zero Imputation columns:")
    logger.info("  (Zero used where 0 is a valid real-world state — not an approximation.)")

    for col in IMPUTE_WITH_ZERO:
        if col not in df.columns:
            continue

        null_count = int(df[col].isnull().sum())

        if null_count == 0:
            logger.info(f"    {col:<35} No nulls — skipping")
            continue

        df[col] = df[col].fillna(0)  # replace NaN with 0

        pct = null_count / len(df) * 100
        description = f"{null_count} nulls ({pct:.1f}%) → 0 (zero coupons used is valid)"
        report.nulls_imputed[col] = description
        logger.info(f"    {col:<35} {description}")

    logger.info("")
    logger.info("  ✓ Null imputation complete")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — CLIP OUT-OF-RANGE NUMERIC VALUES
# ─────────────────────────────────────────────────────────────────────────────

def clip_numeric_ranges(
    df: pd.DataFrame,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Clip numeric values to their allowed physical ranges.

    WHY CLIP INSTEAD OF REJECT?
      A SatisfactionScore of 7 (when max is 5) is clearly a data entry error,
      but the rest of that customer's row — their tenure, their order count,
      their payment mode — is all valid.
      Clipping preserves the row and fixes only the bad value.
      Rejection would throw away 19 valid columns to fix 1 bad value.

    WHY LOG EVERY CLIP?
      Clipping is a compromise. Every clipped value means something was wrong
      upstream. These logs are the evidence trail. If you see 200 clips on
      SatisfactionScore, that means something in the data collection system
      is outputting values > 5. That is a system bug, not a data bug.

    EXCEPTION — OrderAmountHikeFromlastYear:
      This column is NOT in NUMERIC_RANGE_CONSTRAINTS deliberately.
      It represents year-over-year order value change. Negative values mean
      the customer spent less than last year — that is a critical churn signal.
      Clipping it to 0 would destroy that signal. We leave it unconstrained.

    Args:
        df: DataFrame after null imputation
        report: CleaningReport to update

    Returns:
        pd.DataFrame: DataFrame with out-of-range values clipped
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 6 — CLIPPING OUT-OF-RANGE NUMERIC VALUES")
    logger.info("─" * 60)
    logger.info("  Goal: Enforce physical limits on numeric columns")
    logger.info("  Range rules source: config/cleaning_config.py → NUMERIC_RANGE_CONSTRAINTS")
    logger.info("  NOTE: OrderAmountHikeFromlastYear is NOT clipped — negatives are valid signal")

    df = df.copy()  # work on a copy

    any_clipped = False  # flag to track if anything was clipped

    for col, (min_val, max_val) in NUMERIC_RANGE_CONSTRAINTS.items():

        if col not in df.columns:
            continue  # column may not exist — skip gracefully

        # Count how many values are outside the allowed range
        out_of_range = int(((df[col] < min_val) | (df[col] > max_val)).sum())

        if out_of_range == 0:
            continue  # nothing to clip in this column

        # Record before clipping for logging
        any_clipped = True
        col_min_before = df[col].min()
        col_max_before = df[col].max()

        # Clip: values below min_val → set to min_val
        #        values above max_val → set to max_val
        df[col] = df[col].clip(lower=min_val, upper=max_val)

        # Update the report
        report.values_clipped[col] = out_of_range

        # Log with before/after context
        logger.warning(
            f"  ⚠ [{col}] {out_of_range} values clipped: "
            f"range was [{col_min_before:.1f}, {col_max_before:.1f}], "
            f"allowed [{min_val}, {max_val}]"
        )
        logger.warning(
            f"    → This indicates a data quality issue UPSTREAM of this script."
        )

    if not any_clipped:
        logger.info("  ✓ All numeric values are within allowed ranges — no clipping needed")
    else:
        logger.info("  ✓ Clipping complete — check warnings above for upstream issues")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — REJECT UNFIXABLE ROWS
# ─────────────────────────────────────────────────────────────────────────────

def reject_invalid_rows(
    df: pd.DataFrame,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Remove rows that cannot be salvaged by any cleaning operation.

    WHAT MAKES A ROW UNSALVAGEABLE?
      1. Missing Churn label → the row has no target. We cannot train on it
         and we cannot evaluate predictions against it. It is useless.
      2. Invalid Churn value (not 0 or 1) → same problem. If Churn=2 or
         Churn='yes', the model does not know what to learn.
      3. Missing/invalid Gender → this is a profile demographic. Unlike
         behavioural columns (OrderCount, DaySinceLastOrder) which we can
         impute from population statistics, Gender cannot be inferred.
         We could use mode, but that introduces systematic demographic bias.
         Better to reject and note the count.

    WHY ARE THESE THE ONLY REJECTION RULES?
      All other columns either have valid imputation strategies (numeric →
      median/zero) or are categorical with known valid values that can be
      normalised. These three are the true hard failures.

    Args:
        df: DataFrame after clipping
        report: CleaningReport to update

    Returns:
        pd.DataFrame: DataFrame with unfixable rows removed
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 7 — REJECTING UNFIXABLE ROWS")
    logger.info("─" * 60)
    logger.info("  These are rows where no cleaning strategy can produce a valid record.")

    rows_before = len(df)  # count before rejection

    # Build a boolean mask: True = row is valid, False = row should be rejected
    # We start with all True and AND it down with each rejection rule
    valid_mask = pd.Series(True, index=df.index)

    # Rule 1: Target label must exist and be 0 or 1
    if TARGET_COLUMN in df.columns:
        churn_valid = df[TARGET_COLUMN].isin(VALID_CHURN_VALUES)
        invalid_churn = (~churn_valid).sum()
        if invalid_churn > 0:
            logger.warning(
                f"  Rejecting {invalid_churn} rows: Churn label is null or not in {{0, 1}}"
            )
        valid_mask &= churn_valid  # only keep rows where Churn is valid

    # Rule 2: Gender must be Male or Female
    if "Gender" in df.columns:
        gender_valid = df["Gender"].isin(VALID_GENDER_VALUES)
        invalid_gender = (~gender_valid).sum()
        if invalid_gender > 0:
            logger.warning(
                f"  Rejecting {invalid_gender} rows: Gender is null or unknown"
            )
        valid_mask &= gender_valid

    # Apply the mask — keep only valid rows
    df = df[valid_mask].copy()

    rows_after = len(df)
    report.rows_rejected = rows_before - rows_after  # count of rejected rows

    if report.rows_rejected == 0:
        logger.info("  ✓ No rows rejected — all records passed hard validation rules")
    else:
        logger.warning(
            f"  Rejected {report.rows_rejected} rows ({report.rows_rejected/rows_before*100:.2f}% of data)"
        )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — FINAL VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def validate_cleaned_data(df: pd.DataFrame) -> bool:
    """
    Final quality gate — verify the cleaned data meets all requirements.
    Returns True if the data is clean. Returns False if any issue remains.

    WHY A SEPARATE VALIDATION STEP?
      Cleaning and validation are different jobs.
      Cleaning transforms. Validation confirms.
      If this step fails, it means one of the cleaning steps has a bug.
      The failure here points back to which step needs fixing.

    This function does NOT modify the data.

    Args:
        df: the fully cleaned DataFrame
        report: CleaningReport (not modified here — this is read-only)

    Returns:
        bool: True = data passes all checks, False = issues remain
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 8 — FINAL VALIDATION")
    logger.info("─" * 60)

    issues_found = []  # collect all failures before reporting

    # Check 1: Zero remaining nulls
    remaining_nulls = df.isnull().sum()
    total_nulls = remaining_nulls.sum()
    if total_nulls > 0:
        # Find which columns still have nulls
        cols_with_nulls = remaining_nulls[remaining_nulls > 0].to_dict()
        issues_found.append(f"  ✗ {total_nulls} nulls remain: {cols_with_nulls}")
    else:
        logger.info("  ✓ Check 1 PASSED: Zero null values remaining")

    # Check 2: No duplicates
    dup_count = df.duplicated(subset=[CUSTOMER_ID_COLUMN]).sum()
    if dup_count > 0:
        issues_found.append(f"  ✗ {dup_count} duplicate CustomerIDs remain")
    else:
        logger.info("  ✓ Check 2 PASSED: No duplicate CustomerIDs")

    # Check 3: All categorical columns contain only valid values
    for col, valid_values in VALID_CATEGORICAL_VALUES.items():
        if col not in df.columns:
            continue
        # Find any value not in the valid set
        invalid_mask = ~df[col].isin(valid_values)
        invalid_count = invalid_mask.sum()
        if invalid_count > 0:
            invalid_vals = df.loc[invalid_mask, col].unique()
            issues_found.append(
                f"  ✗ [{col}] {invalid_count} invalid values: {invalid_vals}. "
                f"Valid: {sorted(valid_values)}"
            )
        else:
            logger.info(f"  ✓ Check 3 PASSED: [{col}] all values in valid set {sorted(valid_values)}")

    # Check 4: Target column is binary (0 or 1 only)
    invalid_churn = ~df[TARGET_COLUMN].isin(VALID_CHURN_VALUES)
    if invalid_churn.sum() > 0:
        issues_found.append(f"  ✗ Churn column contains non-binary values")
    else:
        logger.info(f"  ✓ Check 4 PASSED: Churn column is clean binary (0/1 only)")

    # Report results
    if issues_found:
        logger.error("")
        logger.error("  VALIDATION FAILED — Issues that must be fixed before DB insertion:")
        for issue in issues_found:
            logger.error(issue)
        return False  # signal failure to caller
    else:
        logger.info("")
        logger.info("  ✓ ALL VALIDATION CHECKS PASSED")
        logger.info("  → Data is ready for database insertion")
        return True  # signal success to caller


# ─────────────────────────────────────────────────────────────────────────────
# STEP 9 — SAVE CLEANED DATA
# ─────────────────────────────────────────────────────────────────────────────

def save_cleaned_data(
    df: pd.DataFrame,
    output_path: str,
) -> None:
    """
    Save the cleaned DataFrame to a CSV file in data/cleaned/.

    WHY CSV AND NOT EXCEL?
      CSV is simpler, smaller, and faster to read for downstream processing.
      Excel (.xlsx) adds formatting overhead we don't need after cleaning.
      pandas reads CSV 3-5x faster than Excel — matters for Stage 2.

    WHY SAVE TO DISK AT ALL?
      Two reasons:
      1. Reproducibility — you can inspect the cleaned data before DB insert
      2. Separation — the seeding script reads from this clean CSV,
         not from the raw Excel. The pipeline stages are decoupled.

    Args:
        df: fully cleaned DataFrame
        output_path: where to save the CSV (e.g. 'data/cleaned/clean.csv')
    """
    logger.info("")
    logger.info("─" * 60)
    logger.info("  STEP 9 — SAVING CLEANED DATA")
    logger.info("─" * 60)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)  # create directory if it doesn't exist

    # Save to CSV without the pandas index column (index=False)
    # The index is just row numbers 0, 1, 2... — meaningless, don't save it
    df.to_csv(output, index=False)

    file_size_kb = output.stat().st_size / 1024  # file size in kilobytes
    logger.info(f"  ✓ Saved {len(df):,} rows to: {output_path}")
    logger.info(f"    File size: {file_size_kb:.1f} KB")
    logger.info(f"    Columns saved ({len(df.columns)}): {list(df.columns)}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def run_cleaning_pipeline(
    input_path: str,
    output_path: str,
) -> tuple[pd.DataFrame, CleaningReport]:
    """
    Run all 9 cleaning steps in sequence.
    This is the single function that orchestrates the full Stage 1 pipeline.

    Called by:
      - scripts/run_cleaning.py   (CLI, for Kaggle CSV)
      - Tests (with test fixture data)

    Args:
        input_path:  path to raw Excel file
        output_path: path to save clean CSV

    Returns:
        Tuple of (cleaned DataFrame, CleaningReport)
        The DataFrame is ready for database insertion.
        The CleaningReport summarises everything that was done.
    """
    # Initialise the report — we'll fill it as each step runs
    report = CleaningReport()

    # Step 1 — Load
    df = load_raw_data(input_path)
    report.input_rows = len(df)  # record original row count

    # Step 2 — Inspect (read-only, does not modify df)
    inspect_raw_data(df)

    # Step 3 — Duplicates
    df = remove_duplicates(df, report)

    # Step 4 — Normalise aliases (MUST run before Step 5 — normalisation can create NaN)
    df = normalise_categorical_aliases(df, report)

    # Step 5 — Impute nulls (MUST run after Step 4 — aliases may introduce NaN)
    df = impute_missing_values(df, report)

    # Step 6 — Clip ranges
    df = clip_numeric_ranges(df, report)

    # Step 7 — Reject unfixable rows
    df = reject_invalid_rows(df, report)

    # Step 8 — Final validation (hard stop if data is still dirty)
    is_valid = validate_cleaned_data(df)
    if not is_valid:
        logger.error("")
        logger.error("  CRITICAL: Validation failed. Cleaned data will NOT be saved.")
        logger.error("  Fix the issues above and re-run the pipeline.")
        sys.exit(1)  # non-zero exit code signals failure to CI/CD pipeline

    # Step 9 — Save
    save_cleaned_data(df, output_path)

    # Record final output row count in report
    report.output_rows = len(df)

    # Print the full summary
    report.print_summary()

    return df, report


# ─────────────────────────────────────────────────────────────────────────────
# API ENTRY POINT — for single records from the frontend
# ─────────────────────────────────────────────────────────────────────────────

def clean_single_record(record: dict) -> tuple[dict, list[str]]:
    """
    Clean and validate a SINGLE record submitted from the frontend form.

    This function applies the SAME cleaning rules as the batch pipeline
    but operates on one Python dict instead of a DataFrame.

    WHY THE SAME RULES?
      The database must contain consistently formatted data regardless of
      whether it came from the Kaggle CSV or from a user registration form.
      Same rules → same canonical values → model sees consistent input.

    DIFFERENCE FROM BATCH PIPELINE:
      For batch data (CSV), we impute nulls with median.
      For frontend data, the form is required — fields cannot be empty.
      The API validates required fields before calling this function.
      So imputation rarely triggers here. If it does, it means the API
      layer has a bug (not enforcing required fields properly).

    Returns:
      (cleaned_record, errors)
      If errors list is non-empty → return HTTP 400, do NOT write to DB.
      If errors is empty → the cleaned_record is safe to insert.

    Args:
        record: dict from the frontend form submission

    Returns:
        Tuple of (cleaned dict, list of error strings)
    """
    errors: list[str] = []    # collect all validation errors
    r = dict(record)          # copy the input — never mutate the caller's dict

    # Apply alias normalisation maps (same as batch pipeline)
    for col, alias_map in ALL_ALIAS_MAPS.items():
        if col in r and r[col] is not None:
            raw_value = str(r[col]).strip()            # strip whitespace — common form issue
            canonical = alias_map.get(raw_value)       # look up in alias map
            if canonical is None:
                # Value not in map — it is unknown
                valid_vals = sorted(set(alias_map.values()))
                errors.append(
                    f"Field '{col}': unknown value '{raw_value}'. "
                    f"Accepted values: {valid_vals}"
                )
            else:
                r[col] = canonical  # replace with canonical form

    # Validate final categorical values
    for col, valid_set in VALID_CATEGORICAL_VALUES.items():
        val = r.get(col)
        if val is not None and val not in valid_set:
            errors.append(
                f"Field '{col}': '{val}' is not a valid value. "
                f"Accepted: {sorted(valid_set)}"
            )

    # Validate numeric ranges
    for col, (min_val, max_val) in NUMERIC_RANGE_CONSTRAINTS.items():
        val = r.get(col)
        if val is not None:
            try:
                val_float = float(val)
                if not (min_val <= val_float <= max_val):
                    errors.append(
                        f"Field '{col}': {val} is out of range. "
                        f"Must be between {min_val} and {max_val}."
                    )
            except (TypeError, ValueError):
                errors.append(f"Field '{col}': '{val}' is not a valid number.")

    return r, errors
