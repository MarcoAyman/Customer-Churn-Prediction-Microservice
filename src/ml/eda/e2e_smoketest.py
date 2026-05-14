"""Full orchestrator smoke test — mocks DatabaseConnection and runs SupabaseEDA end-to-end."""
from __future__ import annotations

import json
import logging
import shutil
import sys
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

# Synthesize a Kaggle-like dataset with 5630 rows ---------------------------------
rng = np.random.default_rng(42)
N = 5630
churn = rng.choice([0, 1], size=N, p=[0.832, 0.168])

# Categorical
login_device = np.where(
    churn == 1,
    rng.choice(["Mobile Phone", "Computer"], size=N, p=[0.75, 0.25]),
    rng.choice(["Mobile Phone", "Computer"], size=N, p=[0.55, 0.45]),
)
payment_opts = ["COD", "Credit Card", "Debit Card", "E wallet", "UPI"]
payment = np.where(
    churn == 1,
    rng.choice(payment_opts, size=N, p=[0.40, 0.15, 0.15, 0.20, 0.10]),
    rng.choice(payment_opts, size=N, p=[0.08, 0.30, 0.25, 0.22, 0.15]),
)
gender = rng.choice(["Male", "Female"], size=N, p=[0.60, 0.40])
marital = rng.choice(["Single", "Married", "Divorced"], size=N, p=[0.35, 0.55, 0.10])
city_tier = np.where(
    churn == 1,
    rng.choice([1, 2, 3], size=N, p=[0.25, 0.35, 0.40]),
    rng.choice([1, 2, 3], size=N, p=[0.45, 0.35, 0.20]),
)
order_cat = rng.choice(
    ["Grocery", "Mobile", "Fashion", "Laptop & Accessory", "Others"],
    size=N, p=[0.15, 0.30, 0.20, 0.20, 0.15],
)
complaint = np.where(
    churn == 1,
    rng.choice([0, 1], size=N, p=[0.55, 0.45]),
    rng.choice([0, 1], size=N, p=[0.85, 0.15]),
)

# Numeric
tenure = np.where(
    churn == 1,
    rng.exponential(scale=3, size=N).clip(0, 30),
    rng.exponential(scale=15, size=N).clip(0, 60),
)
orders = np.where(
    churn == 1,
    rng.poisson(lam=1.5, size=N),
    rng.poisson(lam=5, size=N),
).clip(0, None)
days_since = np.where(
    churn == 1,
    rng.exponential(scale=15, size=N).clip(0, 60),
    rng.exponential(scale=5, size=N).clip(0, 30),
)
hours = rng.exponential(scale=2, size=N).clip(0, 10)
addresses = rng.poisson(lam=4, size=N).clip(1, 20)
devices = rng.poisson(lam=3, size=N).clip(1, 10)
coupons = rng.poisson(lam=2, size=N).clip(0, 20)
cashback = rng.exponential(scale=120, size=N).clip(0, 500)
hike = rng.normal(loc=15, scale=8, size=N).clip(-5, 40)
warehouse = rng.exponential(scale=15, size=N).clip(5, 150)
satisfaction = np.where(
    churn == 1,
    rng.choice([1, 2, 3, 4, 5], size=N, p=[0.25, 0.30, 0.25, 0.15, 0.05]),
    rng.choice([1, 2, 3, 4, 5], size=N, p=[0.05, 0.10, 0.20, 0.35, 0.30]),
)

rows = [
    {
        "customer_id": f"cust_{i:05d}",
        "gender": gender[i],
        "city_tier": int(city_tier[i]),
        "marital_status": marital[i],
        "preferred_payment": payment[i],
        "preferred_login_device": login_device[i],
        "preferred_order_cat": order_cat[i],
        "tenure_months": float(tenure[i]),
        "order_count": int(orders[i]),
        "days_since_last_order": float(days_since[i]),
        "hours_spent_on_app": float(hours[i]),
        "number_of_addresses": int(addresses[i]),
        "number_of_devices_registered": int(devices[i]),
        "coupon_used": int(coupons[i]),
        "cashback_amount": float(cashback[i]),
        "order_amount_hike_from_last_year": float(hike[i]),
        "warehouse_to_home": float(warehouse[i]),
        "satisfaction_score": int(satisfaction[i]),
        "complaint_flag": int(complaint[i]),
        "churn": int(churn[i]),
    }
    for i in range(N)
]

# Mock DB that returns our synthetic rows via the cursor() context-manager protocol.
class MockCursor:
    def __init__(self, rows): self._rows = rows
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def execute(self, sql, params=None): pass
    def fetchall(self): return [tuple(r.values()) for r in self._rows]
    @property
    def description(self):
        return [(k,) + (None,) * 6 for k in self._rows[0].keys()]

class MockDB:
    def __init__(self, rows): self._rows = rows
    def cursor(self): return MockCursor(self._rows)

# Run the orchestrator ------------------------------------------------------------
from src.ml.eda import SupabaseEDA

REPORTS_ROOT = Path("/tmp/e2e_reports")
if REPORTS_ROOT.exists():
    shutil.rmtree(REPORTS_ROOT)

eda = SupabaseEDA(db=MockDB(rows), reports_root=REPORTS_ROOT)
summary = eda.run()

# Verify outputs ------------------------------------------------------------------
print("\n" + "═" * 70)
print("  OUTPUT INSPECTION")
print("═" * 70)

expected_top_level = [
    "summary.json",
    "sanity.json",
    "class_balance.json",
    "correlation.json",
    "cohorts.json",
    "category_churn_rates.json",
]
for name in expected_top_level:
    path = REPORTS_ROOT / name
    assert path.exists(), f"Missing top-level artifact: {name}"
    print(f"  ✓ {name}  ({path.stat().st_size:,} bytes)")

assert (REPORTS_ROOT / "figures" / "manifest.json").exists()
print(f"  ✓ figures/manifest.json  ({(REPORTS_ROOT / 'figures' / 'manifest.json').stat().st_size:,} bytes)")

n_feature_jsons = len(list((REPORTS_ROOT / "features").glob("*.json")))
n_pngs = len(list((REPORTS_ROOT / "figures").glob("*.png")))
print(f"  ✓ features/ — {n_feature_jsons} per-feature JSONs")
print(f"  ✓ figures/ — {n_pngs} PNGs")

# Load summary.json and spot-check its contents
with open(REPORTS_ROOT / "summary.json") as f:
    summary_loaded = json.load(f)

print("\n" + "═" * 70)
print("  SUMMARY.JSON HIGHLIGHTS")
print("═" * 70)
print(f"  status: {summary_loaded['status']}")
print(f"  elapsed: {summary_loaded['elapsed_seconds']}s")
print(f"  dataset: {summary_loaded['dataset']['rows']} rows × {summary_loaded['dataset']['columns']} cols")
print(f"  features analyzed: {summary_loaded['counts']['features']}")
print(f"     - categorical: {summary_loaded['counts']['categorical']}")
print(f"     - numeric: {summary_loaded['counts']['numeric']}")
print(f"  hotspot cohorts: {summary_loaded['counts']['hotspot_cohorts']}")
print(f"  strong correlation pairs: {summary_loaded['counts']['strong_pairs']}")
print(f"  imbalance severity: {summary_loaded['class_balance']['imbalance_severity']}")
print(f"  imbalance ratio: {summary_loaded['class_balance']['imbalance_ratio']:.2f}:1")
print(f"  stage 3.5 strategy: {summary_loaded['class_balance']['strategy_label']}")

print("\n  Top numeric predictors:")
for rec in summary_loaded['top_predictors']['numeric'][:3]:
    print(f"    {rec['name']}  Cohen's d={rec['cohens_d']:+.2f}  signal={rec['signal_strength']}  transform={rec['transform']}")

print("\n  Top categorical predictors:")
for rec in summary_loaded['top_predictors']['categorical'][:3]:
    hotspots = rec.get('hotspot_categories', [])
    print(f"    {rec['name']}  Cramér's V={rec['cramers_v']:.3f}  hotspots={hotspots}")

print("\n  Class balance narrative:")
print(f"    {summary_loaded['narratives']['class_balance']}")

print("\n  Cohorts narrative:")
print(f"    {summary_loaded['narratives']['cohorts']}")

# Verify category_churn_rates.json is well-formed (for Stage 3.2 consumption)
with open(REPORTS_ROOT / "category_churn_rates.json") as f:
    rates = json.load(f)
print("\n" + "═" * 70)
print("  category_churn_rates.json (for Stage 3.2)")
print("═" * 70)
for feat, mapping in list(rates["by_feature"].items())[:3]:
    print(f"  {feat}:")
    for cat, rate in mapping.items():
        print(f"    {cat:<30} → {rate*100:.1f}% churn")

print("\n✓ End-to-end smoke test passed.")
