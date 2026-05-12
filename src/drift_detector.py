"""
drift_detector.py
Schema Drift & Distribution Drift Detector.

Two types of drift detected:
  1. Schema drift    — new/missing columns vs. established baseline
  2. Distribution drift — statistical shifts in key metrics vs. baseline

On first run: establishes baseline (mean, std, distribution fingerprint).
On subsequent runs: compares current run against baseline and alerts.

This solves a real production problem: silent data quality degradation
where pipelines keep running but data quietly becomes unreliable.
"""

import csv
import json
import statistics
import os
from datetime import datetime
from collections import Counter

BASELINE_PATH = "data/drift_baseline.json"
DRIFT_REPORT_PATH = "data/drift_report.json"

# Thresholds for drift alerts
MEAN_DRIFT_THRESHOLD    = 0.15   # 15% shift in mean = alert
STD_DRIFT_THRESHOLD     = 0.25   # 25% shift in std = alert
CATEGORY_DRIFT_THRESHOLD = 0.10  # 10% shift in category distribution = alert
NULL_RATE_THRESHOLD     = 0.05   # >5% null rate on required fields = alert

NUMERIC_FIELDS   = ["amount_usd", "cardholder_avg_spend"]
CATEGORICAL_FIELDS = ["merchant_category", "card_type", "status", "country_code"]
REQUIRED_FIELDS  = ["transaction_id", "timestamp", "merchant_name", "amount_usd"]


def compute_profile(records):
    """Compute statistical profile of current dataset."""
    profile = {
        "computed_at": datetime.now().isoformat(),
        "record_count": len(records),
        "numeric_stats": {},
        "categorical_distributions": {},
        "null_rates": {},
        "schema_columns": list(records[0].keys()) if records else [],
    }

    # Numeric stats
    for field in NUMERIC_FIELDS:
        vals = []
        for r in records:
            try:
                v = float(r.get(field, ""))
                if v > 0:
                    vals.append(v)
            except (ValueError, TypeError):
                pass
        if len(vals) >= 2:
            profile["numeric_stats"][field] = {
                "mean": round(statistics.mean(vals), 4),
                "std": round(statistics.stdev(vals), 4),
                "min": round(min(vals), 4),
                "max": round(max(vals), 4),
                "count": len(vals),
            }

    # Categorical distributions
    for field in CATEGORICAL_FIELDS:
        counts = Counter(r.get(field, "MISSING") for r in records)
        total = sum(counts.values())
        profile["categorical_distributions"][field] = {
            k: round(v / total, 4) for k, v in counts.most_common()
        }

    # Null rates
    for field in REQUIRED_FIELDS:
        null_count = sum(1 for r in records if not r.get(field))
        profile["null_rates"][field] = round(null_count / len(records), 4)

    return profile


def detect_drift(baseline, current):
    """Compare current profile against baseline. Return drift findings."""
    alerts = []
    warnings = []

    # 1. Schema drift — columns added or removed
    baseline_cols = set(baseline.get("schema_columns", []))
    current_cols  = set(current.get("schema_columns", []))
    added   = current_cols - baseline_cols
    removed = baseline_cols - current_cols
    if added:
        alerts.append(f"SCHEMA_DRIFT: New columns detected: {sorted(added)}")
    if removed:
        alerts.append(f"SCHEMA_DRIFT: Columns removed: {sorted(removed)}")

    # 2. Numeric distribution drift
    for field, b_stats in baseline.get("numeric_stats", {}).items():
        c_stats = current.get("numeric_stats", {}).get(field)
        if not c_stats:
            alerts.append(f"MISSING_FIELD_STATS: {field} has no stats in current run")
            continue

        b_mean, c_mean = b_stats["mean"], c_stats["mean"]
        if b_mean > 0:
            mean_drift = abs(c_mean - b_mean) / b_mean
            if mean_drift > MEAN_DRIFT_THRESHOLD:
                alerts.append(
                    f"DISTRIBUTION_DRIFT: {field} mean shifted {mean_drift:.1%} "
                    f"(baseline=${b_mean:.2f} → current=${c_mean:.2f})"
                )

        b_std, c_std = b_stats["std"], c_stats["std"]
        if b_std > 0:
            std_drift = abs(c_std - b_std) / b_std
            if std_drift > STD_DRIFT_THRESHOLD:
                warnings.append(
                    f"VOLATILITY_SHIFT: {field} std shifted {std_drift:.1%} "
                    f"(baseline={b_std:.2f} → current={c_std:.2f})"
                )

    # 3. Categorical distribution drift
    for field, b_dist in baseline.get("categorical_distributions", {}).items():
        c_dist = current.get("categorical_distributions", {}).get(field, {})
        for category, b_freq in b_dist.items():
            c_freq = c_dist.get(category, 0)
            drift = abs(c_freq - b_freq)
            if drift > CATEGORY_DRIFT_THRESHOLD:
                warnings.append(
                    f"CATEGORY_DRIFT: {field}[{category}] "
                    f"shifted {drift:.1%} (baseline={b_freq:.1%} → current={c_freq:.1%})"
                )

    # 4. Null rate changes
    for field, b_null in baseline.get("null_rates", {}).items():
        c_null = current.get("null_rates", {}).get(field, 0)
        if c_null > NULL_RATE_THRESHOLD and c_null > b_null * 1.5:
            alerts.append(
                f"NULL_RATE_SPIKE: {field} null rate {c_null:.1%} "
                f"(baseline={b_null:.1%})"
            )

    return alerts, warnings


def run_drift_detection(clean_path="data/clean_transactions.csv"):
    print(f"\n[drift] Loading records from {clean_path}...")
    with open(clean_path, newline="") as f:
        records = list(csv.DictReader(f))
    print(f"[drift] {len(records)} records loaded")

    current_profile = compute_profile(records)
    is_first_run = not os.path.exists(BASELINE_PATH)

    if is_first_run:
        # First run — establish baseline
        with open(BASELINE_PATH, "w") as f:
            json.dump(current_profile, f, indent=2)
        print(f"[drift] Baseline established ({len(records)} records)")
        print(f"[drift] Baseline saved to {BASELINE_PATH}")
        report = {
            "run_type": "baseline_established",
            "computed_at": current_profile["computed_at"],
            "record_count": len(records),
            "alerts": [],
            "warnings": [],
            "drift_detected": False,
        }
    else:
        # Subsequent run — compare against baseline
        with open(BASELINE_PATH) as f:
            baseline = json.load(f)

        alerts, warnings = detect_drift(baseline, current_profile)
        drift_detected = len(alerts) > 0

        report = {
            "run_type": "drift_check",
            "computed_at": current_profile["computed_at"],
            "baseline_date": baseline.get("computed_at", "unknown"),
            "record_count": len(records),
            "alerts": alerts,
            "warnings": warnings,
            "drift_detected": drift_detected,
            "alert_count": len(alerts),
            "warning_count": len(warnings),
        }

        if drift_detected:
            print(f"[drift] DRIFT DETECTED — {len(alerts)} alerts, {len(warnings)} warnings")
            for a in alerts:
                print(f"[drift]   ALERT: {a}")
        else:
            print(f"[drift] No drift detected vs baseline")

        if warnings:
            for w in warnings:
                print(f"[drift]   WARNING: {w}")

    with open(DRIFT_REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2)

    print(f"[drift] Report saved to {DRIFT_REPORT_PATH}")
    return report


if __name__ == "__main__":
    run_drift_detection()
