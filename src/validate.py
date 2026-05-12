"""
validate.py
Data quality framework — enforces schema contracts, null checks,
business rules, and anomaly detection before load.

Mirrors enterprise data governance practices:
- Schema validation (field presence, type enforcement)
- Null / missing value checks
- Business rule validation (amount > 0, valid status codes)
- Statistical anomaly detection (z-score on amount)
- Lineage tagging (each record stamped with validation metadata)
"""

import csv
import json
import uuid
import statistics
from datetime import datetime


SCHEMA = {
    "transaction_id": str,
    "timestamp": str,
    "card_type": str,
    "merchant_name": str,
    "merchant_category": str,
    "amount_usd": float,
    "currency": str,
    "status": str,
    "country_code": str,
    "is_international": str,
    "cardholder_id": str,
}

VALID_STATUSES = {"approved", "declined", "pending"}
VALID_CURRENCIES = {"USD", "EUR", "GBP", "CAD", "AUD"}
VALID_CARD_TYPES = {"Gold", "Platinum", "Green", "Blue Cash"}
ANOMALY_Z_THRESHOLD = 3.0  # flag if amount is >3 std deviations from mean


def load_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def compute_amount_stats(records):
    amounts = []
    for r in records:
        try:
            v = float(r.get("amount_usd", 0))
            if v > 0:
                amounts.append(v)
        except (ValueError, TypeError):
            pass
    if len(amounts) < 2:
        return 0, 1
    return statistics.mean(amounts), statistics.stdev(amounts)


def validate_record(record, mean_amt, std_amt, run_id):
    errors = []
    warnings = []

    # 1. Null checks
    for field in ["transaction_id", "timestamp", "merchant_name",
                  "amount_usd", "card_type", "status"]:
        if not record.get(field):
            errors.append(f"NULL_FIELD:{field}")

    # 2. Amount must be positive
    try:
        amount = float(record.get("amount_usd", 0))
        if amount <= 0:
            errors.append(f"INVALID_AMOUNT:{amount}")
    except (ValueError, TypeError):
        errors.append("UNPARSEABLE_AMOUNT")
        amount = 0

    # 3. Status must be valid
    if record.get("status") not in VALID_STATUSES:
        errors.append(f"INVALID_STATUS:{record.get('status')}")

    # 4. Currency check
    if record.get("currency") not in VALID_CURRENCIES:
        warnings.append(f"UNEXPECTED_CURRENCY:{record.get('currency')}")

    # 5. Timestamp parseable
    try:
        if record.get("timestamp"):
            datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        errors.append(f"INVALID_TIMESTAMP:{record.get('timestamp')}")

    # 6. Anomaly detection — z-score on amount
    if std_amt > 0 and amount > 0:
        z = abs((amount - mean_amt) / std_amt)
        if z > ANOMALY_Z_THRESHOLD:
            warnings.append(f"AMOUNT_ANOMALY:z={z:.2f}")

    # Lineage metadata
    record["_run_id"] = run_id
    record["_validated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    record["_validation_errors"] = "|".join(errors) if errors else ""
    record["_validation_warnings"] = "|".join(warnings) if warnings else ""
    record["_is_valid"] = len(errors) == 0

    return record, errors, warnings


def run_validation(input_path="data/raw_transactions.csv",
                   clean_path="data/clean_transactions.csv",
                   quarantine_path="data/quarantine_transactions.csv",
                   report_path="data/validation_report.json"):

    run_id = str(uuid.uuid4())[:8]
    print(f"\n[validate] Run ID: {run_id}")
    print(f"[validate] Loading records from {input_path}...")

    records = load_csv(input_path)
    print(f"[validate] {len(records)} records loaded")

    mean_amt, std_amt = compute_amount_stats(records)
    print(f"[validate] Amount stats — mean: ${mean_amt:.2f}, std: ${std_amt:.2f}")

    clean, quarantine = [], []
    total_errors, total_warnings = 0, 0
    error_counts = {}

    for record in records:
        validated, errors, warnings = validate_record(
            record, mean_amt, std_amt, run_id
        )
        total_warnings += len(warnings)
        if errors:
            total_errors += len(errors)
            for e in errors:
                error_counts[e.split(":")[0]] = error_counts.get(e.split(":")[0], 0) + 1
            quarantine.append(validated)
        else:
            clean.append(validated)

    # Write clean
    if clean:
        fields = list(clean[0].keys())
        with open(clean_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(clean)

    # Write quarantine
    if quarantine:
        fields = list(quarantine[0].keys())
        with open(quarantine_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(quarantine)

    # Validation report
    report = {
        "run_id": run_id,
        "executed_at": datetime.now().isoformat(),
        "input_file": input_path,
        "total_records": len(records),
        "clean_records": len(clean),
        "quarantined_records": len(quarantine),
        "pass_rate_pct": round(len(clean) / len(records) * 100, 2),
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "error_breakdown": error_counts,
        "sla_target_pass_rate_pct": 90.0,
        "sla_met": (len(clean) / len(records) * 100) >= 90.0,
    }

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n[validate]  Clean: {len(clean)} |  Quarantined: {len(quarantine)}")
    print(f"[validate] Pass rate: {report['pass_rate_pct']}% | SLA met: {report['sla_met']}")
    print(f"[validate] Report saved to {report_path}")
    return report


if __name__ == "__main__":
    run_validation()
