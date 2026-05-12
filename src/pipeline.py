"""
pipeline.py — AmexFlow v2
Full pipeline orchestrator:
  Stage 1: Generate raw transaction data with realistic fraud patterns
  Stage 2: Data quality validation + quarantine
  Stage 3: Schema & distribution drift detection
  Stage 4: Fraud signal engine — velocity, geo, category, spend outlier
  Stage 5: ETL load to SQLite with full lineage
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from generate_data import generate_transactions, save_raw
from validate import run_validation
from drift_detector import run_drift_detection
from fraud_engine import run_fraud_engine
from etl import run_etl


def run_pipeline():
    print("=" * 60)
    print("  AmexFlow v2 — Financial Transaction Intelligence Pipeline")
    print("  Fraud Detection | Drift Monitoring | Data Governance")
    print("=" * 60)

    os.makedirs("data", exist_ok=True)

    print("\n STAGE 1: Data Generation (with fraud pattern injection)")
    records = generate_transactions(2000)
    save_raw(records)

    print("\n STAGE 2: Data Quality Validation")
    val_report = run_validation(
        input_path="data/raw_transactions.csv",
        clean_path="data/clean_transactions.csv",
        quarantine_path="data/quarantine_transactions.csv",
        report_path="data/validation_report.json"
    )

    print("\n STAGE 3: Schema & Distribution Drift Detection")
    drift_report = run_drift_detection(clean_path="data/clean_transactions.csv")

    print("\n STAGE 4: Fraud Signal Engine")
    fraud_report = run_fraud_engine(
        clean_path="data/clean_transactions.csv",
        output_path="data/fraud_scored_transactions.csv",
        report_path="data/fraud_report.json"
    )

    print("\n STAGE 5: ETL Load")
    manifest = run_etl(
        clean_path="data/fraud_scored_transactions.csv",
        report_path="data/validation_report.json"
    )

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE — EXECUTIVE SUMMARY")
    print("=" * 60)
    print(f"  Records generated          : {val_report['total_records']:,}")
    print(f"  Clean / loaded             : {manifest['records_loaded']:,}")
    print(f"  Quarantined (quality)      : {val_report['quarantined_records']:,}")
    print(f"  Data quality pass rate     : {val_report['pass_rate_pct']}%")
    print(f"  SLA met (≥90%)             : {' YES' if val_report['sla_met'] else ' NO'}")
    print(f"  Drift detected             : {' YES' if drift_report.get('drift_detected') else ' NO'}")
    print(f"  Transactions flagged       : {fraud_report['flagged_for_review']:,} ({fraud_report['flag_rate_pct']}%)")
    print(f"  HIGH/CRITICAL risk         : {fraud_report['high_critical_count']:,}")
    print(f"  Fraud signals fired:")
    for signal, count in fraud_report['signal_breakdown'].items():
        print(f"    {signal:<25}: {count:,}")
    print(f"  Risk tier distribution     : {fraud_report['risk_tier_distribution']}")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()
