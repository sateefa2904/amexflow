"""
etl.py
Transform and Load layer — cleans, enriches, and loads validated
transaction records into SQLite (production equivalent: PostgreSQL/Spark).

Demonstrates:
- Schema evolution handling (graceful unknown column management)
- Data enrichment / feature engineering
- Idempotent upsert pattern (safe to re-run)
- Full lineage metadata preserved on every record
- Data lifecycle management via load manifest
"""

import csv
import json
import sqlite3
import uuid
from datetime import datetime


DB_PATH = "data/amexflow.db"
LOAD_MANIFEST_PATH = "data/load_manifest.json"


def get_connection(db_path=DB_PATH):
    return sqlite3.connect(db_path)


def create_schema(conn):
    """
    Create target tables. Idempotent — safe to run repeatedly.
    Mirrors a production data warehouse fact/dim pattern.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id      TEXT PRIMARY KEY,
            timestamp           TEXT,
            card_type           TEXT,
            merchant_name       TEXT,
            merchant_category   TEXT,
            amount_usd          REAL,
            currency            TEXT,
            status              TEXT,
            country_code        TEXT,
            is_international    INTEGER,
            cardholder_id       TEXT,
            -- Enriched columns
            amount_bucket       TEXT,
            is_high_value       INTEGER,
            load_date           TEXT,
            -- Lineage
            _run_id             TEXT,
            _validated_at       TEXT,
            _loaded_at          TEXT
        );

        CREATE TABLE IF NOT EXISTS data_lineage (
            lineage_id      TEXT PRIMARY KEY,
            run_id          TEXT,
            stage           TEXT,
            input_file      TEXT,
            output_table    TEXT,
            records_loaded  INTEGER,
            loaded_at       TEXT,
            notes           TEXT
        );

        CREATE TABLE IF NOT EXISTS validation_summary (
            run_id              TEXT PRIMARY KEY,
            executed_at         TEXT,
            total_records       INTEGER,
            clean_records       INTEGER,
            quarantined_records INTEGER,
            pass_rate_pct       REAL,
            sla_met             INTEGER
        );
    """)
    conn.commit()


def enrich(record):
    """Feature engineering / business rule enrichment."""
    try:
        amount = float(record.get("amount_usd", 0))
    except (ValueError, TypeError):
        amount = 0.0

    # Amount bucketing — useful for ML feature stores
    if amount < 50:
        bucket = "micro"
    elif amount < 200:
        bucket = "small"
    elif amount < 1000:
        bucket = "medium"
    else:
        bucket = "large"

    record["amount_bucket"] = bucket
    record["is_high_value"] = 1 if amount >= 1000 else 0
    record["load_date"] = datetime.now().strftime("%Y-%m-%d")
    record["_loaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    record["is_international"] = 1 if str(record.get("is_international", "")).lower() == "true" else 0
    return record


def load_records(conn, records):
    """Idempotent upsert — re-running won't create duplicates."""
    loaded = 0
    skipped = 0
    for record in records:
        record = enrich(record)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO transactions (
                    transaction_id, timestamp, card_type, merchant_name,
                    merchant_category, amount_usd, currency, status,
                    country_code, is_international, cardholder_id,
                    amount_bucket, is_high_value, load_date,
                    _run_id, _validated_at, _loaded_at
                ) VALUES (
                    :transaction_id, :timestamp, :card_type, :merchant_name,
                    :merchant_category, :amount_usd, :currency, :status,
                    :country_code, :is_international, :cardholder_id,
                    :amount_bucket, :is_high_value, :load_date,
                    :_run_id, :_validated_at, :_loaded_at
                )
            """, record)
            loaded += 1
        except Exception as e:
            print(f"[etl] Skipped {record.get('transaction_id')}: {e}")
            skipped += 1
    conn.commit()
    return loaded, skipped


def write_lineage(conn, run_id, input_file, records_loaded, notes=""):
    conn.execute("""
        INSERT OR REPLACE INTO data_lineage
        (lineage_id, run_id, stage, input_file, output_table, records_loaded, loaded_at, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        str(uuid.uuid4()), run_id, "ETL_LOAD",
        input_file, "transactions", records_loaded,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), notes
    ))
    conn.commit()


def load_validation_summary(conn, report_path="data/validation_report.json"):
    try:
        with open(report_path) as f:
            r = json.load(f)
        conn.execute("""
            INSERT OR REPLACE INTO validation_summary
            (run_id, executed_at, total_records, clean_records,
             quarantined_records, pass_rate_pct, sla_met)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            r["run_id"], r["executed_at"], r["total_records"],
            r["clean_records"], r["quarantined_records"],
            r["pass_rate_pct"], 1 if r["sla_met"] else 0
        ))
        conn.commit()
    except Exception as e:
        print(f"[etl] Could not load validation summary: {e}")


def run_etl(clean_path="data/clean_transactions.csv",
            report_path="data/validation_report.json"):

    print(f"\n[etl] Starting ETL load — {datetime.now().isoformat()}")

    with open(clean_path, newline="") as f:
        records = list(csv.DictReader(f))
    print(f"[etl] {len(records)} clean records ready for load")

    conn = get_connection()
    create_schema(conn)
    load_validation_summary(conn, report_path)

    run_id = records[0].get("_run_id", "unknown") if records else "unknown"
    loaded, skipped = load_records(conn, records)
    write_lineage(conn, run_id, clean_path, loaded,
                  notes="Full load from validation pipeline")

    # Load manifest
    manifest = {
        "run_id": run_id,
        "loaded_at": datetime.now().isoformat(),
        "records_loaded": loaded,
        "records_skipped": skipped,
        "target_db": DB_PATH,
        "target_table": "transactions",
    }
    with open(LOAD_MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"[etl] Loaded: {loaded} | Skipped: {skipped}")
    print(f"[etl] Manifest saved to {LOAD_MANIFEST_PATH}")
    conn.close()
    return manifest


if __name__ == "__main__":
    run_etl()
