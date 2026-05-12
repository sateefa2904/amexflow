# AmexFlow — Data Governance Documentation

## Overview
AmexFlow is a production-grade financial transaction ETL pipeline built to demonstrate enterprise data engineering practices including data governance, lineage tracking, quality SLAs, and ML-ready feature engineering.

**Stack:** Python · SQLite (PostgreSQL/Spark-compatible) · Airflow (DAG defined) · pytest

---

## Data Catalog

### Table: `transactions` (target)
| Column | Type | Description | PII |
|---|---|---|---|
| transaction_id | TEXT | Unique transaction UUID | No |
| timestamp | TEXT | Transaction datetime (UTC) | No |
| card_type | TEXT | Amex card product tier | No |
| merchant_name | TEXT | Merchant name | No |
| merchant_category | TEXT | Merchant category code | No |
| amount_usd | REAL | Transaction amount in USD | No |
| currency | TEXT | ISO 4217 currency code | No |
| status | TEXT | approved / declined / pending | No |
| country_code | TEXT | ISO 3166 country code | No |
| is_international | INTEGER | 1 if cross-border transaction | No |
| cardholder_id | TEXT | Anonymized cardholder ID | Pseudonymized |
| amount_bucket | TEXT | Enriched: micro/small/medium/large | No |
| is_high_value | INTEGER | Enriched: 1 if amount ≥ $1000 | No |
| load_date | TEXT | Date record was loaded | No |
| _run_id | TEXT | Pipeline run identifier | No |
| _validated_at | TEXT | Validation timestamp | No |
| _loaded_at | TEXT | Load timestamp | No |

### Table: `data_lineage`
Tracks every pipeline execution — what was loaded, when, from where.

### Table: `validation_summary`
Stores pass rates, SLA status, and error counts per run for audit trail.

---

## Data Contract

```yaml
contract:
  name: financial_transactions_v1
  owner: soli.ateefa
  sla:
    pass_rate_minimum: 90%
    freshness_slo: daily by 07:00 UTC
  schema:
    transaction_id: required, unique, string
    timestamp: required, format YYYY-MM-DD HH:MM:SS
    amount_usd: required, float, > 0
    status: required, enum [approved, declined, pending]
    currency: string, enum [USD, EUR, GBP, CAD, AUD]
  quality_rules:
    - no null transaction_id
    - no null merchant_name
    - amount must be positive
    - timestamp must be parseable
    - status must be valid enum value
  anomaly_detection:
    method: z-score
    threshold: 3.0 standard deviations on amount_usd
    action: flag as warning, allow through to warehouse
```

---

## Data Lineage

```
[Source: Raw CSV]
        │
        ▼
[Stage 1: generate_data.py]
  → Generates 1,000 synthetic transaction records
  → Output: data/raw_transactions.csv
        │
        ▼
[Stage 2: validate.py]
  → Schema validation (null checks, type enforcement)
  → Business rule validation (amount > 0, valid status)
  → Anomaly detection (z-score on amount_usd)
  → Lineage tagging (_run_id, _validated_at)
  → Output: data/clean_transactions.csv
  → Quarantine: data/quarantine_transactions.csv
  → Report: data/validation_report.json
        │
        ▼
[Stage 3: etl.py]
  → Data enrichment (amount_bucket, is_high_value)
  → Idempotent upsert to SQLite (transactions table)
  → Lineage written to data_lineage table
  → Output: data/amexflow.db
        │
        ▼
[Stage 4: Airflow DAG (dags/amexflow_dag.py)]
  → Scheduled daily at 06:00 UTC
  → SLA enforcement — halts pipeline if pass rate < 90%
  → XCom metadata passed between tasks
  → Email alerting on failure
```

---

## Running the Pipeline

```bash
# Install dependencies
pip install pytest

# Run the full pipeline
cd src
python pipeline.py

# Run unit tests
cd ..
python -m pytest tests/ -v

# Expected output:
# 1000 records generated
# ~920 clean | ~80 quarantined
# Pass rate: ~92% | SLA: ✅ met
# DB written to data/amexflow.db
```

---

## Production Scaling Notes

| Component | Local (this repo) | Production equivalent |
|---|---|---|
| Data store | SQLite | PostgreSQL / Snowflake |
| Processing | Pandas | Apache Spark (PySpark) |
| Orchestration | pipeline.py | Apache Airflow DAG |
| Data volume | 1,000 rows | Millions of rows/day |
| Alerting | Console logs | Email / PagerDuty |
| Feature store | Enriched columns | Feast / Tecton |

The pipeline is architected for drop-in Spark compatibility — `pandas` DataFrames can be replaced with `pyspark.sql.DataFrame` with minimal code changes.

---

## ML Pipeline Support

The `is_high_value` and `amount_bucket` columns serve as pre-engineered features for downstream ML models (e.g., fraud detection, spend categorization). The validation layer enforces training data quality by quarantining records with missing or anomalous values before they corrupt model inputs.

---

*Built by Soli Ateefa | github.com/sateefa2904*
