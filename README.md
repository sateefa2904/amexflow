# AmexFlow — Financial Transaction Intelligence Pipeline

> A 5-stage Python ETL pipeline built to enterprise data engineering standards, featuring a fraud signal engine, schema drift detection, and ML-ready feature store output.

## What It Does

Ingests raw financial transactions → validates quality against a data contract → detects schema & distribution drift → scores each transaction for fraud risk → loads clean, enriched data to a warehouse with full lineage tracking.

## Stack
`Python` · `SQLite` · `Airflow (DAG)` · `pytest` · `CSV/JSON`  
Production-equivalent: `Apache Spark` · `PostgreSQL` · `Airflow` · `AWS S3`

## Pipeline Architecture
```
Raw Data (2,000 txns)
    │
    ▼
Stage 2: Data Quality Validation
    → Null checks, business rules, z-score anomaly detection
    → Quarantine bad records | SLA enforcement (≥90% pass rate)
    │
    ▼
Stage 3: Schema & Distribution Drift Detection
    → Baseline fingerprinting on first run
    → Alerts on mean shift >15%, null rate spikes, schema changes
    │
    ▼
Stage 4: Fraud Signal Engine
    → Velocity detection (N+ txns in 5-min window)
    → Geographic impossibility (haversine distance math)
    → Category deviation (vs. cardholder spend history)
    → Spend outlier (z-score vs. cardholder baseline)
    → 0–100 risk score → LOW / MEDIUM / HIGH / CRITICAL tier
    │
    ▼
Stage 5: ETL Load
    → Idempotent upsert to SQLite (PostgreSQL-ready)
    → Full lineage written per run
    → ML feature store columns exported
```

## Key Features
- **Fraud Signal Engine** — 4 detection algorithms with no lookahead bias; history built incrementally before each record is scored
- **Geographic Impossibility** — haversine distance math detects physically impossible travel (>3,000km in <2 hours)
- **Drift Detection** — statistical baseline fingerprinting; alerts on distribution shifts, schema changes, null rate spikes
- **Risk Scoring** — weighted 0–100 fraud score with tiered output (LOW/MEDIUM/HIGH/CRITICAL)
- **ML Feature Store** — binary signal flags and risk scores exported as clean feature columns for downstream models
- **Data Governance** — full data catalog, data contracts, lineage tracking per run (see `docs/DATA_GOVERNANCE.md`)
- **SLA Enforcement** — pipeline halts automatically if data quality drops below 90% pass rate
- **Idempotent Loads** — safe to re-run without creating duplicates
- **Airflow DAG** — production orchestration defined with daily schedule and SLA alerting

## Sample Output
```
Records generated       : 2,000
Clean / loaded          : 1,944
Quarantined             : 56
Data quality pass rate  : 97.2%  ✅ SLA met
Drift detected          : NO     ✅
Flagged for review      : 9 (0.46%)
Signal breakdown:
  CATEGORY_DEVIATION    : 527
  SPEND_OUTLIER         : 51
  GEO_IMPOSSIBLE        : 49
Risk tiers: LOW=1872 | MEDIUM=63 | HIGH=9 | CRITICAL=0
```

## Quick Start
```bash
cd src
python3 pipeline.py
```

## Run Tests
```bash
pip install pytest
python -m pytest tests/ -v
```

## Project Structure
```
amexflow/
├── src/
│   ├── generate_data.py    # Synthetic data with injected fraud patterns
│   ├── validate.py         # Data quality framework + SLA enforcement
│   ├── drift_detector.py   # Schema & distribution drift detection
│   ├── fraud_engine.py     # 4-signal fraud detection + risk scoring
│   ├── etl.py              # Transform, enrich & load with lineage
│   └── pipeline.py         # Full orchestrator
├── dags/
│   └── amexflow_dag.py     # Production Airflow DAG
├── tests/
│   └── test_validate.py    # Unit tests for validation logic
└── docs/
    └── DATA_GOVERNANCE.md  # Data catalog, contracts, lineage docs
```

---
*Soli Ateefa · [Portfolio](https://sateefa2904.github.io/soli-portfolio/) · [LinkedIn](https://www.linkedin.com/in/solia)*git add README.md
