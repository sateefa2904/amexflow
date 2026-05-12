# AmexFlow — Financial Transaction Data Pipeline

> End-to-end ETL pipeline with data governance, quality validation, and ML-ready feature engineering — built to enterprise data engineering standards.

## What It Does
Ingests raw financial transaction data → validates quality against a data contract → quarantines bad records → enriches and loads clean data to a warehouse → tracks full lineage end-to-end.

## Stack
`Python` · `SQLite` · `pytest` · `Airflow (DAG)` · `CSV/JSON`  
Production-equivalent: `Apache Spark` · `PostgreSQL` · `Airflow` · `AWS S3`

## Pipeline Architecture
```
Raw Data → Validation → ETL Transform → SQLite Warehouse
              ↓                              ↓
          Quarantine                   Lineage Tracking
```

## Key Features
- **Data Quality Framework** — null checks, business rules, z-score anomaly detection
- **Data Governance** — full data catalog, data contracts, lineage tracking per run
- **SLA Enforcement** — pipeline halts if pass rate drops below 90%
- **Idempotent Loads** — safe to re-run without creating duplicates
- **ML-Ready** — feature engineering (amount buckets, high-value flags) for downstream models
- **Airflow DAG** — production orchestration defined, daily schedule, SLA alerting
- **14 Unit Tests** — full coverage of validation logic via pytest

## Quick Start
```bash
cd src
python pipeline.py
```

## Run Tests
```bash
python -m pytest tests/ -v
```

## Project Structure
```
amexflow/
├── src/
│   ├── generate_data.py   # Synthetic data generation
│   ├── validate.py        # Data quality framework
│   ├── etl.py             # Transform & load with lineage
│   └── pipeline.py        # Full pipeline orchestrator
├── dags/
│   └── amexflow_dag.py    # Airflow production DAG
├── tests/
│   └── test_validate.py   # 14 pytest unit tests
├── docs/
│   └── DATA_GOVERNANCE.md # Data catalog, contracts, lineage
└── data/                  # Generated at runtime
```

---
*Soli Ateefa · [Portfolio](https://sateefa2904.github.io/soli-portfolio/) · [LinkedIn](https://www.linkedin.com/in/solia)*
