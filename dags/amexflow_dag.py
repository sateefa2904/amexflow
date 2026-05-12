"""
amexflow_dag.py
Apache Airflow DAG — Production orchestration for AmexFlow pipeline.

This DAG defines the full pipeline schedule and task dependencies.
Run locally with: airflow dags trigger amexflow_pipeline
Or install Airflow: pip install apache-airflow

NOTE: This file defines the production-ready DAG structure.
      The pipeline logic lives in src/ and runs identically
      whether triggered by Airflow or directly via pipeline.py.
"""

from datetime import datetime, timedelta

# Airflow imports — install with: pip install apache-airflow
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.email import EmailOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    print("[dag] Airflow not installed — DAG structure defined but not executable locally.")
    print("[dag] Install with: pip install apache-airflow")

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

from generate_data import generate_transactions, save_raw
from validate import run_validation
from etl import run_etl


# ── DAG default arguments ─────────────────────────────────
default_args = {
    "owner": "soli.ateefa",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ── Task callables ────────────────────────────────────────
def task_generate(**context):
    """Stage 1: Generate/ingest raw transaction data."""
    os.makedirs("data", exist_ok=True)
    records = generate_transactions(1000)
    save_raw(records)
    context["ti"].xcom_push(key="record_count", value=len(records))
    print(f"[DAG] Generated {len(records)} records")


def task_validate(**context):
    """Stage 2: Run data quality validation and quarantine bad records."""
    report = run_validation()
    context["ti"].xcom_push(key="pass_rate", value=report["pass_rate_pct"])
    context["ti"].xcom_push(key="sla_met", value=report["sla_met"])

    # SLA enforcement — fail task if quality drops below threshold
    if not report["sla_met"]:
        raise ValueError(
            f"[DAG] SLA BREACH: Pass rate {report['pass_rate_pct']}% "
            f"below 90% threshold. Pipeline halted."
        )
    print(f"[DAG] Validation passed — {report['pass_rate_pct']}% clean")


def task_etl(**context):
    """Stage 3: Transform, enrich, and load clean records to data warehouse."""
    manifest = run_etl()
    print(f"[DAG] ETL complete — {manifest['records_loaded']} records loaded")


def task_report(**context):
    """Stage 4: Generate and log pipeline execution summary."""
    pass_rate = context["ti"].xcom_pull(key="pass_rate", task_ids="validate")
    record_count = context["ti"].xcom_pull(key="record_count", task_ids="generate")
    print(f"[DAG] Pipeline summary — {record_count} records, {pass_rate}% pass rate")


# ── DAG definition ────────────────────────────────────────
if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="amexflow_pipeline",
        default_args=default_args,
        description="AmexFlow: Financial transaction ETL with data quality validation",
        schedule_interval="0 6 * * *",  # Daily at 6am
        catchup=False,
        tags=["finance", "data-engineering", "etl", "data-quality"],
    ) as dag:

        generate = PythonOperator(
            task_id="generate",
            python_callable=task_generate,
        )

        validate = PythonOperator(
            task_id="validate",
            python_callable=task_validate,
        )

        etl = PythonOperator(
            task_id="etl",
            python_callable=task_etl,
        )

        report = PythonOperator(
            task_id="report",
            python_callable=task_report,
        )

        # Pipeline dependency chain: generate → validate → etl → report
        generate >> validate >> etl >> report
