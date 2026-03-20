"""
crypto_pipeline_dag.py
-----------------------
Apache Airflow DAG that orchestrates the full ETL pipeline:
    1. Extract  → Fetch data from CoinGecko API
    2. Transform → Clean and validate the data
    3. Load      → Insert into PostgreSQL
    4. Notify    → Log a summary of the pipeline run

Schedule: Every 6 hours
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Make the etl package importable inside Airflow
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from etl.extract import fetch_crypto_data
from etl.transform import transform_crypto_data, get_summary_stats
from etl.load import insert_crypto_records


# ─── Default Args ────────────────────────────────────────────────────────────

default_args = {
    "owner": "lamla",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ─── Task Functions ───────────────────────────────────────────────────────────

def extract_task(**context):
    """Extract raw data from CoinGecko and push to XCom."""
    raw_data = fetch_crypto_data()
    context["ti"].xcom_push(key="raw_data", value=raw_data)
    print(f"[DAG] Extracted {len(raw_data)} raw records.")


def transform_task(**context):
    """Pull raw data from XCom, transform, and push clean records."""
    raw_data = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    clean_data = transform_crypto_data(raw_data)
    summary = get_summary_stats(clean_data)
    context["ti"].xcom_push(key="clean_data", value=clean_data)
    context["ti"].xcom_push(key="summary", value=summary)
    print(f"[DAG] Transformed {len(clean_data)} records.")


def load_task(**context):
    """Pull clean data from XCom and insert into PostgreSQL."""
    clean_data = context["ti"].xcom_pull(key="clean_data", task_ids="transform")

    # Convert fetched_at back to datetime (XCom serialises it as string)
    from datetime import timezone
    for record in clean_data:
        if isinstance(record["fetched_at"], str):
            record["fetched_at"] = datetime.fromisoformat(record["fetched_at"])

    rows_inserted = insert_crypto_records(clean_data)
    print(f"[DAG] Loaded {rows_inserted} rows into PostgreSQL.")


def notify_task(**context):
    """Log a summary of the pipeline run."""
    summary = context["ti"].xcom_pull(key="summary", task_ids="transform")
    print("=" * 50)
    print("✅ CRYPTO ETL PIPELINE RUN COMPLETE")
    print(f"   Total coins:   {summary.get('total_coins')}")
    print(f"   Avg price:     ${summary.get('avg_price_usd')}")
    print(f"   Top gainer:    {summary.get('top_gainer')}")
    print(f"   Top loser:     {summary.get('top_loser')}")
    print(f"   Batch time:    {summary.get('batch_time')}")
    print("=" * 50)


# ─── DAG Definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="crypto_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline: CoinGecko → Transform → PostgreSQL",
    schedule_interval="0 */6 * * *",   # Every 6 hours
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "etl", "data-engineering"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    notify = PythonOperator(
        task_id="notify",
        python_callable=notify_task,
    )

    # ─── Task Dependencies ────────────────────────────────────────────────────
    extract >> transform >> load >> notify
  
