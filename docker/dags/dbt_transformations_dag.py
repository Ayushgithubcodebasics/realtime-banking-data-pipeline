"""
Airflow DAG: dbt Transformations (Silver + Gold layers)
Runs hourly AFTER the bronze load has completed.

Pipeline:
  1. dbt run staging  → create stg_customers, stg_accounts, stg_transactions views
  2. dbt snapshot     → SCD2 tables (snapshots reference staging views so must run after)
  3. dbt run marts    → dim_customers, dim_accounts, fact_transactions
  4. dbt test         → validate data quality
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/banking_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dbt_transformations",
    default_args=default_args,
    description="dbt SCD2 snapshots + staging + marts (Silver & Gold layers)",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dbt", "silver", "gold", "scd2"],
) as dag:

    task_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"{DBT_CMD} run "
            f"--select staging "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
    )

    task_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"{DBT_CMD} snapshot "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
    )

    task_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"{DBT_CMD} run "
            f"--select marts "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
    )

    task_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{DBT_CMD} test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
    )

    task_staging >> task_snapshot >> task_marts >> task_test
    