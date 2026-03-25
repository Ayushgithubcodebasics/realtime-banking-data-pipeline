"""Airflow DAG: dbt transformations."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/banking_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dbt_transformations",
    default_args=DEFAULT_ARGS,
    description="Run staging, snapshots, marts, and tests",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dbt", "silver", "gold"],
) as dag:
    task_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"{DBT_CMD} run --select staging --profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
    )

    task_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"{DBT_CMD} snapshot --profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
    )

    task_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"{DBT_CMD} run --select marts --profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
    )

    task_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{DBT_CMD} test --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_DIR}"
        ),
    )

    task_staging >> task_snapshot >> task_marts >> task_test
