"""
Airflow DAG: MinIO → Snowflake Bronze Load
Runs every 5 minutes. Downloads Parquet files from MinIO and loads them
into the RAW (Bronze) tables in Snowflake using PUT + COPY INTO.
After a successful load it automatically triggers the dbt_transformations DAG.

Key fixes over the original:
  - All env vars read from os.environ (set via docker-compose environment:)
    No dotenv inside Airflow containers - env vars are injected by Docker
  - Uses pyarrow to read Parquet (fastparquet was removed)
  - Uses COPY INTO with MATCH_BY_COLUMN_NAME so column order doesn't matter
  - Tracks which files have been processed to avoid duplicate loads
  - schedule is used instead of deprecated schedule_interval
  - provide_context=True removed (deprecated in Airflow 2.x)
  - Proper XCom return from download task
  - Triggers dbt_transformations DAG after every successful bronze load
"""

import io
import os
import tempfile
from datetime import datetime, timedelta

import boto3
import pyarrow.parquet as pq
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ── Config from env (injected via docker-compose environment:) ─
MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
BUCKET           = os.environ["MINIO_BUCKET"]

SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DB        = os.environ["SNOWFLAKE_DB"]
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

RAW_SCHEMA = "RAW"
TABLES = ["customers", "accounts", "transactions"]

# ── Helper: Snowflake connection ──────────────────────────────
def get_snowflake_conn():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=RAW_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )


# ── Helper: MinIO/S3 client ───────────────────────────────────
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


# ── Task 1: List unprocessed files in MinIO ───────────────────
def list_new_files(**context) -> dict[str, list[str]]:
    """
    Lists all parquet files in MinIO for each table.
    Tracks already-processed keys via Airflow Variable.
    Returns dict of {table: [s3_key, ...]} for unprocessed files.
    """
    from airflow.models import Variable

    s3 = get_s3_client()
    new_files: dict[str, list[str]] = {}

    for table in TABLES:
        var_key = f"processed_keys_{table}"
        try:
            processed = set(Variable.get(var_key, deserialize_json=True))
        except Exception:
            processed = set()

        prefix = f"{table}/"
        paginator = s3.get_paginator("list_objects_v2")
        all_keys = []
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet") and key not in processed:
                    all_keys.append(key)

        new_files[table] = sorted(all_keys)
        print(f"  [{table}] {len(all_keys)} new file(s) to load")

    return new_files


# ── Task 2: Load files into Snowflake ────────────────────────
def load_to_snowflake(**context) -> None:
    """
    Downloads each Parquet file from MinIO, uploads it to a Snowflake
    internal stage, and runs COPY INTO.
    Uses MATCH_BY_COLUMN_NAME so column order is irrelevant.
    Marks loaded files as processed in Airflow Variables.
    """
    from airflow.models import Variable

    new_files: dict[str, list[str]] = context["ti"].xcom_pull(
        task_ids="list_new_files"
    )
    if not new_files or not any(new_files.values()):
        print("No new files to load — skipping")
        return

    s3 = get_s3_client()
    conn = get_snowflake_conn()
    cur = conn.cursor()

    try:
        for table, keys in new_files.items():
            if not keys:
                continue

            cur.execute(f"USE SCHEMA {SNOWFLAKE_DB}.{RAW_SCHEMA}")
            loaded_keys = []

            for s3_key in keys:
                buf = io.BytesIO()
                s3.download_fileobj(BUCKET, s3_key, buf)
                buf.seek(0)

                arrow_table = pq.read_table(buf)
                row_count = len(arrow_table)
                buf.seek(0)

                with tempfile.NamedTemporaryFile(
                    suffix=".parquet", delete=False
                ) as tmp:
                    tmp.write(buf.read())
                    tmp_path = tmp.name

                try:
                    put_sql = (
                        f"PUT 'file://{tmp_path}' @%{table} "
                        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                    )
                    cur.execute(put_sql)

                    copy_sql = f"""
                        COPY INTO {table}
                        FROM @%{table}
                        FILE_FORMAT = (
                            TYPE = PARQUET
                            SNAPPY_COMPRESSION = FALSE
                        )
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                        ON_ERROR = CONTINUE
                        PURGE = TRUE
                    """
                    cur.execute(copy_sql)

                    print(
                        f"  ✅ Loaded {row_count} rows → "
                        f"{SNOWFLAKE_DB}.{RAW_SCHEMA}.{table} "
                        f"[{s3_key.split('/')[-1]}]"
                    )
                    loaded_keys.append(s3_key)

                finally:
                    os.unlink(tmp_path)

            var_key = f"processed_keys_{table}"
            try:
                existing = set(Variable.get(var_key, deserialize_json=True))
            except Exception:
                existing = set()
            Variable.set(
                var_key,
                list(existing | set(loaded_keys)),
                serialize_json=True,
            )

    finally:
        cur.close()
        conn.close()


# ── DAG definition ────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="minio_to_snowflake_bronze",
    default_args=default_args,
    description="Load new MinIO parquet files into Snowflake RAW (Bronze) tables",
    schedule="*/5 * * * *",   # every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "minio", "snowflake"],
) as dag:

    task_list = PythonOperator(
        task_id="list_new_files",
        python_callable=list_new_files,
    )

    task_load = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    # Automatically kick off dbt after every bronze load
    task_trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transformations",
        trigger_dag_id="dbt_transformations",
        wait_for_completion=False,   # fire-and-forget; dbt runs in parallel
        reset_dag_run=True,
    )

    task_list >> task_load >> task_trigger_dbt
    