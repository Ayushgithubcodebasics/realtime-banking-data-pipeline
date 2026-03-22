# ============================================================
# Airflow 2.10.4 - Extended image with all required packages
# Includes: dbt-snowflake, Snowflake connector, boto3, pyarrow
# ============================================================
FROM apache/airflow:2.10.4-python3.11

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-snowflake==5.8.1" \
    "apache-airflow-providers-amazon==9.1.0" \
    "boto3==1.35.36" \
    "pyarrow==18.1.0" \
    "python-dotenv==1.0.1"

RUN pip install --no-cache-dir \
    "snowflake-connector-python==3.13.1"

RUN pip install --no-cache-dir \
    "dbt-core==1.9.5" \
    "dbt-snowflake==1.9.3"
