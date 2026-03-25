# Resume and Interview Notes

## Resume-safe description

Built an end-to-end data engineering pipeline for banking analytics using PostgreSQL, Debezium, Kafka, MinIO, Snowflake, dbt, Airflow, GitHub Actions, and Power BI.

## What this project proves

- CDC ingestion with insert, update, and delete handling
- dimensional modeling with SCD2 snapshots
- warehouse transformations with dbt
- orchestration with Airflow
- CI/CD with GitHub Actions
- DirectQuery-ready semantic serving models for Power BI

## What to say in interviews

1. The original tutorial-style version was tool-heavy but weak on correctness.
2. I fixed the pipeline to deduplicate by CDC timestamp instead of `created_at`.
3. I changed the fact table to join dimensions by effective date range.
4. I fixed CI so tests can no longer fail silently.
5. I added serving models for Power BI, CDC audit, and pipeline health.
6. I documented the run flow for Windows PowerShell and Docker Desktop.
