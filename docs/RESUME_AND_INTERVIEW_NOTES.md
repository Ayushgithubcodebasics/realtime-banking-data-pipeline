# Resume and Interview Notes

## Resume-safe summary

Built an end-to-end CDC data pipeline for banking analytics using PostgreSQL, Debezium, Kafka, MinIO, Snowflake, dbt, Airflow, GitHub Actions, and Power BI.

## What this project demonstrates

- Change data capture ingestion instead of static batch-only processing
- Raw landing-zone design before warehouse transformation
- SCD Type 2 dimensional modeling with dbt snapshots
- Airflow orchestration across ingestion and transformation stages
- Automated validation with Ruff, pytest, and dbt parse in CI
- BI-serving models prepared for Power BI DirectQuery-style reporting

## Strong interview talking points

1. The pipeline handles inserts, updates, and deletes end to end.
2. The raw layer preserves CDC metadata instead of throwing it away early.
3. The marts are designed for historical correctness, not just easy joins.
4. The repo is structured like a project you could hand to another engineer, not just a tutorial notebook.
5. The BI layer is intentionally modeled as serving views rather than exposing raw warehouse tables directly.
