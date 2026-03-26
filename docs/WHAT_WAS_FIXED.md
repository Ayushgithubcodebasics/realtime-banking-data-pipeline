# What Was Fixed

## Data correctness
- The staging layer deduplicates CDC records using CDC timestamps and load timestamps instead of weaker ordering assumptions.
- Monetary values are preserved through CDC ingestion and cast in Snowflake/dbt instead of being silently coerced too early.
- The fact model joins to the correct SCD2 dimension version using effective date windows.

## Reliability
- The Kafka consumer commits offsets only after a successful MinIO upload.
- Malformed events can be written to a DLQ prefix in MinIO.
- Airflow triggers downstream dbt work only after a successful Snowflake RAW load.
- Snowflake copy operations use `ON_ERROR = ABORT_STATEMENT`.

## Developer experience
- Unit tests run without side effects at import time.
- CI validates Ruff, pytest, and `dbt parse`.
- The public docs were scrubbed to remove local-machine paths, local secrets, and migration noise.
- The repository now excludes generated artifacts, caches, and local runtime files.

## Analytics serving
- Power BI serving views are included for dimensions, facts, CDC audit, date, and pipeline health.
- The repo includes a DAX starter pack and a report blueprint instead of pretending a polished PBIX file is already finished.
