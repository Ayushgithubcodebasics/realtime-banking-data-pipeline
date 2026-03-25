# What Was Fixed

## Code and data correctness
- generator now produces insert/update/delete-style activity
- generator updates account balances when transactions occur
- raw money values flow as strings from Debezium and are cast to decimal in Snowflake/dbt
- staging deduplicates by CDC timestamp and load timestamp, not `created_at`
- facts join to dimension versions using SCD2 effective date windows

## Reliability
- Kafka consumer now uses manual offset commits after successful uploads
- malformed events can be sent to a DLQ prefix in MinIO
- consumer is import-safe for unit testing
- Airflow only triggers dbt after successful loads
- Snowflake loads use `ON_ERROR = ABORT_STATEMENT`

## Dev workflow
- CI now fails properly on failing tests
- CI uses `dbt parse` instead of requiring live Snowflake secrets
- Docker Compose includes optional app services for the generator and consumer
- PowerShell run guide added

## Analytics and BI
- added Power BI serving models
- added CDC audit model
- added pipeline health model
- added DAX recommendations and a report blueprint
