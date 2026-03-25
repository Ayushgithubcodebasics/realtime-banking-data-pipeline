# Real-Time Banking Data Engineering Project

An end-to-end data engineering portfolio project that demonstrates:

- PostgreSQL as the OLTP source system
- Debezium + Kafka for CDC streaming
- MinIO as the raw landing zone
- Snowflake as the warehouse
- dbt for staging, SCD2 dimensions, marts, and Power BI serving models
- Airflow orchestration
- GitHub Actions for CI/CD
- Power BI DirectQuery semantic model and dashboard blueprint

This version fixes the biggest gaps from the original repo:

- CI now fails when tests fail
- the generator now produces inserts, updates, and delete-style events
- money values are preserved as exact decimal strings in CDC and cast in Snowflake/dbt
- staging logic now deduplicates by CDC timestamp instead of `created_at`
- facts now join to the correct SCD2 dimension version using effective dates
- Airflow only triggers dbt after a successful Snowflake load
- Power BI-friendly models are included in dbt (`PBI_*`)

---

## Architecture

```text
PostgreSQL -> Debezium -> Kafka -> Consumer -> MinIO -> Airflow -> Snowflake RAW
                                                            -> dbt staging/snapshots/marts
                                                            -> Power BI DirectQuery
```

---

## Repository structure

```text
.
├── .github/workflows/            # CI/CD pipelines
├── banking_dbt/                  # dbt project
├── common/                       # shared config loader
├── consumer/                     # Kafka -> MinIO consumer
├── data-generator/               # Postgres transaction/activity generator
├── docker/dags/                  # Airflow DAGs
├── docs/                         # runbooks and setup notes
├── kafka-debezium/               # connector registration script
├── postgres/                     # OLTP schema
├── powerbi/                      # DAX, theme, and report blueprint
├── scripts/                      # helper PowerShell notes/scripts
└── snowflake/                    # Snowflake setup SQL
```

---

## Data model summary

### PostgreSQL source tables

- `customers`
- `accounts`
- `transactions`

### dbt outputs

#### Staging
- `stg_customers`
- `stg_accounts`
- `stg_transactions`

#### SCD2 dimensions
- `dim_customers`
- `dim_accounts`

#### Facts
- `fact_transactions`

#### Power BI serving models
- `pbi_dim_customers_current`
- `pbi_dim_accounts_current`
- `pbi_fact_transactions`
- `pbi_dim_date`
- `pbi_cdc_audit`
- `pbi_pipeline_health`

---

## Before you start

1. Install Docker Desktop
2. Install Python 3.11+
3. Install Power BI Desktop
4. Create a Snowflake database user/role for this project
5. Copy `.env.example` to `.env`
6. Copy `banking_dbt/.dbt/profiles.yml.example` to `banking_dbt/.dbt/profiles.yml`

---

## Quick start (Windows PowerShell)

See the full walkthrough in `docs/RUN_WINDOWS_POWERSHELL.md`.

Minimal flow:

```powershell
Copy-Item .env.example .env
Copy-Item banking_dbt\.dbt\profiles.yml.example banking_dbt\.dbt\profiles.yml

docker compose up -d --build
python .\kafka-debezium\register_connector.py

docker compose --profile apps up -d generator consumer
```

Open:

- Airflow: `http://localhost:8080`
- Debezium: `http://localhost:8083/connectors`
- MinIO Console: `http://localhost:9001`

---

## Snowflake setup

Run this once in Snowflake:

```sql
-- open and run
snowflake/setup.sql
```

Then update `banking_dbt/.dbt/profiles.yml` with your real credentials.

---

## dbt commands

```powershell
cd banking_dbt

dbt deps
dbt run --select staging
dbt snapshot
dbt run --select marts
dbt test
```

---

## CI/CD

### CI

Runs on every push/PR:

- `ruff check .`
- `pytest tests -v`
- `dbt parse`

### CD

Runs only when Snowflake secrets are configured:

- `dbt run --select staging`
- `dbt snapshot`
- `dbt run --select marts`
- `dbt test`

---

## Power BI recommendation

Do **not** use the original PBIX as the final proof of the project without redesigning it.

Use these serving models instead:

- `PBI_FACT_TRANSACTIONS`
- `PBI_DIM_ACCOUNTS_CURRENT`
- `PBI_DIM_CUSTOMERS_CURRENT`
- `PBI_DIM_DATE`
- `PBI_CDC_AUDIT`
- `PBI_PIPELINE_HEALTH`

Then follow:

- `powerbi/Measures_DAX.md`
- `powerbi/Report_Blueprint.md`
- `powerbi/Banking_Theme.json`

---

## Local validation

```powershell
python -m pip install -r requirements.txt
ruff check .
pytest tests -v
```

---

## Known limitations

- A fully modified `.pbix` file is **not** included because PBIX editing cannot be safely validated in this environment without Power BI Desktop.
- Snowflake execution cannot be fully end-to-end verified here because live credentials are not available.
- Airflow/dbt/Snowflake runtime validation depends on your real Snowflake connection settings.

That said, the codebase, tests, models, runbooks, and Power BI blueprint are prepared so you can run and validate the project properly on your machine.
