# 🏦 Real-Time Banking Data Pipeline

> A production-grade, end-to-end data engineering project demonstrating a real-time
> banking data pipeline built with the modern data stack (2025/2026 tool versions).

```
PostgreSQL 16 → Debezium CDC → Kafka → MinIO → Airflow → Snowflake → dbt → Power BI
```

[![CI — Lint, Test & dbt Compile](https://github.com/Ayushgithubcodebasics/realtime-banking-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Ayushgithubcodebasics/realtime-banking-data-pipeline/actions/workflows/ci.yml)
[![CD — Deploy dbt to Production](https://github.com/Ayushgithubcodebasics/realtime-banking-data-pipeline/actions/workflows/cd.yml/badge.svg)](https://github.com/Ayushgithubcodebasics/realtime-banking-data-pipeline/actions/workflows/cd.yml)

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          OLTP Layer                                     │
│  [PostgreSQL 16]  ←── Fake data inserted by generator.py every 5s      │
│   customers / accounts / transactions tables                            │
│   WAL logical replication enabled (wal_level=logical)                   │
└────────────────────────┬────────────────────────────────────────────────┘
                         │ WAL stream (pgoutput plugin)
                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       CDC Streaming Layer                               │
│  [Debezium Connect 2.6]  captures every INSERT / UPDATE / DELETE        │
│  [Kafka 7.7.1]           streams events on 3 topics:                    │
│    banking_server.public.customers                                      │
│    banking_server.public.accounts                                       │
│    banking_server.public.transactions                                   │
└────────────────────────┬────────────────────────────────────────────────┘
                         │ Kafka consumer (kafka_to_minio.py)
                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Landing Zone                                     │
│  [MinIO]  S3-compatible object store                                    │
│   Parquet files, date-partitioned:                                      │
│   raw/customers/date=YYYY-MM-DD/*.parquet                               │
│   raw/accounts/date=YYYY-MM-DD/*.parquet                                │
│   raw/transactions/date=YYYY-MM-DD/*.parquet                            │
│   Each record includes _cdc_op and _cdc_ts audit metadata               │
└────────────────────────┬────────────────────────────────────────────────┘
                         │ Airflow DAG (every 5 min)
                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Snowflake Data Warehouse                           │
│                                                                         │
│  BANKING.RAW (Bronze layer)          ← loaded by Airflow via PUT+COPY  │
│    customers / accounts / transactions                                  │
│    includes _cdc_op, _cdc_ts, _loaded_at audit columns                  │
│                                                                         │
│  BANKING.ANALYTICS (Silver + Gold)   ← built by dbt                    │
│    stg_customers   (view)   ─ deduplicated, typed                       │
│    stg_accounts    (view)   ─ deduplicated, typed                       │
│    stg_transactions(view)   ─ filtered, typed                           │
│    customers_snapshot (SCD2 table)                                      │
│    accounts_snapshot  (SCD2 table)                                      │
│    dim_customers  (table)   ─ Star Schema dimension                     │
│    dim_accounts   (table)   ─ Star Schema dimension                     │
│    fact_transactions (incremental table) ─ Star Schema fact             │
└────────────────────────┬────────────────────────────────────────────────┘
                         │ DirectQuery
                         ▼
                    [Power BI Dashboard]
```

### Automated DAG flow

```
minio_to_snowflake_bronze  (every 5 min)
  └── list_new_files
  └── load_snowflake
  └── trigger_dbt_transformations  ──→  dbt_transformations (triggered)
                                          └── dbt_run_staging
                                          └── dbt_snapshot
                                          └── dbt_run_marts
                                          └── dbt_test
```

---

## 🛠️ Tech Stack

| Layer | Tool | Version | Purpose |
|---|---|---|---|
| OLTP database | PostgreSQL | 16 | Transactional source with logical replication |
| CDC | Debezium | 2.6.1 | Capture every INSERT / UPDATE / DELETE from WAL |
| Message broker | Apache Kafka | 7.7.1 (Confluent) | Stream CDC events across 3 topics |
| Object storage | MinIO | 2024-12-18 | S3-compatible landing zone for Parquet files |
| Orchestration | Apache Airflow | 2.10.4 | Schedule bronze loads, trigger dbt automatically |
| Data warehouse | Snowflake | — | Bronze / Silver / Gold layered architecture |
| Transformation | dbt | 1.9.5 | Staging views, SCD2 snapshots, Star Schema marts |
| Visualisation | Power BI | — | DirectQuery dashboard on gold tables |
| CI/CD | GitHub Actions | — | Lint → test → dbt compile → dbt deploy |
| Containerisation | Docker | — | All local services in one compose stack |
| Linting | Ruff | 0.6+ | Python code quality |
| Testing | pytest | 8.0+ | Unit tests for generator and CDC logic |

---

## 📁 Project Structure

```
realtime-banking-data-pipeline/
│
├── .env.example                        # Template — copy to .env and fill in values
├── .gitignore                          # Excludes .env, dbt/target, parquet files
├── docker-compose.yml                  # All Docker services with healthchecks
├── dockerfile-airflow.dockerfile       # Extended Airflow image with dbt + snowflake
├── requirements.txt                    # All Python dependencies
├── ruff.toml                           # Linting configuration
│
├── .github/workflows/
│   ├── ci.yml                          # Lint + pytest + dbt compile (main & dev)
│   └── cd.yml                          # dbt staging → snapshot → marts → test (main only)
│
├── postgres/
│   └── init.sql                        # Auto-runs on container start: tables + indexes + replication slot
│
├── snowflake/
│   └── setup.sql                       # Run once in Snowflake: database + schemas + RAW tables
│
├── data-generator/
│   ├── generator.py                    # Inserts fake customers/accounts/transactions into PostgreSQL
│   └── requirements.txt
│
├── kafka-debezium/
│   └── register_connector.py          # Registers Debezium CDC connector with status polling
│
├── consumer/
│   ├── kafka_to_minio.py              # Consumes Kafka topics → writes Parquet to MinIO
│   └── requirements.txt
│
├── docker/dags/
│   ├── minio_to_snowflake_dag.py      # Airflow DAG: MinIO → Snowflake Bronze (every 5 min)
│   └── dbt_transformations_dag.py     # Airflow DAG: dbt Silver + Gold (triggered by bronze)
│
├── banking_dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── sources.yml                # Source definitions with not_null / accepted_values tests
│   │   ├── staging/
│   │   │   ├── staging.yml            # dbt tests for all silver layer models
│   │   │   ├── stg_customers.sql      # Deduplicated view
│   │   │   ├── stg_accounts.sql       # Deduplicated view
│   │   │   └── stg_transactions.sql   # Filtered append-only view
│   │   └── marts/
│   │       ├── marts.yml              # dbt tests for all gold layer models
│   │       ├── dimensions/
│   │       │   ├── dim_customers.sql  # SCD2 customer dimension (materialized: table)
│   │       │   └── dim_accounts.sql   # SCD2 account dimension (materialized: table)
│   │       └── facts/
│   │           └── fact_transactions.sql  # Incremental transaction fact table
│   └── snapshots/
│       ├── customers_snapshot.sql     # SCD2 snapshot: tracks email/name changes
│       └── accounts_snapshot.sql      # SCD2 snapshot: tracks balance/type changes
│
└── tests/
    ├── __init__.py
    └── test_pipeline.py               # 8 pytest tests: generator + CDC payload routing
```

---

## ⚙️ Prerequisites

| Tool | Notes |
|---|---|
| Docker Desktop | Latest — runs all local services |
| Python 3.11+ | `python --version` to check |
| DBeaver | To inspect the PostgreSQL database |
| Snowflake account | Free trial at https://signup.snowflake.com — no card needed |
| Power BI Desktop | Windows only — free download from Microsoft Store |

---

## 🚀 Quick Start

### Step 1 — Clone and configure

```powershell
git clone https://github.com/Ayushgithubcodebasics/realtime-banking-data-pipeline.git
cd realtime-banking-data-pipeline

# Copy the env template and fill in your values
copy .env.example .env
```

Open `.env` and fill in the four Snowflake fields:

```env
SNOWFLAKE_ACCOUNT=orgname-accountname    # Admin → Accounts → hover → copy locator
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

All other values (`POSTGRES_*`, `MINIO_*`, `AIRFLOW_*`) are pre-filled with working defaults.

### Step 2 — Run Snowflake setup (once only)

1. Open Snowflake UI → **Worksheets** → New worksheet
2. Paste the entire contents of `snowflake/setup.sql`
3. Click **Run All**

This creates the `BANKING` database, `RAW` and `ANALYTICS` schemas, and the three bronze tables.

### Step 3 — Start all Docker services

```powershell
docker-compose up -d --build
```

Wait ~3 minutes for all services to become healthy. Check:

```powershell
docker-compose ps
```

All containers should show `healthy` or `running`:

| Container | Port | URL |
|---|---|---|
| banking-postgres | 5432 | DBeaver connection |
| kafka | 29092 | localhost:29092 |
| debezium-connect | 8083 | http://localhost:8083 |
| minio | 9000 / 9001 | http://localhost:9001 |
| airflow-webserver | 8080 | http://localhost:8080 |

Airflow credentials: `admin` / `admin`
MinIO credentials: `minioadmin` / `minioadmin123`

### Step 4 — Install Python dependencies

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Step 5 — Register the Debezium CDC connector

```powershell
cd kafka-debezium
python register_connector.py
cd ..
```

Expected output:
```
✅ Debezium Connect is ready
✅ Connector 'banking-postgres-connector' created successfully
✅ Connector is RUNNING — CDC is active!
```

### Step 6 — Start the Kafka → MinIO consumer

Open a **new terminal window** and keep it running:

```powershell
cd consumer
python kafka_to_minio.py
```

Expected output:
```
🚀 Consumer started — listening for Kafka messages …
✅ MinIO bucket exists: raw
```

### Step 7 — Generate banking data

Open **another new terminal window**:

```powershell
cd data-generator
python generator.py
```

Expected output:
```
🏦 Banking data generator starting …
✅ Connected to PostgreSQL

── Iteration 1 ──────────────────────
  ✅ Inserted 10 customers · 20 accounts · 50 transactions
```

Within seconds the consumer window will print:
```
  [customers   ] op=r id=1
  [transactions] op=r id=1
  📦 Uploaded   50 records → s3://raw/transactions/date=2026-01-15/...parquet
```

### Step 8 — Enable Airflow DAGs

1. Open http://localhost:8080
2. Enable both DAGs by clicking the toggle switch:
   - `minio_to_snowflake_bronze` — auto-runs every 5 minutes, then triggers dbt
   - `dbt_transformations` — triggered automatically after every bronze load
3. Trigger `minio_to_snowflake_bronze` manually once (click ▶) to load existing MinIO data immediately

### Step 9 — Verify data in Snowflake

```sql
-- Bronze layer
SELECT COUNT(*) FROM BANKING.RAW.customers;
SELECT COUNT(*) FROM BANKING.RAW.accounts;
SELECT COUNT(*) FROM BANKING.RAW.transactions;

-- Silver layer (views — auto-refreshing)
SELECT * FROM BANKING.ANALYTICS.stg_customers LIMIT 10;

-- Gold layer
SELECT * FROM BANKING.ANALYTICS.dim_customers WHERE is_current = TRUE LIMIT 10;
SELECT * FROM BANKING.ANALYTICS.fact_transactions LIMIT 10;

-- SCD2 history — any customer with 2 rows means a field change was tracked
SELECT customer_id, email, effective_from, effective_to, is_current
FROM BANKING.ANALYTICS.dim_customers
ORDER BY customer_id, effective_from;
```

### Step 10 — Connect Power BI (DirectQuery)

1. Open Power BI Desktop → **Get Data** → **Snowflake**
2. Server: `your-account.snowflakecomputing.com`
3. Warehouse: `COMPUTE_WH`
4. **Data Connectivity mode: DirectQuery** ← important for real-time
5. Select `BANKING` → `ANALYTICS` → `DIM_CUSTOMERS`, `DIM_ACCOUNTS`, `FACT_TRANSACTIONS`

---

## 🔄 How the Pipeline Works End-to-End

1. `generator.py` inserts fake banking records into PostgreSQL every 5 seconds
2. Debezium reads the PostgreSQL WAL and publishes every change (INSERT/UPDATE/DELETE) to Kafka topics
3. `kafka_to_minio.py` consumes those events, batches 50 records, and writes date-partitioned Parquet files to MinIO — each record carries `_cdc_op` (operation type) and `_cdc_ts` (event timestamp) audit columns
4. Airflow's `minio_to_snowflake_bronze` DAG runs every 5 minutes, finds new Parquet files, PUT-uploads them to Snowflake internal staging, and COPY INTO the RAW tables using `MATCH_BY_COLUMN_NAME`
5. After a successful bronze load, Airflow automatically triggers `dbt_transformations`
6. dbt runs in this exact order: staging views → SCD2 snapshots → dimension/fact tables → data quality tests
7. Power BI queries the gold tables via DirectQuery — any new data in Snowflake appears in dashboards without refreshing

---

## 🏗️ Data Architecture Decisions

### Why PostgreSQL for the source?
Banking data requires ACID guarantees (Atomicity, Consistency, Isolation, Durability) that only SQL databases provide. PostgreSQL's logical replication (WAL) is the most reliable mechanism for CDC.

### Why Debezium + Kafka instead of polling?
Polling the database for changes creates load and misses updates between poll intervals. Debezium reads directly from the WAL — zero additional load on the source, captures every change including deletes, with sub-second latency.

### Why MinIO instead of AWS S3?
MinIO is 100% S3-compatible. The same `boto3` code works with both — swapping to S3 in production requires only an endpoint URL change in `.env`. This keeps the project runnable locally without cloud costs.

### Why Parquet instead of JSON or CSV?
Parquet files are columnar, compressed, and store data type metadata. Compared to CSV: 3–10x smaller file sizes, 5–50x faster analytical queries, no type inference errors on load. Industry standard for data lake storage.

### Why three dbt layers?
- **Staging (Silver)**: Clean and type-cast raw data, deduplicate CDC events, enforce a consistent interface between raw and business logic
- **Snapshots**: Capture historical versions of slowly changing data using dbt's built-in SCD2 mechanism
- **Marts (Gold)**: Business-ready Star Schema optimised for BI tools — dimensions with surrogate keys, incremental fact table with date parts pre-computed

### Why incremental materialisation for fact_transactions?
Each dbt run only processes rows where `transaction_time > MAX(transaction_time)` already in the table. This means run time stays constant as data grows — a full refresh would scan all historical data every hour.

### Why SCD2 for customers and accounts?
If a customer changes their email, or an account changes its type, a simple UPDATE would lose the historical record. SCD2 keeps both the old and new row, marking the old one with `is_current = FALSE` and an `effective_to` date. This is the industry standard for tracking dimension changes in data warehouses.

---

## 🧪 Tests

```powershell
pytest tests/ -v
```

| Test | What it validates |
|---|---|
| `test_random_money_range` | Generator never produces values outside defined bounds |
| `test_random_money_two_decimal_places` | Generator always produces exactly 2 decimal places |
| `test_random_money_never_negative` | Generator never produces negative balances |
| `test_extract_table_name_customers` | Kafka topic → table name extraction |
| `test_extract_table_name_transactions` | Kafka topic → table name extraction |
| `test_cdc_payload_create_uses_after` | INSERT (op=c) routes to `after` field |
| `test_cdc_payload_delete_uses_before` | DELETE (op=d) routes to `before` field |
| `test_cdc_payload_heartbeat_is_skipped` | Heartbeat events (no op) are ignored |
| `test_cdc_metadata_attached_to_record` | `_cdc_op` and `_cdc_ts` correctly attached |

dbt tests run automatically in CI and CD:

```powershell
cd banking_dbt
dbt test
```

Tests cover: `unique` and `not_null` on all primary keys, `accepted_values` on `account_type` (SAVINGS/CHECKING), `transaction_type` (DEPOSIT/WITHDRAWAL/TRANSFER), `status` (COMPLETED/PENDING/FAILED), and `currency` (USD) — across sources, staging, and marts layers.

---

## 🔁 CI / CD Pipeline

### CI — runs on every push to `main` or `dev`, and on pull requests to `main`

```
Job 1: Lint & Unit Tests
  ├── Spins up PostgreSQL 16 service container
  ├── Installs all Python dependencies
  ├── ruff check . (syntax + style linting)
  └── pytest tests/ -v (9 unit tests)

Job 2: dbt Compile (runs after Job 1 passes)
  ├── Installs dbt-snowflake
  ├── Writes profiles.yml from GitHub Secrets
  ├── dbt deps
  └── dbt compile (validates all SQL against live Snowflake)
```

### CD — runs only on push to `main`

```
dbt deps
→ dbt run --select staging    (Silver layer views)
→ dbt snapshot                (SCD2 history tables)
→ dbt run --select marts      (Gold layer tables)
→ dbt test                    (data quality validation)
```

### GitHub Secrets required

| Secret | Where to find it |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Snowflake UI → Admin → Accounts → hover account → copy locator |
| `SNOWFLAKE_USER` | Your Snowflake username |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` (default) |
| `POSTGRES_USER` | `postgres` |
| `POSTGRES_PASSWORD` | Value from your `.env` file |

---

## 🗺️ Snowflake Schema Reference

### BANKING.RAW (Bronze)

| Table | Source | Key columns |
|---|---|---|
| `customers` | PostgreSQL CDC | `id`, `first_name`, `last_name`, `email`, `_cdc_op`, `_cdc_ts`, `_loaded_at` |
| `accounts` | PostgreSQL CDC | `id`, `customer_id`, `account_type`, `balance`, `currency`, `_cdc_op`, `_cdc_ts` |
| `transactions` | PostgreSQL CDC | `id`, `account_id`, `txn_type`, `amount`, `related_account_id`, `status`, `_cdc_op`, `_cdc_ts` |

### BANKING.ANALYTICS (Silver + Gold)

| Object | Type | Description |
|---|---|---|
| `stg_customers` | View | Deduplicated, typed customers |
| `stg_accounts` | View | Deduplicated, typed accounts |
| `stg_transactions` | View | Filtered, typed transactions |
| `customers_snapshot` | Table (SCD2) | Full history of customer changes |
| `accounts_snapshot` | Table (SCD2) | Full history of account changes |
| `dim_customers` | Table | Current + historical customers with `is_current` flag |
| `dim_accounts` | Table | Current + historical accounts with `is_current` flag |
| `fact_transactions` | Incremental Table | Denormalised transaction fact with date parts |

---

## 🔧 What I Would Improve in Production

- **Replace MinIO with AWS S3 or Azure Blob Storage** — MinIO is a local stand-in. In production, managed cloud storage provides SLAs, replication, and lifecycle policies out of the box.
- **Use Airflow Connections instead of environment variables** — Credentials should be stored in Airflow's encrypted Connections store and accessed via `BaseHook.get_connection()`, not read from `os.environ` directly in DAG code.
- **Add a dead-letter queue for failed Kafka messages** — Currently, messages that fail to parse are silently skipped. A DLQ (separate Kafka topic or S3 prefix) would capture them for replay and debugging.
- **Switch from `enable_auto_commit=True` to manual offset commits** in the Kafka consumer — auto-commit can lose messages if the consumer crashes mid-batch before writing to MinIO. Manual commit after a successful `upload_fileobj` gives exactly-once semantics.
- **Add Kafka Schema Registry** — currently, Debezium serialises events as plain JSON. In production, Avro schemas registered with Confluent Schema Registry enforce a contract between producer and consumer, preventing silent data type drift.
- **Parameterise dbt models with `vars`** — the `start_date` variable exists in `dbt_project.yml` but is unused. Production pipelines use vars to scope incremental loads and backfills.
- **Add monitoring and alerting** — Airflow can send email/Slack alerts on DAG failure. Adding Grafana on top of Kafka metrics and MinIO would give full pipeline observability.

---

## 📋 Environment Variables Reference

All variables are defined in `.env` (not committed to git). See `.env.example` for the full template.

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `postgres` | PostgreSQL username |
| `POSTGRES_PASSWORD` | — | PostgreSQL password |
| `POSTGRES_DB` | `banking` | Database name |
| `MINIO_ROOT_USER` | `minioadmin` | MinIO admin username |
| `MINIO_ROOT_PASSWORD` | — | MinIO admin password |
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO endpoint (internal Docker network) |
| `MINIO_BUCKET` | `raw` | Landing bucket name |
| `AIRFLOW_DB_USER` | `airflow` | Airflow metadata DB user |
| `AIRFLOW_DB_PASSWORD` | — | Airflow metadata DB password |
| `SNOWFLAKE_ACCOUNT` | — | Account identifier (`orgname-accountname`) |
| `SNOWFLAKE_USER` | — | Snowflake username |
| `SNOWFLAKE_PASSWORD` | — | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` | Compute warehouse name |
| `SNOWFLAKE_DB` | `BANKING` | Snowflake database name |
| `SNOWFLAKE_SCHEMA` | `ANALYTICS` | Default schema for dbt |
| `SNOWFLAKE_ROLE` | `ACCOUNTADMIN` | Snowflake role |

---

## 📚 Key Concepts Demonstrated

| Concept | Where |
|---|---|
| Change Data Capture (CDC) | Debezium + PostgreSQL WAL |
| Event streaming | Kafka topics with consumer groups and offset tracking |
| Medallion architecture (Bronze/Silver/Gold) | Snowflake RAW → ANALYTICS schemas |
| Slowly Changing Dimensions Type 2 (SCD2) | dbt snapshots for customers and accounts |
| Star Schema | dim_customers, dim_accounts, fact_transactions |
| Incremental dbt models | fact_transactions only processes new rows |
| Data quality testing | dbt tests across all 3 layers + pytest unit tests |
| CI/CD for data pipelines | GitHub Actions with lint, test, compile, deploy stages |
| Secret management | `.env` file excluded from git, GitHub Secrets for CI/CD |
| Idempotent infrastructure | `CREATE IF NOT EXISTS` guards, `PURGE=TRUE` on COPY INTO |
| Containerised development | Full stack in docker-compose with healthchecks |

---

## 🙋 Author

**Ayush Butoliya** — 3rd year Professional Informatics Systems student, Riga, Latvia.
8 months data analyst internship experience.

GitHub: [@Ayushgithubcodebasics](https://github.com/Ayushgithubcodebasics)