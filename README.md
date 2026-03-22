# 🏦 Banking Modern Data Stack

A production-grade, end-to-end data engineering project demonstrating a real-time
banking data pipeline. Built with the **2025/2026 versions** of every tool.

```
PostgreSQL → Debezium CDC → Kafka → MinIO → Airflow → Snowflake → dbt → Power BI
```

---

## 📐 Architecture

| Layer | Tool | Role |
|-------|------|------|
| **OLTP Source** | PostgreSQL 16 | Banking transactional database |
| **CDC / Streaming** | Debezium 2.7 + Kafka 7.7 | Capture every INSERT/UPDATE/DELETE in real-time |
| **Landing Zone** | MinIO | S3-compatible object store (Parquet files) |
| **Orchestration** | Apache Airflow 2.10 | Schedules MinIO → Snowflake loads & dbt runs |
| **Data Warehouse** | Snowflake | Bronze / Silver / Gold layers |
| **Transformation** | dbt 1.9 | Staging views, SCD2 snapshots, Star Schema |
| **Visualisation** | Power BI | Dashboard connected via DirectQuery |
| **CI/CD** | GitHub Actions | Lint → Test → dbt compile → dbt deploy |

### Data Flow

```
[PostgreSQL]
     │  (WAL logical replication)
     ▼
[Debezium Connect]──────► [Kafka Topics]
                                │  (3 topics: customers / accounts / transactions)
                                ▼
                          [Consumer Script]
                                │  (Parquet batches, partitioned by date)
                                ▼
                            [MinIO]
                          raw/customers/date=YYYY-MM-DD/*.parquet
                          raw/accounts/date=YYYY-MM-DD/*.parquet
                          raw/transactions/date=YYYY-MM-DD/*.parquet
                                │
                                │  (Airflow DAG every 1 min)
                                ▼
                         [Snowflake RAW]        ← Bronze layer
                      BANKING.RAW.customers
                      BANKING.RAW.accounts
                      BANKING.RAW.transactions
                                │
                                │  (Airflow DAG hourly → dbt)
                                ▼
                      [Snowflake ANALYTICS]     ← Silver + Gold layers
                      stg_customers (view)
                      stg_accounts (view)
                      stg_transactions (view)
                      customers_snapshot (SCD2)
                      accounts_snapshot (SCD2)
                      dim_customers (table)
                      dim_accounts (table)
                      fact_transactions (incremental)
                                │
                                ▼
                           [Power BI]           ← DirectQuery
```

---

## ⚙️ Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Docker Desktop | Latest | Already installed ✅ |
| DBeaver | Latest | Already installed ✅ |
| Python | 3.11+ | `python --version` |
| Snowflake account | Free trial OK | https://signup.snowflake.com |
| Power BI Desktop | Latest | Windows only; free download |

---

## 🚀 Quick Start (Step-by-Step)

### Step 1 — Clone / Download

```powershell
# In PowerShell
cd C:\Projects    # or wherever you keep code
# Unzip the project folder here, then:
cd banking-modern-datastack
```

### Step 2 — Configure Snowflake credentials

1. Open `.env` in any text editor (VS Code recommended)
2. Fill in the four Snowflake fields:

```env
SNOWFLAKE_ACCOUNT=abc12345-myorg     # Admin → Accounts → Locator
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH       # default is fine
```

> **Finding your account identifier:**
> Snowflake UI → Admin → Accounts → hover your account → copy the locator
> Format is `<orgname>-<accountname>` (e.g. `xy12345-mycompany`)

### Step 3 — Run the Snowflake setup SQL

1. Open Snowflake UI → **Worksheets** → New worksheet
2. Paste the entire contents of `snowflake/setup.sql`
3. Click **Run All**
4. Verify: you should see `BANKING.RAW` schema with 3 tables

### Step 4 — Start all Docker containers

```powershell
# From the project root (where docker-compose.yml is)
docker-compose up -d --build

# Watch the logs — wait until you see airflow-init complete:
docker-compose logs -f airflow-init
# You should see: ✅ Airflow initialised successfully

# Check all containers are healthy (takes ~3-4 minutes on first build):
docker-compose ps
```

All containers and their ports:

| Container | Port | URL |
|-----------|------|-----|
| banking-postgres | 5432 | Connect via DBeaver |
| airflow-postgres | 5433 | Internal only |
| kafka | 29092 | localhost:29092 |
| debezium-connect | 8083 | http://localhost:8083 |
| minio | 9000 / 9001 | http://localhost:9001 (console) |
| airflow-webserver | 8080 | http://localhost:8080 |

### Step 5 — Install Python dependencies (on your host machine)

```powershell
# Create a virtual environment
python -m venv .venv
.venv\Scripts\activate      # Windows PowerShell

# Install all packages
pip install -r requirements.txt
```

### Step 6 — Register the Debezium CDC connector

This tells Debezium to start watching PostgreSQL for changes:

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

You can verify in your browser: http://localhost:8083/connectors

### Step 7 — Start the Kafka → MinIO consumer

Open a **new PowerShell window** and keep it running:

```powershell
cd consumer
python kafka_to_minio.py
```

Expected output:
```
🚀 Consumer started — listening for Kafka messages …
✅ MinIO bucket exists: raw
   Press Ctrl-C to stop
```

### Step 8 — Generate banking data

Open **another new PowerShell window**:

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

Within seconds, you should see the consumer window printing:
```
  [customers   ] op=r id=1
  [accounts    ] op=r id=1
  [transactions] op=r id=1
  📦 Uploaded  50 records → s3://raw/transactions/date=2026-01-15/...parquet
```

### Step 9 — Verify data in MinIO

Open http://localhost:9001 in your browser.
- Username: `minioadmin`
- Password: `minioadmin123`

You should see the `raw` bucket with folders: `customers/`, `accounts/`, `transactions/`

### Step 10 — Enable Airflow DAGs

1. Open http://localhost:8080
2. Login: `admin` / `admin`
3. Enable both DAGs by clicking the toggle:
   - `minio_to_snowflake_bronze` — loads MinIO → Snowflake RAW every minute
   - `dbt_transformations` — runs dbt snapshot + run + test every hour

4. Trigger `minio_to_snowflake_bronze` manually first (click ▶ Play button)
5. Then trigger `dbt_transformations` manually

### Step 11 — Verify data in Snowflake

Run in a Snowflake worksheet:

```sql
-- Check Bronze layer
SELECT COUNT(*) FROM BANKING.RAW.customers;
SELECT COUNT(*) FROM BANKING.RAW.accounts;
SELECT COUNT(*) FROM BANKING.RAW.transactions;

-- Check Gold layer (after dbt runs)
SELECT * FROM BANKING.ANALYTICS.dim_customers WHERE is_current = TRUE LIMIT 10;
SELECT * FROM BANKING.ANALYTICS.dim_accounts WHERE is_current = TRUE LIMIT 10;
SELECT * FROM BANKING.ANALYTICS.fact_transactions LIMIT 10;

-- Check SCD2 history (any customer with multiple rows = change was tracked)
SELECT customer_id, email, effective_from, effective_to, is_current
FROM BANKING.ANALYTICS.dim_customers
ORDER BY customer_id, effective_from;
```

### Step 12 — Connect Power BI (DirectQuery)

1. Open Power BI Desktop
2. **Get Data** → **Snowflake**
3. Server: your Snowflake account URL (e.g. `xy12345-mycompany.snowflakecomputing.com`)
4. Warehouse: `COMPUTE_WH`
5. **Data Connectivity mode: DirectQuery** ← important
6. Navigate to `BANKING` → `ANALYTICS` → select:
   - `DIM_CUSTOMERS`
   - `DIM_ACCOUNTS`
   - `FACT_TRANSACTIONS`
7. Build your dashboard!

Suggested KPI cards:
- Total Transactions: `COUNT(fact_transactions[transaction_id])`
- Total Customers: `COUNTROWS(dim_customers)` filtered `is_current = TRUE`
- Total Accounts: `COUNTROWS(dim_accounts)` filtered `is_current = TRUE`
- Average Balance: `AVERAGE(dim_accounts[balance])` filtered `is_current = TRUE`

---

## 📁 Project Structure

```
banking-modern-datastack/
├── .env                          # ← YOUR SECRETS (never commit)
├── .env.example                  # Safe template
├── .gitignore
├── docker-compose.yml            # All Docker services
├── dockerfile-airflow.dockerfile # Extended Airflow image
├── requirements.txt              # Python dependencies
├── ruff.toml                     # Linting config
│
├── postgres/
│   └── init.sql                  # Schema + replication slot (auto-runs)
│
├── snowflake/
│   └── setup.sql                 # Run once in Snowflake worksheet
│
├── data-generator/
│   ├── .env                      # Generator-specific DB settings
│   ├── requirements.txt
│   └── generator.py              # Inserts fake data into PostgreSQL
│
├── kafka-debezium/
│   ├── .env
│   └── register_connector.py    # Registers Debezium CDC connector
│
├── consumer/
│   ├── .env
│   ├── requirements.txt
│   └── kafka_to_minio.py        # Kafka → MinIO Parquet consumer
│
├── docker/
│   └── dags/
│       ├── minio_to_snowflake_dag.py   # Airflow: MinIO → Snowflake (Bronze)
│       └── dbt_transformations_dag.py  # Airflow: dbt Silver + Gold
│
├── banking_dbt/
│   ├── dbt_project.yml
│   ├── .dbt/
│   │   └── profiles.yml                # Snowflake connection (uses env vars)
│   ├── models/
│   │   ├── sources.yml                 # Raw source definitions + tests
│   │   ├── staging/
│   │   │   ├── stg_customers.sql       # Silver: deduplicated views
│   │   │   ├── stg_accounts.sql
│   │   │   └── stg_transactions.sql
│   │   └── marts/
│   │       ├── marts.yml               # Gold layer tests
│   │       ├── dimensions/
│   │       │   ├── dim_customers.sql   # Gold: SCD2 customer dimension
│   │       │   └── dim_accounts.sql    # Gold: SCD2 account dimension
│   │       └── facts/
│   │           └── fact_transactions.sql  # Gold: incremental fact table
│   └── snapshots/
│       ├── customers_snapshot.sql      # SCD2 tracking for customers
│       └── accounts_snapshot.sql       # SCD2 tracking for accounts
│
└── .github/
    └── workflows/
        ├── ci.yml                      # Lint + test + dbt compile on PR
        └── cd.yml                      # dbt deploy on merge to main
```

---

## 🔍 Troubleshooting

### Airflow containers keep restarting
```powershell
docker-compose logs airflow-init
```
If you see a database error, the airflow-postgres container may not be ready yet.
Wait 30 seconds and run: `docker-compose restart airflow-init`

### Debezium connector shows FAILED status
```powershell
# Check the connect logs
docker-compose logs debezium-connect | tail -50
```
Common cause: PostgreSQL replication slot already exists from a previous run.
Fix:
```sql
-- Run in DBeaver connected to banking-postgres (port 5432)
SELECT pg_drop_replication_slot('banking_slot');
```
Then re-run `python register_connector.py`

### MinIO → Snowflake DAG fails
Check Airflow logs in the UI (click the red task → View Log).
Most common causes:
1. Snowflake credentials wrong in `.env`
2. `snowflake/setup.sql` was not run — RAW tables don't exist
3. COMPUTE_WH is suspended — go to Snowflake and resume it

### dbt test failures
```powershell
# Run dbt manually inside the airflow container:
docker exec -it airflow-scheduler bash
cd /opt/airflow/banking_dbt
dbt debug --profiles-dir /home/airflow/.dbt
dbt test --profiles-dir /home/airflow/.dbt
```

### Consumer not receiving messages
Verify the connector is running:
```
http://localhost:8083/connectors/banking-postgres-connector/status
```
Should show `"state": "RUNNING"` for connector and all tasks.

---

## 🛑 Shutting Down

```powershell
# Stop all containers (data is preserved in named volumes)
docker-compose down

# Stop AND delete all data volumes (full reset)
docker-compose down -v
```

---

## 🔐 GitHub Secrets (for CI/CD)

Add these in your repo: **Settings → Secrets → Actions → New repository secret**

| Secret | Value |
|--------|-------|
| `SNOWFLAKE_ACCOUNT` | Your account identifier |
| `SNOWFLAKE_USER` | Your Snowflake username |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` |
| `POSTGRES_USER` | `postgres` |
| `POSTGRES_PASSWORD` | value from your `.env` |

---

## 📊 Tech Stack Versions (2026)

| Tool | Version |
|------|---------|
| PostgreSQL | 16 |
| Confluent Platform (Zookeeper + Kafka) | 7.7.1 |
| Debezium Connect | 2.7 |
| MinIO | RELEASE.2024-12-18 |
| Apache Airflow | 2.10.4 |
| dbt-core | 1.9.x |
| dbt-snowflake | 1.9.x |
| Python | 3.11 |
| kafka-python-ng | 2.2.x |
| pyarrow | 17.x |
| boto3 | 1.35.x |
