# Real-Time Banking Data Pipeline

An end-to-end data engineering project that captures banking-style change data from PostgreSQL, streams it through Debezium and Kafka, lands raw CDC data in MinIO as Parquet, loads it into Snowflake, transforms it with dbt, orchestrates it with Airflow, and serves analytics-ready models for Power BI.

## Why this project matters

This repository is designed to demonstrate practical data engineering skills, not just tool familiarity. It shows how to:

- ingest **CDC events** instead of relying on static batch files
- preserve a **raw landing zone** before transformation
- build **staging, snapshot, dimension, fact, and BI-serving layers** in dbt
- orchestrate multi-step workflows with **Airflow**
- validate code quality with **pytest, Ruff, and GitHub Actions**
- prepare a warehouse model that is usable for **DirectQuery-style reporting in Power BI**

## Architecture

![End-to-end architecture](docs/images/architecture-overview.png)

**Flow:** PostgreSQL â†’ Debezium â†’ Kafka â†’ Consumer â†’ MinIO â†’ Airflow â†’ Snowflake RAW â†’ dbt staging/snapshots/marts â†’ Power BI

## Tech stack

- **Source system:** PostgreSQL
- **CDC:** Debezium
- **Streaming:** Apache Kafka
- **Object storage / landing zone:** MinIO
- **Warehouse:** Snowflake
- **Transformations:** dbt
- **Orchestration:** Apache Airflow
- **Analytics / BI:** Power BI
- **Language:** Python 3.11
- **Local platform:** Docker Compose
- **CI/CD:** GitHub Actions

## Key features

- End-to-end **CDC pipeline** using Debezium and Kafka
- Table-wise Parquet landing in MinIO for raw data retention
- Snowflake bronze/raw ingestion triggered from Airflow
- dbt staging models with CDC-aware deduplication logic
- SCD Type 2 snapshots for customers and accounts
- Fact model joined to the **correct historical dimension version** using effective date windows
- Power BI-ready serving views for dimensions, facts, date, CDC audit, and pipeline health
- Unit tests for generator and consumer behavior
- CI pipeline for linting, testing, and dbt parse validation

## Project structure

```text
.
â”œâ”€â”€ .github/workflows/              # CI/CD workflows
â”œâ”€â”€ banking_dbt/                    # dbt project: staging, snapshots, marts, BI views
â”œâ”€â”€ common/                         # shared config loading helpers
â”œâ”€â”€ consumer/                       # Kafka -> MinIO CDC consumer
â”œâ”€â”€ data-generator/                 # synthetic banking activity generator
â”œâ”€â”€ docker/dags/                    # Airflow DAGs
â”œâ”€â”€ docs/                           # runbooks, notes, and screenshots
â”œâ”€â”€ kafka-debezium/                 # Debezium connector registration script
â”œâ”€â”€ postgres/                       # source schema/bootstrap SQL
â”œâ”€â”€ powerbi/                        # DAX guidance, theme, and report blueprint
â”œâ”€â”€ scripts/                        # helper scripts
â”œâ”€â”€ snowflake/                      # Snowflake setup SQL
â”œâ”€â”€ docker-compose.yml              # local stack definition
â”œâ”€â”€ dockerfile-airflow.dockerfile   # Airflow image
â””â”€â”€ dockerfile-app.dockerfile       # app image for generator/consumer
```

## Pipeline flow

### 1. Source data generation
A Python generator writes inserts, updates, deletes, and transaction activity into PostgreSQL banking tables.

### 2. CDC capture
Debezium reads PostgreSQL WAL changes and publishes CDC events to Kafka topics.

### 3. Raw ingestion
A Python consumer reads Kafka events, enriches them with CDC metadata, batches them, and writes table-specific Parquet files to MinIO.

### 4. Warehouse loading
An Airflow DAG polls MinIO for new Parquet files and loads only unprocessed files into Snowflake `RAW` tables.

### 5. Transformations
A second Airflow DAG runs dbt in sequence:
- staging models
- SCD2 snapshots
- marts
- tests

### 6. BI serving
Power BI connects to dbt-produced serving models built for direct analytical consumption.

## Data model layers

### RAW layer
Snowflake `RAW` stores landed CDC data from MinIO.

### Staging layer
- `stg_customers`
- `stg_accounts`
- `stg_transactions`

These models standardize types, normalize CDC columns, and deduplicate records using CDC timestamps and load timestamps.

### Snapshot / SCD2 layer
- `customers_snapshot`
- `accounts_snapshot`

### Gold / marts layer
- `dim_customers`
- `dim_accounts`
- `fact_transactions`

### Power BI serving layer
- `pbi_dim_customers_current`
- `pbi_dim_accounts_current`
- `pbi_fact_transactions`
- `pbi_dim_date`
- `pbi_cdc_audit`
- `pbi_pipeline_health`

## Setup

### Prerequisites

- Docker Desktop
- Python **3.11**
- Snowflake account with permissions to create and use the project database objects
- Power BI Desktop for the reporting layer

### Local configuration

1. Copy `.env.example` to `.env`
2. Copy `banking_dbt/.dbt/profiles.yml.example` to `banking_dbt/.dbt/profiles.yml`
3. Fill in your real Snowflake values locally
4. Keep both files out of Git

## How to run

### Start core services

```powershell
docker compose up -d --build
docker compose ps
```

### Register the Debezium connector

```powershell
python .\kafka-debezium\register_connector.py
```

### Start the app services

```powershell
docker compose --profile apps up -d generator consumer
```

### Open the local UIs

- Airflow: `http://localhost:8080`
- Debezium Connect: `http://localhost:8083/connectors`
- MinIO Console: `http://localhost:9001`

### Run dbt manually when needed

```powershell
cd banking_dbt
dbt deps
dbt run --select staging
dbt snapshot
dbt run --select marts
dbt test
cd ..
```

## Proof of execution

### Local services running in Docker
![Docker services running](docs/images/docker-services-running.png)

### Debezium connector successfully registered and running
![Debezium connector](docs/images/debezium-connector-running.png)

### Generator and consumer containers built and started
![Generator and consumer build and start](docs/images/generator-consumer-startup.png)

### Generator producing iterative banking activity
![Generator iterations](docs/images/generator-iterations.png)

### Consumer receiving CDC events and writing Parquet batches
![Consumer writing batches](docs/images/consumer-receiving-events.png)

### MinIO raw bucket organized by source table
![MinIO raw bucket](docs/images/minio-raw-bucket.png)

### Airflow DAG inventory
![Airflow DAG list](docs/images/airflow-dag-list.png)

### Airflow bronze load DAG: MinIO to Snowflake RAW
![Airflow bronze DAG](docs/images/airflow-bronze-dag.png)

### Airflow dbt DAG: staging, snapshot, marts, tests
![Airflow dbt DAG](docs/images/airflow-dbt-dag.png)

### Snowflake raw and analytics objects populated
![Snowflake database structure](docs/images/snowflake-database-structure.png)

## Engineering decisions and improvements

This version improves the project in several important ways:

- **CDC-aware deduplication:** staging models use CDC timestamps and load timestamps instead of weaker ordering assumptions
- **Historical correctness:** the fact table joins to the right SCD2 record using effective date windows
- **Safer consumer behavior:** offsets are committed only after successful upload
- **Raw traceability:** CDC metadata is preserved through ingestion and transformation
- **Operational visibility:** BI-serving models include CDC audit and pipeline health views
- **Automated validation:** tests and linting are wired into GitHub Actions

## Challenges solved

- Converting noisy CDC records into analytics-ready models
- Handling inserts, updates, and deletes consistently across the stack
- Preserving monetary values safely through the ingestion path
- Ensuring dbt marts align with historical dimension versions
- Orchestrating raw loads and downstream transformations cleanly in Airflow
- Packaging the project so it can be run locally with Docker Compose
