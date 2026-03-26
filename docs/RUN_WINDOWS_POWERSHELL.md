# Run Guide (Windows PowerShell)

This guide uses generic paths and public-repo-safe examples. Replace placeholders with your own values.

## 1) Prerequisites

- Docker Desktop running
- Python 3.11 installed and available in PowerShell
- A Snowflake account and credentials
- Power BI Desktop if you want to build the report layer

## 2) Open the project folder

```powershell
cd "C:\path\to\realtime-banking-data-pipeline"
```

## 3) Create your local config files

```powershell
Copy-Item .env.example .env
New-Item -ItemType Directory -Force banking_dbt\.dbt | Out-Null
Copy-Item banking_dbt\.dbt\profiles.yml.example banking_dbt\.dbt\profiles.yml
```

Then edit both files and replace every `change-me-*` placeholder with your real local values.

## 4) Install Python dependencies for local commands

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 5) Start the core platform

```powershell
docker compose up -d --build
docker compose ps
```

Core services:
- PostgreSQL
- Debezium Connect
- Kafka
- Zookeeper
- MinIO
- Airflow webserver
- Airflow scheduler
- Airflow metadata PostgreSQL

## 6) Register the Debezium connector

```powershell
python .\kafka-debezium\register_connector.py
```

## 7) Start the generator and consumer

```powershell
docker compose --profile apps up -d generator consumer
docker compose ps
```

If you want a short demo run instead of an endless generator, set `GENERATOR_MAX_ITERATIONS` in `.env` to a small number such as `5` or `20`.

## 8) Open the local UIs

- Airflow: `http://localhost:8080`
- Debezium Connect: `http://localhost:8083/connectors`
- MinIO Console: `http://localhost:9001`

Use the Airflow admin credentials from your local `.env`.

## 9) Run dbt manually if needed

```powershell
cd banking_dbt
dbt deps
dbt run --select staging
dbt snapshot
dbt run --select marts
dbt test
cd ..
```

## 10) Useful troubleshooting commands

```powershell
docker compose logs -f connect
docker compose logs -f kafka
docker compose logs -f consumer
docker compose logs -f generator
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
```

## 11) Stop the stack

```powershell
docker compose down
```

To remove named volumes as well:

```powershell
docker compose down -v
```
