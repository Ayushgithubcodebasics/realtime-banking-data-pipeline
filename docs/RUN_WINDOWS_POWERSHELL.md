# Windows PowerShell Run Guide

## 1) Open the project folder

```powershell
cd "C:\path\to\realtime-banking-data-pipeline-main"
```

## 2) Create local config files

```powershell
Copy-Item .env.example .env
Copy-Item banking_dbt\.dbt\profiles.yml.example banking_dbt\.dbt\profiles.yml
```

Edit both files before continuing.

## 3) Start core services

```powershell
docker compose up -d --build
```

Check service status:

```powershell
docker compose ps
```

## 4) Register the Debezium connector

```powershell
python .\kafka-debezium\register_connector.py
```

Expected result: the connector should reach `RUNNING`.

## 5) Start the data-producing app services

```powershell
docker compose --profile apps up -d generator consumer
```

## 6) Watch logs

```powershell
docker compose logs -f generator
docker compose logs -f consumer
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

## 7) Open the UIs

- Airflow: `http://localhost:8080`
- Debezium Connect: `http://localhost:8083/connectors`
- MinIO Console: `http://localhost:9001`

## 8) Run dbt manually if needed

```powershell
cd banking_dbt
dbt deps
dbt run --select staging
dbt snapshot
dbt run --select marts
dbt test
cd ..
```

## 9) Validate code quality locally

```powershell
python -m pip install -r requirements.txt
ruff check .
pytest tests -v
```

## 10) Stop everything

```powershell
docker compose --profile apps down
```

To remove volumes too:

```powershell
docker compose --profile apps down -v
```
