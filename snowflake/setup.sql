-- ============================================================
-- Snowflake Setup Script
-- Run this ONCE in a Snowflake worksheet (as ACCOUNTADMIN)
-- BEFORE starting the pipeline.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ── 1. Database & Warehouses ──────────────────────────────────
CREATE DATABASE IF NOT EXISTS BANKING;
USE DATABASE BANKING;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- ── 2. Schemas ────────────────────────────────────────────────
-- RAW       = Bronze layer  (loaded by Airflow from MinIO parquet)
-- ANALYTICS = Silver/Gold layer (built by dbt)
CREATE SCHEMA IF NOT EXISTS BANKING.RAW;
CREATE SCHEMA IF NOT EXISTS BANKING.ANALYTICS;

-- ── 3. RAW Tables (Bronze Layer) ─────────────────────────────
-- These receive raw CDC events from MinIO via Airflow.
-- Column names MUST match the Postgres/consumer column names exactly
-- so MATCH_BY_COLUMN_NAME works in COPY INTO.
-- _cdc_op: Debezium operation type (r=read, c=create, u=update, d=delete)
-- _cdc_ts:  Debezium event timestamp in milliseconds (epoch)

USE SCHEMA BANKING.RAW;

CREATE TABLE IF NOT EXISTS customers (
    id          INTEGER,
    first_name  VARCHAR,
    last_name   VARCHAR,
    email       VARCHAR,
    created_at  TIMESTAMP_TZ,
    _cdc_op     VARCHAR,
    _cdc_ts     BIGINT,
    _loaded_at  TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS accounts (
    id              INTEGER,
    customer_id     INTEGER,
    account_type    VARCHAR,
    balance         FLOAT,
    currency        VARCHAR,
    created_at      TIMESTAMP_TZ,
    _cdc_op         VARCHAR,
    _cdc_ts         BIGINT,
    _loaded_at      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS transactions (
    id                  BIGINT,
    account_id          INTEGER,
    txn_type            VARCHAR,
    amount              FLOAT,
    related_account_id  INTEGER,
    status              VARCHAR,
    created_at          TIMESTAMP_TZ,
    _cdc_op             VARCHAR,
    _cdc_ts             BIGINT,
    _loaded_at          TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── 4. Verify setup ───────────────────────────────────────────
SHOW TABLES IN SCHEMA BANKING.RAW;

-- ── After running the pipeline, dbt will auto-create: ─────────
-- BANKING.ANALYTICS.customers_snapshot   (SCD2 via dbt snapshot)
-- BANKING.ANALYTICS.accounts_snapshot    (SCD2 via dbt snapshot)
-- BANKING.ANALYTICS.stg_customers        (view)
-- BANKING.ANALYTICS.stg_accounts         (view)
-- BANKING.ANALYTICS.stg_transactions     (view)
-- BANKING.ANALYTICS.dim_customers        (table - from snapshot)
-- BANKING.ANALYTICS.dim_accounts         (table - from snapshot)
-- BANKING.ANALYTICS.fact_transactions    (incremental table)
