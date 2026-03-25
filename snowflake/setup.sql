-- ============================================================
-- Snowflake setup script
-- Run once before starting the pipeline.
-- ============================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS BANKING;
USE DATABASE BANKING;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE SCHEMA IF NOT EXISTS BANKING.RAW;
CREATE SCHEMA IF NOT EXISTS BANKING.ANALYTICS;

USE SCHEMA BANKING.RAW;

CREATE TABLE IF NOT EXISTS customers (
    id           INTEGER,
    first_name   VARCHAR,
    last_name    VARCHAR,
    email        VARCHAR,
    created_at   TIMESTAMP_TZ,
    updated_at   TIMESTAMP_TZ,
    _cdc_op      VARCHAR,
    _cdc_ts      BIGINT,
    _is_deleted  BOOLEAN,
    _loaded_at   TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS accounts (
    id           INTEGER,
    customer_id  INTEGER,
    account_type VARCHAR,
    balance      VARCHAR,
    currency     VARCHAR,
    created_at   TIMESTAMP_TZ,
    updated_at   TIMESTAMP_TZ,
    _cdc_op      VARCHAR,
    _cdc_ts      BIGINT,
    _is_deleted  BOOLEAN,
    _loaded_at   TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS transactions (
    id                  BIGINT,
    account_id          INTEGER,
    txn_type            VARCHAR,
    amount              VARCHAR,
    related_account_id  INTEGER,
    status              VARCHAR,
    created_at          TIMESTAMP_TZ,
    _cdc_op             VARCHAR,
    _cdc_ts             BIGINT,
    _is_deleted         BOOLEAN,
    _loaded_at          TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

SHOW TABLES IN SCHEMA BANKING.RAW;
