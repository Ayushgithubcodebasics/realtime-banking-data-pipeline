{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='sync_all_columns'
) }}

WITH base_transactions AS (
    SELECT *
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    -- Watermark filter: only process records loaded after the latest record
    -- already in the fact table. This replaces a NOT IN subquery which:
    --   1. Performs a full table scan on every run (O(n) and growing)
    --   2. Silently returns zero rows if any transaction_id is NULL
    -- The unique_key config handles any late-arriving duplicates via MERGE.
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),
account_versions AS (
    SELECT * FROM {{ ref('dim_accounts') }}
),
customer_versions AS (
    SELECT * FROM {{ ref('dim_customers') }}
)
SELECT
    t.transaction_id,
    a.account_sk,
    c.customer_sk,
    t.account_id,
    a.customer_id,
    c.full_name AS customer_name,
    a.account_type,
    a.currency,
    t.transaction_type,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_time,
    DATE(t.transaction_time) AS transaction_date,
    DATE_PART('year', t.transaction_time) AS txn_year,
    DATE_PART('month', t.transaction_time) AS txn_month,
    DATE_PART('dow', t.transaction_time) AS txn_day_of_week,
    t.cdc_event_time,
    t.cdc_operation,
    t.loaded_at
FROM base_transactions t
LEFT JOIN account_versions a
    ON t.account_id = a.account_id
   AND t.transaction_time >= a.effective_from
   AND (
        a.effective_to IS NULL
        OR t.transaction_time < a.effective_to
   )
LEFT JOIN customer_versions c
    ON a.customer_id = c.customer_id
   AND t.transaction_time >= c.effective_from
   AND (
        c.effective_to IS NULL
        OR t.transaction_time < c.effective_to
   )
