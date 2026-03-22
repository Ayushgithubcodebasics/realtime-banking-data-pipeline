{{ config(
    materialized  = 'incremental',
    unique_key    = 'transaction_id',
    on_schema_change = 'sync_all_columns'
) }}

/*
  fact_transactions — Gold layer transaction fact table (Star Schema)
  Incremental: only processes rows added since the last run.
  Joins to current account/customer data for denormalisation.
*/

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    -- Only pick up rows newer than the latest we've already loaded
    WHERE transaction_time > (
        SELECT COALESCE(MAX(transaction_time), '2000-01-01'::TIMESTAMP_TZ)
        FROM {{ this }}
    )
    {% endif %}
),

-- Join to current (is_current=TRUE) account and customer records
accounts AS (
    SELECT account_id, customer_id, account_type, currency
    FROM {{ ref('dim_accounts') }}
    WHERE is_current = TRUE
),

customers AS (
    SELECT customer_id, full_name
    FROM {{ ref('dim_customers') }}
    WHERE is_current = TRUE
)

SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    c.full_name                                        AS customer_name,
    a.account_type,
    a.currency,
    t.transaction_type,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_time,

    -- Date parts for BI tool slicing
    DATE(t.transaction_time)                           AS transaction_date,
    DATE_PART('year',  t.transaction_time)             AS txn_year,
    DATE_PART('month', t.transaction_time)             AS txn_month,
    DATE_PART('dow',   t.transaction_time)             AS txn_day_of_week,

    t.load_timestamp
FROM transactions t
LEFT JOIN accounts  a ON t.account_id  = a.account_id
LEFT JOIN customers c ON a.customer_id = c.customer_id
