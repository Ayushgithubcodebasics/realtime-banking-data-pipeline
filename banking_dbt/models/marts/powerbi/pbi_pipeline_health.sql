{{ config(materialized='view') }}

WITH raw_metrics AS (
    SELECT 'raw_customers' AS metric_name, COUNT(*)::NUMBER AS metric_value, MAX(_loaded_at) AS metric_ts
    FROM {{ source('raw', 'customers') }}
    UNION ALL
    SELECT 'raw_accounts', COUNT(*)::NUMBER, MAX(_loaded_at)
    FROM {{ source('raw', 'accounts') }}
    UNION ALL
    SELECT 'raw_transactions', COUNT(*)::NUMBER, MAX(_loaded_at)
    FROM {{ source('raw', 'transactions') }}
),
gold_metrics AS (
    SELECT 'fact_transactions' AS metric_name, COUNT(*)::NUMBER AS metric_value, MAX(loaded_at) AS metric_ts
    FROM {{ ref('fact_transactions') }}
    UNION ALL
    SELECT 'current_customers', COUNT(*)::NUMBER, MAX(record_updated_at)
    FROM {{ ref('pbi_dim_customers_current') }}
    UNION ALL
    SELECT 'current_accounts', COUNT(*)::NUMBER, MAX(record_updated_at)
    FROM {{ ref('pbi_dim_accounts_current') }}
)
SELECT * FROM raw_metrics
UNION ALL
SELECT * FROM gold_metrics
