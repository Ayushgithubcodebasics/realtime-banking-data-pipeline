{{ config(materialized='view') }}

WITH raw_metrics AS (
    SELECT
        'raw_customers' AS metric_name,
        COUNT(*)::NUMBER AS metric_value,
        CAST(MAX(_loaded_at) AS TIMESTAMP_NTZ) AS metric_ts
    FROM {{ source('raw', 'customers') }}

    UNION ALL

    SELECT
        'raw_accounts' AS metric_name,
        COUNT(*)::NUMBER AS metric_value,
        CAST(MAX(_loaded_at) AS TIMESTAMP_NTZ) AS metric_ts
    FROM {{ source('raw', 'accounts') }}

    UNION ALL

    SELECT
        'raw_transactions' AS metric_name,
        COUNT(*)::NUMBER AS metric_value,
        CAST(MAX(_loaded_at) AS TIMESTAMP_NTZ) AS metric_ts
    FROM {{ source('raw', 'transactions') }}
),

gold_metrics AS (
    SELECT
        'fact_transactions' AS metric_name,
        COUNT(*)::NUMBER AS metric_value,
        CAST(MAX(loaded_at) AS TIMESTAMP_NTZ) AS metric_ts
    FROM {{ ref('fact_transactions') }}

    UNION ALL

    SELECT
        'current_customers' AS metric_name,
        COUNT(*)::NUMBER AS metric_value,
        CAST(MAX(record_updated_at) AS TIMESTAMP_NTZ) AS metric_ts
    FROM {{ ref('pbi_dim_customers_current') }}

    UNION ALL

    SELECT
        'current_accounts' AS metric_name,
        COUNT(*)::NUMBER AS metric_value,
        CAST(MAX(record_updated_at) AS TIMESTAMP_NTZ) AS metric_ts
    FROM {{ ref('pbi_dim_accounts_current') }}
)

SELECT
    metric_name,
    metric_value,
    metric_ts
FROM raw_metrics

UNION ALL

SELECT
    metric_name,
    metric_value,
    metric_ts
FROM gold_metrics