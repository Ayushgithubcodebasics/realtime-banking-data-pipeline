{{ config(materialized='view') }}

WITH unified AS (
    SELECT 'customers' AS source_table, cdc_operation, cdc_event_time
    FROM {{ ref('stg_customers') }}
    UNION ALL
    SELECT 'accounts' AS source_table, cdc_operation, cdc_event_time
    FROM {{ ref('stg_accounts') }}
    UNION ALL
    SELECT 'transactions' AS source_table, cdc_operation, cdc_event_time
    FROM {{ ref('stg_transactions') }}
)
SELECT
    source_table,
    cdc_operation,
    DATE(cdc_event_time) AS cdc_date,
    COUNT(*) AS event_count,
    MAX(cdc_event_time) AS latest_event_time
FROM unified
GROUP BY 1, 2, 3
