{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        CAST(id AS INTEGER) AS customer_id,
        TRIM(CAST(first_name AS VARCHAR)) AS first_name,
        TRIM(CAST(last_name AS VARCHAR)) AS last_name,
        LOWER(TRIM(CAST(email AS VARCHAR))) AS email,
        CAST(created_at AS TIMESTAMP_NTZ) AS created_at,
        TO_TIMESTAMP_NTZ(_cdc_ts / 1000) AS cdc_event_time,
        LOWER(TRIM(CAST(_cdc_op AS VARCHAR))) AS cdc_operation,
        FALSE AS is_deleted,
        CAST(_loaded_at AS TIMESTAMP_NTZ) AS loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _cdc_ts DESC, _loaded_at DESC
        ) AS rn
    FROM {{ source('raw', 'customers') }}
    WHERE id IS NOT NULL
),

base_current AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        created_at,
        CAST(
            COALESCE(created_at, cdc_event_time, loaded_at, CURRENT_TIMESTAMP())
            AS TIMESTAMP_NTZ
        ) AS record_updated_at,
        cdc_event_time,
        cdc_operation,
        is_deleted,
        loaded_at
    FROM ranked
    WHERE rn = 1
),

missing_customer_ids AS (
    SELECT DISTINCT
        CAST(a.customer_id AS INTEGER) AS customer_id
    FROM {{ source('raw', 'accounts') }} a
    LEFT JOIN base_current c
        ON CAST(a.customer_id AS INTEGER) = c.customer_id
    WHERE a.customer_id IS NOT NULL
      AND c.customer_id IS NULL
),

placeholder_customers AS (
    SELECT
        customer_id,
        'Unknown' AS first_name,
        'Customer' AS last_name,
        'unknown_customer_' || customer_id || '@placeholder.local' AS email,
        CAST(NULL AS TIMESTAMP_NTZ) AS created_at,
        TO_TIMESTAMP_NTZ('1900-01-01 00:00:00') AS record_updated_at,
        TO_TIMESTAMP_NTZ('1900-01-01 00:00:00') AS cdc_event_time,
        'r' AS cdc_operation,
        FALSE AS is_deleted,
        TO_TIMESTAMP_NTZ('1900-01-01 00:00:00') AS loaded_at
    FROM missing_customer_ids
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    record_updated_at,
    cdc_event_time,
    cdc_operation,
    is_deleted,
    loaded_at
FROM base_current

UNION ALL

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    record_updated_at,
    cdc_event_time,
    cdc_operation,
    is_deleted,
    loaded_at
FROM placeholder_customers