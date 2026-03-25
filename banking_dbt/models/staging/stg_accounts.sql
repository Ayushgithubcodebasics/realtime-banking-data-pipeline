{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        CAST(id AS INTEGER) AS account_id,
        CAST(customer_id AS INTEGER) AS customer_id,
        CAST(account_type AS VARCHAR) AS account_type,
        TRY_TO_DECIMAL(balance, 18, 2) AS balance,
        CAST(currency AS VARCHAR) AS currency,
        CAST(created_at AS TIMESTAMP_TZ) AS created_at,
        CAST(updated_at AS TIMESTAMP_TZ) AS updated_at,
        TO_TIMESTAMP_TZ(_cdc_ts / 1000) AS cdc_event_time,
        CAST(_cdc_op AS VARCHAR) AS cdc_operation,
        COALESCE(CAST(_is_deleted AS BOOLEAN), FALSE) AS is_deleted,
        CAST(_loaded_at AS TIMESTAMP_TZ) AS loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _cdc_ts DESC, _loaded_at DESC
        ) AS rn
    FROM {{ source('raw', 'accounts') }}
    WHERE id IS NOT NULL
)

SELECT
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    COALESCE(updated_at, created_at, cdc_event_time) AS record_updated_at,
    cdc_event_time,
    cdc_operation,
    is_deleted,
    loaded_at
FROM ranked
WHERE rn = 1
