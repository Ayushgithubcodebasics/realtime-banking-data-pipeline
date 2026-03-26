{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        CAST(id AS INTEGER) AS account_id,
        CAST(customer_id AS INTEGER) AS customer_id,
        CASE
            WHEN UPPER(TRIM(CAST(account_type AS VARCHAR))) IN ('SAVINGS', 'CHECKING')
                THEN UPPER(TRIM(CAST(account_type AS VARCHAR)))
            ELSE 'CHECKING'
        END AS account_type,
        TO_DECIMAL(balance, 18, 2) AS balance,
        UPPER(TRIM(CAST(currency AS VARCHAR))) AS currency,
        CAST(created_at AS TIMESTAMP_NTZ) AS created_at,
        TO_TIMESTAMP_NTZ(_cdc_ts / 1000) AS cdc_event_time,
        LOWER(TRIM(CAST(_cdc_op AS VARCHAR))) AS cdc_operation,
        FALSE AS is_deleted,
        CAST(_loaded_at AS TIMESTAMP_NTZ) AS loaded_at,
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