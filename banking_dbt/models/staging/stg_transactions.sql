{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        CAST(id AS BIGINT) AS transaction_id,
        CAST(account_id AS INTEGER) AS account_id,
        UPPER(TRIM(CAST(txn_type AS VARCHAR))) AS transaction_type,
        TO_DECIMAL(amount, 18, 2) AS amount,
        CAST(related_account_id AS INTEGER) AS related_account_id,
        UPPER(TRIM(CAST(status AS VARCHAR))) AS status,
        CAST(created_at AS TIMESTAMP_NTZ) AS transaction_time,
        TO_TIMESTAMP_NTZ(_cdc_ts / 1000) AS cdc_event_time,
        LOWER(TRIM(CAST(_cdc_op AS VARCHAR))) AS cdc_operation,
        FALSE AS is_deleted,
        CAST(_loaded_at AS TIMESTAMP_NTZ) AS loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _cdc_ts DESC, _loaded_at DESC
        ) AS rn
    FROM {{ source('raw', 'transactions') }}
    WHERE id IS NOT NULL
      AND account_id IS NOT NULL
)

SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    related_account_id,
    status,
    transaction_time,
    cdc_event_time,
    cdc_operation,
    is_deleted,
    loaded_at
FROM ranked
WHERE rn = 1
  AND amount > 0