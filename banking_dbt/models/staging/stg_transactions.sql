{{ config(materialized='view') }}

SELECT
    CAST(id AS BIGINT) AS transaction_id,
    CAST(account_id AS INTEGER) AS account_id,
    CAST(txn_type AS VARCHAR) AS transaction_type,
    TRY_TO_DECIMAL(amount, 18, 2) AS amount,
    CAST(related_account_id AS INTEGER) AS related_account_id,
    CAST(status AS VARCHAR) AS status,
    CAST(created_at AS TIMESTAMP_TZ) AS transaction_time,
    TO_TIMESTAMP_TZ(_cdc_ts / 1000) AS cdc_event_time,
    CAST(_cdc_op AS VARCHAR) AS cdc_operation,
    COALESCE(CAST(_is_deleted AS BOOLEAN), FALSE) AS is_deleted,
    CAST(_loaded_at AS TIMESTAMP_TZ) AS loaded_at
FROM {{ source('raw', 'transactions') }}
WHERE id IS NOT NULL
  AND account_id IS NOT NULL
  AND TRY_TO_DECIMAL(amount, 18, 2) > 0
  AND COALESCE(CAST(_is_deleted AS BOOLEAN), FALSE) = FALSE
