{{ config(materialized='view') }}

/*
  Staging: Transactions (Silver layer)
  No deduplication needed — transactions are an append-only ledger.
  All records are kept. Casts and renames for consistency.
*/

SELECT
    CAST(id                 AS BIGINT)       AS transaction_id,
    CAST(account_id         AS INTEGER)      AS account_id,
    CAST(txn_type           AS VARCHAR)      AS transaction_type,
    CAST(amount             AS FLOAT)        AS amount,
    CAST(related_account_id AS INTEGER)      AS related_account_id,  -- NULL for non-transfers
    CAST(status             AS VARCHAR)      AS status,
    CAST(created_at         AS TIMESTAMP_TZ) AS transaction_time,
    CURRENT_TIMESTAMP                        AS load_timestamp
FROM {{ source('raw', 'transactions') }}
WHERE id IS NOT NULL
  AND account_id IS NOT NULL
  AND amount > 0
