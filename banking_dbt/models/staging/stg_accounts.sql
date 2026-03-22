{{ config(materialized='view') }}

/*
  Staging: Accounts (Silver layer)
  Deduplicates and casts the raw accounts table.
  Keeps the latest record per account_id (most recent created_at).
*/

WITH deduplicated AS (
    SELECT
        CAST(id           AS INTEGER)      AS account_id,
        CAST(customer_id  AS INTEGER)      AS customer_id,
        CAST(account_type AS VARCHAR)      AS account_type,
        CAST(balance      AS FLOAT)        AS balance,
        CAST(currency     AS VARCHAR)      AS currency,
        CAST(created_at   AS TIMESTAMP_TZ) AS created_at,
        CURRENT_TIMESTAMP                  AS load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY created_at DESC
        )                                  AS rn
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
    load_timestamp
FROM deduplicated
WHERE rn = 1
