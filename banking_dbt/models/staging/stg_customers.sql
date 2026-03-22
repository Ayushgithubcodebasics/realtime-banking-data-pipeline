{{ config(materialized='view') }}

/*
  Staging: Customers (Silver layer)
  Deduplicates the raw table: keeps the most recently created record per
  customer_id.  Casts all types explicitly.

  NOTE: The RAW table now uses flat Snowflake columns (not a VARIANT column),
  so we select column names directly instead of the original v:field:: syntax.
*/

WITH deduplicated AS (
    SELECT
        CAST(id          AS INTEGER)     AS customer_id,
        CAST(first_name  AS VARCHAR)     AS first_name,
        CAST(last_name   AS VARCHAR)     AS last_name,
        CAST(email       AS VARCHAR)     AS email,
        CAST(created_at  AS TIMESTAMP_TZ) AS created_at,
        CURRENT_TIMESTAMP                AS load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY created_at DESC
        )                                AS rn
    FROM {{ source('raw', 'customers') }}
    WHERE id IS NOT NULL
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    load_timestamp
FROM deduplicated
WHERE rn = 1
