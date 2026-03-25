{{ config(materialized='view') }}

SELECT *
FROM {{ ref('dim_accounts') }}
WHERE is_current = TRUE
  AND is_deleted = FALSE
