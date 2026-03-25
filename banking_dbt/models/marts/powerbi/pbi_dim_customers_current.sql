{{ config(materialized='view') }}

SELECT *
FROM {{ ref('dim_customers') }}
WHERE is_current = TRUE
  AND is_deleted = FALSE
