{{ config(materialized='table') }}

/*
  dim_customers — Gold layer customer dimension (Star Schema)
  Built from the SCD2 snapshot so we have full change history.

  is_current = TRUE  → the row reflects the customer's current data
  is_current = FALSE → a historical version of the customer's data
*/

SELECT
    -- Surrogate key (dbt snapshot provides this)
    dbt_scd_id                                         AS customer_sk,

    -- Natural key
    customer_id,

    -- Business attributes
    first_name,
    last_name,
    first_name || ' ' || last_name                     AS full_name,
    email,

    -- Source timestamps
    created_at                                         AS customer_created_at,

    -- SCD2 effective dates
    dbt_valid_from                                     AS effective_from,
    dbt_valid_to                                       AS effective_to,

    -- Convenience flag: is this the active (current) row?
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,

    -- Audit
    dbt_updated_at                                     AS record_updated_at

FROM {{ ref('customers_snapshot') }}
