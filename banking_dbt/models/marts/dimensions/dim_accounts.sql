{{ config(materialized='table') }}

/*
  dim_accounts — Gold layer account dimension (Star Schema)
  Built from the SCD2 snapshot so balance history is preserved.

  is_current = TRUE  → the row reflects the account's current state
  is_current = FALSE → a historical version of the account
*/

SELECT
    -- Surrogate key
    dbt_scd_id                                         AS account_sk,

    -- Natural keys
    account_id,
    customer_id,

    -- Business attributes
    account_type,
    balance,
    currency,

    -- Source timestamp
    created_at                                         AS account_created_at,

    -- SCD2 effective dates
    dbt_valid_from                                     AS effective_from,
    dbt_valid_to                                       AS effective_to,

    -- Convenience flag
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,

    -- Audit
    dbt_updated_at                                     AS record_updated_at

FROM {{ ref('accounts_snapshot') }}
