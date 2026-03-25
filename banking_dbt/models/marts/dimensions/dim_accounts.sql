{{ config(materialized='table') }}

SELECT
    dbt_scd_id AS account_sk,
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at AS account_created_at,
    record_updated_at,
    cdc_event_time,
    cdc_operation,
    is_deleted,
    dbt_valid_from AS effective_from,
    dbt_valid_to AS effective_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
    dbt_updated_at AS snapshot_updated_at
FROM {{ ref('accounts_snapshot') }}
