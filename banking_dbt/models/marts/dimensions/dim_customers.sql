{{ config(materialized='table') }}

SELECT
    dbt_scd_id AS customer_sk,
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,
    email,
    created_at AS customer_created_at,
    record_updated_at,
    cdc_event_time,
    cdc_operation,
    is_deleted,
    dbt_valid_from AS effective_from,
    dbt_valid_to AS effective_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
    dbt_updated_at AS snapshot_updated_at
FROM {{ ref('customers_snapshot') }}
