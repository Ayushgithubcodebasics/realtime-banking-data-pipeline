{% snapshot customers_snapshot %}

{{
    config(
        target_schema = 'ANALYTICS',
        unique_key    = 'customer_id',
        strategy      = 'check',
        check_cols    = ['first_name', 'last_name', 'email'],
        invalidate_hard_deletes = false
    )
}}

/*
  SCD Type 2 snapshot for customers.
  dbt adds: dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to
  - dbt_valid_to IS NULL  → current (active) record
  - dbt_valid_to IS NOT NULL → historical (expired) record
*/

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}
