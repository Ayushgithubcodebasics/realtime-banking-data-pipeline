{% snapshot customers_snapshot %}

{{
    config(
        target_schema='ANALYTICS',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='record_updated_at'
    )
}}

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}
