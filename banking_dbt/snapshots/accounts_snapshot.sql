{% snapshot accounts_snapshot %}

{{
    config(
        target_schema='ANALYTICS',
        unique_key='account_id',
        strategy='timestamp',
        updated_at='record_updated_at'
    )
}}

SELECT * FROM {{ ref('stg_accounts') }}

{% endsnapshot %}
