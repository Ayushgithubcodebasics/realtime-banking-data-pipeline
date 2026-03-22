{% snapshot accounts_snapshot %}

{{
    config(
        target_schema = 'ANALYTICS',
        unique_key    = 'account_id',
        strategy      = 'check',
        check_cols    = ['account_type', 'balance', 'currency'],
        invalidate_hard_deletes = false
    )
}}

/*
  SCD Type 2 snapshot for accounts.
  Tracks changes to account_type, balance, and currency over time.
  Particularly useful for auditing balance changes.
*/

SELECT * FROM {{ ref('stg_accounts') }}

{% endsnapshot %}
