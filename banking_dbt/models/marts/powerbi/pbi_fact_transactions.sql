{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fact_transactions') }}
