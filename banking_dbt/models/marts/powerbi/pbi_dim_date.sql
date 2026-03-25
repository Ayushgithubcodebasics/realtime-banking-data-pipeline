{{ config(materialized='table') }}

WITH bounds AS (
    SELECT
        COALESCE(MIN(transaction_date), TO_DATE('2024-01-01')) AS min_date,
        COALESCE(MAX(transaction_date), DATEADD('day', 365, CURRENT_DATE)) AS max_date
    FROM {{ ref('fact_transactions') }}
),
spine AS (
    SELECT DATEADD(day, SEQ4(), min_date) AS date_day
    FROM bounds, TABLE(GENERATOR(ROWCOUNT => 5000))
    QUALIFY date_day <= max_date
)
SELECT
    date_day,
    YEAR(date_day) AS year_no,
    MONTH(date_day) AS month_no,
    MONTHNAME(date_day) AS month_name,
    DAY(date_day) AS day_no,
    DAYNAME(date_day) AS day_name,
    WEEK(date_day) AS week_no,
    QUARTER(date_day) AS quarter_no,
    TO_VARCHAR(date_day, 'YYYY-MM') AS year_month,
    TO_VARCHAR(date_day, 'YYYY-MM-DD') AS date_key
FROM spine
