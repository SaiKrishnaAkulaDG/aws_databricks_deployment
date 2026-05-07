{% set target_weeks_str = var('target_weeks', '[]') %}
{% set target_weeks_list = fromjson(target_weeks_str) %}

{{
  config(
    materialized='table',
    post_hook=(
      [
        "COPY (SELECT week_start_date, week_end_date, account_id, total_purchases, avg_purchase_amount, total_payments, total_fees, total_interest, closing_balance, _computed_at, _pipeline_run_id FROM main.gold_weekly_account_summary) TO 's3://{{ var(\"s3_bucket\") }}/gold/weekly_account_summary/data.parquet' (FORMAT PARQUET)"
      ]
      if target_weeks_list | length > 0 else []
    )
  )
}}

-- WARNING: This model must only be executed through pipeline.py.
-- Direct invocation via `dbt run --select gold_weekly_account_summary` bypasses
-- pipeline.py and the pipeline/gold_weekly_control.parquet enforcement gate,
-- causing all weeks to be recomputed and overwriting closing_balance values.
-- There is no structural enforcement for this — it is a named procedural control.

{% if target_weeks_list | length == 0 %}

SELECT
    CAST(NULL AS DATE)      AS week_start_date,
    CAST(NULL AS DATE)      AS week_end_date,
    CAST(NULL AS VARCHAR)   AS account_id,
    CAST(NULL AS BIGINT)    AS total_purchases,
    CAST(NULL AS DOUBLE)    AS avg_purchase_amount,
    CAST(NULL AS DOUBLE)    AS total_payments,
    CAST(NULL AS DOUBLE)    AS total_fees,
    CAST(NULL AS DOUBLE)    AS total_interest,
    CAST(NULL AS DOUBLE)    AS closing_balance,
    CAST(NULL AS TIMESTAMP) AS _computed_at,
    CAST(NULL AS VARCHAR)   AS _pipeline_run_id
WHERE false

{% else %}

{% set weekly_file = 's3://' ~ var('s3_bucket') ~ '/gold/weekly_account_summary/data.parquet' %}
{% if execute %}
  {% set file_exists = run_query("SELECT COUNT(*) FROM glob('" ~ weekly_file ~ "')").rows[0][0] > 0 %}
{% else %}
  {% set file_exists = false %}
{% endif %}

WITH target_week_defs AS (
    {% for week in target_weeks_list %}
    SELECT
        CAST('{{ week.week_start }}' AS DATE) AS week_start,
        CAST('{{ week.week_end }}' AS DATE)   AS week_end
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),

silver_txn AS (
    SELECT account_id, transaction_date, _signed_amount, transaction_code
    FROM read_parquet('s3://{{ var("s3_bucket") }}/silver/transactions/**/*.parquet')
    WHERE _is_resolvable = true
),

silver_tc AS (
    SELECT transaction_code, transaction_type
    FROM read_parquet('s3://{{ var("s3_bucket") }}/silver/transaction_codes/data.parquet')
),

silver_accounts AS (
    SELECT account_id, current_balance
    FROM read_parquet('s3://{{ var("s3_bucket") }}/silver/accounts/data.parquet')
),

filtered_txn AS (
    SELECT
        st.account_id,
        twd.week_start,
        twd.week_end,
        st._signed_amount,
        tc.transaction_type
    FROM silver_txn st
    JOIN silver_tc tc ON st.transaction_code = tc.transaction_code
    JOIN target_week_defs twd
        ON st.transaction_date >= twd.week_start
        AND st.transaction_date <= twd.week_end
),

new_weekly AS (
    SELECT
        ft.week_start                                                                           AS week_start_date,
        ft.week_end                                                                             AS week_end_date,
        ft.account_id,
        COUNT(*) FILTER (WHERE ft.transaction_type = 'PURCHASE')                               AS total_purchases,
        AVG(ft._signed_amount) FILTER (WHERE ft.transaction_type = 'PURCHASE')                 AS avg_purchase_amount,
        COALESCE(SUM(ft._signed_amount) FILTER (WHERE ft.transaction_type = 'PAYMENT'), 0.0)   AS total_payments,
        COALESCE(SUM(ft._signed_amount) FILTER (WHERE ft.transaction_type = 'FEE'), 0.0)       AS total_fees,
        COALESCE(SUM(ft._signed_amount) FILTER (WHERE ft.transaction_type = 'INTEREST'), 0.0)  AS total_interest,
        CURRENT_TIMESTAMP                                                                       AS _computed_at,
        '{{ var("run_id") }}'                                                                   AS _pipeline_run_id
    FROM filtered_txn ft
    GROUP BY ft.week_start, ft.week_end, ft.account_id
),

new_with_balance AS (
    SELECT
        nw.week_start_date,
        nw.week_end_date,
        nw.account_id,
        nw.total_purchases,
        nw.avg_purchase_amount,
        nw.total_payments,
        nw.total_fees,
        nw.total_interest,
        sa.current_balance AS closing_balance,
        nw._computed_at,
        nw._pipeline_run_id
    FROM new_weekly nw
    JOIN silver_accounts sa ON nw.account_id = sa.account_id
)

{% if file_exists %}

, existing_weekly AS (
    SELECT * FROM read_parquet('{{ weekly_file }}')
    WHERE week_start_date NOT IN (
        SELECT DISTINCT week_start FROM target_week_defs
    )
)

SELECT * FROM new_with_balance
UNION ALL
SELECT * FROM existing_weekly

{% else %}

SELECT * FROM new_with_balance

{% endif %}

{% endif %}
