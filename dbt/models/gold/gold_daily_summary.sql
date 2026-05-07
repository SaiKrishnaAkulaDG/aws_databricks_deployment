{{
  config(
    materialized='table',
    post_hook=[
      "COPY (SELECT transaction_date, total_transactions, total_signed_amount, transactions_by_type, online_transactions, instore_transactions, total_unresolvable_transactions, total_unresolvable_amount, _computed_at, _pipeline_run_id, _source_period_start, _source_period_end FROM main.gold_daily_summary) TO 's3://{{ var(\"s3_bucket\") }}/gold/daily_summary/data.parquet' (FORMAT PARQUET)"
    ]
  )
}}

-- WARNING: This model must only be executed through pipeline.py.
-- Direct dbt invocation bypasses pipeline.py control plane.

WITH silver_txn AS (
    SELECT transaction_date, transaction_code, _is_resolvable, _signed_amount, channel
    FROM read_parquet('s3://{{ var("s3_bucket") }}/silver/transactions/**/*.parquet')
),

silver_tc AS (
    SELECT transaction_code, transaction_type
    FROM read_parquet('s3://{{ var("s3_bucket") }}/silver/transaction_codes/data.parquet')
),

period_bounds AS (
    SELECT
        MIN(transaction_date) AS min_date,
        MAX(transaction_date) AS max_date,
        MIN(transaction_date) AS _source_period_start,
        MAX(transaction_date) AS _source_period_end
    FROM silver_txn
),

date_spine AS (
    SELECT
        unnest(generate_series(
            CAST(min_date AS TIMESTAMP),
            CAST(max_date AS TIMESTAMP),
            INTERVAL '1 day'
        ))::DATE AS transaction_date
    FROM period_bounds
),

silver_with_type AS (
    SELECT
        st.transaction_date,
        st._is_resolvable,
        st._signed_amount,
        st.channel,
        tc.transaction_type
    FROM silver_txn st
    LEFT JOIN silver_tc tc ON st.transaction_code = tc.transaction_code
)

SELECT
    ds.transaction_date,
    COUNT(*) FILTER (WHERE swt._is_resolvable = true)
        AS total_transactions,
    COALESCE(SUM(swt._signed_amount) FILTER (WHERE swt._is_resolvable = true), 0.0)
        AS total_signed_amount,
    {
        'PURCHASE': {
            'count': COUNT(*) FILTER (WHERE swt.transaction_type = 'PURCHASE' AND swt._is_resolvable = true),
            'sum_signed_amount': COALESCE(SUM(swt._signed_amount) FILTER (WHERE swt.transaction_type = 'PURCHASE' AND swt._is_resolvable = true), 0.0)
        },
        'PAYMENT': {
            'count': COUNT(*) FILTER (WHERE swt.transaction_type = 'PAYMENT' AND swt._is_resolvable = true),
            'sum_signed_amount': COALESCE(SUM(swt._signed_amount) FILTER (WHERE swt.transaction_type = 'PAYMENT' AND swt._is_resolvable = true), 0.0)
        },
        'FEE': {
            'count': COUNT(*) FILTER (WHERE swt.transaction_type = 'FEE' AND swt._is_resolvable = true),
            'sum_signed_amount': COALESCE(SUM(swt._signed_amount) FILTER (WHERE swt.transaction_type = 'FEE' AND swt._is_resolvable = true), 0.0)
        },
        'INTEREST': {
            'count': COUNT(*) FILTER (WHERE swt.transaction_type = 'INTEREST' AND swt._is_resolvable = true),
            'sum_signed_amount': COALESCE(SUM(swt._signed_amount) FILTER (WHERE swt.transaction_type = 'INTEREST' AND swt._is_resolvable = true), 0.0)
        }
    }::STRUCT(
        PURCHASE STRUCT(count BIGINT, sum_signed_amount DOUBLE),
        PAYMENT  STRUCT(count BIGINT, sum_signed_amount DOUBLE),
        FEE      STRUCT(count BIGINT, sum_signed_amount DOUBLE),
        INTEREST STRUCT(count BIGINT, sum_signed_amount DOUBLE)
    ) AS transactions_by_type,
    COUNT(*) FILTER (WHERE swt._is_resolvable = true AND swt.channel = 'ONLINE')
        AS online_transactions,
    COUNT(*) FILTER (WHERE swt._is_resolvable = true AND swt.channel = 'IN_STORE')
        AS instore_transactions,
    COUNT(*) FILTER (WHERE swt._is_resolvable = false)
        AS total_unresolvable_transactions,
    COALESCE(SUM(swt._signed_amount) FILTER (WHERE swt._is_resolvable = false), 0.0)
        AS total_unresolvable_amount,
    CURRENT_TIMESTAMP                                                                  AS _computed_at,
    '{{ var("run_id") }}'                                                              AS _pipeline_run_id,
    pb._source_period_start,
    pb._source_period_end
FROM date_spine ds
CROSS JOIN period_bounds pb
LEFT JOIN silver_with_type swt ON ds.transaction_date = swt.transaction_date
GROUP BY ds.transaction_date, pb._source_period_start, pb._source_period_end
