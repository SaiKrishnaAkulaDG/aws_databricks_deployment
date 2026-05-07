{{
  config(
    materialized='table',
    post_hook=[
      "COPY (SELECT * FROM main.silver_accounts) TO 's3://{{ var(\"s3_bucket\") }}/silver/accounts/data.parquet' (FORMAT PARQUET);"
    ]
  )
}}

WITH step1_quality_check AS (
  SELECT
    account_id,
    customer_name,
    account_status,
    credit_limit,
    current_balance,
    open_date,
    billing_cycle_start,
    billing_cycle_end,
    _source_file,
    _ingested_at,
    _pipeline_run_id,
    CASE
      WHEN account_id IS NULL OR account_id = '' OR
           open_date IS NULL OR credit_limit IS NULL OR
           current_balance IS NULL OR billing_cycle_start IS NULL OR
           billing_cycle_end IS NULL OR account_status IS NULL OR
           account_status = ''
        THEN 'NULL_REQUIRED_FIELD'
      WHEN account_status NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
        THEN 'INVALID_ACCOUNT_STATUS'
      ELSE NULL
    END AS _rejection_reason
  FROM read_parquet('s3://{{ var("s3_bucket") }}/bronze/accounts/date={{ var("target_date") }}/data.parquet')
),

step2_rejected AS (
  SELECT
    account_id,
    customer_name,
    account_status,
    credit_limit,
    current_balance,
    open_date,
    billing_cycle_start,
    billing_cycle_end,
    _source_file,
    _ingested_at,
    _pipeline_run_id,
    CURRENT_TIMESTAMP AS _rejected_at,
    _rejection_reason
  FROM step1_quality_check
  WHERE _rejection_reason IS NOT NULL
),

step3a_incoming_passing AS (
  SELECT
    account_id,
    customer_name,
    account_status,
    credit_limit,
    current_balance,
    open_date,
    billing_cycle_start,
    billing_cycle_end,
    _source_file,
    _ingested_at AS _bronze_ingested_at,
    _pipeline_run_id,
    CURRENT_TIMESTAMP AS _record_valid_from,
    1 AS _is_incoming
  FROM step1_quality_check
  WHERE _rejection_reason IS NULL
),

step3b_existing AS (
  -- pipeline.py always creates silver/accounts/data.parquet placeholder before running this model,
  -- so the file is guaranteed to exist; no run_query glob check needed.
  {% set silver_file = 's3://' ~ var('s3_bucket') ~ '/silver/accounts/data.parquet' %}

  SELECT
    account_id,
    customer_name,
    account_status,
    credit_limit,
    current_balance,
    open_date,
    billing_cycle_start,
    billing_cycle_end,
    _source_file,
    _bronze_ingested_at,
    _pipeline_run_id,
    _record_valid_from,
    0 AS _is_incoming
  FROM read_parquet('{{ silver_file }}')
  WHERE account_id NOT IN (SELECT account_id FROM step3a_incoming_passing)
),

step3c_unioned AS (
  SELECT * FROM step3a_incoming_passing
  UNION ALL
  SELECT * FROM step3b_existing
),

step3d_deduped AS (
  SELECT DISTINCT ON (account_id)
    account_id,
    customer_name,
    account_status,
    credit_limit,
    current_balance,
    open_date,
    billing_cycle_start,
    billing_cycle_end,
    _source_file,
    _bronze_ingested_at,
    _pipeline_run_id,
    _record_valid_from
  FROM step3c_unioned
  ORDER BY account_id, _is_incoming DESC, _record_valid_from DESC
)

SELECT
  account_id,
  customer_name,
  account_status,
  credit_limit,
  current_balance,
  open_date,
  billing_cycle_start,
  billing_cycle_end,
  _source_file,
  _bronze_ingested_at,
  _pipeline_run_id,
  _record_valid_from
FROM step3d_deduped
