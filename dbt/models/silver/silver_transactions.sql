{{
  config(
    materialized='table',
    post_hook=[
      "COPY (SELECT transaction_id, account_id, transaction_date, amount, transaction_code, merchant_name, channel, _source_file, _bronze_ingested_at, _pipeline_run_id, _promoted_at, _is_resolvable, _signed_amount, _missing_merchant_name FROM main.silver_transactions WHERE _rejection_reason IS NULL) TO 's3://{{ var(\"s3_bucket\") }}/silver/transactions/date={{ var(\"target_date\") }}/data.parquet' (FORMAT PARQUET)",
      "COPY (SELECT transaction_id, account_id, transaction_date, amount, transaction_code, merchant_name, channel, _source_file, _pipeline_run_id, _rejected_at, _rejection_reason FROM main.silver_transactions WHERE _rejection_reason IS NOT NULL) TO 's3://{{ var(\"s3_bucket\") }}/silver/quarantine/date={{ var(\"target_date\") }}/rejected.parquet' (FORMAT PARQUET)",
      "SELECT CASE WHEN (SELECT COUNT(*) FROM read_parquet('s3://{{ var(\"s3_bucket\") }}/bronze/transactions/date={{ var(\"target_date\") }}/data.parquet')) != (SELECT COUNT(*) FROM read_parquet('s3://{{ var(\"s3_bucket\") }}/silver/transactions/date={{ var(\"target_date\") }}/data.parquet')) + (SELECT COUNT(*) FROM read_parquet('s3://{{ var(\"s3_bucket\") }}/silver/quarantine/date={{ var(\"target_date\") }}/rejected.parquet')) THEN error('INV-05 FAIL: bronze_count != silver_count + quarantine_count for date={{ var(\"target_date\") }}') END"
    ]
  )
}}

WITH bronze_source AS (
  SELECT * FROM read_parquet('s3://{{ var("s3_bucket") }}/bronze/transactions/date={{ var("target_date") }}/data.parquet')
),

existing_silver_ids AS (
  {% set silver_glob = 's3://' ~ var('s3_bucket') ~ '/silver/transactions/**/*.parquet' %}
  {% if execute %}
    {% set silver_exists = run_query("SELECT COUNT(*) FROM glob('" ~ silver_glob ~ "')").rows[0][0] > 0 %}
  {% else %}
    {% set silver_exists = false %}
  {% endif %}

  {% if silver_exists %}
  SELECT transaction_id
  FROM read_parquet('{{ silver_glob }}', filename=true)
  WHERE filename NOT LIKE '%date={{ var("target_date") }}%'
  {% else %}
  SELECT CAST(NULL AS VARCHAR) AS transaction_id WHERE false
  {% endif %}
),

silver_tc AS (
  SELECT transaction_code, debit_credit_indicator, transaction_type
  FROM read_parquet('s3://{{ var("s3_bucket") }}/silver/transaction_codes/data.parquet')
),

silver_accounts AS (
  SELECT account_id
  FROM read_parquet('s3://{{ var("s3_bucket") }}/silver/accounts/data.parquet')
),

quality_classified AS (
  SELECT
    b.*,
    CASE
      WHEN b.transaction_id IS NULL OR b.transaction_id = ''
           OR b.account_id IS NULL OR b.account_id = ''
           OR b.transaction_date IS NULL
           OR b.amount IS NULL
           OR b.transaction_code IS NULL OR b.transaction_code = ''
           OR b.channel IS NULL OR b.channel = ''
        THEN 'NULL_REQUIRED_FIELD'
      WHEN b.amount <= 0
        THEN 'INVALID_AMOUNT'
      WHEN b.transaction_id IN (SELECT transaction_id FROM existing_silver_ids)
        THEN 'DUPLICATE_TRANSACTION_ID'
      WHEN b.transaction_code NOT IN (SELECT transaction_code FROM silver_tc)
        THEN 'INVALID_TRANSACTION_CODE'
      WHEN b.channel NOT IN ('ONLINE', 'IN_STORE')
        THEN 'INVALID_CHANNEL'
      ELSE NULL
    END AS _rejection_reason
  FROM bronze_source b
),

silver_candidates AS (
  SELECT * FROM quality_classified WHERE _rejection_reason IS NULL
),

silver_signed AS (
  SELECT
    sc.*,
    tc.debit_credit_indicator,
    tc.transaction_type,
    CASE
      WHEN tc.debit_credit_indicator = 'DR' THEN sc.amount
      WHEN tc.debit_credit_indicator = 'CR' THEN -1.0 * sc.amount
    END AS _signed_amount
  FROM silver_candidates sc
  JOIN silver_tc tc ON sc.transaction_code = tc.transaction_code
),

silver_records AS (
  SELECT
    ss.transaction_id,
    ss.account_id,
    ss.transaction_date,
    ss.amount,
    ss.transaction_code,
    ss.merchant_name,
    ss.channel,
    ss._source_file,
    ss._ingested_at AS _bronze_ingested_at,
    '{{ var("run_id") }}' AS _pipeline_run_id,
    CURRENT_TIMESTAMP AS _promoted_at,
    CASE WHEN ss.account_id IN (SELECT account_id FROM silver_accounts) THEN TRUE ELSE FALSE END AS _is_resolvable,
    ss._signed_amount,
    CASE WHEN ss.transaction_type = 'PURCHASE' AND ss.merchant_name IS NULL THEN TRUE ELSE FALSE END AS _missing_merchant_name,
    CAST(NULL AS TIMESTAMP) AS _rejected_at,
    CAST(NULL AS VARCHAR) AS _rejection_reason
  FROM silver_signed ss
),

quarantine_records AS (
  SELECT
    qc.transaction_id,
    qc.account_id,
    qc.transaction_date,
    qc.amount,
    qc.transaction_code,
    qc.merchant_name,
    qc.channel,
    qc._source_file,
    CAST(NULL AS TIMESTAMP) AS _bronze_ingested_at,
    '{{ var("run_id") }}' AS _pipeline_run_id,
    CAST(NULL AS TIMESTAMP) AS _promoted_at,
    CAST(NULL AS BOOLEAN) AS _is_resolvable,
    CAST(NULL AS DOUBLE) AS _signed_amount,
    CAST(NULL AS BOOLEAN) AS _missing_merchant_name,
    CURRENT_TIMESTAMP AS _rejected_at,
    qc._rejection_reason
  FROM quality_classified qc
  WHERE qc._rejection_reason IS NOT NULL
)

SELECT * FROM silver_records
UNION ALL
SELECT * FROM quarantine_records

-- WARNING: This model must only be executed through pipeline.py.
-- Direct dbt invocation bypasses pipeline.py control plane.
