{{
  config(
    materialized='table',
    post_hook=[
      "COPY (SELECT * FROM main.silver_accounts) TO '/app/data/silver/accounts/data.parquet' (FORMAT PARQUET);"
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
  FROM read_parquet('/app/data/bronze/accounts/date={{ var("target_date") }}/data.parquet')
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
  {% set silver_file = '/app/data/silver/accounts/data.parquet' %}
  {% if execute %}
    {% set file_exists = run_query("SELECT COUNT(*) FROM glob('" ~ silver_file ~ "')").rows[0][0] > 0 %}
  {% else %}
    {% set file_exists = false %}
  {% endif %}

  {% if file_exists %}
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

  UNION ALL
  {% endif %}

  SELECT
    CAST(NULL AS VARCHAR) AS account_id,
    CAST(NULL AS VARCHAR) AS customer_name,
    CAST(NULL AS VARCHAR) AS account_status,
    CAST(NULL AS DECIMAL) AS credit_limit,
    CAST(NULL AS DECIMAL) AS current_balance,
    CAST(NULL AS DATE) AS open_date,
    CAST(NULL AS DATE) AS billing_cycle_start,
    CAST(NULL AS DATE) AS billing_cycle_end,
    CAST(NULL AS VARCHAR) AS _source_file,
    CAST(NULL AS TIMESTAMP) AS _bronze_ingested_at,
    CAST(NULL AS VARCHAR) AS _pipeline_run_id,
    CAST(NULL AS TIMESTAMP) AS _record_valid_from,
    0 AS _is_incoming
  WHERE false
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
