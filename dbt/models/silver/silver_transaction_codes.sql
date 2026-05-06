{{
  config(materialized='table')
}}

WITH bronze_tc AS (
  SELECT
    transaction_code,
    description,
    debit_credit_indicator,
    transaction_type,
    affects_balance,
    _source_file,
    _ingested_at AS _bronze_ingested_at,
    _pipeline_run_id,
    CURRENT_TIMESTAMP AS _promoted_at
  FROM read_parquet('/app/data/bronze/transaction_codes/data.parquet')
)

SELECT * FROM bronze_tc
