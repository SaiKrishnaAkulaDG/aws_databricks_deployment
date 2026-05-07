# S3-Direct Refactor Plan

**Goal:** Write pipeline output (bronze/silver/gold/pipeline) directly to S3 instead of the local EC2 EBS volume. Source CSVs stay at `/app/source/` (cloned from GitHub). The post-run `aws s3 sync` step in GitHub Actions is removed entirely.

**Bucket:** `cc-transaction-databricks-datalake-2026`

---

## Files to Change (17 total — 16 changed, 1 new)

### New File

| File | What |
|------|------|
| `pipeline/s3_utils.py` | S3 utility module: `configure_duckdb_s3(conn)`, `s3_key_exists(bucket, key)`, `atomic_parquet_put(bucket, key, df)`, `parse_s3_uri(uri)` |

### Requirements & Config

| File | Change |
|------|--------|
| `requirements.txt` | Add `boto3>=1.34.0` and `pyarrow>=14.0.0` |
| `.env.example` | Add `S3_BUCKET=cc-transaction-databricks-datalake-2026` and `AWS_DEFAULT_REGION=us-east-1` |
| `docker-compose.yml` | Add `network_mode: host` (needed for container to reach EC2 IMDS for IAM credentials); add `S3_BUCKET` and `AWS_DEFAULT_REGION` to `environment` |
| `Dockerfile` | Add `RUN python3 -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs; INSTALL parquet;'); c.close()"` to pre-install DuckDB extensions at image build time |

### dbt Config

| File | Change |
|------|--------|
| `dbt/profiles.yml` | Add `httpfs` to extensions list; add `settings: {s3_region: us-east-1}` |
| `pipeline/dbt_runner.py` | Add `"s3_bucket": os.environ.get("S3_BUCKET", "cc-transaction-databricks-datalake-2026")` to `default_vars` in `derive_execution_order()` |

### Bronze Loaders (3 files — identical pattern)

All three files (`bronze_accounts.py`, `bronze_transactions.py`, `bronze_transaction_codes.py`):

- Import `configure_duckdb_s3`, `s3_key_exists` from `pipeline.s3_utils`
- Read `bucket = os.environ["S3_BUCKET"]` at top of function
- Build `s3_key = "bronze/..."` and `target_path = f"s3://{bucket}/{s3_key}"`
- Replace `os.path.exists(target_path)` → `s3_key_exists(bucket, s3_key)` for idempotency gate
- Remove `os.makedirs`, `temp_path`, `os.rename`, `os.remove`
- Add `configure_duckdb_s3(conn)` to both DuckDB connections (write and count)
- DuckDB `COPY ... TO '{target_path}'` now writes directly to S3 (no temp file — S3 PutObject is atomic)

### dbt SQL Models (5 files)

All paths change from `/app/data/...` to `s3://{{ var("s3_bucket") }}/...`

**`dbt/models/silver/silver_transaction_codes.sql`**
- Line 16: `read_parquet('/app/data/bronze/transaction_codes/data.parquet')` → `read_parquet('s3://{{ var("s3_bucket") }}/bronze/transaction_codes/data.parquet')`

**`dbt/models/silver/silver_accounts.sql`**
- Line 5 (post_hook COPY target): `/app/data/silver/accounts/data.parquet` → `s3://{{ var(\"s3_bucket\") }}/silver/accounts/data.parquet`
- Line 34 (bronze source): `/app/data/bronze/accounts/date=...` → `s3://{{ var("s3_bucket") }}/bronze/accounts/date=.../data.parquet`
- Line 76 (`silver_file` var): `/app/data/silver/accounts/data.parquet` → `'s3://' ~ var('s3_bucket') ~ '/silver/accounts/data.parquet'`
- Line 98 (`read_parquet` of existing silver): uses `silver_file` variable — auto-updated

**`dbt/models/silver/silver_transactions.sql`**
- Lines 5–7 (post_hook COPY targets for silver, quarantine, INV-05 check): all local paths → S3 URIs with `var(\"s3_bucket\")`
- Line 13 (bronze source): `/app/data/bronze/transactions/date=...` → S3 URI
- Line 17 (`silver_glob` var): `/app/data/silver/transactions/**/*.parquet` → `'s3://' ~ var('s3_bucket') ~ '/silver/transactions/**/*.parquet'`
- Lines 34, 40 (silver_tc, silver_accounts reads): local paths → S3 URIs

**`dbt/models/gold/gold_daily_summary.sql`**
- Line 5 (post_hook COPY): `/app/data/gold/daily_summary/data.parquet` → S3 URI
- Lines 15, 20 (silver_txn, silver_tc reads): local paths → S3 URIs

**`dbt/models/gold/gold_weekly_account_summary.sql`**
- Line 9 (post_hook COPY): `/app/data/gold/weekly_account_summary/data.parquet` → S3 URI
- Line 40 (`weekly_file` var): `/app/data/gold/weekly_account_summary/data.parquet` → `'s3://' ~ var('s3_bucket') ~ '/gold/weekly_account_summary/data.parquet'`
- Lines 57, 62, 67 (silver reads): local paths → S3 URIs
- Line 122 (`existing_weekly` read): uses `weekly_file` variable — auto-updated

### Control Plane & Run Log

**`pipeline/control_plane.py`**
- Import `configure_duckdb_s3`, `s3_key_exists`, `atomic_parquet_put`, `parse_s3_uri` from `pipeline.s3_utils`
- `get_watermark`: add `configure_duckdb_s3(conn)` — DuckDB httpfs reads S3 URIs transparently
- `advance_watermark`: remove `os.makedirs`, remove temp-file + `os.replace`. Replace with `atomic_parquet_put(bucket, key, df_new)` where `bucket, key = parse_s3_uri(control_path)`. S3 PutObject is atomic — no temp file needed.
- `get_computed_weeks`: add `configure_duckdb_s3(conn)`
- `record_computed_weeks`: replace `os.path.exists` with `s3_key_exists`; remove `os.makedirs`; replace `COPY df_combined TO '...'` with `atomic_parquet_put(bucket, key, df_combined)`
- `get_uncomputed_weeks`: add `configure_duckdb_s3(conn)` — S3 glob `/**/*.parquet` works natively

**`pipeline/run_log.py`**
- Import `configure_duckdb_s3`, `s3_key_exists`, `atomic_parquet_put`, `parse_s3_uri` from `pipeline.s3_utils`
- `flush(target_path)`:
  - Replace `os.path.exists(target_path)` with DuckDB try/except on `read_parquet` (consistent with existing pattern)
  - Replace `COPY df_combined TO '{target_path}'` with `atomic_parquet_put(bucket, key, df_combined)`
  - Change hardcoded `fallback_path = "/app/data/pipeline/pipeline_runlog_fallback.jsonl"` → `/tmp/pipeline_runlog_fallback.jsonl`
  - Remove `os.makedirs(os.path.dirname(fallback_path))`
- `check_unlogged_run(control_path, run_log_path)`:
  - Add `configure_duckdb_s3(conn)` to DuckDB connections
  - Replace `os.path.exists(run_log_path)` with a DuckDB try/except (same "No files found" pattern)

### Main Pipeline

**`pipeline/pipeline.py`** — most surface area

- Add imports: `from pipeline.s3_utils import configure_duckdb_s3, s3_key_exists, atomic_parquet_put`
- `initialise_control_plane(data_dir)` → `initialise_control_plane(s3_bucket: str)`:
  - Build S3 keys: `f"{s3_bucket}/pipeline/control.parquet"` etc.
  - Remove `os.makedirs`
  - Replace `os.path.exists` with `s3_key_exists`
  - Replace DuckDB `COPY ... TO` with `atomic_parquet_put` for empty schema parquets
  - Change `fallback_path` to `/tmp/pipeline_runlog_fallback.jsonl`
- `process_transaction_codes_step`:
  - Add `s3_bucket = os.environ["S3_BUCKET"]`
  - Remove `os.makedirs('/app/data/silver/transaction_codes', exist_ok=True)`
  - Add `configure_duckdb_s3(conn)` to both DuckDB connections
  - Change all `/app/data/...` paths to `s3://{s3_bucket}/...`
- `process_date_sequence`:
  - Add `s3_bucket = os.environ["S3_BUCKET"]`
  - Remove all 3 `os.makedirs` calls (lines 243–245)
  - Replace `placeholder_path = '/app/data/silver/accounts/data.parquet'` + `os.path.exists` check + DuckDB COPY with: `s3_key_exists` check + `atomic_parquet_put` of empty DataFrame with correct schema
  - Replace `stream_dbt_layer` calls — add `"s3_bucket": s3_bucket` to vars dicts
  - Add `configure_duckdb_s3(conn)` to all DuckDB connections
  - Change all `/app/data/...` paths to S3 URIs
  - Replace `os.path.exists(quarantine_path)` with DuckDB try/except
- `process_gold_step`:
  - Add `s3_bucket = os.environ["S3_BUCKET"]`
  - Remove both `os.makedirs` calls (lines 523–524)
  - Replace both `os.path.exists` placeholder checks with `s3_key_exists` + `atomic_parquet_put`
  - Add `configure_duckdb_s3(conn)` to DuckDB connections for count queries
  - Change `/app/data/gold/...` paths to S3 URIs
- `__main__` block:
  - Change `initialise_control_plane("/app/data")` → `initialise_control_plane(os.environ["S3_BUCKET"])`
  - Change all 5 path constants to S3 URIs:
    ```python
    S3_BUCKET = os.environ["S3_BUCKET"]
    CONTROL_PATH          = f"s3://{S3_BUCKET}/pipeline/control.parquet"
    WEEKLY_CONTROL_PATH   = f"s3://{S3_BUCKET}/pipeline/gold_weekly_control.parquet"
    RUN_LOG_PATH          = f"s3://{S3_BUCKET}/pipeline/run_log.parquet"
    FALLBACK_PATH         = "/tmp/pipeline_runlog_fallback.jsonl"
    SILVER_PATH           = f"s3://{S3_BUCKET}/silver/transactions"
    ```

### CI/CD

**`.github/workflows/run-pipeline.yml`**
- Delete the entire "Sync data to S3" step (lines 128–141) — data is written to S3 directly during the pipeline run
- Update "Verify pipeline outputs" step: change all `/app/data/...` paths to `s3://cc-transaction-databricks-datalake-2026/...`; add `con.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-east-1'")` before queries

---

## Key Design Decisions

### INV-40 Atomicity on S3
The current code uses `write to .tmp + os.replace()` (POSIX atomic rename). S3 does not support atomic rename.

**Solution:** S3 `PutObject` is atomic at the object level — either the full object is written or nothing is written. For **bulk data** (bronze, silver, gold parquet files), DuckDB `COPY ... TO 's3://...'` writes directly with no temp file needed. For **control-plane files** (`control.parquet`, `gold_weekly_control.parquet`, `run_log.parquet`), `atomic_parquet_put` uses `pyarrow.write_table` to serialize in memory then `boto3.put_object` in a single call — preserving INV-40.

### S3 Credentials in Docker
The container runs on EC2 with an IAM instance profile. With `network_mode: host` in `docker-compose.yml`, the container shares the host's network and can reach the EC2 Instance Metadata Service (169.254.169.254) to get temporary credentials. Both `boto3` and DuckDB `httpfs` automatically use the credential chain without any hardcoded keys.

### Fallback JSONL Path
The fallback JSONL (`pipeline_runlog_fallback.jsonl`) is the last-resort path when S3 write fails. Writing it to S3 when S3 is broken is self-defeating, so it stays local. Changed from `/app/data/pipeline/` to `/tmp/` since the `data/` directory is no longer a required volume.

### `s3_bucket` as dbt Variable
The bucket name is injected as `s3_bucket` dbt var via `model_vars` in each `stream_dbt_layer` call. dbt models reference it as `{{ var("s3_bucket") }}`. This keeps the bucket name configurable without hardcoding it in SQL.

### `data/` Volume Mount
`./data:/app/data:rw` stays in `docker-compose.yml` — dbt still needs to write `lake.duckdb` to `/app/data/lake.duckdb` as its working catalog. The catalog is ephemeral (recreated each run); the durable data is now fully on S3.

---

## What Does NOT Change
- `/app/source/` mounts and source CSV reads — INV-01 unaffected
- Pipeline control flow, error handling, orchestration logic
- dbt DAG topology and SQL logic — only path strings change
- `pipeline/dbt_runner.py` subprocess invocation
- `deploy-infra.yml`, `teardown-infra.yml`
- EC2 IAM role — already has `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on the bucket

---

## Implementation Order

1. `requirements.txt` + `Dockerfile` + `.env.example` + `docker-compose.yml` — foundation
2. `pipeline/s3_utils.py` — new utility module
3. `pipeline/dbt_runner.py` — add `s3_bucket` to default_vars
4. `pipeline/bronze_accounts.py`, `bronze_transactions.py`, `bronze_transaction_codes.py` — self-contained, testable
5. `pipeline/control_plane.py`, `pipeline/run_log.py` — control plane (highest risk)
6. `dbt/profiles.yml` + all 5 dbt SQL models
7. `pipeline/pipeline.py` — main orchestrator (most surface area)
8. `.github/workflows/run-pipeline.yml` — remove sync step, update verify step
