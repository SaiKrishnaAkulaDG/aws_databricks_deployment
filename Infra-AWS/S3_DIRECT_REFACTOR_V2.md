# S3-Direct Refactor Plan (v2 — IMDS Credential Migration)

**Goal:** Write pipeline output (bronze/silver/gold/pipeline) directly to S3 instead of the local EC2 EBS volume. Source CSVs stay at `/app/source/` (cloned from GitHub). The post-run `aws s3 sync` step in GitHub Actions is removed entirely.

**Bucket:** `cc-transaction-databricks-datalake-2026`  
**Updated:** 2026-05-08 — corrected credential architecture after Session 4 validation

---

## Files Changed (17 total — 16 changed, 1 new)

### New File

| File | What |
|------|------|
| `pipeline/s3_utils.py` | S3 utility module: `configure_duckdb_s3(conn)`, `s3_key_exists(bucket, key)`, `atomic_parquet_put(bucket, key, df)`, `parse_s3_uri(uri)` |

### Requirements & Config

| File | Change |
|------|--------|
| `requirements.txt` | Add `boto3>=1.34.0` and `pyarrow>=14.0.0` |
| `.env.example` | Add `S3_BUCKET=cc-transaction-databricks-datalake-2026` and `AWS_DEFAULT_REGION=us-east-1` only — no AWS credential vars (resolved at runtime via IMDS) |
| `docker-compose.yml` | Add `network_mode: host` (container shares host network to reach EC2 IMDS at `169.254.169.254`); add `S3_BUCKET` and `AWS_DEFAULT_REGION` to `environment` |
| `Dockerfile` | Add `RUN python3 -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs; INSTALL parquet;'); c.close()"` to pre-install DuckDB extensions at image build time |

### dbt Config

| File | Change |
|------|--------|
| `dbt/profiles.yml` | Add `httpfs` to extensions list; add `settings` block with `s3_region: us-east-1` and `env_var()` refs for `s3_access_key_id`, `s3_secret_access_key`, `s3_session_token` — populated at runtime from `os.environ` |
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
- Removed `run_query(glob(...))` compile-time file-exists check — replaced with `{% set file_exists = true %}` (pipeline.py guarantees placeholder existence before dbt runs)

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
- Removed `run_query(glob(...))` compile-time check — replaced with `{% set file_exists = true %}`

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
  - Non-S3 path guard: if path is not an S3 URI, skip `parse_s3_uri` (fallback JSONL writes locally)
  - Replace `COPY df_combined TO '{target_path}'` with `atomic_parquet_put(bucket, key, df_combined)`
  - `"404"` added to exception check — DuckDB httpfs raises `"404 (Not Found)"` for missing S3 objects, not `"No files found"`
  - Change hardcoded `fallback_path = "/app/data/pipeline/pipeline_runlog_fallback.jsonl"` → `/tmp/pipeline_runlog_fallback.jsonl`
  - Remove `os.makedirs(os.path.dirname(fallback_path))`
- `check_unlogged_run(control_path, run_log_path)`:
  - Add `configure_duckdb_s3(conn)` to DuckDB connections
  - `"404"` added to both exception checks for `control.parquet` and `run_log.parquet` reads

### Main Pipeline

**`pipeline/pipeline.py`** — most surface area

- Add imports: `from pipeline.s3_utils import configure_duckdb_s3, s3_key_exists, atomic_parquet_put`
- `initialise_control_plane(data_dir)` → `initialise_control_plane(s3_bucket: str)`:
  - Build S3 keys: `f"{s3_bucket}/pipeline/control.parquet"` etc.
  - Remove `os.makedirs`
  - Replace `os.path.exists` with `s3_key_exists`
  - Replace DuckDB `COPY ... TO` with `atomic_parquet_put` for empty schema parquets
  - Placeholder parquets use `pa.table()` with **explicit pyarrow types** (`pa.date32()` for date columns) — `pd.Series(dtype='object')` writes `string` type to parquet, causing UNION ALL type mismatches with `DATE` columns from bronze data
  - Stale-empty placeholder check: if placeholder exists with 0 rows (left by prior failed run), recreate it
  - Change `fallback_path` to `/tmp/pipeline_runlog_fallback.jsonl`
- `process_date_sequence` and `process_gold_step`:
  - Add `s3_bucket = os.environ["S3_BUCKET"]`
  - Remove all `os.makedirs` calls
  - Replace all `os.path.exists` placeholder checks with `s3_key_exists` + `atomic_parquet_put`
  - Add `configure_duckdb_s3(conn)` to all DuckDB connections
  - Change all `/app/data/...` paths to S3 URIs
- `__main__` block:
  - Change `initialise_control_plane("/app/data")` → `initialise_control_plane(os.environ["S3_BUCKET"])`
  - Change all 5 path constants to S3 URIs

### CI/CD

**`.github/workflows/run-pipeline.yml`**
- "Ensure .env" step: changed from conditional (`if absent`) to unconditional — always overwrites from `.env.example`. Prevents stale expired credentials persisting across runs.
- "Inject AWS credentials into .env" step: **removed entirely** — credentials flow via IMDS, not `.env`
- `docker builder prune -f` added before every `docker compose build` — prevents BuildKit snapshot corruption on long-running instances
- "Sync data to S3" step: **removed** — data is written to S3 directly during the pipeline run
- "Verify pipeline outputs" step: queries S3 paths directly via DuckDB + `configure_duckdb_s3`

**`Infra-AWS/cf-cc-transactions-lake.yaml`**
- `MetadataOptions` added to EC2 instance:
  ```yaml
  MetadataOptions:
    HttpEndpoint: enabled
    HttpTokens: required
    HttpPutResponseHopLimit: 2
  ```
  Docker bridge adds one extra hop; default limit of 1 blocks IMDS from containers.

---

## Key Design Decisions

### INV-40 Atomicity on S3
The current code uses `write to .tmp + os.replace()` (POSIX atomic rename). S3 does not support atomic rename.

**Solution:** S3 `PutObject` is atomic at the object level — either the full object is written or nothing is written. For **bulk data** (bronze, silver, gold parquet files), DuckDB `COPY ... TO 's3://...'` writes directly with no temp file needed. For **control-plane files** (`control.parquet`, `gold_weekly_control.parquet`, `run_log.parquet`), `atomic_parquet_put` uses `pyarrow.write_table` to serialize in memory then `boto3.put_object` in a single call — preserving INV-40.

### S3 Credentials in Docker

**DuckDB 0.10.0 `httpfs` does NOT auto-resolve the EC2 IMDS credential chain.** Removing explicit credential injection causes HTTP 403 on all S3 operations. The correct flow:

1. `boto3.Session().get_credentials()` resolves credentials from EC2 instance role via IMDS (`network_mode: host` + hop limit 2 ensures IMDS is reachable)
2. `configure_duckdb_s3(conn)` injects credentials into DuckDB via `SET s3_access_key_id / s3_secret_access_key / s3_session_token`
3. `configure_duckdb_s3(conn)` also sets `os.environ["AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN"]` so dbt subprocesses inherit them
4. `dbt/profiles.yml` reads credentials via `env_var('AWS_ACCESS_KEY_ID', '')` — populated from `os.environ` when `configure_duckdb_s3` runs before dbt is launched

`.env.example` contains only `S3_BUCKET` and `AWS_DEFAULT_REGION` — no AWS credential vars.

### Placeholder Parquet Types
Silver and gold placeholder parquets must be created with `pa.table()` using explicit pyarrow types. Using `pd.Series(dtype='object')` produces `string` type in parquet, which causes `UNION ALL` type mismatches when combined with `DATE`-typed columns from bronze data. Use `pa.date32()` for all date columns in placeholder schemas.

### Stale Empty Placeholder Check
If a previous run failed after creating a placeholder but before writing real data, the placeholder file exists with 0 rows. `s3_key_exists` returns `True`, so the pipeline would skip creation and attempt to use the empty file — causing downstream type errors. Fix: after `s3_key_exists` returns True, read the row count; if 0, recreate the placeholder.

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
8. `.github/workflows/run-pipeline.yml` — remove sync step, always-overwrite .env, docker builder prune, update verify step
9. `Infra-AWS/cf-cc-transactions-lake.yaml` — add MetadataOptions hop limit 2
