# PR: feature/s3-direct-writes → main

## Summary

Refactor the pipeline to write all outputs (bronze, silver, gold, control plane, run log) directly to S3 via DuckDB `httpfs` and `boto3`. Removes the local EBS data volume as a durable storage layer and eliminates the post-run `aws s3 sync` step from the GitHub Actions workflow.

**Branch:** `feature/s3-direct-writes`  
**Date:** 2026-05-07  
**Validated via:** GitHub Actions runs `25492219411` (historical) and `25492672505` (incremental) — both passing.

---

## What Changed and Why

### New File — `pipeline/s3_utils.py`
Central S3 utility module. All other files import from here.
- `configure_duckdb_s3(conn)` — fetches credentials from boto3's chain (env vars → IAM instance profile) and injects via DuckDB `SET` commands. DuckDB 0.10.0 `httpfs` does not auto-resolve EC2 IMDS inside Docker — explicit injection is required.
- `s3_key_exists(bucket, key)` — boto3 `head_object` check; replaces `os.path.exists`.
- `atomic_parquet_put(bucket, key, df)` — serializes `pd.DataFrame` or `pa.Table` to parquet bytes in memory and writes via a single `boto3.put_object`. Preserves INV-40 atomicity.
- `parse_s3_uri(uri)` — splits `s3://bucket/key` into `(bucket, key)`.

### `pipeline/pipeline.py`
- All local path constants changed to `s3://` URIs.
- `initialise_control_plane(data_dir)` → `initialise_control_plane(s3_bucket)`.
- All `os.makedirs` and `os.path.exists` calls removed.
- `configure_duckdb_s3(conn)` added to every `duckdb.connect()` block.
- Silver and gold placeholder parquets rewritten using `pa.table()` with explicit pyarrow types (`pa.date32()` for date columns). Previously `pd.Series(dtype='object')` was used — this writes `string` type to parquet and caused UNION ALL type mismatches with `DATE` columns from bronze data.
- Stale-empty placeholder check: if a placeholder exists with 0 rows (left by a prior failed run), recreate it with correct types.

### `pipeline/run_log.py`
- `flush()`: guard added for non-S3 paths (fallback JSONL writes locally without calling `parse_s3_uri`). `"404"` added to the "file not found" exception check — DuckDB httpfs raises `"404 (Not Found)"` for missing S3 files, not `"No files found"`.
- `check_unlogged_run()`: same `"404"` check added for both `control.parquet` and `run_log.parquet` reads.

### `pipeline/control_plane.py`
- All local file paths → S3 URIs.
- `configure_duckdb_s3` added to all DuckDB connections.
- `os.replace` (temp-file atomic rename) replaced with `atomic_parquet_put` for all control-plane writes.

### Bronze Loaders (`bronze_accounts.py`, `bronze_transactions.py`, `bronze_transaction_codes.py`)
- `COPY ... TO 's3://...'` writes directly — no temp file, no local mkdir.
- `s3_key_exists` replaces `os.path.exists` for the idempotency gate.
- `configure_duckdb_s3` added to every DuckDB connection.

### `pipeline/dbt_runner.py`
- `s3_bucket` added to `default_vars` in `derive_execution_order()`.
- Improved JSON log streaming — all dbt events printed; stderr merged with stdout.

### `dbt/profiles.yml`
- `s3_access_key_id`, `s3_secret_access_key`, `s3_session_token` added via `env_var()` in the settings block. dbt runs its own DuckDB sessions — credentials must be injected through the profile.

### dbt SQL Models (5 files)
All paths changed from `/app/data/...` to `s3://{{ var("s3_bucket") }}/...` in source reads and post_hook COPY targets.

`silver_accounts.sql` and `gold_weekly_account_summary.sql`: removed `run_query(glob(...))` compile-time file-exists checks — these ran at compile time, failed silently, and had no error visibility. Replaced with `{% set file_exists = true %}` — `pipeline.py` guarantees placeholder existence before dbt runs.

### `docker-compose.yml`
- `network_mode: host` — container shares host network to reach EC2 IMDS at `169.254.169.254`.
- `S3_BUCKET` and `AWS_DEFAULT_REGION` added to environment.

### `requirements.txt`
- `boto3>=1.34.0`, `pyarrow>=14.0.0` added.

### `.env.example`
- `S3_BUCKET`, `AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` added.

### `.github/workflows/run-pipeline.yml`
- "Ensure .env exists on EC2" moved **before** "Rebuild Docker image" — `docker compose build` reads `.env` at build time.
- "Inject AWS credentials into .env" step added — fetches temporary credentials from EC2 IMDS via IMDSv2 and writes to `/app/.env`.
- "Sync data to S3" step **removed** — data is written to S3 directly during the run.
- "Verify pipeline outputs" step updated — queries S3 paths.

---

## Invariants

| Invariant | Status |
|-----------|--------|
| INV-40 (atomicity) | ✅ S3 `PutObject` is atomic; `atomic_parquet_put` serializes in memory before upload |
| INV-01 (source immutability) | ✅ `source/` unchanged; no write operations target `/app/source` |
| INV-19 (run log append-only) | ✅ `flush()` reads existing rows then writes combined set via `atomic_parquet_put` |
| INV-14 (weekly gate) | ✅ Gold weekly computed only when `week_end_date <= processed_date` |

---

## Problems Fixed During Validation (8 total)

| # | Problem | Fix |
|---|---------|-----|
| 1 | HTTP 403 — DuckDB httpfs doesn't resolve IMDS in Docker | `configure_duckdb_s3()` + `profiles.yml` settings + IMDS step in workflow |
| 2 | `.env not found` during Docker build | Moved "Ensure .env" before "Rebuild Docker image" in workflow |
| 3 | `FATAL: Not an S3 URI` — fallback path passed to `parse_s3_uri` | Non-S3 path guard in `flush()` |
| 4 | `silver_accounts` UNION ALL type mismatch | `pa.date32()` for date columns in placeholder |
| 5 | Stale placeholder not recreated on retry | Row count check — recreate if 0 rows |
| 6 | `run_log.parquet` HTTP 404 not caught in `flush()` | `"404" in str(e)` added to exception check |
| 7 | `run_log.parquet` HTTP 404 not caught in `check_unlogged_run()` | Same fix, second code path |
| 8 | `gold_weekly` UNION ALL type mismatch | Same `pa.date32()` fix + stale check |

Full details: `Infra-AWS/S3_DIRECT_WRITES_TROUBLESHOOTING.md`

---

## Documentation Added

| File | Purpose |
|------|---------|
| `Infra-AWS/S3_DIRECT_REFACTOR.md` | Refactor plan — file inventory, design decisions, implementation order |
| `Infra-AWS/S3_DIRECT_WRITES_TROUBLESHOOTING.md` | 8 problems: error message, root cause, code fix, commit |
| `Infra-AWS/RUNBOOK_S3_DIRECT.md` | Updated operational runbook — S3 verify commands, no sync step, S3 reset |
| `sessions/S08_session_log.md` | Session log — all 17 commits, 8 bugs, passing run IDs, sign-off |

---

## Test Evidence

| Mode | Dates | GitHub Actions Run | Result |
|------|-------|--------------------|--------|
| Historical | 2024-01-01 → 2024-01-06 | `25492219411` | ✅ Passed |
| Incremental | 2024-01-07 | `25492672505` | ✅ Passed |

Watermark after both runs: `2024-01-07`  
S3 outputs confirmed: `bronze/`, `silver/`, `gold/`, `pipeline/`
