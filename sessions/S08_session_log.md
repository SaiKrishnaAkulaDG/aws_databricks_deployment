# S08 Session Log — S3-Direct Writes Deployment

**Session:** S08 — S3-Direct Writes: AWS End-to-End Validation  
**Branch:** `feature/s3-direct-writes`  
**Date:** 2026-05-07  
**Status:** ✅ COMPLETE  
**Engineer:** Sai Krishna Akula

---

## Session Objective

Refactor the pipeline so all outputs (bronze, silver, gold, control plane) write directly to S3 via DuckDB `httpfs` instead of the local EC2 EBS volume. Remove the post-run `aws s3 sync` step entirely. Validate end-to-end via GitHub Actions: historical mode (2024-01-01 → 2024-01-06) followed by incremental mode (2024-01-07).

---

## Commit History

| # | Commit | Description |
|---|--------|-------------|
| 1 | `7b43075` | Add S3-direct refactor plan doc — full file inventory and design decisions |
| 2 | `61490f3` | Refactor pipeline to write directly to S3 (remove local data/ layer) |
| 3 | `1d7f563` | run-pipeline: pull feature branch + rebuild Docker image on EC2 |
| 4 | `39fc182` | Fix HTTP 403: inject IMDS credentials into DuckDB and dbt env |
| 5 | `1fe6793` | Fix: move Ensure .env step before Rebuild Docker image |
| 6 | `cc4f4eb` | Fix run_log flush on non-S3 paths; inject dbt DuckDB credentials |
| 7 | `e3a5d17` | Capture and print dbt error events and stderr for diagnostics |
| 8 | `37aa5d2` | Dump full dbt output on failure for diagnostics |
| 9 | `c61e196` | Print all dbt event messages and data payloads for full error visibility |
| 10 | `6b03e61` | Print every dbt event to surface the actual model error |
| 11 | `395e87d` | Remove run_query glob checks — pipeline guarantees placeholders exist |
| 12 | `19fac35` | Fix silver_accounts placeholder parquet type mismatch |
| 13 | `18c3737` | Recreate silver_accounts placeholder if stale empty file exists on S3 |
| 14 | `ccb11ff` | Handle HTTP 404 from DuckDB httpfs when run_log.parquet doesn't exist yet |
| 15 | `797abda` | Handle HTTP 404 in check_unlogged_run when run_log.parquet absent |
| 16 | `81f9c4f` | Fix gold_weekly placeholder parquet type mismatch + stale empty check |
| 17 | `739bad4` | Add S3-direct-writes troubleshooting doc |

---

## Files Changed (17 total)

| File | Change |
|------|--------|
| `pipeline/s3_utils.py` | **New.** `configure_duckdb_s3()`, `s3_key_exists()`, `atomic_parquet_put()`, `parse_s3_uri()` |
| `pipeline/pipeline.py` | All paths local → S3; placeholders use `pa.table()` with explicit pyarrow types; stale-empty check on silver + gold weekly |
| `pipeline/run_log.py` | `flush()` non-S3 guard; `"404"` added to exception checks in `flush()` and `check_unlogged_run()` |
| `pipeline/control_plane.py` | All local paths → S3; `configure_duckdb_s3` on every DuckDB connection; `atomic_parquet_put` for all control writes |
| `pipeline/bronze_accounts.py` | Local COPY → S3 COPY; `s3_key_exists` idempotency gate; `configure_duckdb_s3` |
| `pipeline/bronze_transactions.py` | Same pattern as `bronze_accounts.py` |
| `pipeline/bronze_transaction_codes.py` | Same pattern as `bronze_accounts.py` |
| `pipeline/dbt_runner.py` | `s3_bucket` added to `default_vars`; improved JSON log streaming for diagnostics |
| `dbt/profiles.yml` | `s3_access_key_id`, `s3_secret_access_key`, `s3_session_token` via `env_var()` in settings block |
| `dbt/models/silver/silver_transaction_codes.sql` | All paths `/app/data/...` → `s3://{{ var("s3_bucket") }}/...` |
| `dbt/models/silver/silver_accounts.sql` | Paths updated; removed `run_query(glob(...))` file-exists check |
| `dbt/models/silver/silver_transactions.sql` | All paths updated |
| `dbt/models/gold/gold_daily_summary.sql` | All paths updated |
| `dbt/models/gold/gold_weekly_account_summary.sql` | All paths updated; removed `run_query` check; `file_exists = true` |
| `docker-compose.yml` | `network_mode: host`; `S3_BUCKET` and `AWS_DEFAULT_REGION` env vars |
| `requirements.txt` | Added `boto3>=1.34.0`, `pyarrow>=14.0.0` |
| `.env.example` | Added `S3_BUCKET`, `AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` |
| `.github/workflows/run-pipeline.yml` | "Ensure .env" moved before Docker build; "Inject AWS credentials into .env" step added; "Sync data to S3" step removed; verify step updated to query S3 |

---

## Problems Encountered and Resolved

| # | Problem | Commits |
|---|---------|---------|
| 1 | HTTP 403 on all S3 ops — DuckDB httpfs doesn't resolve IMDS in Docker | `39fc182` |
| 2 | `.env not found` during Docker build — step ordering wrong | `1fe6793` |
| 3 | `FATAL: Not an S3 URI` — `parse_s3_uri` called on fallback JSONL path | `cc4f4eb` |
| 4 | `silver_accounts` UNION ALL type mismatch — `object` dtype → string in placeholder | `19fac35` |
| 5 | Stale empty placeholder not recreated on retry — `s3_key_exists` returned True | `18c3737` |
| 6 | HTTP 404 not caught in `run_log.flush()` — "No files found" didn't match | `ccb11ff` |
| 7 | HTTP 404 not caught in `check_unlogged_run()` — same pattern, different path | `797abda` |
| 8 | `gold_weekly` UNION ALL type mismatch — same object dtype bug as #4 | `81f9c4f` |

Full root causes, error messages, and code changes: `Infra-AWS/S3_DIRECT_WRITES_TROUBLESHOOTING.md`

---

## GitHub Actions Run Results

| Mode | Dates | Run ID | Result |
|------|-------|--------|--------|
| Historical | 2024-01-01 → 2024-01-06 | `25492219411` | ✅ Passed |
| Incremental | 2024-01-07 | `25492672505` | ✅ Passed |

EC2 instance: `i-018df9bc857748709`  
S3 bucket: `cc-transaction-databricks-datalake-2026`  
Watermark after both runs: `2024-01-07`

---

## Session Outcomes

- ✅ Pipeline writes all outputs directly to S3 — no local EBS dependency
- ✅ `aws s3 sync` step removed from GitHub Actions workflow
- ✅ All 8 S3-credential and type-mismatch bugs identified and fixed
- ✅ Historical (Jan 1–6) + Incremental (Jan 7) both passing end-to-end via GitHub Actions
- ✅ Troubleshooting guide committed: `Infra-AWS/S3_DIRECT_WRITES_TROUBLESHOOTING.md`
- ✅ Refactor plan committed: `Infra-AWS/S3_DIRECT_REFACTOR.md`

---

## Engineer Sign-Off

Historical pipeline (2024-01-01 → 2024-01-06) and incremental pipeline (2024-01-07) both pass via GitHub Actions on `feature/s3-direct-writes`. S3-direct writes validated. Branch ready for PR to `main`.

**Signed:** Sai Krishna Akula  
**Date:** 2026-05-07
