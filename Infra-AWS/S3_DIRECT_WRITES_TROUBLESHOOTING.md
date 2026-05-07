# S3-Direct Writes — Problems, Root Causes, and Fixes

Branch: `feature/s3-direct-writes`  
Date: 2026-05-07  
All runs via GitHub Actions workflow `run-pipeline.yml`.

---

## Problem 1 — HTTP 403 on Every S3 Read/Write from DuckDB

### Error
```
HTTP Error: 403 Forbidden
```
Seen in all dbt model runs and in Python DuckDB connections when reading/writing `s3://...` paths.

### Root Cause
DuckDB 0.10.0 `httpfs` does not automatically resolve EC2 Instance Metadata Service (IMDS) credentials when running inside a Docker container — even with `network_mode: host`. The DuckDB session has no AWS credentials, so every S3 request returns 403.

### Fix A — `pipeline/s3_utils.py`: explicit credential injection for Python connections
Added `configure_duckdb_s3(conn)` which fetches credentials from boto3's chain (env vars → IAM instance profile) and injects them via `SET` commands:

```python
def configure_duckdb_s3(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_region='{region}';")
    session = boto3.Session(region_name=region)
    creds = session.get_credentials().get_frozen_credentials()
    conn.execute(f"SET s3_access_key_id='{frozen.access_key}';")
    conn.execute(f"SET s3_secret_access_key='{frozen.secret_key}';")
    if frozen.token:
        conn.execute(f"SET s3_session_token='{frozen.token}';")
```

Called on every `duckdb.connect()` throughout `pipeline.py`, `control_plane.py`, `run_log.py`.

### Fix B — `dbt/profiles.yml`: inject credentials into dbt DuckDB sessions
dbt runs its own DuckDB sessions — `configure_duckdb_s3` doesn't reach them. Added credentials via `env_var()` in the settings block:

```yaml
settings:
  s3_region: us-east-1
  s3_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID', '') }}"
  s3_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY', '') }}"
  s3_session_token: "{{ env_var('AWS_SESSION_TOKEN', '') }}"
```

### Fix C — `.github/workflows/run-pipeline.yml`: write IMDS credentials into `.env`
Added a step that fetches temporary credentials from IMDS on the EC2 host and writes them to `/app/.env` so docker compose passes them as environment variables:

```yaml
- name: Inject AWS credentials into .env
  run: |
    ssh -i /tmp/ec2-key.pem -o StrictHostKeyChecking=no ubuntu@${{ steps.get-ip.outputs.ip }} '
      TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 3600") &&
      ROLE=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" \
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/") &&
      CREDS=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" \
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/$ROLE") &&
      KEY_ID=$(echo "$CREDS" | python3 -c "import sys,json; print(json.load(sys.stdin)[\"AccessKeyId\"])") &&
      SECRET=$(echo "$CREDS" | python3 -c "import sys,json; print(json.load(sys.stdin)[\"SecretAccessKey\"])") &&
      TOKEN_VAL=$(echo "$CREDS" | python3 -c "import sys,json; print(json.load(sys.stdin)[\"Token\"])") &&
      sudo sed -i "/^AWS_ACCESS_KEY_ID=/d; /^AWS_SECRET_ACCESS_KEY=/d; /^AWS_SESSION_TOKEN=/d" /app/.env &&
      echo "AWS_ACCESS_KEY_ID=$KEY_ID" | sudo tee -a /app/.env > /dev/null &&
      echo "AWS_SECRET_ACCESS_KEY=$SECRET" | sudo tee -a /app/.env > /dev/null &&
      echo "AWS_SESSION_TOKEN=$TOKEN_VAL" | sudo tee -a /app/.env > /dev/null'
```

**Commit:** `39fc182`

---

## Problem 2 — Docker Build Fails: `.env not found`

### Error
```
env file /app/.env not found
```
Seen during `docker compose build` step in GitHub Actions.

### Root Cause
In the workflow, "Ensure .env exists on EC2" was ordered **after** "Rebuild Docker image". `docker compose build` reads `.env` even for the build phase — it fails immediately if the file doesn't exist on a fresh EC2.

### Fix — `.github/workflows/run-pipeline.yml`: reorder steps
Moved "Ensure .env exists on EC2" to run **before** "Rebuild Docker image":

```
Before: Pull code → Rebuild Docker → Ensure .env → Inject credentials → Run pipeline
After:  Pull code → Ensure .env → Rebuild Docker → Inject credentials → Run pipeline
```

**Commit:** `1fe6793`

---

## Problem 3 — `FATAL: Not an S3 URI: /tmp/pipeline_runlog_fallback.jsonl`

### Error
```
FATAL: Not an S3 URI: /tmp/pipeline_runlog_fallback.jsonl
AssertionError
```
The real underlying error (dbt failure) was masked by this crash in the error handler itself.

### Root Cause
`run_log.py flush()` called `parse_s3_uri(target_path)` unconditionally. When a pipeline error occurred and the fallback path `/tmp/pipeline_runlog_fallback.jsonl` was passed, `parse_s3_uri` raised `AssertionError` before anything was written.

### Fix — `pipeline/run_log.py`: guard non-S3 paths
```python
def flush(self, target_path: str):
    if not target_path.startswith("s3://"):
        with open(target_path, "a") as f:
            for entry in self._buffer:
                f.write(json.dumps(entry) + "\n")
        self._buffer = []
        return
    # ... S3 path logic below
```

**Commit:** `cc4f4eb`

---

## Problem 4 — `silver_accounts` dbt Model Fails: UNION ALL Type Mismatch

### Error
```
1 of 1 ERROR creating sql table model main.silver_accounts ... [ERROR in 0.62s]
```
No explicit error message in dbt output — the model failed silently at 0.62s.

### Root Cause
`pipeline.py` created the `silver/accounts/data.parquet` placeholder using `pd.Series(dtype='object')` for date columns (`open_date`, `billing_cycle_start`, `billing_cycle_end`). When pandas `object` dtype is written via pyarrow, it becomes `string` type in parquet.

`silver_accounts.sql` UNIONs two CTEs:
- `step3a_incoming_passing` — from bronze parquet, date columns have **`DATE`** type
- `step3b_existing` — from the placeholder, date columns have **`STRING`** type

DuckDB raises a type mismatch error on the UNION ALL.

### Fix A — `pipeline/s3_utils.py`: `atomic_parquet_put` accepts `pa.Table`
```python
def atomic_parquet_put(bucket: str, key: str, df) -> None:
    table = df if isinstance(df, pa.Table) else pa.Table.from_pandas(df)
    # ... write to S3
```

### Fix B — `pipeline/pipeline.py`: placeholder uses explicit `pa.date32()` types
```python
import pyarrow as pa

_placeholder_schema = pa.schema([
    pa.field('account_id', pa.string()),
    pa.field('customer_name', pa.string()),
    pa.field('account_status', pa.string()),
    pa.field('credit_limit', pa.float64()),
    pa.field('current_balance', pa.float64()),
    pa.field('open_date', pa.date32()),            # was pd.Series(dtype='object')
    pa.field('billing_cycle_start', pa.date32()),  # was pd.Series(dtype='object')
    pa.field('billing_cycle_end', pa.date32()),    # was pd.Series(dtype='object')
    pa.field('_source_file', pa.string()),
    pa.field('_bronze_ingested_at', pa.timestamp('ns')),
    pa.field('_pipeline_run_id', pa.string()),
    pa.field('_record_valid_from', pa.timestamp('ns')),
])
placeholder_table = pa.table(
    {name: pa.array([], type=_placeholder_schema.field(name).type)
     for name in _placeholder_schema.names},
    schema=_placeholder_schema
)
atomic_parquet_put(s3_bucket, placeholder_key, placeholder_table)
```

**Commit:** `19fac35`

---

## Problem 5 — Stale Empty Placeholder Blocks Re-Creation on Retry

### Error
Same as Problem 4 — `silver_accounts` keeps failing across retries after the fix in Problem 4 was pushed.

### Root Cause
The placeholder from a previous failed run (0 rows, wrong string types) was already on S3. The creation code checked `if not s3_key_exists(s3_bucket, placeholder_key)` — since the key existed, it skipped recreation entirely. The stale file with wrong types remained on S3 indefinitely across retries.

### Fix — `pipeline/pipeline.py`: row count check triggers re-creation
```python
_needs_placeholder = not s3_key_exists(s3_bucket, placeholder_key)
if not _needs_placeholder:
    # Recreate if 0 rows — stale empty placeholder from a prior failed run
    try:
        with duckdb.connect() as _conn:
            configure_duckdb_s3(_conn)
            _count = _conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('s3://{s3_bucket}/{placeholder_key}')"
            ).fetchone()[0]
        if _count == 0:
            _needs_placeholder = True
    except Exception:
        pass
if _needs_placeholder:
    # ... create with correct pyarrow types
```

Applied to both `silver_accounts` and `gold_weekly_account_summary` placeholders.

**Commit:** `18c3737`

---

## Problem 6 — `run_log.parquet` HTTP 404 Not Caught, Pipeline Crashes at Flush

### Error (first time)
```
FATAL: HTTP Error: Unable to connect to URL
"https://cc-transaction-databricks-datalake-2026.s3.amazonaws.com/pipeline/run_log.parquet": 404 (Not Found)
```
Seen after all dbt models passed — pipeline exited 1 at the very last step.

### Root Cause
On first run `run_log.parquet` doesn't exist on S3. `run_log.py flush()` tries to read existing data to append to it. DuckDB httpfs raises `"404 (Not Found)"` — not the same string as `"No files found"` which was the only handled case. The inner except re-raised, the outer except wrote to the JSONL fallback then re-raised, and the pipeline exited 1.

### Fix — `pipeline/run_log.py flush()`: add `"404"` to the except check
```python
except Exception as e:
    err_str = str(e)
    if "No files found" in err_str or "404" in err_str or isinstance(e, FileNotFoundError):
        df_combined = df_buffer   # treat 404 same as missing file
    else:
        raise
```

**Commit:** `ccb11ff`

---

## Problem 7 — `check_unlogged_run` HTTP 404 Blocks Next Run

### Error (second run after Problem 6)
```
CONTROL PLANE ERROR: HTTP Error: Unable to connect to URL
"https://cc-transaction-databricks-datalake-2026.s3.amazonaws.com/pipeline/run_log.parquet": 404 (Not Found)
```
The pipeline exited immediately at startup before processing any dates.

### Root Cause
The previous run (Problem 6) had written `control.parquet` (watermark advanced successfully) but failed to write `run_log.parquet`. On the next run, `check_unlogged_run()` reads `control.parquet`, finds a `updated_by_run_id` from the prior run, then tries to read `run_log.parquet` to check if that run ID has any log entries. Since `run_log.parquet` still doesn't exist, it gets HTTP 404 — same unhandled error, same crash.

### Fix — `pipeline/run_log.py check_unlogged_run()`: add `"404"` to both reads
```python
# Reading control.parquet
except Exception as e:
    err = str(e)
    if "No files found" in err or "404" in err or isinstance(e, FileNotFoundError):
        return None
    raise

# Reading run_log.parquet
except Exception as e:
    err = str(e)
    if "No files found" in err or "404" in err or isinstance(e, FileNotFoundError):
        return prior_run_id   # treat missing log as unlogged run — safe recovery
    raise
```

**Commit:** `797abda`

---

## Problem 8 — `gold_weekly_account_summary` Fails in Incremental Run

### Error
```
2 of 2 ERROR creating sql table model main.gold_weekly_account_summary ... [ERROR in 0.81s]
ERROR in gold step: gold_weekly_account_summary failed
```

### Root Cause
Historical run (Jan 1–6) had no complete weeks (week Jan 1–7 needs all 7 days), so `target_weeks_list` was empty → the COPY post_hook in `gold_weekly_account_summary` didn't fire → `gold/weekly_account_summary/data.parquet` was never written to S3.

Incremental run (Jan 7) completed the week → `process_gold_step` checked `s3_key_exists` → file absent → created placeholder with `pd.Series(dtype='object')` for `week_start_date` / `week_end_date` → pyarrow wrote them as `string` type → UNION ALL between `DATE` (computed) and `string` (placeholder) → type mismatch → model error.

Same root cause as Problem 4.

### Fix — `pipeline/pipeline.py process_gold_step()`: pyarrow types + stale check
```python
_weekly_schema = pa.schema([
    pa.field('week_start_date', pa.date32()),   # was pd.Series(dtype='object')
    pa.field('week_end_date', pa.date32()),     # was pd.Series(dtype='object')
    pa.field('account_id', pa.string()),
    pa.field('total_purchases', pa.int64()),
    pa.field('avg_purchase_amount', pa.float64()),
    pa.field('total_payments', pa.float64()),
    pa.field('total_fees', pa.float64()),
    pa.field('total_interest', pa.float64()),
    pa.field('closing_balance', pa.float64()),
    pa.field('_computed_at', pa.timestamp('ns')),
    pa.field('_pipeline_run_id', pa.string()),
])
# Plus the same stale-empty check as Problem 5
```

**Commit:** `81f9c4f`

---

## Final Passing Runs

| Mode | Dates | GitHub Actions Run | Result |
|------|-------|--------------------|--------|
| Historical | 2024-01-01 → 2024-01-06 | `25492219411` | ✓ Passed |
| Incremental | 2024-01-07 | `25492672505` | ✓ Passed |

---

## Summary of All Files Changed

| File | What Changed |
|------|-------------|
| `pipeline/s3_utils.py` | `configure_duckdb_s3()` injects boto3 creds into DuckDB; `atomic_parquet_put()` accepts `pa.Table` or `pd.DataFrame` |
| `pipeline/pipeline.py` | All placeholders use `pa.table()` with explicit pyarrow types; stale-empty check on silver and gold weekly placeholders; `import pyarrow as pa` |
| `pipeline/run_log.py` | `flush()` handles non-S3 paths; `"404"` added to exception checks in `flush()` and `check_unlogged_run()` |
| `dbt/profiles.yml` | `s3_access_key_id`, `s3_secret_access_key`, `s3_session_token` via `env_var()` in settings block |
| `dbt/models/silver/silver_accounts.sql` | Removed `run_query(glob(...))` file-exists check — replaced with `{% set file_exists = true %}` (placeholder guaranteed by pipeline.py) |
| `dbt/models/gold/gold_weekly_account_summary.sql` | Same — removed `run_query` check, hardcoded `file_exists = true` |
| `.github/workflows/run-pipeline.yml` | Step order: `.env` before Docker build; "Inject AWS credentials into .env" step added |

---

## Key Pattern: Placeholder Parquet Types

**Rule:** Any placeholder parquet created with `pd.Series(dtype='object')` for a date column will have `string` type in parquet. When a dbt model UNIONs this with real data (which has `DATE` type from DuckDB CSV inference), the UNION ALL fails silently.

**Always use pyarrow explicitly for date columns:**
```python
pa.field('column_name', pa.date32())   # for DATE columns
pa.field('column_name', pa.timestamp('ns'))  # for TIMESTAMP columns
```

**Always add stale-empty check** when a placeholder can persist across failed runs:
```python
if s3_key_exists(...) and COUNT(*) == 0:
    recreate_placeholder()
```
