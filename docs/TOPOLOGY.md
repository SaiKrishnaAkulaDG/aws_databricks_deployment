# TOPOLOGY.md — Credit Card Financial Transactions Lake

**PBVI Phase:** 8 — Discovery
**Document Type:** System topology and data flow artifact
**Created:** Phase 8 completion
**Status:** FINAL

---

## 1. System Topology Overview

The Credit Card Financial Transactions Lake is a batch Medallion architecture (Bronze → Silver → Gold) implemented with Python orchestration (`pipeline.py`) and dbt transformations. Data flows through four distinct layers with explicit control-plane enforcement.

### Architecture Style: Orchestrated Transformation Pipeline

```
SOURCE CSV FILES
    ↓
┌───────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Python + DuckDB write)                       │
│ • transactions (partitioned: date)                         │
│ • accounts (partitioned: date)                             │
│ • transaction_codes (static reference)                     │
└───────────────────────────────────────────────────────────┘
    ↓
┌───────────────────────────────────────────────────────────┐
│ SILVER LAYER (dbt models + quality gates)                 │
│ • transactions (partitioned: date, globally deduplicated) │
│ • accounts (latest per account_id, non-partitioned)       │
│ • transaction_codes (static reference)                    │
│ • quarantine (partitioned: date, rejection reasons)       │
└───────────────────────────────────────────────────────────┘
    ↓
┌───────────────────────────────────────────────────────────┐
│ GOLD LAYER (dbt models + aggregation)                     │
│ • daily_summary (one row per transaction_date)            │
│ • weekly_account_summary (one row per account/week)       │
└───────────────────────────────────────────────────────────┘
    ↓
CONTROL PLANE (pipeline.py writes)
    ├─ control.parquet (watermark + last run_id)
    ├─ gold_weekly_control.parquet (computed weeks registry)
    └─ run_log.parquet (audit trail, append-only)
```

---

## 2. Physical Data Flows

### 2.1 Historical Pipeline (First Run)

**Trigger:** `python pipeline/pipeline.py --mode historical --start-date 2024-01-01 --end-date 2024-01-07`

**Flow:**
1. **Initialisation Phase**
   - Validate source files exist for date range
   - Initialise `pipeline/control.parquet` with `last_processed_date = NULL` (pre-run state)
   - Initialise `pipeline/gold_weekly_control.parquet` with no weeks computed
   - Generate run_id (UUID)

2. **Per-Date Processing Loop** (one iteration per date)
   - **Accounts Bronze Load:** Python reads `source/accounts_YYYY-MM-DD.csv`, writes to `bronze/accounts/date=YYYY-MM-DD/`
   - **Accounts Silver Promotion:** dbt runs `silver_accounts` model (upsert on `account_id`)
   - **Transactions Bronze Load:** Python reads `source/transactions_YYYY-MM-DD.csv`, writes to `bronze/transactions/date=YYYY-MM-DD/`
   - **Transactions Silver Promotion:** dbt runs `silver_transactions` model (validates, deduplicates, assigns sign)
   - **Gold Daily Aggregation:** dbt runs `gold_daily_summary` model
   - **Gold Weekly Aggregation** (if week boundary crossed): dbt runs `gold_weekly_account_summary` model

3. **Control-Plane Finalisation**
   - Write newly computed weeks to `pipeline/gold_weekly_control.parquet`
   - Advance watermark in `pipeline/control.parquet` to current date
   - Flush accumulated run log entries to `pipeline/run_log.parquet`

4. **Error Handling**
   - On any failure: do not advance watermark; log failure with `status = FAILED` or `SKIPPED` entries
   - On compile failure: write ORCHESTRATION sentinel row (`model_name = DBT_COMPILE`, `layer = ORCHESTRATION`)
   - On run log flush failure: write to `.jsonl` fallback file; recover on next successful run via `UNLOGGED_RUN` synthetic row

**Output Atomicity:**
- Bronze writes are idempotent via partition existence check
- Silver writes are atomic per date partition
- Gold writes replace entire date/week partitions on re-run
- Watermark advances only after all three layers and control updates complete

---

### 2.2 Incremental Pipeline (Subsequent Runs)

**Trigger:** `python pipeline/pipeline.py --mode incremental`

**Flow:**
1. Read `last_processed_date` from `pipeline/control.parquet`
2. Compute target date = `last_processed_date + 1 day`
3. If source file absent: exit as no-op (watermark unchanged)
4. If source file exists: execute same per-date sequence as historical pipeline
5. Watermark advances to target date only on full success

**Key Guarantee:** One date per invocation. Repeating the same invocation multiple times produces no change to data or watermark.

---

### 2.3 Gold Weekly Control Gatekeeping

**Managed by:** `pipeline.py` (not dbt)

**Flow:**
1. Before invoking Gold weekly dbt model:
   - Read `pipeline/gold_weekly_control.parquet`
   - Identify uncomputed weeks in date range
   - Pass only uncomputed weeks to dbt model as variables
2. After dbt completes:
   - Write newly computed week entries to `gold_weekly_control.parquet`
   - Update `computed_at` and `computed_by_run_id` columns
3. Watermark advances only after control table write succeeds

**Why in Python:** dbt models are stateless transformations. A model that reads prior output to decide its own behaviour violates that contract. Python orchestration maintains this boundary.

---

## 3. Layer-to-Layer Contracts

### Bronze Layer (Read Boundary)

**Input Sources:**
- `source/transactions_YYYY-MM-DD.csv` (daily transaction delta)
- `source/accounts_YYYY-MM-DD.csv` (daily account delta or snapshot on Day 1)
- `source/transaction_codes.csv` (static reference, loaded once)

**Output Destination:**
- `data/bronze/transactions/date=YYYY-MM-DD/data.parquet`
- `data/bronze/accounts/date=YYYY-MM-DD/data.parquet`
- `data/bronze/transaction_codes/data.parquet`

**Enforcement Mechanism:**
- Idempotency: Partition existence check before read
- Atomicity: Write to temp file, rename atomically
- Audit trail: Three columns added (`_source_file`, `_ingested_at`, `_pipeline_run_id`)
- No modifications allowed on re-run (INV-02, INV-03)

---

### Silver Layer (Transformation Boundary)

**Input Sources:**
- Bronze layer (same-date partitions for transactions/accounts)
- `silver/transaction_codes/data.parquet` (joined for sign assignment)

**Output Destinations:**
- `data/silver/transactions/date=YYYY-MM-DD/data.parquet` (resolvable records)
- `data/silver/accounts/data.parquet` (latest per account_id)
- `data/silver/transaction_codes/data.parquet` (reference copy)
- `data/silver/quarantine/date=YYYY-MM-DD/rejected.parquet` (rejected records)

**Quality Rules Applied:**
- **Transactions:**
  - NULL field validation (INV-26)
  - `transaction_code` validation against Silver transaction codes (INV-09)
  - Amount validation (positive values only)
  - `account_id` resolution (flag false if unresolvable, INV-10)
  - Global deduplication on `transaction_id` across all prior partitions (INV-06)
  - Sign assignment from `debit_credit_indicator` (INV-08)
  - `_missing_merchant_name` flag for PURCHASE types (GAP 4)

- **Accounts:**
  - Latest record per `account_id` (upsert, INV-36)
  - No duplicates (INV-07)

- **Quarantine:**
  - Exhaustive rejection codes only (INV-26)
  - Original record preserved for audit

**Enforcement Mechanism:**
- Atomicity: dbt table materialisation (replace on re-run)
- Deduplication: Global join against prior Silver partitions
- Traceability: All records inherit `_pipeline_run_id` from Bronze
- Accounting: Bronze = Silver + Quarantine per date (INV-05)

---

### Gold Layer (Aggregation Boundary)

**Input Sources:**
- `silver/transactions/**/*.parquet` (all dates, `_is_resolvable = true` only for main aggregates)
- Control gate: `pipeline/gold_weekly_control.parquet` (weeks already computed)

**Output Destinations:**
- `data/gold/daily_summary/data.parquet` (one row per processed transaction_date)
- `data/gold/weekly_account_summary/data.parquet` (one row per account/week with resolvable transactions)

**Aggregations:**
- **Daily Summary:**
  - `total_transactions` (count of resolvable Silver records)
  - `total_signed_amount` (sum of `_signed_amount` for resolvable records)
  - `total_unresolvable_transactions` (count of unresolvable records)
  - `total_unresolvable_amount` (sum for unresolvable records)
  - One row per calendar date, even if zero transactions (INV-44)

- **Weekly Account Summary:**
  - `opening_balance` (from prior week or account snapshot)
  - `total_transactions` (count of resolvable Silver transactions in week)
  - `total_signed_amount` (sum of `_signed_amount` in week)
  - `closing_balance` (fixed at first computation time, INV-14)
  - One row per `(account_id, week_start_date)` with ≥1 resolvable transactions (INV-38)

**Enforcement Mechanism:**
- Atomicity: dbt table materialisation with forced re-computation (INV-41)
- Control gate: Python reads `gold_weekly_control` before dbt execution (INV-14, INV-42)
- Idempotency: Same input + control gate + table materialisation = same output
- Traceability: All records have `_pipeline_run_id` from Silver source

---

## 4. Control-Plane Artifacts

### control.parquet — Watermark and Run Tracking

**Schema:**
```
last_processed_date     DATE          (next date to process on incremental run)
updated_at              TIMESTAMP     (when watermark last advanced)
updated_by_run_id       STRING        (UUID of run that advanced watermark)
```

**State Machine:**
```
NOT_INITIALIZED
    ↓ (first historical run)
last_processed_date = NULL, updated_by_run_id = RUN-1
    ↓ (date 2024-01-01 succeeds)
last_processed_date = 2024-01-01, updated_by_run_id = RUN-1
    ↓ (date 2024-01-02 succeeds)
last_processed_date = 2024-01-02, updated_by_run_id = RUN-2
    ↓ (date 2024-01-03 fails at Silver)
last_processed_date = 2024-01-02, updated_by_run_id = RUN-2 (unchanged)
    ↓ (date 2024-01-03 retried, succeeds)
last_processed_date = 2024-01-03, updated_by_run_id = RUN-3
```

**Reading Points:**
- Start of incremental pipeline: read `last_processed_date`
- Run log recovery: read `updated_by_run_id` to detect unlogged prior runs
- Gold weekly gate: use `updated_by_run_id` to avoid recomputing same run

---

### gold_weekly_control.parquet — Computed Weeks Registry

**Schema:**
```
week_start_date         DATE          (Monday of week, ISO week)
week_end_date           DATE          (Sunday of week)
computed_at             TIMESTAMP     (when this week was first computed)
computed_by_run_id      STRING        (UUID of run that computed this week)
```

**Purpose:**
- Gate: "Is this week already computed?" before Gold weekly dbt invocation
- Registry: "Which weeks have closing_balance locked in?" to prevent recomputation
- Enforcement: Every week in Gold must have matching entry (INV-14)

**Write Sequence:**
1. dbt computes Gold weekly rows for uncomputed weeks
2. `pipeline.py` queries Gold weekly output to identify newly computed weeks
3. `pipeline.py` appends rows to `gold_weekly_control.parquet`
4. Watermark advances only after step 3 completes

---

### run_log.parquet — Append-Only Audit Trail

**Schema:**
```
run_id                  STRING        (UUID, one per pipeline invocation)
started_at              TIMESTAMP     (when this component started)
completed_at            TIMESTAMP     (when this component completed)
model_name              STRING        (dbt model or Bronze loader or DBT_COMPILE sentinel)
layer                   STRING        (BRONZE, SILVER, GOLD, ORCHESTRATION sentinel)
status                  STRING        (SUCCESS, FAILED, SKIPPED)
records_processed       INTEGER       (input row count before transformation)
records_written         INTEGER       (output row count after transformation)
records_rejected        INTEGER       (quarantine row count for Silver models only)
error_message           TEXT          (stack trace or description for FAILED/UNLOGGED_RUN rows)
```

**One-to-One Mapping:** One row per executed component per run. If a run processes dates 2024-01-01 through 2024-01-03, and each date invokes 4 models, the run log has 12 SUCCESS rows + control overhead.

**Recovery Mechanism:**
1. If run log flush fails: write failure to `.jsonl` fallback file; `pipeline.py` continues
2. On next successful run: read watermark's `updated_by_run_id`; check if any run log entries exist for that run_id
3. If no entries: write synthetic `UNLOGGED_RUN` row with explanation before proceeding
4. Fallback file is not automatically deleted — engineer must clean up manually if needed

---

## 5. Module Boundaries

| Module | Responsibility | API Surface | Stateful |
|---|---|---|---|
| **Bronze Loaders** (Python) | Read CSV, add audit cols, write Parquet atomically | File I/O, idempotency gate | ✗ (stateless, driven by partition existence) |
| **dbt Silver Models** | Quality enforcement, deduplication, sign assignment | Read Bronze, write Silver/Quarantine Parquet | ✗ (stateless transformations) |
| **dbt Gold Models** | Aggregation and summary computation | Read Silver, write Gold Parquet | ✗ (stateless; weekly control gate managed externally) |
| **pipeline.py** | Orchestration, sequencing, control-plane writes | CLI args, file I/O, subprocess management | ✓ (manages watermark, run log buffer, control table state) |
| **Control Plane** (parquet files) | State registry and audit trail | Read/write via `pipeline.py` and DuckDB | ✓ (persisted state) |

---

## 6. Failure Modes and Recovery

### Failure During Bronze Ingestion

```
pipeline.py: Bronze load fails for 2024-01-03
    ↓
Watermark stays at 2024-01-02
Run log: FAILED row written (status=FAILED, model_name=bronze_transactions, error_message=...)
    ↓
Next pipeline invocation: reads watermark=2024-01-02, target date=2024-01-03
    ↓
Partition existence check: 2024-01-03 does not exist → proceed with fresh load
    ↓
On success: watermark advances to 2024-01-03, new run log entries written
```

### Failure During Silver Promotion

```
pipeline.py: Silver transactions promotion fails for 2024-01-03
    ↓
Gold not invoked (SKIPPED rows written for gold_daily_summary, gold_weekly_account_summary)
Watermark stays at 2024-01-02
Run log: FAILED row for silver_transactions, SKIPPED rows for downstream models
    ↓
Next invocation: target date=2024-01-03 re-runs from Bronze (partition exists)
    ↓
Bronze idempotency gate: partition 2024-01-03 exists → skip Bronze re-read, re-read succeeds
    ↓
Silver promotion re-attempted, succeeds → watermark advances
```

### Failure During Run Log Flush

```
pipeline.py: Watermark advances to 2024-01-05
pipeline.py: Attempts to flush run log buffer to parquet
    ↓
Flush fails (I/O error, permission, etc.)
    ↓
Failure logged to pipeline_runlog_fallback.jsonl
pipeline.py exits (no exception re-raised)
    ↓
Next invocation: pipeline.py reads watermark=2024-01-05, updated_by_run_id=RUN-5
    ↓
Query run_log.parquet for any row with run_id=RUN-5: returns no rows
    ↓
pipeline.py writes UNLOGGED_RUN synthetic row explaining prior flush failure
    ↓
New run proceeds normally with 2024-01-06
    ↓
Both runs eventually visible in audit trail; data correctness preserved
```

---

## 7. Data Movement Checklist

During each pipeline run, data moves through these checkpoints in order:

1. **Source CSV Read** → Validate file existence, structure
2. **Bronze Write** → Partition existence check, atomic write, audit columns added
3. **Silver Read from Bronze** → All layers join, deduplicate, validate
4. **Silver Write** → Atomic partition write, quarantine partition created
5. **Gold Read from Silver** → Filtered for `_is_resolvable = true` (weekly control gate applied)
6. **Gold Write** → Atomic partition write, overwrites any prior content
7. **Control-Plane Write** → Watermark advances, gold_weekly_control updated, run_log buffer flushed
8. **Pipeline Exit** → All data layers, control tables, run log are consistent

---

*Topology document completes Phase 8 discovery for system-wide data flow and control-plane architecture.*
