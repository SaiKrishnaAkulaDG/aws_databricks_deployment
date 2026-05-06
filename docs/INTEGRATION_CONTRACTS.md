# INTEGRATION_CONTRACTS.md — Component I/O Specifications

**PBVI Phase:** 8 — Discovery  
**Document Type:** Integration contract specifications  
**Created:** Phase 8 completion  
**Status:** FINAL

---

## Overview

This document specifies the input and output contracts for each major pipeline component, including file paths, schemas, constraints, and invariants at each boundary.

---

## 1. Bronze Layer Contracts

### 1.1 Bronze Transactions Loader

**Component:** Python loader in `pipeline/pipeline.py`

**Input Contract:**
- **Source:** `source/transactions_YYYY-MM-DD.csv` (daily transaction delta)
- **Format:** CSV with headers
- **Required Columns:** `transaction_id`, `transaction_code`, `account_id`, `transaction_date`, `amount`, `transaction_type`, `merchant_name`, `channel`
- **Constraints:** 
  - No header modifications between files
  - Each row represents one transaction
  - `amount` is positive (sign is applied in Silver via transaction_code)
  - File must exist in `source/` before loader runs

**Output Contract:**
- **Destination:** `data/bronze/transactions/date=YYYY-MM-DD/data.parquet`
- **Partitioning:** By `transaction_date` (ISO date format YYYY-MM-DD)
- **Materialisation:** Table (parquet partition)
- **Atomicity:** Temp file + atomic rename on success

**Output Schema:**
```
transaction_id         STRING        NOT NULL (from CSV)
transaction_code       STRING        NOT NULL (from CSV)
account_id             STRING        NOT NULL (from CSV)
transaction_date       DATE          NOT NULL (from CSV)
amount                 DECIMAL(10,2) NOT NULL (positive, sign applied in Silver)
transaction_type       STRING        NOT NULL (from CSV)
merchant_name          VARCHAR       NULLABLE (from CSV)
channel                STRING        NOT NULL (from CSV)
_source_file           VARCHAR       NOT NULL (populated by loader: 'transactions_YYYY-MM-DD.csv')
_ingested_at           TIMESTAMP     NOT NULL (populated by loader: current timestamp)
_pipeline_run_id       STRING        NOT NULL (passed by pipeline orchestrator)
```

**Invariants Enforced:**
- INV-01: Source file not modified
- INV-02: Partition existence check (skip if exists)
- INV-03: Partition never modified after first write
- INV-04: Audit columns non-null
- INV-40: Atomic write

**Verification:**
```bash
duckdb << EOF
SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/date=2024-01-01/data.parquet')
WHERE _source_file IS NULL OR _ingested_at IS NULL OR _pipeline_run_id IS NULL;
EOF
```

Expected: 0

---

### 1.2 Bronze Accounts Loader

**Component:** Python loader in `pipeline/pipeline.py`

**Input Contract:**
- **Source:** `source/accounts_YYYY-MM-DD.csv` (daily account delta, or full snapshot on Day 1)
- **Format:** CSV with headers
- **Required Columns:** `account_id`, `customer_id`, `account_status`, `credit_limit`, `current_balance`, `date_opened`
- **Constraints:**
  - Day 1 treated as full snapshot establishing baseline
  - Days 2–7 are true deltas (only new and changed records)
  - File must exist in `source/` before loader runs

**Output Contract:**
- **Destination:** `data/bronze/accounts/date=YYYY-MM-DD/data.parquet`
- **Partitioning:** By date (YYYY-MM-DD)
- **Materialisation:** Table (parquet partition)
- **Atomicity:** Temp file + atomic rename on success

**Output Schema:**
```
account_id             STRING        NOT NULL (from CSV)
customer_id            STRING        NOT NULL (from CSV)
account_status         STRING        NOT NULL (from CSV: 'ACTIVE', 'INACTIVE', 'CLOSED')
credit_limit           DECIMAL(12,2) NOT NULL (from CSV)
current_balance        DECIMAL(12,2) NOT NULL (from CSV)
date_opened            DATE          NOT NULL (from CSV)
_source_file           VARCHAR       NOT NULL (populated by loader: 'accounts_YYYY-MM-DD.csv')
_ingested_at           TIMESTAMP     NOT NULL (populated by loader: current timestamp)
_pipeline_run_id       STRING        NOT NULL (passed by pipeline orchestrator)
```

**Invariants Enforced:**
- INV-01: Source file not modified
- INV-02: Partition existence check
- INV-03: Partition immutable after first write
- INV-04: Audit columns non-null
- INV-40: Atomic write

---

### 1.3 Bronze Transaction Codes Loader

**Component:** Python loader in `pipeline/pipeline.py`

**Input Contract:**
- **Source:** `source/transaction_codes.csv` (static reference, loaded once)
- **Format:** CSV with headers
- **Required Columns:** `transaction_code`, `transaction_type`, `debit_credit_indicator`, `affects_balance`
- **Constraints:**
  - Loaded only once during historical pipeline initialisation
  - Not reloaded on incremental runs
  - File is static for exercise duration

**Output Contract:**
- **Destination:** `data/bronze/transaction_codes/data.parquet`
- **Partitioning:** None (single reference file)
- **Materialisation:** Table (parquet)
- **Atomicity:** Temp file + atomic rename on success

**Output Schema:**
```
transaction_code       STRING        NOT NULL (primary key)
transaction_type       STRING        NOT NULL ('PURCHASE', 'REFUND', 'ADJUSTMENT', 'FEE')
debit_credit_indicator STRING        NOT NULL ('DR' for debit/credit, 'CR' for credit/charge)
affects_balance        BOOLEAN       NOT NULL (TRUE if affects account balance)
_source_file           VARCHAR       NOT NULL (populated: 'transaction_codes.csv')
_ingested_at           TIMESTAMP     NOT NULL (populated: current timestamp)
_pipeline_run_id       STRING        NOT NULL (passed by pipeline orchestrator)
```

**Invariants Enforced:**
- INV-01: Source file not modified
- INV-04: Audit columns non-null
- INV-40: Atomic write

**Note:** No partition existence check for this loader — file is loaded once at historical pipeline start.

---

## 2. Silver Layer Contracts

### 2.1 Silver Transactions (dbt Model)

**Component:** dbt model `silver_transactions.sql` in `dbt/models/silver/`

**Input Contract:**
- **Source 1:** `bronze/transactions/date={partition_date}/data.parquet` (Bronze source, current date)
- **Source 2:** `silver/accounts/data.parquet` (Silver Accounts, for `account_id` validation)
- **Source 3:** `silver/transaction_codes/data.parquet` (Silver TC, for sign assignment)
- **Dependencies:** `ref('stg_silver_accounts')`, `ref('stg_silver_transaction_codes')`

**Input Constraints:**
- Bronze transaction records are complete and unmodified
- Silver accounts contains latest per `account_id`
- Silver transaction codes is authoritative for debit_credit_indicator

**Output Contract:**
- **Destination:** `data/silver/transactions/date=YYYY-MM-DD/data.parquet`
- **Partitioning:** By date
- **Materialisation:** `table` (drop and recreate, never incremental)
- **Atomicity:** dbt ensures atomic write

**Output Schema:**
```
transaction_id         STRING        NOT NULL (from Bronze, de-duplicated globally)
transaction_code       STRING        NOT NULL (from Bronze, validated)
account_id             STRING        NOT NULL (from Bronze)
transaction_date       DATE          NOT NULL (from Bronze)
amount                 DECIMAL(10,2) NOT NULL (from Bronze)
transaction_type       STRING        NOT NULL (from Bronze)
merchant_name          VARCHAR       NULLABLE (from Bronze)
channel                STRING        NOT NULL (from Bronze)
_signed_amount         DECIMAL(10,2) NOT NULL (amount * sign from TC)
_is_resolvable         BOOLEAN       NOT NULL (FALSE if account_id not in Silver accounts)
_missing_merchant_name BOOLEAN       NOT NULL (TRUE if txn_type='PURCHASE' AND merchant_name IS NULL)
_source_file           VARCHAR       NOT NULL (inherited from Bronze)
_ingested_at           TIMESTAMP     NOT NULL (inherited from Bronze)
_pipeline_run_id       STRING        NOT NULL (inherited from Bronze, preserved)
```

**Quality Rules Applied:**
- NULL field validation (required fields from Bronze non-null)
- `transaction_code` must exist in Silver transaction_codes
- Amount must be positive
- `account_id` resolution: flag unresolvable, do not quarantine
- Global deduplication on `transaction_id` (check all prior partitions)
- Sign assignment: DR = positive, CR = negative
- `_missing_merchant_name` flag

**Quarantine Routing (Rejected Records):**
Records rejected during promotion are written to `data/silver/quarantine/date=YYYY-MM-DD/rejected.parquet` with:
```
{all original Bronze columns}
_rejection_reason      STRING        NOT NULL (from {NULL_REQUIRED_FIELD, INVALID_AMOUNT, DUPLICATE_TRANSACTION_ID, INVALID_TRANSACTION_CODE, INVALID_CHANNEL, INVALID_ACCOUNT_STATUS})
_rejected_at           TIMESTAMP     NOT NULL (current timestamp)
_rejected_by_run_id    STRING        NOT NULL (current pipeline run_id)
```

**Invariants Enforced:**
- INV-05: Bronze = Silver + Quarantine per date
- INV-06: No duplicate `transaction_id` globally
- INV-08: Sign assignment from TC only
- INV-09: Invalid TC codes → quarantine
- INV-10: Unresolvable account_id → flag, not quarantine
- INV-26: Quarantine codes exhaustive
- INV-37: Read only Silver transaction codes
- INV-45: `_missing_merchant_name` flag

**Verification:**
```bash
bash verification/verify_silver_transactions.sh
```

---

### 2.2 Silver Accounts (dbt Model)

**Component:** dbt model `silver_accounts.sql` in `dbt/models/silver/`

**Input Contract:**
- **Source:** `bronze/accounts/date={partition_date}/data.parquet` (Bronze source)
- **Dependency:** Must run after Bronze accounts load for same date

**Input Constraints:**
- Bronze accounts records are complete
- All required columns present and non-null

**Output Contract:**
- **Destination:** `data/silver/accounts/data.parquet` (non-partitioned, single file)
- **Materialisation:** `table` with upsert logic on `account_id`
- **Atomicity:** dbt ensures atomic write

**Output Schema:**
```
account_id             STRING        NOT NULL (primary key)
customer_id            STRING        NOT NULL (from latest Bronze delta)
account_status         STRING        NOT NULL (from latest Bronze delta)
credit_limit           DECIMAL(12,2) NOT NULL (from latest Bronze delta)
current_balance        DECIMAL(12,2) NOT NULL (from latest Bronze delta, treated as immutable per week in Gold)
date_opened            DATE          NOT NULL (from latest Bronze delta)
_source_file           VARCHAR       NOT NULL (inherited from Bronze)
_ingested_at           TIMESTAMP     NOT NULL (inherited from Bronze)
_pipeline_run_id       STRING        NOT NULL (inherited from Bronze, preserved)
_last_updated_date     DATE          NOT NULL (date when this record was last updated)
```

**Upsert Logic:**
- For each incoming account_id: check if exists in Silver accounts
- If exists: replace entire record with new version (no partial updates)
- If not exists: insert new record
- Result: exactly one record per `account_id` at all times

**Invariants Enforced:**
- INV-07: One record per `account_id` (no duplicates)
- INV-36: Upsert replaces existing record (not append)

---

### 2.3 Silver Transaction Codes (Reference Copy)

**Component:** dbt model `silver_transaction_codes.sql` in `dbt/models/silver/`

**Input Contract:**
- **Source:** `bronze/transaction_codes/data.parquet` (Bronze source)

**Output Contract:**
- **Destination:** `data/silver/transaction_codes/data.parquet` (non-partitioned)
- **Materialisation:** `table` (static, loaded once)

**Output Schema:**
```
transaction_code       STRING        NOT NULL (primary key)
transaction_type       STRING        NOT NULL
debit_credit_indicator STRING        NOT NULL ('DR' or 'CR')
affects_balance        BOOLEAN       NOT NULL
_source_file           VARCHAR       NOT NULL (inherited from Bronze)
_ingested_at           TIMESTAMP     NOT NULL (inherited from Bronze)
_pipeline_run_id       STRING        NOT NULL (inherited from Bronze, preserved)
```

**No transformations applied** — direct copy of Bronze TC plus inherited audit columns.

---

## 3. Gold Layer Contracts

### 3.1 Gold Daily Summary (dbt Model)

**Component:** dbt model `gold_daily_summary.sql` in `dbt/models/gold/`

**Input Contract:**
- **Source 1:** `silver/transactions/**/*.parquet` (all partitions, filtered on `_is_resolvable`)
- **Dependency:** dbt `ref('silver_transactions')` plus all prior partitions

**Input Constraints:**
- Silver transactions must have `_signed_amount` (sign applied)
- Must filter on `_is_resolvable = true` for main aggregates
- Must include `_is_resolvable = false` records separately for unresolvable columns

**Output Contract:**
- **Destination:** `data/gold/daily_summary/data.parquet`
- **Partitioning:** None (single table)
- **Materialisation:** `table` (drop and recreate, never incremental)
- **Atomicity:** dbt ensures atomic write
- **Grain:** One row per calendar `transaction_date` processed, even if zero transactions

**Output Schema:**
```
transaction_date              DATE        NOT NULL (primary key)
total_transactions            INTEGER     NOT NULL (count of resolvable Silver records)
total_signed_amount           DECIMAL(12,2) NOT NULL (sum of _signed_amount, resolvable only)
total_unresolvable_transactions INTEGER     NOT NULL (count of _is_resolvable = false records)
total_unresolvable_amount     DECIMAL(12,2) NOT NULL (sum of _signed_amount, unresolvable only)
_pipeline_run_id              STRING      NOT NULL (from Silver source records)
computed_at                   TIMESTAMP   NOT NULL (when this row was computed)
```

**Invariants Enforced:**
- INV-11: Uses only `_is_resolvable = true` for main aggregates
- INV-12: Reads only from Silver
- INV-13: One row per `transaction_date` (no duplicates)
- INV-41: `table` materialisation (no incremental)
- INV-44: Row exists for every processed date, even if zero resolvable

**Verification:**
```bash
bash verification/verify_gold.sh
```

---

### 3.2 Gold Weekly Account Summary (dbt Model)

**Component:** dbt model `gold_weekly_account_summary.sql` in `dbt/models/gold/`

**Input Contract:**
- **Source:** `silver/transactions/**/*.parquet` (all partitions, filtered on `_is_resolvable = true`)
- **Control Gate:** `pipeline.py` provides dbt variable `weeks` = list of uncomputed weeks (ISO week_start_date values)
- **Dependency:** dbt `ref('silver_transactions')` and `ref('silver_accounts')`

**Input Constraints:**
- Must not read `gold_weekly_control.parquet` directly (managed by Python)
- Must accept only weeks provided in dbt variables
- Must filter on `_is_resolvable = true` for aggregation

**Output Contract:**
- **Destination:** `data/gold/weekly_account_summary/data.parquet`
- **Partitioning:** None (single table)
- **Materialisation:** `table` (drop and recreate, never incremental)
- **Atomicity:** dbt ensures atomic write
- **Grain:** One row per `(account_id, week_start_date)` with ≥1 resolvable Silver transaction in that week

**Output Schema:**
```
account_id            STRING        NOT NULL (part of primary key)
week_start_date       DATE          NOT NULL (Monday of week, ISO week, part of primary key)
week_end_date         DATE          NOT NULL (Sunday of week)
opening_balance       DECIMAL(12,2) NULLABLE (balance at week start from Silver accounts snapshot)
total_transactions    INTEGER       NOT NULL (count of resolvable Silver txns in week)
total_signed_amount   DECIMAL(12,2) NOT NULL (sum of _signed_amount, resolvable only)
closing_balance       DECIMAL(12,2) NOT NULL (balance fixed at first computation time, immutable)
_pipeline_run_id      STRING        NOT NULL (from Silver source records)
computed_at           TIMESTAMP     NOT NULL (when this row was computed)
```

**Critical Constraint — Idempotency:**
- `closing_balance` is fixed at first computation time
- Re-running the model for a week already in `gold_weekly_control` must never happen
- If control entry is absent but Gold data exists: full overwrite required (INV-42)

**Invariants Enforced:**
- INV-11: Uses only `_is_resolvable = true` for aggregates
- INV-12: Reads only from Silver
- INV-14: One row per (account_id, week_start_date), immutable after computation
- INV-38: Row exists only for accounts with ≥1 resolvable txn in week
- INV-39: Cross-file consistency with daily summary
- INV-41: `table` materialisation (no incremental)

**Note — Procedural Control (IG-07):**
This model must ONLY be executed through `pipeline.py`. Direct invocation via `dbt run --select gold_weekly_account_summary` bypasses the control gate and recomputes all weeks, overwriting `closing_balance` values and violating idempotency. **A prominent warning comment in the model file documents this constraint.**

---

## 4. Control-Plane Contracts

### 4.1 Pipeline Control Table

**File Path:** `data/pipeline/control.parquet`

**Ownership:** `pipeline.py` (Python orchestrator only)

**Schema:**
```
last_processed_date       DATE        NOT NULL (watermark: next target date = watermark + 1)
updated_at                TIMESTAMP   NOT NULL (when watermark last advanced)
updated_by_run_id         STRING      NOT NULL (run_id of the invocation that advanced watermark)
```

**Cardinality:** Single row (state machine)

**Write Semantics:**
- Advances only after Bronze, Silver, Gold complete successfully for a date
- Failure at any layer prevents advancement
- Value is authoritative — incremental pipeline computes `last_processed_date + 1`

**Read Points:**
- Incremental pipeline start: read `last_processed_date` to compute target date
- Run log recovery: read `updated_by_run_id` to detect unlogged prior runs

**Invariants Enforced:**
- INV-15: Advances only on full success
- INV-17: Incremental processes exactly watermark + 1
- INV-32: Absent on first run (initialise with appropriate defaults)

---

### 4.2 Gold Weekly Control Table

**File Path:** `data/pipeline/gold_weekly_control.parquet`

**Ownership:** `pipeline.py` (Python orchestrator only)

**Schema:**
```
week_start_date       DATE        NOT NULL (Monday of week, ISO week, primary key)
week_end_date         DATE        NOT NULL (Sunday of week)
computed_at           TIMESTAMP   NOT NULL (when this week was first computed)
computed_by_run_id    STRING      NOT NULL (run_id that computed this week)
```

**Cardinality:** One row per computed week

**Write Semantics:**
- Before dbt invocation: read control table, identify uncomputed weeks
- Pass only uncomputed weeks to dbt model as variables
- After dbt completes: append rows for newly computed weeks
- Watermark advances only after control table write succeeds

**Read Points:**
- Before Gold weekly dbt invocation: read to filter weeks
- Verification: cross-check with weeks present in Gold output

**Invariants Enforced:**
- INV-14: One row per computed week; prevents recomputation
- INV-31: Written before watermark advances
- INV-42: If Gold has data but control has no entry: overwrite on re-run

---

### 4.3 Pipeline Run Log

**File Path:** `data/pipeline/run_log.parquet`

**Ownership:** `pipeline.py` (writes via async buffer after watermark advances)

**Schema:**
```
run_id                STRING        NOT NULL (UUID generated at pipeline start)
started_at            TIMESTAMP     NOT NULL (component start time)
completed_at          TIMESTAMP     NOT NULL (component completion time)
model_name            STRING        NOT NULL (dbt model name or 'bronze_transactions', 'bronze_accounts', etc., or sentinel 'DBT_COMPILE', 'UNLOGGED_RUN')
layer                 STRING        NOT NULL ('BRONZE', 'SILVER', 'GOLD', 'ORCHESTRATION' for sentinels)
status                STRING        NOT NULL ('SUCCESS', 'FAILED', 'SKIPPED')
records_processed     INTEGER       NOT NULL (input row count)
records_written       INTEGER       NOT NULL (output row count, queried from Parquet files)
records_rejected      INTEGER       NULLABLE (quarantine count for Silver models only, NULL for others)
error_message         TEXT          NULLABLE (stack trace or explanation for FAILED/UNLOGGED_RUN)
```

**Cardinality:** Append-only; one row per component execution per run

**Write Semantics:**
- Entries accumulated in memory buffer during pipeline execution
- Real-time parsing of dbt JSON log (`NodeStart`, `NodeFinished` events)
- On process exit: add SKIPPED rows for non-executed models
- After watermark advances: flush entire buffer to parquet in single append operation

**Fallback Mechanism:**
- On flush failure: write buffer to `.jsonl` fallback file
- On next successful run: detect mismatch between watermark `updated_by_run_id` and run_log entries
- Write synthetic `UNLOGGED_RUN` row explaining prior flush failure

**Sentinel Values:**
- `model_name = 'DBT_COMPILE'`, `layer = 'ORCHESTRATION'`, `status = 'FAILED'` → dbt compile failed
- `model_name = 'UNLOGGED_RUN'` → prior run's log entries failed to flush; now recovered

**Read Points:**
- Verification scripts: query to validate audit trail
- Analysts: trace `_pipeline_run_id` from Gold back to Silver and Bronze

**Invariants Enforced:**
- INV-19: Append-only (no modification or deletion)
- INV-20A: One SUCCESS row per executed model per run
- INV-20B: Failed runs have at least one FAILED row
- INV-20C: Flush failures recovered via UNLOGGED_RUN
- INV-22: Every Silver/Gold `_pipeline_run_id` traceable to run_log SUCCESS row

**Verification:**
```bash
bash verification/verify_audit_trail.sh
```

---

## 5. File Path Reference

| Component | Path | Partitioned | Ownership | Grain |
|---|---|---|---|---|
| Source Transactions | `source/transactions_YYYY-MM-DD.csv` | Per date | External system | Per transaction |
| Source Accounts | `source/accounts_YYYY-MM-DD.csv` | Per date | External system | Per account delta/snapshot |
| Source Transaction Codes | `source/transaction_codes.csv` | — | External system | Per code (static) |
| Bronze Transactions | `data/bronze/transactions/date=YYYY-MM-DD/data.parquet` | By date | Bronze loader | Per transaction |
| Bronze Accounts | `data/bronze/accounts/date=YYYY-MM-DD/data.parquet` | By date | Bronze loader | Per account record |
| Bronze Transaction Codes | `data/bronze/transaction_codes/data.parquet` | — | Bronze loader | Per code |
| Silver Transactions | `data/silver/transactions/date=YYYY-MM-DD/data.parquet` | By date | dbt model | Per transaction (deduplicated) |
| Silver Accounts | `data/silver/accounts/data.parquet` | — | dbt model | Per account (latest) |
| Silver Transaction Codes | `data/silver/transaction_codes/data.parquet` | — | dbt model | Per code |
| Silver Quarantine | `data/silver/quarantine/date=YYYY-MM-DD/rejected.parquet` | By date | dbt model | Per rejected record |
| Gold Daily Summary | `data/gold/daily_summary/data.parquet` | — | dbt model | Per calendar date |
| Gold Weekly Summary | `data/gold/weekly_account_summary/data.parquet` | — | dbt model | Per (account_id, week) |
| Pipeline Control | `data/pipeline/control.parquet` | — | pipeline.py | Single row (state) |
| Gold Weekly Control | `data/pipeline/gold_weekly_control.parquet` | — | pipeline.py | Per computed week |
| Run Log | `data/pipeline/run_log.parquet` | — | pipeline.py | Per component execution |
| Run Log Fallback | `data/pipeline/pipeline_runlog_fallback.jsonl` | — | pipeline.py | Per failed flush |

---

*Integration Contracts document completes Phase 8 discovery of all I/O boundaries and schemas.*
