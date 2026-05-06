# INVARIANTS.md
## Credit Card Financial Transactions Lake
**PBVI Phase:** 2 — Invariant Definition
**Brief Version:** 1.0 + REQUIREMENTS_GAPS_COVER.md (all gaps closed)
**Architecture Version:** ARCHITECTURE.md — DECIDED
**Status:** COMPLETE — engineer sign-off required before Phase 3

---

## Authorship and Challenge Record

| Step | Status |
|---|---|
| Step 0 — Data touch point map | COMPLETE — 13 touch points mapped |
| Step 1 — Engineer draft + Claude challenge | COMPLETE — 5 rounds of challenge and revision |
| Step 1b — Sufficiency check against ARCHITECTURE.md | COMPLETE — all architecture gaps closed |
| Breakpoint audit | COMPLETE — 17 breakpoints evaluated |
| Final gap sweep | COMPLETE — all touch points and breakpoints covered |

**Total invariants in set:** 38
**GLOBAL invariants:** 2 (INV-01, INV-22)
**TASK-SCOPED invariants:** 36
**Reclassified to implementation guidance:** INV-16, INV-21, INV-23, INV-28, INV-29, INV-30 + 3 architecture items

---

## How to Read This Document

Each invariant is stated as a falsifiable condition — a test can be written that fails if the condition is violated. Invariants are not goals or design principles; they are constraints. If any invariant is violated, the system is broken regardless of what else works.

**Classification key:**
- **GLOBAL** — applies to every task in the build regardless of what is being built. Embedded in Claude.md Section 2.
- **TASK-SCOPED** — applies only to tasks that touch the named component or data boundary. Embedded in the relevant task prompts in EXECUTION_PLAN.md.

**Category key:**
- **data correctness** — violation corrupts data or breaks analytical trust
- **operational** — violation breaks pipeline execution guarantees or audit integrity

---

## Section 1 — Global Invariants

These two invariants apply to every task without exception. They are embedded in Claude.md and enforced at every build session.

---

### INV-01 — Source Files Are Read-Only
**Category:** data correctness
**Classification:** GLOBAL

Source CSV files in `source/` must never be modified, overwritten, deleted, or renamed by any pipeline component. The pipeline reads from `source/` only.

**Why this matters:** Any mutation of source CSVs breaks auditability and makes it impossible to trace Gold outputs back to original data. The pipeline is a consumer of source files, not an owner.

**Verification:** `git status source/` after any pipeline run must show no modifications. File modification timestamps on source CSVs must not change between pipeline runs.

---

### INV-22 — All Silver and Gold Records Must Have a Traceable `_pipeline_run_id`
**Category:** data correctness
**Classification:** GLOBAL

Every record in Silver (transactions, accounts, transaction codes, quarantine) and Gold (daily summary, weekly account summary) must have a non-null `_pipeline_run_id` that is traceable to a corresponding row in `pipeline/run_log.parquet` with `status = SUCCESS` for the same `run_id` and `model_name`.

**Why this matters:** `_pipeline_run_id` is the primary linkage between data records and pipeline execution metadata. Without a non-null and traceable run_id, lineage from Gold → Silver → Bronze cannot be established, breaking auditability and the verification guarantees in Section 10.5 of the brief.

**Verification:**
```sql
-- No Silver or Gold record should have a null run_id
SELECT COUNT(*) FROM read_parquet('silver/transactions/**/*.parquet')
WHERE _pipeline_run_id IS NULL;
-- Expected: 0

-- Every run_id in Silver must appear in the run log with SUCCESS
SELECT DISTINCT _pipeline_run_id FROM read_parquet('silver/transactions/**/*.parquet')
EXCEPT
SELECT run_id FROM read_parquet('pipeline/run_log.parquet')
WHERE status = 'SUCCESS';
-- Expected: 0 rows
```

---

## Section 2 — Task-Scoped Invariants

### 2.1 Bronze Layer Invariants

---

### INV-02 — Bronze Idempotency via Partition Existence Check
**Category:** data correctness
**Classification:** TASK-SCOPED (Bronze loaders)

Bronze ingestion enforces idempotency via partition existence check. Before reading any source file, the Bronze loader must check whether the target partition (`bronze/{entity}/date=YYYY-MM-DD/`) already exists. If the partition exists, ingestion must be skipped entirely for that entity and date. The source file must not be re-read.

**Why this matters:** Without this gate, re-runs re-read source files and risk duplicating data in Bronze, violating the write-once guarantee and causing downstream Silver deduplication failures and inflated Gold aggregates.

**Verification:**
```sql
-- Row count in Bronze must equal row count in source CSV after re-run
SELECT COUNT(*) FROM read_parquet('bronze/transactions/date=2024-01-01/data.parquet');
-- Must match: line count of transactions_2024-01-01.csv (excluding header)
```

---

### INV-03 — Bronze Partitions Are Immutable After First Write
**Category:** data correctness
**Classification:** TASK-SCOPED (Bronze loaders)

Bronze Parquet partitions must never be overwritten, modified, or deleted after their initial write. `bronze/transactions/date=YYYY-MM-DD/data.parquet`, `bronze/accounts/date=YYYY-MM-DD/data.parquet`, and `bronze/transaction_codes/data.parquet` are permanent once written.

**Why this matters:** Overwriting Bronze destroys raw history and invalidates the audit traceability from Gold → Silver → Bronze. Bronze is the system of record for what arrived in the source file.

**Verification:** File modification timestamp of any Bronze partition must not change between pipeline runs. A re-run must not change the `mtime` of any existing Bronze partition file.

---

### INV-04 — Bronze Audit Columns Must Be Non-Null
**Category:** data correctness
**Classification:** TASK-SCOPED (Bronze loaders)

Every Bronze record must include all three audit columns populated with non-null values at write time: `_source_file` (originating CSV filename), `_ingested_at` (timestamp when written to Bronze), and `_pipeline_run_id` (unique identifier of the pipeline run).

**Why this matters:** Missing or null audit column values break lineage tracing from Gold → Silver → Bronze. `_pipeline_run_id` is the connective tissue for all audit queries; a null value makes the record untraceable.

**Verification:**
```sql
SELECT COUNT(*) FROM read_parquet('bronze/transactions/**/*.parquet')
WHERE _source_file IS NULL OR _ingested_at IS NULL OR _pipeline_run_id IS NULL;
-- Expected: 0
```

---

### INV-40 — Bronze and Silver Partition Writes Must Be Atomic
**Category:** data correctness
**Classification:** TASK-SCOPED (Bronze loaders, Silver promotion models)

Bronze and Silver Parquet partition writes must be atomic. A failed or partial write must not leave a partition in a state that is treated as complete on subsequent runs. The pipeline must implement write atomicity (e.g., write to a temporary file then rename atomically) and must validate partition completeness before applying partition existence checks (Bronze) or deduplication logic (Silver).

**Why this matters:** A partial partition that passes the existence check (Bronze) or deduplication check (Silver) results in permanently incomplete data — Bronze skips re-ingestion leaving fewer rows than the source file, and Silver quarantines remaining records as `DUPLICATE_TRANSACTION_ID`, causing silent data loss with no recovery path.

**Verification:**
```sql
-- After any re-run following a simulated partial write,
-- Bronze row count must match source file row count exactly
SELECT COUNT(*) FROM read_parquet('bronze/transactions/date=2024-01-01/data.parquet');
```

---

### 2.2 Silver Layer Invariants

---

### INV-05 — Silver Promotion Is a Total Partition of Bronze Records
**Category:** data correctness
**Classification:** TASK-SCOPED (Silver promotion models)

Every record in Bronze transactions must exit Silver promotion into exactly one destination — Silver transactions or Silver quarantine. No record may be silently dropped. The accounting invariant: `COUNT(bronze transactions for date) = COUNT(silver transactions for date) + COUNT(quarantine for date)`.

**Why this matters:** Silent data loss at Silver promotion is undetectable in downstream layers. A dropped record never appears in Gold and never appears in quarantine — it simply vanishes with no audit trace.

**Verification:**
```sql
SELECT
  b.bronze_count,
  s.silver_count,
  q.quarantine_count,
  b.bronze_count - s.silver_count - q.quarantine_count AS discrepancy
FROM
  (SELECT COUNT(*) AS bronze_count FROM read_parquet('bronze/transactions/date=2024-01-01/data.parquet')) b,
  (SELECT COUNT(*) AS silver_count FROM read_parquet('silver/transactions/date=2024-01-01/data.parquet')) s,
  (SELECT COUNT(*) AS quarantine_count FROM read_parquet('silver/quarantine/date=2024-01-01/rejected.parquet')) q;
-- discrepancy must be 0
```

---

### INV-06 — Silver Transactions Are Globally Deduplicated on `transaction_id`
**Category:** data correctness
**Classification:** TASK-SCOPED (Silver transactions model)

No `transaction_id` may appear more than once across all Silver transactions partitions combined. A `transaction_id` that already exists in any Silver partition must be rejected to quarantine with rejection reason `DUPLICATE_TRANSACTION_ID` — it must not be written to Silver again.

**Why this matters:** Duplicate transactions inflate financial aggregates and break reporting accuracy. Deduplication must be global across all date partitions, not only within the current date's partition.

**Verification:**
```sql
SELECT transaction_id, COUNT(*) AS cnt
FROM read_parquet('silver/transactions/**/*.parquet')
GROUP BY transaction_id
HAVING cnt > 1;
-- Expected: 0 rows
```

---

### INV-07 — Silver Accounts Maintains Exactly One Latest Record Per `account_id`
**Category:** data correctness
**Classification:** TASK-SCOPED (Silver accounts model)

`silver/accounts/data.parquet` must contain exactly one record per `account_id` at all times. No `account_id` may appear more than once.

**Why this matters:** Multiple versions of the same account would cause inconsistent joins in Silver transaction promotion and incorrect balance calculations in Gold weekly aggregates.

**Verification:**
```sql
SELECT account_id, COUNT(*) AS cnt
FROM read_parquet('silver/accounts/data.parquet')
GROUP BY account_id
HAVING cnt > 1;
-- Expected: 0 rows
```

---

### INV-08 — Transaction Codes Are the Sole Authority for Sign Assignment
**Category:** data correctness
**Classification:** TASK-SCOPED (Silver transactions model)

Sign assignment for `_signed_amount` must be derived exclusively from `debit_credit_indicator` in `silver/transaction_codes/data.parquet`. The pipeline must not apply sign logic based on its own rules, transaction type inference, amount thresholds, or any source other than the Transaction Codes dimension. DR = positive (increases balance); CR = negative (decreases balance).

**Why this matters:** Any custom sign logic introduces financial inconsistencies — incorrect balance direction in aggregates — that cannot be detected by comparing row counts and can persist silently across all Gold outputs.

**Verification:**
```sql
-- All DR-coded transactions must have positive _signed_amount
SELECT COUNT(*) FROM read_parquet('silver/transactions/**/*.parquet') t
JOIN read_parquet('silver/transaction_codes/data.parquet') tc
  ON t.transaction_code = tc.transaction_code
WHERE tc.debit_credit_indicator = 'DR' AND t._signed_amount < 0;
-- Expected: 0

-- All CR-coded transactions must have negative _signed_amount
SELECT COUNT(*) FROM read_parquet('silver/transactions/**/*.parquet') t
JOIN read_parquet('silver/transaction_codes/data.parquet') tc
  ON t.transaction_code = tc.transaction_code
WHERE tc.debit_credit_indicator = 'CR' AND t._signed_amount > 0;
-- Expected: 0
```

---

### INV-09 — Invalid `transaction_code` Records Must Be Quarantined
**Category:** data correctness
**Classification:** TASK-SCOPED (Silver transactions model)

Any transaction record whose `transaction_code` is not present in `silver/transaction_codes/data.parquet` must be written to quarantine with `_rejection_reason = 'INVALID_TRANSACTION_CODE'`. Such records must not enter Silver transactions.

**Why this matters:** Allowing records with unrecognised `transaction_code` into Silver makes sign assignment undefined and corrupts the `_signed_amount` field, producing incorrect financial aggregates in Gold with no visible failure signal.

**Verification:**
```sql
-- No Silver transaction should have a transaction_code absent from Silver transaction_codes
SELECT COUNT(*) FROM read_parquet('silver/transactions/**/*.parquet') t
LEFT JOIN read_parquet('silver/transaction_codes/data.parquet') tc
  ON t.transaction_code = tc.transaction_code
WHERE tc.transaction_code IS NULL;
-- Expected: 0
```

---

### INV-10 — `UNRESOLVABLE_ACCOUNT_ID` Is Flagged in Silver, Not Quarantined
**Category:** data correctness
**Classification:** TASK-SCOPED (Silver transactions model)

A transaction record whose `account_id` is not found in `silver/accounts/data.parquet` at promotion time must be written to Silver transactions with `_is_resolvable = false`. It must not be written to quarantine. It must be excluded from Gold aggregations (except the unresolvable exposure columns in Gold daily summary).

**Why this matters:** An unresolvable account_id may be a timing issue — the account delta has not yet arrived — rather than a genuine data error. Quarantining would permanently discard potentially valid transactions. The flag-only approach preserves the record while excluding it from analytical outputs until the issue is resolved.

**Verification:**
```sql
-- Unresolvable records must appear in Silver, not quarantine
SELECT COUNT(*) FROM read_parquet('silver/transactions/**/*.parquet')
WHERE _is_resolvable = false;
-- Must be >= 0 (presence is valid)

-- No quarantine record should have rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID'
SELECT COUNT(*) FROM read_parquet('silver/quarantine/**/*.parquet')
WHERE _rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID';
-- Expected: 0
```

---

### INV-11 — Gold Aggregations Use Correct `_is_resolvable` Filter
**Category:** data correctness
**Classification:** TASK-SCOPED (Gold models)

All financial aggregations and counts in Gold must be computed exclusively from Silver records where `_is_resolvable = true`, except for `total_unresolvable_transactions` and `total_unresolvable_amount` in Gold daily summary, which must be computed exclusively from Silver records where `_is_resolvable = false`.

**Why this matters:** Mixing resolvable and unresolvable records in financial aggregations produces misleading results. The unresolvable exposure columns exist precisely to make the exclusion visible — computing them from the wrong population defeats their purpose.

**Verification:**
```sql
-- Gold daily total must match Silver resolvable-only sum
SELECT
  g.transaction_date,
  g.total_signed_amount AS gold_total,
  SUM(s._signed_amount) AS silver_total
FROM read_parquet('gold/daily_summary/data.parquet') g
JOIN read_parquet('silver/transactions/**/*.parquet') s
  ON g.transaction_date = s.transaction_date AND s._is_resolvable = true
GROUP BY g.transaction_date, g.total_signed_amount
HAVING gold_total != silver_total;
-- Expected: 0 rows
```

---

### INV-12 — Gold Is Computed Exclusively from Silver
**Category:** data correctness
**Classification:** TASK-SCOPED (Gold models)

Gold dbt models must read exclusively from Silver layer Parquet files. No Gold model may read from Bronze layer files, source CSV files, or any intermediate file outside the Silver layer.

**Why this matters:** Bypassing Silver skips all quality enforcement and sign assignment. Gold computed from Bronze would include records that failed Silver promotion — quarantined records, duplicates, invalid codes — and would not have `_signed_amount` applied.

**Verification:** `grep -r "bronze/" models/gold/` must return no results. All `FROM` clauses in Gold models must reference Silver sources only.

---

### INV-13 — Gold Daily Summary Has Exactly One Row Per Processed `transaction_date`
**Category:** data correctness
**Classification:** TASK-SCOPED (Gold daily summary model)

`gold/daily_summary/data.parquet` must contain exactly one row per `transaction_date` for every date processed by the pipeline, including dates where all transactions were quarantined (see INV-44).

**Why this matters:** Duplicate rows indicate aggregation errors. Missing rows make it impossible to distinguish "date not processed" from "date processed with no valid data."

**Verification:**
```sql
SELECT transaction_date, COUNT(*) AS cnt
FROM read_parquet('gold/daily_summary/data.parquet')
GROUP BY transaction_date
HAVING cnt > 1;
-- Expected: 0 rows
```

---

### INV-14 — Gold Weekly Aggregates Are Computed Exactly Once Per Week and Are Immutable Thereafter
**Category:** data correctness
**Classification:** TASK-SCOPED (Gold weekly model + pipeline.py orchestration)

Gold weekly account summary must contain exactly one row per `(account_id, week_start_date)` and these rows must be immutable after first computation. `pipeline.py` must read `pipeline/gold_weekly_control.parquet` before invoking the Gold weekly dbt model and must pass only uncomputed weeks as model variables. Already-computed weeks must never be passed to the model. The `closing_balance` for a week is fixed at the time of first computation and must not be updated by subsequent runs.

**Why this matters:** Without this control-plane gate, previously computed weeks could be recomputed and overwrite `closing_balance` values, violating idempotency and corrupting historical financial snapshots. The Gold weekly control table is the structural enforcement mechanism — not a convention.

**Verification:**
```sql
-- After two pipeline runs covering the same week, row count must be identical
SELECT week_start_date, account_id, COUNT(*) AS cnt
FROM read_parquet('gold/weekly_account_summary/data.parquet')
GROUP BY week_start_date, account_id
HAVING cnt > 1;
-- Expected: 0 rows

-- gold_weekly_control must have an entry for every week in Gold weekly summary
SELECT DISTINCT week_start_date
FROM read_parquet('gold/weekly_account_summary/data.parquet')
EXCEPT
SELECT week_start_date FROM read_parquet('pipeline/gold_weekly_control.parquet');
-- Expected: 0 rows
```

---

### INV-26 — Quarantine Records Must Include a Valid Non-Null `_rejection_reason`
**Category:** data correctness
**Classification:** TASK-SCOPED (Silver promotion models)

Every record in `silver/quarantine/` must have a non-null `_rejection_reason` drawn exclusively from the exhaustive code list defined in Section 5 of the brief: `NULL_REQUIRED_FIELD`, `INVALID_AMOUNT`, `DUPLICATE_TRANSACTION_ID`, `INVALID_TRANSACTION_CODE`, `INVALID_CHANNEL`, `INVALID_ACCOUNT_STATUS`.

**Why this matters:** Missing or non-standard rejection reasons eliminate visibility into data quality issues and prevent analysts from diagnosing the cause of quarantined records. The exhaustive code list is the contract — arbitrary strings are not valid.

**Verification:**
```sql
SELECT _rejection_reason, COUNT(*) FROM read_parquet('silver/quarantine/**/*.parquet')
WHERE _rejection_reason NOT IN (
  'NULL_REQUIRED_FIELD','INVALID_AMOUNT','DUPLICATE_TRANSACTION_ID',
  'INVALID_TRANSACTION_CODE','INVALID_CHANNEL','INVALID_ACCOUNT_STATUS'
) OR _rejection_reason IS NULL
GROUP BY _rejection_reason;
-- Expected: 0 rows
```

---

### INV-36 — Silver Accounts Upsert Must Replace the Existing Record
**Category:** data correctness
**Classification:** TASK-SCOPED (Silver accounts model)

Silver accounts promotion must upsert on `account_id` such that an incoming delta record for an existing `account_id` replaces the existing record as the latest version. Retaining the prior version alongside the delta (append) or discarding the delta and keeping the prior version are both violations. Re-running accounts promotion for a date already processed must produce identical output to the first run.

**Why this matters:** A faulty upsert that retains stale records while still producing one row per `account_id` passes the uniqueness check in INV-07 but silently preserves outdated account state — incorrect credit limits, stale balances — leading to incorrect downstream Gold aggregations. The violation is undetectable through row count checks alone.

**Verification:**
```sql
-- After processing a delta with a known balance change for account_id X,
-- Silver must reflect the new balance, not the prior one
SELECT current_balance FROM read_parquet('silver/accounts/data.parquet')
WHERE account_id = 'KNOWN_UPDATED_ACCOUNT_ID';
-- Must return the delta file value, not the prior value
```

---

### INV-37 — Silver Transaction Promotion Must Read Exclusively from Silver Transaction Codes
**Category:** data correctness
**Classification:** TASK-SCOPED (Silver transactions model)

Silver transaction promotion must join exclusively to `silver/transaction_codes/data.parquet` for `transaction_code` validation and `debit_credit_indicator` lookup. Joining to `bronze/transaction_codes/data.parquet` or any source CSV file is a violation of the Medallion layer boundary contract.

**Why this matters:** Using Bronze or source files instead of the Silver reference violates the architectural guarantee that Silver reads from Silver only. Although results appear identical in this static dataset, the coupling to Bronze creates a hidden dependency that breaks the layer separation contract and would silently fail in any scenario where Bronze and Silver transaction codes diverge.

**Verification:** `grep -r "bronze/transaction_codes" models/silver/` and `grep -r "source/transaction_codes" models/silver/` must both return no results.

---

### INV-41 — Gold Layer Outputs Must Be Written Atomically Per Run
**Category:** data correctness
**Classification:** TASK-SCOPED (Gold models)

Gold Parquet outputs must be written atomically per run. A failed or partial Gold write must not persist as a valid output. On re-run, Gold outputs for the affected date or week must be overwritten completely — not appended to. Gold dbt models must use `table` materialisation, not `incremental`.

**Why this matters:** A partial Gold write creates inconsistent aggregates that appear structurally valid but are incomplete, leading to silent analytical errors and reconciliation mismatches. `incremental` materialisation would append to existing Gold outputs on re-run rather than overwriting, permanently corrupting aggregates.

**Verification:** `grep -r "materialized='incremental'" models/gold/` must return no results. Gold models must declare `materialized='table'` in `dbt_project.yml` or model config blocks.

---

### INV-42 — Gold Overwrite Required When `gold_weekly_control` Entry Is Absent
**Category:** data correctness
**Classification:** TASK-SCOPED (Gold weekly model + pipeline.py orchestration)

If Gold data for a given week exists in `gold/weekly_account_summary/data.parquet` but the corresponding `week_start_date` entry is absent from `pipeline/gold_weekly_control.parquet`, the next pipeline run must overwrite the Gold data for that week completely and then write the control table entry before advancing the watermark. The partial-success state (Gold written, control table not updated) must always resolve to a full, correct re-computation.

**Why this matters:** A crash between the Gold write and the control table write leaves the system in a state where INV-14's read gate would re-compute the week (absent from control table) but Gold data already exists. Without an explicit overwrite requirement, the re-computation may append rather than replace, producing duplicate or inconsistent rows.

**Verification:**
```sql
-- Simulate: Gold weekly data exists for a week but gold_weekly_control has no entry.
-- After next pipeline run, exactly one row per (account_id, week_start_date) must exist
-- and gold_weekly_control must have the entry.
SELECT week_start_date, COUNT(*) FROM read_parquet('pipeline/gold_weekly_control.parquet')
GROUP BY week_start_date;
-- Every week in Gold weekly summary must appear here exactly once
```

---

### INV-44 — Gold Daily Summary Must Contain a Row for Every Processed Date
**Category:** data correctness
**Classification:** TASK-SCOPED (Gold daily summary model)

Gold daily summary must contain exactly one row per `transaction_date` processed by the pipeline, even when all transactions for that date were quarantined and zero resolvable Silver records exist for that date. Such rows must be present with zero counts and zero or null aggregate values.

**Why this matters:** Absence of a row for a fully-quarantined date makes it impossible for analysts to distinguish between "date was processed but contained no valid data" and "date has not yet been processed." Silent gaps in the daily summary undermine analytical trust and break completeness verification.

**Verification:**
```sql
-- For any date where all transactions were quarantined,
-- Gold daily summary must still have a row with total_transactions = 0
SELECT transaction_date, total_transactions
FROM read_parquet('gold/daily_summary/data.parquet')
WHERE transaction_date = '2024-01-01';
-- Must return one row with total_transactions = 0
```

---

### INV-38 — Gold Weekly Account Summary Row Completeness
**Category:** data correctness
**Classification:** TASK-SCOPED (Gold weekly model)

Gold weekly account summary must contain exactly one row per `(account_id, week_start_date)` for each account that has at least one Silver transaction with `_is_resolvable = true` in that calendar week. Accounts with no resolvable transactions in a week must be excluded entirely. Duplicate rows for the same `(account_id, week_start_date)` are a violation.

**Why this matters:** Incorrect inclusion of accounts with zero resolvable transactions inflates weekly counts. Incorrect exclusion of accounts with resolvable transactions silently drops valid data from weekly aggregates. Both violations appear structurally valid and cannot be detected by row count checks alone.

**Verification:**
```sql
-- Every account in Gold weekly must have at least one resolvable Silver transaction in that week
SELECT g.account_id, g.week_start_date
FROM read_parquet('gold/weekly_account_summary/data.parquet') g
LEFT JOIN (
  SELECT account_id, DATE_TRUNC('week', transaction_date) AS week_start
  FROM read_parquet('silver/transactions/**/*.parquet')
  WHERE _is_resolvable = true
) s ON g.account_id = s.account_id AND g.week_start_date = s.week_start
WHERE s.account_id IS NULL;
-- Expected: 0 rows
```

---

### INV-39 — Cross-File Gold Consistency
**Category:** data correctness
**Classification:** TASK-SCOPED (Gold models)

Every `account_id` appearing in `gold/weekly_account_summary/data.parquet` for a given `week_start_date` must have at least one corresponding record in `gold/daily_summary/data.parquet` for a `transaction_date` falling within that same calendar week.

**Why this matters:** Inconsistency between Gold outputs indicates partial writes or aggregation sequencing errors. An account appearing in the weekly summary with no traceable daily summary entries breaks analyst reconciliation — weekly aggregates cannot be verified against daily totals, undermining trust in both outputs.

**Verification:**
```sql
SELECT w.account_id, w.week_start_date
FROM read_parquet('gold/weekly_account_summary/data.parquet') w
WHERE NOT EXISTS (
  SELECT 1 FROM read_parquet('gold/daily_summary/data.parquet') d
  WHERE d.transaction_date >= w.week_start_date
    AND d.transaction_date <= w.week_end_date
    AND d.total_transactions > 0
);
-- Expected: 0 rows
```

---

### 2.3 Operational and Control-Plane Invariants

---

### INV-15 — Watermark Advances Only After Full Pipeline Success
**Category:** operational
**Classification:** TASK-SCOPED (pipeline.py orchestration)

The `last_processed_date` watermark in `pipeline/control.parquet` must advance only after Bronze, Silver, and Gold all complete successfully for a given date. A failure at any layer — Bronze ingestion, Silver promotion, Gold aggregation, or `gold_weekly_control` write — must prevent watermark advancement. The watermark must remain at its prior value and the failed date must be reprocessable on the next run.

**Why this matters:** Advancing the watermark past a date that did not fully complete causes the incremental pipeline to skip that date permanently, producing a silent gap in all downstream Silver and Gold outputs with no recovery path (backfill is out of scope).

**Verification:**
```sql
-- After a simulated Silver failure, watermark must not have advanced
SELECT last_processed_date FROM read_parquet('pipeline/control.parquet');
-- Must equal the last successfully completed date, not the failed date
```

---

### INV-17 — Incremental Pipeline Processes Exactly One Date
**Category:** operational
**Classification:** TASK-SCOPED (incremental pipeline)

The incremental pipeline must process exactly one date per invocation: `last_processed_date + 1 day` as read from `pipeline/control.parquet`. It must not process any other date — not watermark + 2, not a prior date, not a date derived from file system inspection.

**Why this matters:** Processing the wrong date breaks watermark ordering and idempotency guarantees. Processing multiple dates in a single incremental run violates the per-date atomicity model and makes partial-failure recovery ambiguous.

**Verification:** After an incremental run, `last_processed_date` in `pipeline/control.parquet` must equal exactly the prior watermark + 1 day — no more, no less.

---

### INV-18 — Missing Source File in Incremental Mode Is a No-Op
**Category:** operational
**Classification:** TASK-SCOPED (incremental pipeline)

If the source file for the target date (`transactions_YYYY-MM-DD.csv` or `accounts_YYYY-MM-DD.csv`) does not exist in `source/`, the incremental pipeline must treat this as a no-op — no Bronze writes, no Silver promotion, no Gold computation, no watermark advancement, and no run log rows with status = FAILED. The pipeline must exit cleanly.

**Why this matters:** Treating file absence as a failure would block the pipeline on weekends, holidays, or delayed file delivery. The brief defines this as a defined valid state, not an error condition.

**Verification:** After an incremental run with no source file for the target date, `pipeline/control.parquet` watermark must be unchanged and no new Bronze partitions must exist.

---

### INV-19 — Run Log Is Append-Only
**Category:** operational
**Classification:** TASK-SCOPED (run log writer)

`pipeline/run_log.parquet` must be append-only. No existing row in the run log may ever be modified, updated, or deleted. Every pipeline run adds new rows; it does not alter prior rows.

**Why this matters:** Modifying past run log entries destroys audit integrity. The run log is the historical record of all pipeline executions — it must be a complete, unaltered chronicle to serve its audit function.

**Verification:** Row count in `pipeline/run_log.parquet` must be monotonically non-decreasing across pipeline runs. `SELECT COUNT(*) FROM run_log` after run N+1 must be >= `SELECT COUNT(*) FROM run_log` after run N.

---

### INV-20A — Successful Runs Must Produce One `SUCCESS` Row Per Executed Model
**Category:** operational
**Classification:** TASK-SCOPED (pipeline.py orchestration)

Every successful pipeline run must produce exactly one run log row per executed model (Bronze loaders and dbt models) with `status = SUCCESS`, `started_at`, `completed_at`, `records_processed`, `records_written`, and for Silver models, `records_rejected`. These rows must be written via the async buffer flush after watermark advancement.

**Why this matters:** Model-level run log entries are required to trace execution, validate row counts, and provide the audit trail linking Bronze, Silver, and Gold outputs to a specific pipeline run via `_pipeline_run_id`.

**Verification:**
```sql
SELECT model_name, COUNT(*) AS cnt
FROM read_parquet('pipeline/run_log.parquet')
WHERE run_id = '[CURRENT_RUN_ID]' AND status = 'SUCCESS'
GROUP BY model_name
HAVING cnt > 1;
-- Expected: 0 rows (exactly one SUCCESS row per model per run)
```

---

### INV-20B — Failed Runs Must Produce at Least One Traceable Run Log Row
**Category:** operational
**Classification:** TASK-SCOPED (pipeline.py orchestration)

Every failed pipeline run must produce at least one run log row. If failure occurs before any model execution (e.g., dbt compile failure), a row with `model_name = 'DBT_COMPILE'`, `layer = 'ORCHESTRATION'`, and `status = 'FAILED'` must be written. If failure occurs during model execution, a `FAILED` row must be written for the specific model that failed.

**Why this matters:** Without a run log entry for failed runs, pipeline failures become invisible in the audit trail, preventing diagnosis and breaking the traceability guarantees of Section 10.5. Silent failures are the exact problem this system is designed to prevent.

**Verification:**
```sql
-- After a simulated compile failure, run log must have the ORCHESTRATION row
SELECT * FROM read_parquet('pipeline/run_log.parquet')
WHERE model_name = 'DBT_COMPILE' AND layer = 'ORCHESTRATION' AND status = 'FAILED';
-- Must return at least one row
```

---

### INV-20C — Run Log Flush Failure Recovery via `UNLOGGED_RUN` Synthetic Row
**Category:** operational
**Classification:** TASK-SCOPED (pipeline.py orchestration)

If the run log buffer flush to `pipeline/run_log.parquet` fails after a successful pipeline run, `pipeline.py` must write the failed flush details to a local `.jsonl` fallback file. On the subsequent successful pipeline run, `pipeline.py` must detect the prior unlogged run (via `updated_by_run_id` in `pipeline/control.parquet` matching no run log entries) and write a synthetic row with `model_name = 'UNLOGGED_RUN'` and an explanatory `error_message` before proceeding with the new run.

**Why this matters:** The async buffering design decouples run log writes from pipeline success. Without the recovery mechanism, successful pipeline runs could leave no audit trace — creating gaps in lineage that are undetectable until an analyst attempts to trace a Gold record.

**Verification:**
```sql
-- After simulated flush failure followed by a successful run,
-- run log must contain the UNLOGGED_RUN synthetic row
SELECT * FROM read_parquet('pipeline/run_log.parquet')
WHERE model_name = 'UNLOGGED_RUN';
-- Must return one row referencing the prior run_id
```

---

### INV-24 — Intra-Pipeline Processing Order Must Be Enforced
**Category:** data correctness
**Classification:** TASK-SCOPED (pipeline.py orchestration)

Within each date, `pipeline.py` must enforce the following processing order as the primary control mechanism:
1. Accounts Bronze load
2. Accounts Silver promotion (dbt)
3. Transactions Bronze load
4. Transactions Silver promotion (dbt)

Additionally, Silver transaction_codes must be promoted before any Silver transaction promotion for any date. dbt model `ref()` dependencies provide secondary enforcement within dbt invocations but cannot enforce ordering relative to Bronze ingestion — `pipeline.py` is the authoritative ordering guarantee.

**Why this matters:** Processing transactions before accounts for the same date causes legitimate transactions to be flagged as `UNRESOLVABLE_ACCOUNT_ID` — a false positive that permanently excludes valid transactions from Gold (backfill is out of scope). Processing transactions before transaction_codes prevents `transaction_code` validation and sign assignment entirely.

**Verification:** `pipeline.py` execution log must show accounts Bronze and Silver completing before transactions Bronze for the same date on every run.

---

### INV-31 — `gold_weekly_control` Must Be Written Before Watermark Advances
**Category:** operational
**Classification:** TASK-SCOPED (pipeline.py orchestration)

After a successful Gold weekly aggregation run, `pipeline.py` must write the newly computed week entries to `pipeline/gold_weekly_control.parquet` before advancing the watermark in `pipeline/control.parquet`. If the `gold_weekly_control` write fails, the watermark must not advance.

**Why this matters:** If the control table is not updated before watermark advancement, the system loses track of computed weeks on the next run. INV-14's read gate will treat the week as uncomputed and recompute it, overwriting `closing_balance` values and violating idempotency.

**Verification:**
```sql
-- After every successful run that included a new week,
-- gold_weekly_control must have an entry for that week
SELECT week_start_date FROM read_parquet('pipeline/gold_weekly_control.parquet')
ORDER BY week_start_date DESC LIMIT 1;
-- Must equal the most recent week computed in Gold weekly summary
```

---

### INV-32 — Absent Control-Plane Files Are a Valid Initial State
**Category:** operational
**Classification:** TASK-SCOPED (pipeline.py orchestration)

The absence of `pipeline/control.parquet` and `pipeline/gold_weekly_control.parquet` must be treated as a valid initial state by `pipeline.py`. On first run (historical pipeline), `pipeline.py` must initialise both files with correct empty/baseline state before any processing begins. Raising an unhandled exception on absent control-plane files is a violation.

**Why this matters:** Without explicit initialisation handling, the pipeline cannot determine the starting watermark or computed weeks on first run, producing undefined behaviour — an unhandled file-not-found exception that blocks the historical load entirely.

**Verification:** Running the historical pipeline against a clean directory (no `pipeline/` files) must succeed and produce both control-plane files. No error must be raised for absent control-plane files at startup.

---

### INV-33 — All Gold Parquet Files Must Be Fully Written Before Watermark Advances
**Category:** data correctness
**Classification:** TASK-SCOPED (pipeline.py orchestration)

All Gold layer Parquet outputs (`gold/daily_summary/data.parquet`, `gold/weekly_account_summary/data.parquet`) must be fully written and their file handles closed before `pipeline/control.parquet` watermark is advanced. No partial Gold write may be present when the pipeline reports success.

**Why this matters:** Advancing the watermark before Gold files are fully written creates a state where downstream consumers — analysts querying Gold via DuckDB CLI — see an incomplete dataset while the system believes the date is fully processed. The Gold layer is the analyst-facing exit surface; a torn write at this boundary causes silent analytical errors.

**Verification:** File sizes of Gold Parquet files must not change between the watermark advancement timestamp and the next pipeline run. A DuckDB query against Gold files immediately after pipeline completion must not raise a file-format or truncation error.

---

### INV-34 — Run Log Row Counts Must Be Derived from Parquet Queries
**Category:** data correctness
**Classification:** TASK-SCOPED (pipeline.py orchestration)

`records_written` and `records_rejected` fields in the run log must be derived by querying the corresponding output Parquet files immediately after model completion (on `NodeFinished` event). These values must not be sourced from dbt execution metadata such as `run_results.json` or dbt internal row counters.

**Why this matters:** dbt's internal row counts reflect rows affected by the final SQL statement only and do not represent total records written or rejected as distinct values. Using dbt counters would produce incorrect run log metrics silently, making the run log misleading as an audit tool and breaking downstream traceability.

**Verification:**
```sql
-- For a known date with N Bronze transactions, Silver should show records_written = M and records_rejected = N-M
SELECT records_written, records_rejected
FROM read_parquet('pipeline/run_log.parquet')
WHERE model_name = 'silver_transactions' AND run_id = '[CURRENT_RUN_ID]';
-- records_written + records_rejected must equal Bronze row count for same date
```

---

### INV-35 — Non-Executed Models Must Produce `SKIPPED` Run Log Rows
**Category:** operational
**Classification:** TASK-SCOPED (pipeline.py orchestration)

Models that are not executed due to an earlier failure in the same pipeline run must produce a run log row with `status = 'SKIPPED'` for that `run_id` and `model_name`. These rows are written by `pipeline.py` at subprocess exit — not by dbt. Absence of a `SKIPPED` row for any non-executed model is a run log integrity violation.

**Why this matters:** Without explicit `SKIPPED` entries, the run log provides an incomplete picture of pipeline execution. An analyst tracing a partial failure cannot distinguish between "model did not run" and "model was never part of this pipeline" without `SKIPPED` entries. The absence is an audit gap, not a neutral state.

**Verification:**
```sql
-- After a simulated Silver failure, downstream Gold models must have SKIPPED rows
SELECT model_name, status FROM read_parquet('pipeline/run_log.parquet')
WHERE run_id = '[FAILED_RUN_ID]' AND status = 'SKIPPED';
-- Must return rows for all Gold models that did not execute
```

---

### INV-43 — Corrupt or Unreadable Control-Plane Files Must Halt the Pipeline
**Category:** operational
**Classification:** TASK-SCOPED (pipeline.py orchestration)

If `pipeline/control.parquet` or `pipeline/gold_weekly_control.parquet` exists but is unreadable, structurally invalid, or corrupted, `pipeline.py` must halt execution immediately and record a `FAILED` run log entry with an explanatory `error_message`. It must not default to an assumed safe state (e.g., treating all weeks as uncomputed or resetting the watermark to zero) or reinitialise the files silently.

**Why this matters:** Silent fallback or silent reinitialisation risks large-scale data inconsistencies — recomputing all Gold weekly aggregates and overwriting `closing_balance` values, or reprocessing already-completed dates. These are unrecoverable without manual intervention and are worse outcomes than a visible pipeline failure.

**Verification:** Injecting a corrupt `pipeline/control.parquet` must produce a `FAILED` run log entry and no data writes. The watermark file must not be silently reinitialised.

---

## Section 3 — Schema Additions and Extensions

These items extend the schemas defined in the Requirements Brief. They are recorded here as the authoritative source before EXECUTION_PLAN.md task prompts are written.

### 3.1 Silver Transactions — Additional Columns

| Column | Type | Nullable | Source |
|---|---|---|---|
| `_missing_merchant_name` | BOOLEAN | NOT NULL | GAP 4 — TRUE when `transaction_type = 'PURCHASE'` AND `merchant_name IS NULL`; FALSE otherwise. Non-blocking flag only. |

### 3.2 Gold Daily Summary — Additional Columns

| Column | Type | Source |
|---|---|---|
| `total_unresolvable_transactions` | INTEGER | GAP 2 — Count of Silver records where `_is_resolvable = false` for this date |
| `total_unresolvable_amount` | DECIMAL | GAP 2 — Sum of `_signed_amount` for `_is_resolvable = false` Silver records for this date |

### 3.3 `gold_weekly_control` Schema

New control-plane artifact at `pipeline/gold_weekly_control.parquet`. Week-grain — one row per computed week.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `week_start_date` | DATE | NOT NULL | Monday of the computed calendar week (ISO week) |
| `week_end_date` | DATE | NOT NULL | Sunday of the computed calendar week |
| `computed_at` | TIMESTAMP | NOT NULL | When this week's Gold aggregates were first computed |
| `computed_by_run_id` | STRING | NOT NULL | The `run_id` of the pipeline run that computed this week |

**Primary key:** `(week_start_date)`
**Note:** `account_id` is not included — this table tracks week-level computation status, not per-account status. Per-account data lives in `gold/weekly_account_summary/data.parquet`.

### 3.4 Run Log Schema Extension — ORCHESTRATION Sentinel Values

The run log includes a non-standard sentinel value for pipeline-level orchestration failures that occur before any dbt model execution:

| Field | Value | Meaning |
|---|---|---|
| `model_name` | `DBT_COMPILE` | Reserved sentinel — pipeline failed during dbt compile phase |
| `layer` | `ORCHESTRATION` | Reserved sentinel — not a dbt layer |
| `status` | `FAILED` | Compile failure — no models executed |

`DBT_COMPILE` is not a dbt model and must not be interpreted as one. Once dbt model execution begins, all run log entries must use actual model names. A FAILED row at model execution time must use the model's actual `model_name`. Downstream non-executed models must use `status = 'SKIPPED'` with their actual `model_name`.

---

## Section 4 — Implementation Guidance

These items were considered as invariants and rejected because they fail the harm and detectability test (violations are immediately visible or non-falsifiable), are fully covered by existing invariants, or are procedural controls that cannot be structurally enforced. They are recorded here for embedding in EXECUTION_PLAN.md task prompts and Claude.md as design principles.

---

**IG-01 (from INV-16) — Watermark Value Is Exact and Authoritative**
*Embed in: pipeline control table task prompt*
The watermark value in `pipeline/control.parquet` is the exact definition of last fully processed date — it must not be read as advisory or approximate. Any code that reads the watermark must treat it as authoritative. This is fully covered by INV-15.

---

**IG-02 (from INV-21) — Run Log Buffer Must Deduplicate Before Flush**
*Embed in: run log writer task prompt*
The run log buffer must deduplicate on `(run_id, model_name)` before flushing to `pipeline/run_log.parquet`. A duplicate entry does not corrupt data but violates audit log integrity and must be treated as a build error.

---

**IG-03 (from INV-23) — DAG-Derived Execution Order**
*Embed in: pipeline.py orchestration task prompt*
`pipeline.py` must derive model execution order from `target/manifest.json` via topological sort of the dbt DAG, not from a hardcoded model list. The dbt DAG is the single source of truth for intra-layer ordering. `pipeline.py` enforces inter-layer ordering (Silver completes before Gold starts). A hardcoded model list is a maintenance liability that will silently diverge from the DAG.

---

**IG-04 (from INV-28) — Control-Plane Files Are the Sole State Source**
*Embed in: pipeline.py orchestration task prompt*
Pipeline state decisions (next date to process, which weeks are computed, run traceability) must be read exclusively from `pipeline/control.parquet` and `pipeline/gold_weekly_control.parquet`. No in-memory state, no file-system inference, and no fallback logic that derives state by inspecting Bronze, Silver, or Gold layer data.

---

**IG-05 (from INV-29) — Idempotency Is a System-Wide Design Principle**
*Embed in: Claude.md*
Idempotency is a system-wide requirement. Every task must verify that re-running against already-processed input produces no change to any layer's row count or content. The specific idempotency conditions are enforced by individual invariants (INV-02, INV-06, INV-14, INV-41) — this principle reminds engineers that no task is exempt.

---

**IG-06 (from INV-30) — Post-Run Bronze/Silver/Quarantine Accounting Check Is Mandatory**
*Embed in: verification task prompt*
Post-run verification must assert that total Bronze transaction row count equals the sum of Silver transaction rows and quarantine rows across all partitions. This check is mandatory for every pipeline run and must pass for the run to be considered successful. It is the operationalisation of INV-05.

---

**IG-07 (from ARCH-GAP-2) — Direct dbt Invocation of Gold Weekly Model Is Prohibited**
*Embed in: `gold_weekly_account_summary.sql` model file as a prominent warning comment; also in Gold weekly task prompt*
This model must only be executed through `pipeline.py`. Direct invocation via `dbt run --select gold_weekly_account_summary` bypasses `pipeline.py` and the `pipeline/gold_weekly_control.parquet` enforcement gate, causing all weeks to be recomputed and overwriting `closing_balance` values. There is no structural enforcement for this — it is a named procedural control.

---

**IG-08 (from ARCH-RISK-1) — dbt Version Pin and JSON Log Schema Verification**
*Embed in: Docker/requirements task prompt and Claude.md maintenance notes*
The dbt-core dependency must be pinned to version 1.7.x (with dbt-duckdb 1.7.x) in `requirements.txt`. The `pipeline.py` JSON log parser depends on the `NodeStart` and `NodeFinished` event field paths, which are not a guaranteed stable public API across dbt versions. Any upgrade beyond dbt-core 1.7.x requires a mandatory verification step: execute `dbt run --log-format json` against a test model and confirm that `NodeStart` and `NodeFinished` field paths match those expected by the parser. This step is not optional on any dbt upgrade.

---

**IG-09 — Gold Models Must Use `table` Materialisation**
*Embed in: Gold model task prompts and dbt_project.yml task prompt*
Gold dbt models (`gold_daily_summary`, `gold_weekly_account_summary`) must use `materialized='table'` in `dbt_project.yml` or model config blocks. `incremental` materialisation is prohibited for Gold models — it would append to existing Gold outputs on re-run rather than overwriting, violating INV-41.

---

**IG-10 — Out-of-Sequence Source Files Are Silently Ignored**
*Embed in: incremental pipeline task prompt*
The incremental pipeline processes exactly the file for watermark + 1 day (INV-17). Any source file present in `source/` for a date other than the target date — whether a future date or a prior date — must be silently ignored. No error is raised; no processing occurs for that file. The pipeline is not responsible for file arrival sequencing.

---

## Section 5 — Reclassified and Removed Invariants

| Original ID | Disposition | Reason | Where Recorded |
|---|---|---|---|
| INV-16 | Reclassified → IG-01 | Duplicates INV-15; not independently testable | Section 4 |
| INV-21 | Reclassified → IG-02 | Detectable via normal inspection; not silent harm | Section 4 |
| INV-23 | Reclassified → IG-03 | Loud failure mode on violation; not silent harm | Section 4 |
| INV-25 | Merged into INV-24 | Same category and enforcement scope as INV-24 | INV-24 |
| INV-27 | Merged into INV-14 | Two sides of the same design decision; unified | INV-14 |
| INV-28 | Reclassified → IG-04 | Not falsifiable; design principle not a constraint | Section 4 |
| INV-29 | Reclassified → IG-05 | Summary goal; specific conditions covered individually | Section 4 |
| INV-30 | Reclassified → IG-06 | Fully covered by INV-05; reframed as mandatory check | Section 4 |

---

## Section 6 — Engineer Sign-Off

I confirm that:
- The invariant set has been challenged against all five tests (goal vs. constraint, enforcement scope, bundling, coverage, harm and detectability)
- All 13 data touch points are covered by at least one invariant
- All 17 breakpoints are covered by at least one invariant or a combination of invariants
- All gaps identified in ARCHITECTURE.md have been closed
- All reclassified items are recorded in Section 4 with their embedding targets
- The schema additions in Section 3 are complete and consistent with REQUIREMENTS_GAPS_COVER.md

**Signed:** Pratham
**Date:** 15/04/2026

*Ready for Phase 3 — Execution Planning*
