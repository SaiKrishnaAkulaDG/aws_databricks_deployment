# MODULE_CONTRACTS.md — Component Boundaries and Interfaces

**PBVI Phase:** 8 — Discovery
**Document Type:** Module contract definitions
**Created:** Phase 8 completion
**Status:** FINAL

---

## 1. Contract Overview

A module contract is a formally defined boundary between two system components. Each contract specifies:
- **Provider:** The component that fulfills the contract
- **Consumer:** The component that depends on the contract
- **Guarantee:** What the provider promises
- **Obligation:** What the consumer must do to use the guarantee
- **Violation Signal:** How a violation becomes observable

---

## 2. Bronze-Silver Contract

### Contract: "Bronze Provides Complete, Immutable History"

**Provider:** Bronze Layer (Python loaders + partitioned Parquet)

**Consumers:** Silver promotion models (dbt)

**Guarantee:**
1. Every source CSV record is written to Bronze exactly once per pipeline run
2. Bronze partition for a given date, once written, is never modified
3. All records include audit columns: `_source_file`, `_ingested_at`, `_pipeline_run_id`
4. `_pipeline_run_id` is non-null and traceable to `pipeline/run_log.parquet`

**Obligation (Silver's responsibility):**
1. Read only from Bronze, not from source files directly
2. Join to `silver/transaction_codes/data.parquet` for sign assignment, not to Bronze transaction codes
3. Preserve `_pipeline_run_id` from Bronze — do not generate new run IDs or lose this linkage
4. Never write back to Bronze

**Enforcement Invariants:**
- INV-01: Source files are read-only
- INV-02: Bronze idempotency via partition existence check
- INV-03: Bronze partitions immutable after first write
- INV-04: Bronze audit columns non-null

**Violation Signals:**
- Silver records with null `_pipeline_run_id` → INV-22 failure
- Silver record with transaction_code not in Silver transaction codes → INV-09 failure
- `git status source/` shows modifications → INV-01 failure
- Bronze partition mtime changes on re-run → INV-03 failure

**Implementation:**
- Bronze loader implements partition existence check before reading CSV
- Bronze loader uses atomic write (temp file + rename)
- dbt models ref Bronze sources via `source()` references only
- Silver promotion models filter on `_pipeline_run_id` to correlate audit trail

---

## 3. Silver-Gold Contract

### Contract: "Silver Provides Quality-Checked, Signed, Delineated Records"

**Provider:** Silver Layer (dbt promotion models + partitioned Parquet + quarantine)

**Consumers:** Gold aggregation models (dbt)

**Guarantee:**
1. Every Silver transaction record has `_signed_amount` correctly assigned from transaction codes
2. Duplicate transaction IDs are excluded (one global `transaction_id` per system)
3. Invalid transaction codes are quarantined, not passed to Silver
4. Account resolution status is explicitly marked: `_is_resolvable = true/false`
5. Quarantine contains only records with valid rejection reasons from exhaustive list
6. Total accounting: Bronze = Silver + Quarantine per date
7. All records include `_pipeline_run_id` traceable to prior Bronze records

**Obligation (Gold's responsibility):**
1. Read only from Silver, not from Bronze or source files
2. Filter aggregations on `_is_resolvable = true` (mandatory, except unresolvable totals)
3. Preserve `_pipeline_run_id` — do not modify or lose this linkage
4. Treat `closing_balance` from weekly aggregates as immutable (managed by control plane)
5. Never write back to Silver

**Enforcement Invariants:**
- INV-05: Total partition audit (Bronze = Silver + Quarantine)
- INV-06: Global transaction deduplication
- INV-08: Sign assignment from transaction codes only
- INV-09: Invalid transaction codes quarantined
- INV-10: Unresolvable account_id flagged, not quarantined
- INV-26: Quarantine rejection reasons exhaustive
- INV-37: Silver reads only Silver transaction codes

**Violation Signals:**
- Gold record for resolvable transaction with null `_signed_amount` → INV-08 failure
- Gold includes duplicate transaction_id → INV-06 failure
- Quarantine contains rejection reason outside exhaustive list → INV-26 failure
- Quarantine contains `UNRESOLVABLE_ACCOUNT_ID` → INV-10 failure
- Gold daily total ≠ Silver resolvable sum for date → INV-11 failure

**Implementation:**
- Silver promotion models preserve all Bronze audit columns including `_pipeline_run_id`
- Sign assignment happens via dbt computed column + left join to Silver transaction codes
- Deduplication uses window functions checking prior partitions + explicit dbt test
- Quarantine writes happen in-model via conditional INSERT into separate partition
- Gold models use dbt computed columns to filter on `_is_resolvable = true`

---

## 4. Control-Plane Contract (Watermark)

### Contract: "Watermark Is the Single Source of Truth for Pipeline State"

**Provider:** pipeline.py (orchestrator)

**Consumers:** Incremental pipeline logic, Bronze idempotency gates

**Guarantee:**
1. `last_processed_date` in `pipeline/control.parquet` is the exact definition of "last successfully processed date"
2. Watermark advances only after all three layers (Bronze, Silver, Gold) complete successfully for a date
3. Watermark never advances for a date where Silver or Gold failed
4. Incremental pipeline processes exactly watermark + 1 day, never any other date
5. `updated_by_run_id` in control table traces to a SUCCESS entry in run_log for that run_id

**Obligation (Consumer's responsibility):**
1. Read watermark as authoritative — do not infer pipeline state from filesystem inspection
2. Check partition existence against watermark + 1 day only; do not look ahead or behind
3. Treat watermark as immutable once read — do not write to control table except via pipeline.py
4. Handle absent control table on first run (initialise with watermark = NULL or pre-run state)

**Enforcement Invariants:**
- INV-15: Watermark advances only on full success
- INV-17: Incremental pipeline processes exactly one date
- INV-18: Missing source file is a no-op (watermark unchanged)
- INV-32: Absent control files are valid initial state (must initialise)

**Violation Signals:**
- Watermark value does not match any run_id in run_log → INV-15 failure
- Incremental pipeline processes two dates in one invocation → INV-17 failure
- Partition exists but watermark has not advanced to that date → synchronisation failure
- Control table corrupted or unreadable → INV-43 failure (halt pipeline)

**Implementation:**
- `pipeline.py` reads control table once at startup
- `pipeline.py` writes control table only after all three layers complete and run_log flushed
- Bronze loader reads watermark once, uses it for partition existence check
- Incremental mode: compute target_date = watermark + 1 day and process only that date
- On startup: check for control table; if absent, initialise with appropriate defaults

---

## 5. Control-Plane Contract (Gold Weekly Control)

### Contract: "Gold Weekly Control Prevents Double-Computation of Weeks"

**Provider:** pipeline.py (orchestrator) + `pipeline/gold_weekly_control.parquet`

**Consumers:** Gold weekly account summary dbt model (indirectly via pipeline.py gating)

**Guarantee:**
1. Before Gold weekly dbt model executes, `pipeline.py` reads `gold_weekly_control.parquet`
2. Only weeks absent from control table are passed to dbt model as variables
3. After dbt completes, newly computed weeks are written to control table with `computed_by_run_id`
4. `closing_balance` from prior weeks is never recomputed (immutable)
5. Every week in Gold output has matching entry in control table (INV-14)

**Obligation (dbt model's responsibility):**
1. Compute only the weeks provided by `pipeline.py` via dbt variables
2. Never read `gold_weekly_control.parquet` directly (it is managed outside dbt)
3. Never invoke the model directly via `dbt run --select gold_weekly_account_summary` (bypasses control gate)
4. Treat `closing_balance` as immutable per week

**Enforcement Invariants:**
- INV-14: One row per (account_id, week_start_date) after computation
- INV-31: Control table written before watermark advances
- INV-42: Gold overwrite required if control entry absent for existing week

**Violation Signals:**
- Gold has row for week not in control table → INV-14 failure
- Control table has entry but Gold has no matching week → mismatch
- Same week computed twice with different closing_balance values → INV-14 failure (idempotency lost)
- Direct `dbt run --select gold_weekly_account_summary` invocation → control gate bypassed (procedural risk)

**Implementation:**
- `pipeline.py` reads control table before dbt invocation (step 1)
- `pipeline.py` computes `uncomputed_weeks` = weeks in date range minus weeks in control table
- `pipeline.py` calls `dbt run --select gold_weekly_account_summary --vars '{"weeks": [...]}'`
- After dbt completes: `pipeline.py` queries Gold output to identify newly written weeks
- `pipeline.py` appends rows to control table with current timestamp and run_id
- Watermark advances only after step 5 completes

---

## 6. Run Log Contract

### Contract: "Run Log Is a Permanent Audit Trail"

**Provider:** pipeline.py (buffer manager) + `pipeline/run_log.parquet` + fallback `.jsonl`

**Consumers:** Analysts, audit systems, verification scripts

**Guarantee:**
1. One row per component execution per pipeline invocation
2. Each row includes start/end timestamps, status, record counts, and error messages
3. Run log is append-only — no modification or deletion of prior rows
4. Every `_pipeline_run_id` in Silver/Gold is traceable to a SUCCESS run log entry for that run_id
5. Every failed or skipped component has a corresponding run log row
6. If flush fails: synthetic `UNLOGGED_RUN` row written on next successful run

**Obligation (Analyst/Consumer's responsibility):**
1. Treat run_log entries as complete and accurate; do not infer state from absence of rows
2. Understand sentinel values: `DBT_COMPILE` (layer=ORCHESTRATION) indicates compile failure before any models run
3. Be aware of `UNLOGGED_RUN` synthetic rows — they indicate prior flush failure recovery, not a named component
4. For tracing: query run_log with `(run_id, model_name, status)` to find execution timeline

**Enforcement Invariants:**
- INV-19: Run log append-only (no modification of existing rows)
- INV-20A: One SUCCESS row per executed model per run
- INV-20B: Failed runs have at least one FAILED row
- INV-20C: Flush failures detected and recovered via `UNLOGGED_RUN` synthetic row
- INV-22: Every Silver/Gold `_pipeline_run_id` traceable to run_log SUCCESS row

**Violation Signals:**
- Silver record with `_pipeline_run_id = X` but no run_log entry with run_id = X → INV-22 failure
- Run log row modified or deleted → INV-19 failure
- No run log entry for day that succeeded (only visible when run_log.parquet.mtime stops advancing) → run_log flush silently failed
- Analyst can query run_log and find all failures and skips → contract satisfied

**Implementation:**
- `pipeline.py` accumulates run log entries in in-memory buffer during execution
- `NodeStart` and `NodeFinished` events from dbt stdout are parsed in real time, buffer entries added
- On subprocess exit: add `SKIPPED` entries for models not executed
- After watermark advances: flush buffer to `pipeline/run_log.parquet` as single append
- On flush failure: write buffer to `.jsonl` fallback with timestamp; continue without exception
- On next run: check watermark `updated_by_run_id` against run_log; if missing, write `UNLOGGED_RUN` row before proceeding

---

## 7. dbt-Pipeline Contract

### Contract: "dbt Models Are Stateless Transformations"

**Provider:** dbt models (Silver and Gold)

**Consumers:** pipeline.py orchestrator

**Guarantee:**
1. Each dbt model is deterministic: same input data + same parameters = same output
2. Models have no side effects outside the Parquet output (no config writes, no external calls)
3. Model execution order within a layer is determined by `ref()` dependencies
4. Models do not read their own prior output to make execution decisions (no dbt incremental patterns)
5. Model materialisation is `table` (drop and recreate) — never `incremental` for Gold

**Obligation (pipeline.py's responsibility):**
1. Invoke dbt with identical parameters across re-runs for the same date
2. Enforce inter-layer sequencing: all Silver models complete before any Gold model starts
3. Manage control gates outside dbt: Gold weekly control passed as dbt variables, not read by dbt
4. Handle model failure — do not retry from within dbt post-hooks; retry at orchestrator level
5. Derive execution order from dbt DAG at runtime, not hardcode model lists

**Enforcement Invariants:**
- INV-24: Intra-pipeline processing order enforced by `pipeline.py`
- INV-41: Gold models use `table` materialisation (no incremental)
- IG-03: DAG-derived execution order via `dbt compile` manifest
- IG-09: Gold models must use `materialized='table'`

**Violation Signals:**
- Gold model uses `materialized='incremental'` → INV-41 failure (append instead of replace on re-run)
- dbt model reads control table directly → statefulness violation
- Model output differs on identical input/params re-run → determinism violation
- Pipeline succeeds but dbt log shows models in wrong order → INV-24 failure

**Implementation:**
- dbt models use only `ref()` and `source()` references for data input
- dbt `profiles.yml` and `dbt_project.yml` define materialisation = `table` for Gold models
- No dbt post-hooks modify data or state
- `pipeline.py` calls `dbt compile` to load manifest and derive DAG order
- `pipeline.py` calls `dbt run --select model_name` per model in derived sequence
- dbt `--log-format json` streams events to `pipeline.py` for real-time buffer population

---

## 8. Source CSV Contract

### Contract: "Source Files Are Read-Only Inputs"

**Provider:** External system (finance/transactions platform)

**Consumers:** Bronze loaders (Python)

**Guarantee:**
1. Source CSV files are provided in `source/` directory
2. File format is stable: headers, data types, column order are consistent per entity
3. Files are not modified or deleted while pipeline is running
4. Transaction records are append-only in source system (never updated or deleted)
5. Account records are deltas (Day 1 is full snapshot, Days 2–7 are changes only)

**Obligation (Bronze Loader's responsibility):**
1. Never write to, modify, delete, or rename source CSV files
2. Validate file structure (headers, required columns) at read time
3. Read source files as immutable — treat file content as a fixed snapshot
4. Do not assume file deletion means "no new data for this date" — handle absent files as no-op

**Enforcement Invariants:**
- INV-01: Source files are read-only
- INV-18: Missing source file is a no-op (incremental pipeline)
- IG-10: Out-of-sequence source files are silently ignored

**Violation Signals:**
- `git status source/` shows file modifications → INV-01 failure
- Bronze loader attempts to delete or rename source file → INV-01 violation
- Same source file read produces different results → data consistency issue (external system problem)

**Implementation:**
- Bronze loader opens source files in read-only mode
- Bronze loader validates file format at startup
- Bronze loader logs source file properties (path, row count, mtime) in Bronze audit column `_source_file`
- No write operations target `source/` directory

---

## 9. Audit Trail Contract (Bronze → Silver → Gold)

### Contract: "Every Gold Record Is Traceable to Bronze Via `_pipeline_run_id`"

**Provider:** All three layers (Bronze, Silver, Gold) + run log

**Consumers:** Verification scripts, analysts, audit systems

**Guarantee:**
1. Every Gold record has a `_pipeline_run_id` value
2. That run_id exists in `pipeline/run_log.parquet` with `status = SUCCESS`
3. The run_log row identifies the exact model that produced this Gold record
4. Following the run_id back to Silver and Bronze is possible via `_pipeline_run_id`
5. No Gold record has a run_id that failed (no data from failed runs persists)

**Obligation (Each Layer's responsibility):**
- Bronze: Populate `_pipeline_run_id` at write time
- Silver: Preserve `_pipeline_run_id` from Bronze source records
- Gold: Preserve `_pipeline_run_id` from Silver source records
- pipeline.py: Ensure every run_id that produced persistent data has run_log entries

**Enforcement Invariants:**
- INV-22: All Silver and Gold records have non-null, traceable `_pipeline_run_id`
- INV-04: Bronze audit columns non-null at write
- INV-19: Run log append-only (no deletion of traces)
- INV-20A: Successful runs have one SUCCESS row per model

**Violation Signals:**
- Gold record with null or missing `_pipeline_run_id` → INV-22 failure
- `_pipeline_run_id` value in Gold not found in run_log → traceability broken
- Run_log row missing for a model that wrote data → audit trail incomplete
- Verification query: `SELECT DISTINCT _pipeline_run_id FROM gold/... EXCEPT SELECT run_id FROM run_log WHERE status='SUCCESS'` returns rows → INV-22 failure

**Implementation:**
- Bronze loader generates run_id at pipeline startup, passes to all loaders
- All loaders populate `_pipeline_run_id` in output records
- dbt models use computed column or source column pass-through to preserve `_pipeline_run_id`
- Verification script in Section 10 of requirements brief implements the audit trail check

---

## 10. Contract Violation Escalation

### Severity Levels

**Level 1 — Data Corruption (halt processing):**
- INV-01, INV-04, INV-22: Null or missing audit columns, source file modified
- INV-03: Bronze partition overwrites
- INV-43: Control table unreadable

**Action:** Write FAILED run log entry, do not advance watermark, halt pipeline.

**Level 2 — Silent Data Loss (visible after post-run verification):**
- INV-05: Bronze ≠ Silver + Quarantine
- INV-06, INV-09: Duplicate transactions, invalid codes not quarantined
- INV-11: Gold includes unresolvable records
- INV-14: Duplicate (account_id, week_start_date) in Gold

**Action:** Verification script fails; engineer must investigate and correct at data source.

**Level 3 — Idempotency Loss (visible on re-run):**
- INV-02: Bronze partition re-written on re-run
- INV-14: Gold weekly recomputed with different closing_balance
- INV-41: Gold incremental mode appends instead of replaces

**Action:** Re-run produces different output; audit trail will show multiple run_ids for same date. Engineer must investigate root cause.

**Level 4 — Procedural Risk (no structural enforcement):**
- IG-07: Gold weekly model invoked directly via `dbt run --select` (bypasses control gate)
- Risk 2 (ARCHITECTURE.md): Same as above

**Action:** Warning in model file; documented procedural control. Violation is possible but requires intentional violation by engineer.

---

*Module contracts document completes Phase 8 discovery of all component boundaries and guarantees.*
