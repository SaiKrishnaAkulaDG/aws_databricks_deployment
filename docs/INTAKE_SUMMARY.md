# INTAKE_SUMMARY.md — Project Scope and Build Summary

**PBVI Phase:** 8 — Discovery  
**Document Type:** Executive intake and scope summary  
**Created:** Phase 8 completion  
**Status:** FINAL

---

## 1. Project Framing

### What Was Built

A Medallion architecture (Bronze → Silver → Gold) data lake that ingests daily credit card transaction CSV extracts, enforces data quality rules at each layer boundary, produces queryable Gold aggregations, and maintains a complete, traceable audit trail from raw file to Gold aggregate.

### Problem Solved

Financial services analysts and risk teams previously accessed raw CSV files directly, producing inconsistent results and leaving no audit trail. This system consolidates data ingestion, quality enforcement, and aggregation into a single, re-runnable pipeline with full traceability.

### Out of Scope (Explicit Non-Goals)

- Risk computation or credit decisioning
- Modification of source system records
- Backfill of specific historical dates to correct errors
- SCD Type 2 history for accounts (latest record only)
- Streaming or near-realtime ingestion (batch pipeline only)
- Schema evolution (CSV schema fixed)
- Production deployment, monitoring, or alerting infrastructure
- Resolution of `_is_resolvable = false` records (requires backfill)

---

## 2. Data Model Overview

### First-Class Entities

**Transactions (fact)** — Central entity. One record per transaction processed on a given date. Append-only in source — never updated or deleted. Sign assigned in Silver from transaction codes.

**Accounts (slowly changing dimension, simplified)** — One record per account in Silver (latest only). Daily deltas in source; Day 1 is full snapshot. Silver upserts on `account_id`.

**Transaction Codes (static reference)** — Loaded once during historical initialisation. Maps `transaction_code` to `transaction_type`, `debit_credit_indicator`, and `affects_balance`. Authoritative for sign assignment.

### Layer Outputs

**Bronze Layer:**
- `data/bronze/transactions/date=YYYY-MM-DD/data.parquet` (partitioned by date)
- `data/bronze/accounts/date=YYYY-MM-DD/data.parquet` (partitioned by date)
- `data/bronze/transaction_codes/data.parquet` (static reference)
- Plus audit columns: `_source_file`, `_ingested_at`, `_pipeline_run_id`

**Silver Layer:**
- `data/silver/transactions/date=YYYY-MM-DD/data.parquet` (quality-checked, deduplicated, signed)
- `data/silver/accounts/data.parquet` (latest per account_id)
- `data/silver/transaction_codes/data.parquet` (reference copy)
- `data/silver/quarantine/date=YYYY-MM-DD/rejected.parquet` (rejected records with rejection codes)
- Plus derived columns: `_signed_amount`, `_is_resolvable`, `_missing_merchant_name`

**Gold Layer:**
- `data/gold/daily_summary/data.parquet` (one row per calendar date, aggregated)
- `data/gold/weekly_account_summary/data.parquet` (one row per account/week, immutable)
- Includes unresolvable exposure columns in daily summary

**Control Plane:**
- `data/pipeline/control.parquet` (watermark: `last_processed_date`, timing, run_id trace)
- `data/pipeline/gold_weekly_control.parquet` (week-grain registry of computed weeks)
- `data/pipeline/run_log.parquet` (append-only audit trail with per-model execution metadata)

---

## 3. Pipeline Modes

### Historical Mode (Initial Load)

**Command:** `python pipeline/pipeline.py --mode historical --start-date 2024-01-01 --end-date 2024-01-07`

**Behavior:**
- Processes entire date range in sequence
- Initialises control-plane files (watermark, control tables)
- For each date: Accounts Bronze → Accounts Silver → Transactions Bronze → Transactions Silver → Gold aggregations
- Advances watermark only on full success
- Writes run log entries in real time from dbt JSON log stream
- Flushes run log buffer after watermark advances

**Atomicity Guarantee:** Date-level atomicity — partial failure leaves watermark unchanged; reprocessing yields identical output.

### Incremental Mode (Operational Runs)

**Command:** `python pipeline/pipeline.py --mode incremental`

**Behavior:**
- Reads watermark from `pipeline/control.parquet`
- Processes exactly watermark + 1 day
- If source file absent: no-op (watermark unchanged)
- If source file present: runs same per-date sequence as historical mode
- Same success/failure semantics and watermark advancement

**Idempotency Guarantee:** Running the same incremental invocation multiple times produces no change to data or watermark.

---

## 4. Key Architectural Decisions

### Decision 1: Orchestration Model (Candidate B)

`pipeline.py` calls `dbt run --select model_name` per model in sequence derived from dbt DAG. dbt models are the exclusive mechanism for Silver and Gold transformations. Bronze ingestion is Python + DuckDB. All control-plane state owned by `pipeline.py`, not by dbt.

**Why:** Satisfies both brief constraints (dbt for Silver/Gold, Python for Bronze) while maintaining Python ownership of control plane. dbt transforms; Python decides when, in what order, and what to record.

### Decision 2: DAG-Derived Execution Order

At pipeline startup, `pipeline.py` calls `dbt compile` to generate `target/manifest.json`. Topological sort of manifest derives correct execution sequence. Replaces hardcoded model list.

**Why:** Makes dbt DAG the single source of truth for execution order. Eliminates maintenance drift when new models are added.

### Decision 3: Real-Time Run Log via dbt JSON Log Streaming

`pipeline.py` invokes dbt with `--log-format json` and consumes stdout as real-time event stream. `NodeStart` and `NodeFinished` events parsed as they arrive. Run log entries written per model at completion, not after layer finishes.

**Why:** Distinguishes per-model timing and counts without parsing artifacts. Handles skipped models correctly on process exit.

### Decision 4: Async Buffered Run Log Flush

Run log entries accumulated in memory, flushed to `run_log.parquet` as single append after watermark advances. On flush failure: write to `.jsonl` fallback. On next run: detect via `updated_by_run_id` mismatch and write synthetic `UNLOGGED_RUN` row.

**Why:** Decouples run log write from pipeline correctness. Flush failure cannot affect data — watermark and data are authoritative.

### Decision 5: `gold_weekly_control` Managed by `pipeline.py`

`pipeline.py` reads control table before invoking Gold weekly model, passes only uncomputed weeks to dbt. After dbt completes, `pipeline.py` updates control table with newly computed weeks.

**Why:** dbt models are stateless. A model that reads its own prior output to decide execution violates that contract. Python orchestration maintains this boundary cleanly.

### Decision 6: `dbt compile` Failure = ORCHESTRATION Sentinel Row

If `dbt compile` fails, `pipeline.py` writes run log row with `model_name = DBT_COMPILE`, `layer = ORCHESTRATION`, `status = FAILED`. Closes audit gap for compile failures that occur before any models run.

**Why:** Prevents silent pipeline failures with no audit trace.

### Decision 7: Intra-Date Processing Order Enforced by `pipeline.py`

Within each date: Accounts Bronze → Accounts Silver → Transactions Bronze → Transactions Silver. `pipeline.py` enforces; dbt `ref()` dependencies reinforce.

**Why:** Prevents false `UNRESOLVABLE_ACCOUNT_ID` flags when transactions are processed before their account records appear.

### Decision 8: Day 1 Accounts = Full Snapshot

Day 1 accounts file is treated as a full snapshot establishing baseline state. Days 2–7 are true deltas.

**Why:** Alternate interpretation (true delta from unseen state) would leave Silver Accounts incomplete at Day 1, causing high unresolvable rate.

### Decision 9: Unresolvable Exposure Columns in Gold Daily

Two columns added to `gold/daily_summary/data.parquet`:
- `total_unresolvable_transactions` (count of unresolvable Silver records)
- `total_unresolvable_amount` (sum of amounts for unresolvable records)

**Why:** Analysts can reconcile Gold totals against Silver without inspecting lower layers.

### Decision 10: `_missing_merchant_name` Flag in Silver

Derived column added to Silver Transactions (BOOLEAN, NOT NULL). TRUE when `transaction_type='PURCHASE'` AND `merchant_name IS NULL`; FALSE otherwise. Non-blocking.

**Why:** Surfaces data quality issue (PURCHASE without merchant) without creating new rejection code.

---

## 5. Quality Rules Applied

### Bronze Layer

- **Completeness:** All source CSV rows copied to Bronze
- **Immutability:** Partitions never modified after first write
- **Audit Trail:** Three audit columns added (`_source_file`, `_ingested_at`, `_pipeline_run_id`)
- **Idempotency:** Partition existence check prevents re-ingestion on re-run

### Silver Layer

**Transactions:**
- NULL field validation (fail if required fields null)
- `transaction_code` validation (must exist in Silver transaction_codes)
- Amount validation (must be positive)
- `account_id` resolution (flag as unresolvable if not found in Silver accounts, not quarantine)
- Global deduplication on `transaction_id` (one per entire system)
- Sign assignment from `debit_credit_indicator` only (not custom logic)
- `_missing_merchant_name` flag for visibility

**Accounts:**
- Upsert on `account_id` (latest record only, no history)
- No duplicates per account

**Quarantine:**
- Only records with valid rejection codes from exhaustive list
- Original record preserved for audit

### Gold Layer

**Daily Summary:**
- One row per calendar date (even if zero resolvable transactions)
- Aggregates: count and sum of resolvable Silver transactions only
- Unresolvable exposure columns: count and sum of unresolvable Silver records
- Atomic write (replace on re-run, no incremental append)

**Weekly Account Summary:**
- One row per `(account_id, week_start_date)` with ≥1 resolvable Silver transaction
- `closing_balance` fixed at first computation time (immutable on re-run)
- Control table enforces: no recomputation of past weeks
- Atomic write (replace on re-run)

---

## 6. Audit and Traceability

### `_pipeline_run_id` Linkage

Every Gold record has `_pipeline_run_id` → traceable to Silver records with same run_id → traceable to Bronze records with same run_id → traceable to run log entry documenting exact model that produced output.

### Run Log Structure

One row per component execution per pipeline invocation:
- `run_id` (UUID)
- `started_at`, `completed_at` (per-component timing)
- `model_name` (dbt model or Bronze loader or sentinel)
- `layer` (BRONZE, SILVER, GOLD, ORCHESTRATION)
- `status` (SUCCESS, FAILED, SKIPPED)
- `records_processed`, `records_written`, `records_rejected` (queried from output files, not dbt metadata)
- `error_message` (for failures)

### Watermark and Control State

**`control.parquet`:**
- `last_processed_date` (exact definition of next target date)
- `updated_at` (when watermark last advanced)
- `updated_by_run_id` (which run advanced it)

Advances only after Bronze, Silver, and Gold complete successfully. Failure at any layer prevents advancement.

**`gold_weekly_control.parquet`:**
- `week_start_date`, `week_end_date`
- `computed_at`, `computed_by_run_id`

Tracks which weeks have been computed to prevent recomputation (INV-14 enforcement).

---

## 7. Known Limitations and Deferred Patterns

### Limitation: No Backfill Mechanism

`_is_resolvable = false` records are permanent, point-in-time states. No backfill exists to resolve them once flagged. Requires external intervention or manual correction.

**Impact:** Legitimate transactions that arrive with missing account records cannot be retroactively promoted to Gold.

### Limitation: SCD Type 2 Deferred

Silver Accounts retains latest record only. No history of account attribute changes (credit limit, balance, status). Rebuilding historical state of an account on a past date is not possible.

**Impact:** Weekly `closing_balance` reflects account state at first computation time, not historical account state for that week.

### Limitation: `gold_weekly_control` Invisible to dbt Lineage

Control table is managed by `pipeline.py` outside dbt. Direct invocation of Gold weekly model via `dbt run --select gold_weekly_account_summary` bypasses control gate and recomputes all weeks.

**Impact:** Procedural control only (warning in model file). No structural enforcement. Requires engineer discipline.

### Deferred: Streaming Ingestion

Current architecture is batch only. Streaming or near-realtime variants deferred to future.

### Deferred: Schema Evolution

CSV schema is fixed. Production would require schema registry or versioned schema definitions.

---

## 8. Verification and Sign-Off

### Phase 7 Completion Criteria Met

- ✓ Full historical pipeline runs cleanly from clean state
- ✓ All Section 10 verification commands pass
- ✓ Idempotency proof passes across all layers
- ✓ Audit trail complete and traceable
- ✓ `verification/REGRESSION_SUITE.sh` assembled and passes
- ✓ Engineer signs off on Phase 8 entry criteria

### Artifacts Produced

**Core Documents (Frozen):**
- REQUIREMENTS_BRIEF.md (v1.0)
- REQUIREMENTS_GAPS_COVER.md (7 gaps resolved)
- ARCHITECTURE.md (10 decisions, 4 risks, 5 assumptions)
- INVARIANTS.md (38 invariants, all embedded in tasks)
- EXECUTION_PLAN.md (7 sessions, 27 tasks, all complete)
- Claude.md (v1.0, frozen execution contract)

**Verification Outputs:**
- `verification/verify_bronze.sh`
- `verification/verify_silver_transactions.sh`
- `verification/verify_silver_accounts.sh`
- `verification/verify_gold.sh`
- `verification/verify_section10.sh`
- `verification/verify_audit_trail.sh`
- `verification/REGRESSION_SUITE.sh` (Phase 8 required output)

**Session Logs (7 sessions, all complete):**
- S1: Project Scaffold and Infrastructure ✓
- S2: Bronze Layer ✓
- S3: Silver — Reference Data and Accounts ✓
- S4: Silver — Transactions ✓
- S5: Gold Layer ✓
- S6: Pipeline Orchestration ✓
- S7: End-to-End Verification ✓

**Phase 8 Discovery Artifacts:**
- TOPOLOGY.md (system topology and data flows)
- MODULE_CONTRACTS.md (component boundary contracts)
- INVARIANT_CATALOGUE.md (invariant quick reference)
- INTAKE_SUMMARY.md (this document — scope and build summary)
- INTEGRATION_CONTRACTS.md (I/O schemas and paths)
- RISK_REGISTER.md (known risks and technical debt)
- ANNOTATION_CHECKLIST.md (Phase 8 entry criteria confirmation)

---

## 9. Technology Stack

| Component | Version | Purpose |
|---|---|---|
| Python | 3.11 | Bronze ingestion, pipeline orchestration |
| dbt-core | 1.7.9 | Silver and Gold transformation models |
| dbt-duckdb | 1.7.4 | DuckDB adapter for dbt |
| DuckDB | 0.10.0 | Execution engine for SQL and Parquet I/O |
| Docker Compose | v2 | Containerized environment |
| Apache Parquet | — | Data storage format (via DuckDB) |

**Why DuckDB:** Embedded database with excellent Parquet support, sufficient for this batch pipeline, no external server required.

---

## 10. Engineering Team and Phases

**Engineer:** Pratham Bajaj  
**Phase Timeline:**
- Phase 1 — Decide: Architecture decisions (DECIDED 16/04/2026)
- Phase 2 — Define Invariants: Constraint set (SIGNED 15/04/2026)
- Phase 3 — Execution Planning: Task breakdown (COMPLETE 16/04/2026)
- Phase 4 — Design Gate: Verification against ARCHITECTURE.md
- Phase 5 — Frozen Execution Contract: Claude.md locked
- Phase 6–7 — Build Sessions: 7 sessions, 27 tasks (COMPLETE 27/04/2026)
- Phase 8 — Discovery: This document + 6 other discovery artifacts (IN PROGRESS)

---

*Intake Summary completes Phase 8 discovery intake and scope documentation.*
