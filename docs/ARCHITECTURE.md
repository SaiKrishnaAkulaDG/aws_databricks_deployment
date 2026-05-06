# ARCHITECTURE.md
## Credit Card Financial Transactions Lake
**PBVI Phase:** 1 — Decide
**Brief Version:** 1.0 + REQUIREMENTS_GAPS_COVER.md (all gaps closed)
**Status:** DECIDED — ready for Phase 2

---

## 1. Problem Framing

### What This System Solves

A financial services client processes credit card transactions across multiple channels daily. Data analysts and risk teams currently access raw CSV extract files directly — bypassing quality control, producing inconsistent results when analysts work from different file versions, and leaving no audit trail of how raw data became the numbers being acted on.

This system implements a Medallion architecture (Bronze → Silver → Gold) data lake that:

- Ingests daily CSV extract files for transactions, accounts, and transaction codes
- Enforces defined quality rules at each layer boundary
- Produces Gold-layer aggregations queryable via DuckDB with confidence
- Maintains a complete, traceable audit trail from raw file to Gold aggregate
- Is re-runnable without producing duplicates or incorrect aggregations

### What This System Explicitly Does Not Solve

- Risk computation or credit decisioning — the system surfaces data, it does not act on it
- Modification of source system records — source CSVs are read-only
- Backfill of specific historical dates to correct errors — out of scope per Section 9
- SCD Type 2 history for accounts — Silver retains latest record only, by design
- Streaming or near-realtime ingestion — this is a batch pipeline
- Schema evolution — CSV schema is fixed for this exercise
- Resolution of `_is_resolvable = false` records — requires backfill (out of scope)
- Production deployment, monitoring, or alerting infrastructure

---

## 2. Key Design Decisions

### Decision 1 — Selected Architecture: Candidate B (Python Orchestrator with Per-Model Execution Control, dbt-Native Transformations)

**What was decided:**
`pipeline.py` is the authoritative orchestrator. It calls `dbt run --select model_name` per model in a sequence derived from the dbt DAG at runtime. dbt models are the exclusive mechanism for all Silver and Gold transformations. Bronze ingestion is Python + DuckDB directly. All control-plane state (run log, watermark, `gold_weekly_control`) is owned by `pipeline.py`, not by dbt.

**Rationale:**
The brief mandates two non-negotiable constraints (Section 8): dbt is the exclusive transformation mechanism for Silver and Gold; Python + DuckDB handles Bronze ingestion. Candidate B satisfies both while maintaining Python ownership of the control plane — which is the correct architectural boundary. dbt transforms data. Python decides when, in what order, and what to record about it.

**Alternatives rejected:**

*Candidate A — Thin Python Orchestrator, single `dbt run` invocation:*
Rejected for four compounding reasons. First, run log entries at model grain require either dbt post-hooks or post-execution parsing of `run_results.json` — both are fragile and push control-plane logic into dbt's execution context. Second, `gold_weekly_control` idempotency cannot be implemented cleanly inside dbt without violating dbt's stateless transformation contract — a model that reads its own prior output to decide whether to skip is architecturally wrong. Third, a mid-run failure leaves `manifest.json` and `run_results.json` in a partially updated state that `pipeline.py` must parse defensively. Fourth, the run log is only writable after dbt post-hooks or artifact parsing completes — it is not a first-class pipeline output in this architecture.

*Candidate C — Unified Python Runner, dbt as SQL Engine Only:*
Rejected because it eliminates dbt's value proposition beyond SQL execution. The brief's constraint is that dbt is the exclusive transformation mechanism — Candidate C satisfies the letter of this constraint but not its intent. When `pipeline.py` controls every aspect of execution and dbt is reduced to a SQL template engine, transformation logic drifts into Python over time. The boundary between orchestration and transformation blurs, and dbt's lineage, testability, and documentation capabilities are progressively wasted. Candidate C also concentrates the most Python complexity in the orchestrator — the highest maintenance burden of the three options.

---

### Decision 2 — DAG-Derived Execution Order via `dbt compile`

**What was decided:**
At the start of each pipeline run, `pipeline.py` calls `dbt compile` to generate `target/manifest.json`. It then performs a topological sort of the manifest's model nodes to derive the correct execution sequence. The hardcoded model list is replaced by this derived sequence. Per-layer tag execution is used within each layer (`tag:silver`, `tag:gold`) — dbt handles intra-layer ordering via `ref()` dependencies. `pipeline.py` enforces inter-layer ordering (Silver completes before Gold starts).

**Rationale:**
Without this, `pipeline.py` maintains a parallel execution sequence that can drift from the dbt DAG whenever a new model is added. Two sources of truth for execution order will eventually diverge — silently producing incorrect Gold output when a new model is inserted between existing ones without updating `pipeline.py`. The `dbt compile` approach makes the dbt DAG the single source of truth. `pipeline.py` is a consumer of it, not a parallel definition.

**Alternatives rejected:**

*Hardcoded model list in `pipeline.py`:* Rejected because it creates a maintenance dependency — every new dbt model requires a corresponding update to `pipeline.py`. This dependency is not enforced by any tooling and will eventually be violated.

*Single `dbt run` for all models:* Rejected — this is Candidate A's approach, which was rejected at Decision 1 for reasons documented there.

---

### Decision 3 — Real-Time Run Log Writing via dbt JSON Log Streaming

**What was decided:**
`pipeline.py` invokes dbt with `--log-format json` and consumes dbt's stdout as a real-time event stream using `subprocess.Popen`. `NodeStart` and `NodeFinished` events are parsed as they arrive. Run log entries are written per model at the moment each model completes — not after the layer finishes, and not via artifact parsing. Row counts (`records_written`, `records_rejected`) are obtained by querying the output Parquet files immediately after each `NodeFinished` event via DuckDB. SKIPPED entries for models that did not execute are written when the subprocess exits.

**Rationale:**
The brief requires one run log row per model per pipeline invocation with individual timing and counts (Section 6.2). The run log is described as the connective tissue for audit tracing. Per-layer execution (Decision 2) means a single dbt subprocess covers multiple models — without streaming, `pipeline.py` cannot distinguish per-model timing or identify the exact failure point without parsing `run_results.json` after the fact. JSON log streaming eliminates artifact parsing entirely, gives real-time per-model entries, and handles skipped models correctly on process exit.

**Alternatives rejected:**

*Post-execution `run_results.json` parsing:* Rejected. Row counts in `run_results.json` reflect rows affected by the final SQL statement only — not `records_processed`, `records_written`, and `records_rejected` as distinct values. Skipped models are absent from results rather than marked — requiring a manifest diff. This is defensive parsing that runs on every execution and deepens `pipeline.py` complexity unnecessarily.

*dbt post-hooks for run log writing:* Rejected. Post-hooks run inside dbt's execution context. A post-hook failure entangles run log integrity with transformation correctness. dbt has no mechanism to roll back a completed transformation if its post-hook fails — leaving an inconsistency between what was computed and what the run log records.

*Per-model `dbt run --select` subprocess calls:* Rejected for performance. N models = N manifest loads. For a 7-day training exercise this is tolerable but the pattern does not scale and was superseded by the per-layer approach in Decision 2.

---

### Decision 4 — Async Buffered Run Log Flush to Resolve Split Failure Risk

**What was decided:**
Run log entries are accumulated in an in-memory buffer throughout pipeline execution. The buffer is flushed to `pipeline/run_log.parquet` as a single append operation after the watermark is successfully advanced — the last step of a successful pipeline run. If the flush fails, `pipeline.py` logs the failure to a local `.jsonl` fallback file. On the next run, `pipeline.py` detects a previous `run_id` in the watermark's `updated_by_run_id` field with no corresponding run log entries, and writes a synthetic `UNLOGGED_RUN` row with an explanatory `error_message`.

**Rationale:**
The run log is not a control signal — it is audit output. Section 6.2 confirms this: the run log records execution metadata for traceability, not for pipeline decision-making. Since `pipeline.py` never reads the run log to make control decisions, the run log write does not need to be in the critical path. Decoupling it means a flush failure cannot affect pipeline correctness — the watermark and the data are the authoritative state. This eliminates the split failure scenario where dbt succeeds but `pipeline.py` crashes before control-plane writes complete, leaving correct data with no audit trail.

**Alternatives rejected:**

*Write-ahead PENDING rows per model:* Rejected. Requires either modifying existing run log rows (violating the brief's append-only contract in Section 6.2) or writing two rows per model per run, adding query complexity for consumers. Adds startup recovery logic that increases `pipeline.py` complexity.

*Sentinel commit file:* Rejected. Introduces a second control-plane artifact (`pipeline/runs/RUN-ID.complete.json`) not defined in the brief. Adds startup recovery logic. Two control-plane artifacts doing overlapping jobs is an avoidable complexity.

*Retry with fallback file:* Rejected. Does not solve process crashes — retries require the process to be alive. The fallback file is a second artifact requiring manual reconciliation.

---

### Decision 5 — `gold_weekly_control` Managed by `pipeline.py`, Not by dbt

**What was decided:**
`pipeline.py` reads `pipeline/gold_weekly_control.parquet` before invoking the Gold weekly model to determine which weeks have already been computed. It passes only uncomputed weeks to dbt as model variables. After dbt writes the new week's rows, `pipeline.py` updates `gold_weekly_control` to record the newly computed weeks. The Gold weekly dbt model receives only the weeks it needs to compute — it has no knowledge of prior computation state.

**Rationale:**
GAP 3 in the REQUIREMENTS_GAPS_COVER.md establishes that `closing_balance` is fixed at first computation time to preserve idempotency. A dbt model cannot implement skip logic without reading its own prior output — which violates dbt's stateless transformation contract. A post-hook writing to a control table entangles control-plane state with transformation execution. Both patterns push control-plane decisions into dbt, which was not designed for them. Python ownership of `gold_weekly_control` keeps the boundary clean: dbt transforms, Python controls.

**Alternatives rejected:**

*dbt incremental materialisation for Gold weekly:* Rejected. The `dbt run --full-refresh` escape hatch exists in dbt-core 1.7.x and cannot be disabled from within the project. Any engineer on the project can run it, wiping prior weeks and violating the idempotency requirement. A guarantee that depends on engineers never running a standard dbt command is not a structural guarantee.

*dbt pre-hook reading `gold_weekly_control`:* Rejected. Pre-hooks run inside dbt's execution context. A model that reads a control table to decide its own behaviour is no longer a pure transformation — its output depends on the state of a table modified by previous runs of the same model.

---

### Decision 6 — `dbt compile` Failure Produces an ORCHESTRATION Row in the Run Log

**What was decided:**
The `dbt compile` step at pipeline startup is treated as a named pipeline operation. If it fails, `pipeline.py` writes a run log row with `model_name = DBT_COMPILE` and `layer = ORCHESTRATION` with `status = FAILED` and the compilation error message. No model rows are written because no models ran. The watermark does not advance.

**Rationale:**
Without this, a compile failure leaves a pipeline invocation with no run log trace — the pipeline failed before any model was reached, so no model-level rows exist. The brief's Section 10.5 requires that every `_pipeline_run_id` appearing in Silver has a corresponding run log row. A compile failure writes nothing to Silver, so this check is not violated — but the absence of any run log entry for the failed invocation is an audit gap. The ORCHESTRATION row closes this gap and gives engineers a queryable signal for "pipeline failed at compile, no data was processed."

**Alternatives rejected:**

*No run log entry for compile failures:* Rejected. Silent pipeline failures with no audit trail are the exact problem this system is designed to prevent in the data layer. The same principle applies to the pipeline itself.

---

### Decision 7 — Intra-Date Processing Order Enforced by `pipeline.py`

**What was decided:**
Within each date, `pipeline.py` enforces the following sequence:
1. Accounts Bronze load
2. Accounts Silver promotion (dbt)
3. Transactions Bronze load
4. Transactions Silver promotion (dbt)

dbt model dependencies provide secondary enforcement — `silver_transactions` has a `ref()` dependency on `silver_accounts`. But dbt dependency ordering applies only within a single dbt invocation and cannot enforce ordering relative to Bronze ingestion, which is managed externally in Python. `pipeline.py` is the primary guarantee.

**Rationale:**
GAP 5 in the REQUIREMENTS_GAPS_COVER.md identifies that Silver transaction promotion validates `account_id` against Silver Accounts. If transactions are promoted before accounts for the same date, legitimate transactions will produce false `UNRESOLVABLE_ACCOUNT_ID` flags. The ordering must be enforced at the orchestrator level, not left to dbt's DAG alone, because Bronze ingestion is outside dbt's execution context.

---

### Decision 8 — Day 1 Accounts File Treated as Full Snapshot

**What was decided:**
The Day 1 accounts file is treated as a full snapshot establishing complete baseline state for the historical load. Days 2–7 are true deltas. This assumption is required for the historical pipeline to initialise a complete and usable dataset from the provided files.

**Rationale:**
GAP 1 in the REQUIREMENTS_GAPS_COVER.md documents this decision in full. If Day 1 were treated as a true delta from an unseen prior state, Silver Accounts would be incomplete at Day 1, causing Silver transaction promotion to produce false `UNRESOLVABLE_ACCOUNT_ID` flags at high volume. The historical load would be unusable.

---

### Decision 9 — Gold Daily Summary Exposes Unresolvable Transaction Counts

**What was decided:**
Two additional columns are added to `gold/daily_summary/data.parquet`:
- `total_unresolvable_transactions` (INTEGER) — count of Silver transactions where `_is_resolvable = false` for this date
- `total_unresolvable_amount` (DECIMAL) — sum of `_signed_amount` for those records

**Rationale:**
GAP 2 in the REQUIREMENTS_GAPS_COVER.md documents this decision. Without these columns, analysts querying Gold see aggregate totals that silently exclude a subset of Silver records. They cannot assess data completeness or identify days with significant unresolvable activity without inspecting lower layers. These columns make the exclusion visible without exposing records that should not be in Gold.

---

### Decision 10 — `_missing_merchant_name` Flag Added to Silver Transactions

**What was decided:**
A derived column `_missing_merchant_name` (BOOLEAN, NOT NULL) is added to Silver Transactions. It is `TRUE` when `transaction_type = 'PURCHASE'` AND `merchant_name IS NULL`, and `FALSE` in all other cases. It does not affect promotion or rejection logic.

**Rationale:**
GAP 4 in the REQUIREMENTS_GAPS_COVER.md documents this decision. The brief implies PURCHASE transactions should have a populated `merchant_name` but defines no rejection rule for a null value. Without this column, PURCHASE transactions with null merchant names pass Silver promotion silently with no visibility in any layer. The flag surfaces the issue without extending the exhaustive rejection code list in Section 5.

---

## 3. Challenge My Decisions

**Decision 1 — Candidate B:**
*Strongest argument against:* `pipeline.py` becomes a non-trivial stateful application with its own test requirements. The brief's verification expectations (Section 10) are entirely focused on data layer correctness — not on orchestrator correctness. A complex orchestrator that is not explicitly verified is a hidden risk surface.
*Assessment:* Valid concern, not a rejection. The orchestrator complexity is a known and documented shortcoming. It is mitigated by: (a) deriving execution order from the dbt DAG rather than maintaining it independently, (b) keeping all transformation logic in dbt models, (c) limiting `pipeline.py`'s responsibilities to orchestration and control-plane writes. Phase 3 must include explicit verification tasks for `pipeline.py` behaviour — idempotency, watermark advancement, SKIPPED entry generation — that the brief does not currently specify.

**Decision 2 — `dbt compile` for DAG derivation:**
*Strongest argument against:* `dbt compile` is an additional failure point. If compilation fails due to a broken model added by a new engineer, the pipeline stops before any data is processed. This creates a tight coupling between model authorship quality and pipeline operability.
*Assessment:* Valid concern, partially mitigated. Decision 6 ensures compile failures produce a run log entry, so the failure is visible. The coupling is real but correct — a model with a broken `ref()` reference should not run. Catching it at compile time is preferable to catching it mid-execution after partial data has been written.

**Decision 3 — JSON log streaming:**
*Strongest argument against:* The dbt JSON log event schema is not a versioned public API. Field paths have changed between dbt minor versions. Any dbt upgrade past 1.7.x that changes `NodeStart` or `NodeFinished` field names will silently break run log writing.
*Assessment:* Valid concern, mitigated by the version pin. The brief fixes dbt-core at 1.7.x. Within that pin the schema is stable. The mitigation requires an explicit upgrade verification step — documented as a maintenance discipline requirement below.

**Decision 4 — Async buffered run log flush:**
*Strongest argument against:* Run log entries are not written in real time. If someone queries the run log mid-run, they see nothing for the current run. This reduces the run log's usefulness as a live operational monitor.
*Assessment:* Rejected as a blocking concern for this system. The brief describes the run log as a post-hoc traceability tool, not a live monitor. There is no serving API layer (Section 9). Analysts query Gold directly via DuckDB CLI. Real-time run log visibility is not a stated requirement.

**Decision 5 — `gold_weekly_control` in Python:**
*Strongest argument against:* `gold_weekly_control` is invisible to dbt lineage. An engineer running `dbt run --select gold_weekly_account_summary` directly bypasses `pipeline.py`, skips the control table check, and recomputes all weeks — overwriting `closing_balance` values. This is a procedural risk with no structural enforcement.
*Assessment:* Valid concern, not fully resolvable within the brief's constraints. Mitigation: a prominent warning comment in `gold_weekly_account_summary.sql` stating that direct dbt invocation bypasses `gold_weekly_control` and must not be used outside of `pipeline.py`. This is a documented procedural control, not a structural guarantee. The alternative — implementing the check inside dbt — violates the stateless transformation contract and was rejected at Decision 5 for documented reasons.

**Decision 6 — ORCHESTRATION row for compile failures:**
*Strongest argument against:* Introduces a sentinel value (`model_name = DBT_COMPILE`, `layer = ORCHESTRATION`) not defined in the brief's run log schema. Consumers of the run log must know these sentinel values exist and filter them appropriately.
*Assessment:* Valid concern, addressable in Phase 3. The run log schema extension must be documented as a gap resolution decision (same pattern as REQUIREMENTS_GAPS_COVER.md) and included in any run log query documentation produced during Phase 3.

---

## 4. Key Risks

**Risk 1 — JSON log schema drift on dbt upgrade.**
The `pipeline.py` JSON log parser is coupled to dbt-core 1.7.x event schema. Any dbt version upgrade must include a verification step: run `dbt run --log-format json` against a test model and confirm `NodeStart` and `NodeFinished` field paths before deploying the upgrade. This is a named maintenance discipline requirement — not optional on any upgrade.

**Risk 2 — `gold_weekly_control` bypassed by direct dbt invocation.**
An engineer running `dbt run --select gold_weekly_account_summary` directly bypasses `pipeline.py` and the `gold_weekly_control` check. All weeks would be recomputed, overwriting fixed `closing_balance` values and violating idempotency. Mitigation: prominent warning in the dbt model file. Structural enforcement is not possible without violating dbt's stateless contract.

**Risk 3 — `pipeline.py` complexity not covered by brief verification expectations.**
Section 10 verification commands cover data layer correctness only. Orchestrator correctness — correct SKIPPED entry generation, correct watermark non-advancement on partial failure, correct `gold_weekly_control` updates — is not covered. Phase 3 must add explicit verification tasks for these behaviours.

**Risk 4 — Split failure leaves run log gap on flush failure.**
If `pipeline.py` crashes after the watermark advances but before the run log buffer flushes, the run log has no entries for that run. The synthetic `UNLOGGED_RUN` row written on the next run is less informative than real per-model entries. Data correctness is preserved; audit trail completeness is not. This is a documented and accepted trade-off under Decision 4.

---

## 5. Key Assumptions

- The Day 1 accounts file is a full snapshot establishing complete baseline state. Days 2–7 are true deltas. (GAP 1)
- Transaction codes are static for the duration of this exercise and are loaded once during historical pipeline initialisation. (Section 2.2)
- The dbt-core 1.7.x JSON log event schema (`NodeStart`, `NodeFinished`) is stable within the 1.7.x version pin. (Decision 3)
- The scaffold's source CSV files are the authoritative date range definition. The date range is not parameterised in this document — it is captured in the execution plan parameter table. (GAP 7)
- `_is_resolvable = false` is a permanent, point-in-time state within this exercise. No backfill mechanism exists to correct unresolvable records. (GAP 2)
- `closing_balance` in Gold weekly aggregates reflects the account balance at first computation time for each week. Subsequent balance updates do not retroactively update Gold. (GAP 3)

---

## 6. Open Questions

**OQ-1 — `gold_weekly_control` schema.**
GAP 3 deferred the `gold_weekly_control` schema to this architecture artifact. The schema must be defined before Phase 3. Minimum required columns: `week_start_date` (DATE), `week_end_date` (DATE), `computed_at` (TIMESTAMP), `computed_by_run_id` (STRING). Additional columns may be required once the Gold weekly dbt model is specified in Phase 3.

**OQ-2 — Run log sentinel values for ORCHESTRATION rows.**
Decision 6 introduces `model_name = DBT_COMPILE` and `layer = ORCHESTRATION` as sentinel values not defined in the brief. These must be formally documented as a schema extension before Phase 3 and included in run log query documentation.

**OQ-3 — Verification coverage for `pipeline.py` orchestrator behaviours.**
Section 10 verification commands do not cover orchestrator correctness. Phase 3 must define verification commands for: correct SKIPPED entry generation on partial failure, correct watermark non-advancement when Silver fails, correct `gold_weekly_control` update after Gold weekly runs, and `UNLOGGED_RUN` synthetic row appearance on run log flush failure.

**OQ-4 — Exact date range of the historical load.**
GAP 7 documents that the date range is defined by the scaffold's source files. This must be captured as an explicit parameter in the execution plan before Phase 3 begins.

---

## 7. Future Enhancements (Parking Lot)

**Backfill pipeline.**
A dedicated backfill pipeline to reprocess specific historical dates and resolve `_is_resolvable = false` records. Requires watermark guard logic to prevent future-date processing. Explicitly out of scope per Section 9. Documented here as the highest-priority production pattern deferred from this exercise.

**SCD Type 2 for Silver Accounts.**
Full history of account attribute changes (credit limit, status, balance). Currently deferred — Silver retains latest record only. Without SCD Type 2, there is no way to reconstruct what an account's credit limit was on a historical date. The current simplification is acceptable for this exercise but is a known analytical limitation.

**Streaming or near-realtime ingestion.**
The current architecture is batch only. A streaming variant would require a different ingestion mechanism and Bronze partitioning strategy. Out of scope per Section 9.

**Schema evolution support.**
CSV schema is fixed for this exercise. Production would require a schema registry or version-controlled schema definitions to handle source changes safely.

**`gold_weekly_control` visibility in dbt lineage.**
Currently invisible to `dbt docs generate`. A future enhancement could register `gold_weekly_control` as a dbt source, making it visible in lineage without granting dbt write access to it.

---

## 8. Data Model

### First-Class Entities

**Transactions (fact)**
The central fact entity. One record per transaction processed on a given date. Append-only in the source system — transactions are never updated or deleted. Sign is assigned in Silver using `transaction_codes.debit_credit_indicator`. The source always provides positive amounts.

**Accounts (slowly changing dimension, simplified)**
One record per account in Silver — latest state only. Daily delta files in the source contain only new or changed records. Silver upserts on `account_id`. No history retained (SCD Type 2 deferred to parking lot).

**Transaction Codes (static reference)**
Loaded once during historical pipeline initialisation. Maps `transaction_code` to `transaction_type`, `debit_credit_indicator`, and `affects_balance`. Authoritative source for sign assignment in Silver. Does not change during the exercise.

### Layer Entities

**Bronze — Transactions, Accounts, Transaction Codes**
Exact copies of source CSV content plus three pipeline audit columns (`_source_file`, `_ingested_at`, `_pipeline_run_id`). Immutable after initial write. Partitioned by date for transactions and accounts. Single reference file for transaction codes.

**Silver — Transactions**
Promoted from Bronze after passing all quality rules. Adds `_signed_amount` (amount with sign applied from transaction codes), `_is_resolvable` (false if account not found at promotion time), and `_missing_merchant_name` (true if PURCHASE with null merchant name). Partitioned by source date. Deduplicated across all partitions on `transaction_id`.

**Silver — Accounts**
Latest record per `account_id`. Non-partitioned single file. Upserted on each delta load.

**Silver — Transaction Codes**
Single reference file promoted from Bronze once. Non-partitioned.

**Silver — Quarantine**
Records rejected during Silver promotion. Partitioned by source date. Contains original source record plus rejection audit columns including `_rejection_reason` from the exhaustive code list in Section 5.

**Gold — Daily Transaction Summary**
One record per calendar day. Aggregates resolvable Silver transactions. Includes unresolvable counts and amounts (GAP 2 addition).

**Gold — Weekly Account Transaction Aggregates**
One record per account per calendar week (Monday–Sunday). Only accounts with at least one resolvable transaction in the week are included. `closing_balance` fixed at first computation time.

### Control-Plane Entities

**Pipeline Control Table** (`pipeline/control.parquet`)
Single row. Tracks `last_processed_date` (watermark), `updated_at`, `updated_by_run_id`. Advances only after Bronze, Silver, and Gold all complete successfully for a date.

**Pipeline Run Log** (`pipeline/run_log.parquet`)
Append-only. One row per dbt model (or Bronze loader or ORCHESTRATION operation) per pipeline invocation. The audit trail connecting Gold aggregates back to Silver and Bronze via `_pipeline_run_id`.

**Gold Weekly Control** (`pipeline/gold_weekly_control.parquet`)
Week-grain control table tracking which weekly aggregates have been computed. Prevents recomputation of past weeks. Managed exclusively by `pipeline.py`. Schema to be finalised in Phase 3 (OQ-1).

---

## 9. Traceability Index

| Decision | Traced to |
|---|---|
| Candidate B selection | Section 7 (fixed stack), Section 8 (dbt transformation constraint, Python Bronze constraint), Section 6.2 (run log model grain) |
| `dbt compile` DAG derivation | Section 8 (idempotency), implied constraint — two sources of truth for execution order will drift |
| JSON log streaming | Section 6.2 (run log model grain, started_at/completed_at per model), Section 8 (idempotency) |
| Async buffered run log flush | Section 6.2 (append-only run log), Section 8 (idempotency — data correctness preserved on split failure) |
| `gold_weekly_control` in Python | GAP 3 (closing_balance fixed at first computation), Section 8 (idempotency requirement) |
| ORCHESTRATION row for compile failures | Section 6.2 (run log as audit trail), Section 10.5 (every pipeline_run_id traceable) |
| Intra-date processing order | GAP 5 (accounts before transactions within each date) |
| Day 1 full snapshot assumption | GAP 1 |
| Unresolvable counts in Gold Daily Summary | GAP 2 |
| `_missing_merchant_name` flag | GAP 4 |

---

*Document produced at Phase 1 Decide. All decisions are engineer-authored and engineer-confirmed. Ready for Phase 2 — Invariant Definition.*
