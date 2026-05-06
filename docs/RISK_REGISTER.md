# RISK_REGISTER.md — Known Risks, Limitations, and Technical Debt

**PBVI Phase:** 8 — Discovery  
**Document Type:** Risk register and technical debt log  
**Created:** Phase 8 completion  
**Status:** FINAL

---

## 1. Critical Risks (High Impact, Requires Mitigation)

### Risk 1 — JSON Log Schema Drift on dbt Upgrade

**Severity:** HIGH  
**Category:** Integration Risk  
**Introduced by:** Decision 3 (JSON log streaming for run log)

**Description:**
The `pipeline.py` JSON log parser is tightly coupled to dbt-core 1.7.x event schema. The parser expects specific field paths in `NodeStart` and `NodeFinished` events. Any dbt version upgrade beyond 1.7.x that changes event field names or structure will silently break run log writing without compiler error.

**Why it matters:**
- Silent failure: Pipeline completes, data is written, but run log entries are not recorded
- Audit trail gap: `_pipeline_run_id` values in Silver/Gold will have no corresponding run_log entries
- Undetectable: Verification will fail at audit trail check (INV-22) only after upgrade is deployed
- No backwards compatibility: dbt version is a requirement.txt pin

**Current Mitigation:**
- Version pin in requirements.txt: `dbt-core==1.7.9`, `dbt-duckdb==1.7.4`
- Named maintenance discipline (IG-08): On any dbt upgrade, engineer must:
  1. Run `dbt run --log-format json` against test model
  2. Verify `NodeStart` and `NodeFinished` field paths match expected schema
  3. Do NOT upgrade on production until verification passes

**Residual Risk:**
- Mitigation is procedural, not structural
- Relies on engineer discipline to perform verification step
- No automated tooling enforces this verification

**Follow-Up Action:**
Consider future enhancement: automated schema validation in `pipeline.py` that compares expected field paths against actual JSON log events on first run. Would detect schema drift immediately and halt with clear error message.

---

### Risk 2 — `gold_weekly_control` Bypassed by Direct dbt Invocation

**Severity:** HIGH  
**Category:** Procedural Control Failure  
**Introduced by:** Decision 5 (Control table managed by Python, not dbt)

**Description:**
An engineer running `dbt run --select gold_weekly_account_summary` directly bypasses `pipeline.py` and the control table check. The model will compute all weeks present in the date range, overwriting `closing_balance` values for already-computed weeks.

**Why it matters:**
- Violates idempotency: Same week computed multiple times with different `closing_balance` values
- Silent data corruption: No error is raised; Gold is overwritten silently
- Undetectable by normal verification: INV-14 check (`SELECT ... GROUP BY ... HAVING COUNT(*) > 1`) will only catch duplicate rows in the same data file, not history of overwrites
- Permanent: Once past weeks are recomputed, historical `closing_balance` values are lost

**Current Mitigation:**
- Prominent warning comment in `gold_weekly_account_summary.sql` file
- Documented in Decision 5 section of ARCHITECTURE.md
- Listed in IG-07 (Implementation Guidance)
- This risk register document

**Residual Risk:**
- Mitigation is procedural documentation only — no structural enforcement
- New engineer unfamiliar with the system could easily violate this constraint
- No tooling prevents direct dbt invocation

**Structural Constraints Attempted and Rejected:**
- Cannot implement skip logic in dbt model (violates stateless contract)
- Cannot use dbt incremental materialisation (exposes `--full-refresh` escape hatch)
- Cannot add pre-hook to read control table (entangles state with transformation)

**Follow-Up Actions:**
1. Mandatory: Add to team onboarding — "Gold weekly model must only run via pipeline.py"
2. Possible: dbt macro to read control table and explicitly fail if invoked outside pipeline (documented workaround, adds coupling)
3. Consider: Future enhancement — move control gate into a shared dbt package or pre-hook with proper error messaging

---

### Risk 3 — `pipeline.py` Complexity Not Fully Covered by Verification

**Severity:** MEDIUM-HIGH  
**Category:** Test Coverage Gap  
**Introduced by:** Decision 1 (Orchestrator-based architecture)

**Description:**
Section 10 verification commands in the requirements brief cover data layer correctness only. Orchestrator correctness is not fully verified:
- Correct SKIPPED entry generation on partial failure
- Correct watermark non-advancement when Silver fails
- Correct `gold_weekly_control` updates
- Proper recovery via `UNLOGGED_RUN` synthetic row

These behaviours are critical to system correctness but are not explicitly tested.

**Why it matters:**
- Silent orchestration failures could result in:
  - Watermark advancing despite failed date (causes permanent gap on incremental runs)
  - Control table entries written without corresponding data (or vice versa)
  - Run log entries missing for successful runs (audit trail gap)
- Data may be correct, but control-plane state becomes corrupted
- Difficult to detect: System appears to function, but audit trail and idempotency guarantees are violated

**Current Mitigation:**
- Tasks 6.1–6.4 in EXECUTION_PLAN.md include explicit verification of orchestrator behaviours:
  - Task 6.2: Watermark advancement tests
  - Task 6.3: Idempotency proof (manual: re-run full pipeline, compare row counts)
  - Task 6.4: Gold weekly control correctness
- Task 7.3 includes audit trail verification
- Manual idempotency proof (re-run entire 7-day pipeline, verify no change)

**Residual Risk:**
- Manual tests are not automated; not run on every deployment
- Idempotency proof is labour-intensive (7+ days of data, 2+ full runs)
- No continuous integration testing of orchestrator behaviours

**Follow-Up Action:**
Phase 8+: Develop automated orchestrator test suite covering:
- Simulated Silver failure → verify watermark not advanced, SKIPPED entries present
- Simulated run log flush failure → verify UNLOGGED_RUN recovery
- Simulated partial dbt execution (one model fails) → verify downstream models SKIPPED
- These would be integration tests running against local DuckDB database

---

## 2. Moderate Risks (Medium Impact, Mitigated or Deferred)

### Risk 4 — Split Failure: Data Written, Run Log Not Written

**Severity:** MEDIUM  
**Category:** Control-Plane Integrity  
**Introduced by:** Decision 4 (Async buffered run log flush)

**Description:**
If `pipeline.py` successfully completes all three layers (Bronze, Silver, Gold) and advances watermark, but then crashes before flushing the run log buffer to parquet, the system is left in a state where:
- Data is present in all three layers with valid `_pipeline_run_id` values
- Run log entries for that run_id are absent from `run_log.parquet`
- Audit trail has a gap
- On next run: `UNLOGGED_RUN` synthetic row is written, partially explaining the situation

**Why it matters:**
- Audit trail completeness is compromised
- Analysts tracing a `_pipeline_run_id` will not find corresponding run_log entries
- The synthetic `UNLOGGED_RUN` row is less informative than per-model entries with timing and counts

**Design Trade-Off Rationale:**
- Decoupling run log from pipeline success ensures data correctness is never compromised by run log write failures
- Run log is audit output only, not a control signal
- Data correctness is more important than audit completeness

**Current Mitigation:**
- `.jsonl` fallback file captures failed flush details
- Next successful run detects situation via watermark `updated_by_run_id` mismatch
- Synthetic `UNLOGGED_RUN` row written with explanation
- Run log is still append-only; no data loss

**Residual Risk:**
- Audit trail remains incomplete; no per-model timing or counts for failed flush
- Engineer must manually inspect `.jsonl` fallback file to understand what actually ran
- If fallback file is also lost (disk failure, etc.), audit trail becomes irrecoverable

**This is an accepted trade-off.** Data correctness is prioritised over audit completeness (documented in Decision 4).

---

### Risk 5 — No Backfill Mechanism for Unresolvable Accounts

**Severity:** MEDIUM  
**Category:** Data Quality / Operational Limitation  
**Introduced by:** Design + Requirements (GAP 2, out of scope)

**Description:**
Legitimate transactions flagged as `_is_resolvable = false` because their account record arrived later are permanently excluded from Gold. No backfill mechanism exists to reprocess these records once their accounts are available.

**Scenario:**
- Day 3: Transaction arrives for account_id = 'A123', but account record not yet in Silver accounts
- Transaction flagged: `_is_resolvable = false`, excluded from Gold daily and weekly aggregates
- Day 4: Account record for 'A123' arrives and is loaded to Silver
- **Problem:** Day 3's transaction for 'A123' remains unresolvable and unfixed

**Why it matters:**
- Unresolvable rate is not zero in real-world scenarios (account files may be delayed, late-arriving dims)
- Financial impact: Transactions excluded from reported aggregates
- If unresolvable rate is high, Gold aggregates do not represent true financial state
- No way to fix historical data

**Current Mitigation:**
- `_is_resolvable` flag makes the issue visible (not silent)
- Gold Daily Summary includes `total_unresolvable_transactions` and `total_unresolvable_amount` for visibility
- Unresolvable rate is monitored (documented in GAP 1) but does not block pipeline
- Known limitation documented in INTAKE_SUMMARY.md

**Residual Risk:**
- No structural solution available within scope
- Data quality depends on source system's timing (account delivery schedule)

**Follow-Up Action:**
Phase 8+: Develop backfill pipeline (out of scope for this exercise, documented in ARCHITECTURE.md Section 7 Parking Lot) that:
- Reprocesses historical dates to resolve unresolvable accounts
- Watermark protection: prevent future-date processing during backfill
- Implements resolution with careful control over which dates are reprocessed

---

### Risk 6 — SCD Type 2 Deferred (No Account History)

**Severity:** MEDIUM  
**Category:** Analytical Limitation  
**Introduced by:** Design + Requirements (ARCHITECTURE.md Section 9)

**Description:**
Silver Accounts retains only the latest record per account_id. No history of account attribute changes (credit limit, balance, status, date_opened) is preserved. This means:
- Cannot reconstruct what an account's credit limit was on a historical date
- `closing_balance` in Gold Weekly reflects balance at first computation time only, not historical balance
- Any account status or limit change between weeks is not captured historically

**Why it matters:**
- Risk analysis may require historical credit limits or status
- Regulatory reporting may require historical account state
- The current design loses information irreversibly

**Current Mitigation:**
- Documented as a known limitation (ARCHITECTURE.md, INTAKE_SUMMARY.md)
- System is scoped for this exercise; SCD Type 2 is explicitly deferred
- Silver Bronze layer DOES preserve all historical deltas; could be replayed if needed
- Run log provides timestamps for each account update

**Residual Risk:**
- Historical account state cannot be easily reconstructed
- Future analytical requests may require this data

**Follow-Up Action:**
Phase 8+: Implement SCD Type 2 for Silver Accounts as a future enhancement:
- Add `effective_date`, `end_date` columns
- Track multiple versions of each account
- Enable historical reconstruction
- Update Gold weekly `closing_balance` computation to use correct account state for each week

---

## 3. Operational Limitations (Documented but Not Risks)

### Limitation 1 — No Automated Run Log Verification

**Description:** Section 10.5 verification expects analysts to query the run log manually. No automated verification script checks run log integrity.

**Mitigation:** `verify_audit_trail.sh` created in Task 7.3 — provides template for automated checks.

---

### Limitation 2 — Schema Evolution Not Supported

**Description:** CSV schema is fixed for this exercise. Production would require schema registry or versioned schemas.

**Mitigation:** Documented in ARCHITECTURE.md Section 9 (deferred).

---

### Limitation 3 — Streaming Ingestion Not Supported

**Description:** Pipeline is batch-only. Daily CSV files are the input mechanism. Near-realtime or streaming variants require different architecture.

**Mitigation:** Documented in ARCHITECTURE.md Section 9 (deferred, out of scope).

---

## 4. Technical Debt Identified During Build

### Debt Item 1 — Quarantine Write May Be Duplicated

**Category:** Implementation / Code Quality  
**Session Identified:** S4 (Silver Transactions)  
**Description:**
The dbt model writes rejected records to the quarantine partition. The write logic could potentially be duplicated if the model is re-run without proper idempotency safeguards (e.g., if partition existence check is not enforced).

**Current State:** Addressed by INV-40 (atomic partition write). Partition writes are atomic; re-run triggers dbt `table` materialisation which drops and recreates.

**Residual:** No actual implementation debt; invariant coverage is sufficient.

---

### Debt Item 2 — Rejection Logic May Be Duplicated Across Models

**Category:** Code Duplication  
**Session Identified:** S4 (Silver Transactions)  
**Description:**
Multiple dbt models implement similar validation and quarantine logic (NULL checks, code validation, deduplication). If rejection rules change, multiple models must be updated.

**Current State:** This is accepted as part of explicit model design — each Silver model owns its quarantine logic for its entity. No central rejection dispatcher.

**Mitigation:** Documented rejection codes are exhaustive (Section 5 of requirements brief, INV-26). Changes to rejection logic are infrequent and well-coordinated.

**Residual:** Low-priority code quality issue; not a correctness risk.

---

### Debt Item 3 — dbt Post-Hook Atomicity Risk

**Category:** Design / Architecture  
**Session Identified:** S6 (Pipeline Orchestration)  
**Decision Context:** Decision 3 rejected dbt post-hooks for run log writing

**Description:**
Using dbt post-hooks to write run log entries would entangle run log integrity with transformation success. If a post-hook fails after a model completes, dbt has no mechanism to roll back the transformation. This creates an inconsistency: data is written (correct) but run log entry is not (incorrect).

**Current Mitigation:** Architecture decision 4 (async buffered flush) explicitly rejects post-hooks. Run log entries are written by `pipeline.py` consuming dbt JSON log stream, not by dbt hooks.

**Residual:** No debt; design is intentional and documented.

---

### Debt Item 4 — Control-Plane File Corruption Could Halt Pipeline

**Category:** Operational Robustness  
**Session Identified:** S6 (Pipeline Orchestration)  
**Description:**
If `pipeline/control.parquet` or `pipeline/gold_weekly_control.parquet` becomes corrupted or unreadable, `pipeline.py` must halt with a clear error (per INV-43). The alternative (silent reinitialisation with assumed defaults) would risk recomputing all weeks or reprocessing all dates.

**Current Mitigation:** INV-43 enforcement — corrupted control files halt pipeline with FAILED run log entry.

**Residual:** Operator must manually repair control files or delete them and reinitialise. No self-healing mechanism.

**Follow-Up:** Consider adding:
1. Control file backup before any write
2. Automatic validation of control file structure at startup
3. Diagnostic tools to inspect and repair control files

---

## 5. Risk Monitoring and Escalation

### Risk Escalation Criteria

| Severity | Signal | Action |
|---|---|---|
| CRITICAL | Data loss or audit trail gap detected | Halt pipeline, investigate immediately |
| HIGH | Verification failures (REGRESSION_SUITE.sh fails) | Investigate before advancing watermark |
| MEDIUM | Manual test failure (e.g., idempotency proof) | Document issue, assess impact, plan remediation |
| LOW | Code quality issues or deferred patterns | Schedule as technical debt / Phase 8+ |

### Monitoring Recommendations

**Daily/Per-Run:**
- Run `verification/REGRESSION_SUITE.sh` after pipeline completion
- Check watermark advancement (should increase by 1 per successful run)
- Check run_log for FAILED or UNLOGGED_RUN entries

**Weekly:**
- Review unresolvable transaction rate (should be <5% for healthy data)
- Verify quarantine entries have valid rejection codes
- Spot-check Gold aggregates against Silver layer

**Monthly:**
- Idempotency proof (re-run recent week, verify no data change)
- Audit trail completeness check (verify all `_pipeline_run_id` values have run_log entries)
- Control-plane state consistency (verify watermark aligns with data present)

---

## 6. Risk Register Sign-Off

**Prepared by:** Pratham Bajaj (Engineer)  
**Date:** Phase 8 completion  
**Status:** All identified risks documented. No BLOCKING risks. Three HIGH risks require awareness and procedural discipline. System ready for Phase 8 sign-off.

| Risk ID | Severity | Status | Owner | Next Review |
|---|---|---|---|---|
| Risk 1 (JSON schema drift) | HIGH | Mitigated (procedural) | Engineering team | On dbt upgrade |
| Risk 2 (Control gate bypass) | HIGH | Mitigated (documentation) | Engineering team | Monthly |
| Risk 3 (Orchestrator coverage) | MEDIUM-HIGH | Mitigated (manual tests) | Engineering team | Phase 8+ |
| Risk 4 (Split failure) | MEDIUM | Accepted trade-off | Documented | Ongoing |
| Risk 5 (No backfill) | MEDIUM | Deferred | Phase 8+ backlog | On demand |
| Risk 6 (SCD Type 2 deferred) | MEDIUM | Deferred | Phase 8+ backlog | On demand |

---

*Risk Register documents Phase 8 discovery of system risks, limitations, and technical debt.*
