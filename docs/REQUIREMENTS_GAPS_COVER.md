# Requirements Gaps Cover
## Credit Card Lake Exercise
**Brief Version:** 1.0
**Session Date:** 2026-04-09
**Status:** All gaps closed — ready for Phase 1

---

## Purpose

This document records all gaps identified in the Requirements Brief (v1.0) during pre-Phase 1 interrogation, the reasoning behind each resolution, and the decisions authored and confirmed by the engineer. It serves as the authoritative record of brief amendments and design decisions made before planning begins.

This document does not modify the original brief. It sits alongside it as a gap resolution record.

---

## Gap Register

### GAP 1 — Day 1 Accounts File: Full Snapshot vs True Delta

**Brief Section:** 2.3 (Accounts)

**Gap:** The brief defines the accounts file as a daily delta containing new and changed records only. It does not specify what the Day 1 file contains during historical pipeline initialisation — whether it is a full snapshot establishing baseline state, or a true delta from a prior invisible state.

**Why it matters:** If Day 1 is a true delta from an unseen prior state, Silver Accounts will be incomplete at Day 1, causing Silver transaction promotion to produce false UNRESOLVABLE_ACCOUNT_ID flags at high volume. This would undermine the integrity of the historical load.

**Engineer Decision:**
The Day 1 accounts file is treated as a full snapshot that establishes the complete baseline state for the historical load. Days 2–7 are true deltas. This assumption is required for the historical pipeline to initialise a complete and usable dataset from the provided files.

**Additional Decision — Unresolvable Rate Monitoring:**
The proportion of `_is_resolvable = false` records shall be monitored for data quality purposes but does not block pipeline execution. A threshold-based pipeline failure rule was considered and deliberately excluded from pipeline control logic. This is a documented design decision, not an omission.

---

### GAP 2 — `_is_resolvable` Analytical Impact Not Exposed in Gold

**Brief Section:** 4.2.1 (Silver Transactions), 4.3.1 (Gold Daily Summary)

**Gap:** The brief correctly defines `_is_resolvable = false` as a flag-only condition that excludes records from Gold. However, it does not expose the count or amount of excluded records anywhere in the Gold layer, leaving analysts unable to reconcile Gold totals against Silver without inspecting lower layers.

**Why it matters:** Analysts querying Gold will see aggregate totals that silently exclude a subset of Silver records. Without visibility into the exclusion volume, they cannot assess data completeness or identify days with significant unresolvable activity.

**Engineer Decision:**
Two additional columns are added to the Gold Daily Summary table (`gold/daily_summary/data.parquet`):

| Column | Type | Description |
|---|---|---|
| `total_unresolvable_transactions` | INTEGER | Count of Silver transactions where `_is_resolvable = false` for this date |
| `total_unresolvable_amount` | DECIMAL | Sum of `_signed_amount` for Silver transactions where `_is_resolvable = false` for this date |

These columns represent `_is_resolvable = false` Silver records only. Quarantined records are excluded — they never enter Silver and are already auditable via the run log and quarantine partitions.

These columns are not added to the Gold Weekly Account Summary — per-account unresolvable tracking is not aligned with the current weekly aggregation design.

`_is_resolvable = false` is treated as a permanent, point-in-time state within this exercise. Backfill is out of scope. This is a known limitation of the pipeline design and is documented here rather than in external documentation.

---

### GAP 3 — `closing_balance` Idempotency Conflict in Gold Weekly Summary

**Brief Section:** 4.3.2 (Gold Weekly Account Aggregates), 4.2.2 (Silver Accounts)

**Gap:** The brief defines `closing_balance` as `current_balance` from Silver Accounts "as of week_end_date (or most recent available)." Silver Accounts is non-partitioned and retains only the latest record per account. There is no mechanism to retrieve what an account's balance was at a specific historical date. Using the latest balance at query time means Gold results would change across runs, violating the idempotency requirement in Section 8.

**Why it matters:** A Gold weekly summary that produces different `closing_balance` values depending on when the Gold model runs cannot be treated as a stable analytical output. This directly conflicts with the idempotency requirement.

**Engineer Decision:**
"Most recent available" is interpreted as the balance at the time the week is first computed. Gold outputs for past periods are not recomputed once written. This preserves idempotency at the cost of temporal accuracy — if an account's balance is updated after a week's Gold record is first produced, the `closing_balance` for that week will not reflect the later update.

This is a known and documented limitation of the simplified Silver Accounts model (no SCD Type 2).

**Structural Addition — Gold Weekly Control Table:**
A separate control table `gold_weekly_control` is introduced at the week grain to track which weekly aggregates have been computed. Each row represents a `week_end_date` and its computation status. Previously computed weeks are not recomputed. This table is separate from the existing pipeline control table, which operates at the date grain — combining them would create a grain mismatch.

The schema of `gold_weekly_control` is held for definition in the project architecture artifact. It is not added to the brief.

---

### GAP 4 — No Quality Rule for Null `merchant_name` on PURCHASE Transactions

**Brief Section:** 2.1 (Transactions), 5.1 (Transaction Rejection Rules)

**Gap:** The brief states that `merchant_name` is null for non-purchase types, implying it should be populated for PURCHASE transactions. However, Section 5.1 defines no rejection rule or flag for a PURCHASE transaction where `merchant_name` is null. Section 5 also states the rejection code list is exhaustive.

**Why it matters:** A PURCHASE transaction with a null `merchant_name` currently passes Silver promotion silently. This is a data quality issue with no visibility in any layer.

**Engineer Decision:**
A new non-blocking derived column `_missing_merchant_name` (BOOLEAN, NOT NULL) is added to the Silver Transactions schema. This does not introduce a new rejection code and does not reuse an existing one — the exhaustive list in Section 5.1 is preserved.

Population logic:
- `TRUE` when `transaction_type = 'PURCHASE'` AND `merchant_name IS NULL`
- `FALSE` in all other cases, including non-PURCHASE transactions where null `merchant_name` is expected

This column does not affect promotion or rejection logic. It is provided for data quality visibility only.

**Schema Note Added to Section 4.2.1:**
"The Silver Transactions table includes a derived column `_missing_merchant_name` (BOOLEAN, NOT NULL) to surface non-blocking data quality issues that do not result in rejection."

---

### GAP 5 — Intra-Date Processing Order Not Specified

**Brief Section:** 3.1 (Historical Load Pipeline)

**Gap:** The brief specifies that transaction codes must be loaded to Silver before any transaction processing begins. However, it does not specify whether accounts must be promoted to Silver before transactions for the same date. Silver transaction promotion validates `account_id` against Silver Accounts — if transactions are promoted before accounts for the same date, false UNRESOLVABLE_ACCOUNT_ID flags will result.

**Why it matters:** The ordering gap would cause legitimate transactions to be flagged as unresolvable on the same date their account record first appears.

**Engineer Decision:**
Within each date, the processing order is:

1. Accounts Bronze load
2. Accounts Silver promotion
3. Transactions Bronze load
4. Transactions Silver promotion

This ordering is enforced by the pipeline runner (`pipeline.py`) as the primary guarantee. dbt model dependencies provide a secondary enforcement — `silver_transactions` depends on `silver_accounts`. dbt dependency ordering applies only within a dbt invocation and cannot enforce ordering relative to Bronze ingestion, which is managed externally in Python.

---

### GAP 6 — Run Log Authoritativeness Not Defined

**Brief Section:** 6.2 (Pipeline Run Log)

**Gap:** The brief states the run log is append-only and never overwritten. On a re-run of an already-processed date, the run log will contain multiple rows for the same model and date. The brief does not define which row is authoritative or how analysts should interpret multiple entries for the same execution unit.

**Why it matters:** The brief points analysts to `_pipeline_run_id` as the connective tissue for tracing Gold aggregates back to Silver and Bronze. Without clarity on which run_id is authoritative, analysts may over-rely on the run log and reach incorrect conclusions in cases of partial failure or re-run.

**Engineer Decision:**
The run log is an append-only execution history and does not represent the authoritative state of the pipeline. The authoritative pipeline state is defined by the pipeline control table watermark and the data present in the Gold layer.

"Latest successful run" must be interpreted at the layer level, not globally. In cases of partial success, different layers may reflect different run_ids. Analysts should treat the latest fully successful run (as reflected in Gold and the watermark) as the source of truth.

**Addition to Section 6.2:**
"The run log is an append-only execution history and does not represent the authoritative state of the pipeline. The authoritative state is defined by the pipeline control table and the data present in the Gold layer."

---

### GAP 7 — Date Range Not Defined in Brief

**Brief Section:** 10 (Verification Expectations)

**Gap:** Section 10.1 references "7 date partitions" and "7 source CSV files" without stating the actual date range of the historical load. The brief is not self-contained for planning purposes.

**Engineer Decision:**
The date range is treated as a parameter defined by the scaffold's source files. This is acceptable for a controlled training environment where the scaffold is a fixed companion artifact. The date range should be explicitly captured in the project architecture artifact or execution plan parameter table — not assumed from the scaffold at build time.

---

## Schema Additions Summary

| Layer | Table | Column | Type | Nullable | Notes |
|---|---|---|---|---|---|
| Silver | Transactions | `_missing_merchant_name` | BOOLEAN | NOT NULL | TRUE when PURCHASE and merchant_name IS NULL; FALSE otherwise |
| Gold | Daily Summary | `total_unresolvable_transactions` | INTEGER | — | Count of `_is_resolvable = false` Silver records per date |
| Gold | Daily Summary | `total_unresolvable_amount` | DECIMAL | — | Sum of `_signed_amount` for `_is_resolvable = false` Silver records per date |

---

## Structural Additions Summary

| Artifact | Description | Location |
|---|---|---|
| `gold_weekly_control` | Week-grain control table tracking computed Gold weekly aggregates. Prevents recomputation of past weeks and preserves idempotency. Schema to be defined in architecture artifact. | To be documented in ARCHITECTURE.md |

---

## Decision Log

| Gap | Decision Type | Outcome |
|---|---|---|
| GAP 1 | Assumption | Day 1 accounts = full snapshot. Unresolvable rate monitored, does not block. |
| GAP 2 | Schema addition | Two columns added to Gold Daily Summary for unresolvable visibility. |
| GAP 3 | Design constraint + structural addition | Closing balance fixed at first computation time. `gold_weekly_control` table introduced. |
| GAP 4 | Schema addition | `_missing_merchant_name` added to Silver Transactions. Non-blocking. |
| GAP 5 | Ordering rule | Accounts before transactions within each date. Pipeline runner enforces; dbt reinforces. |
| GAP 6 | Documentation addition | Run log is execution history only. Control table and Gold layer are authoritative state. |
| GAP 7 | Documentation task | Date range to be captured as parameter in architecture or execution plan artifact. |

---

*Document authored by engineer. Gaps identified and challenged in CD session prior to Phase 1. All decisions signed off by engineer before project initialisation.*
