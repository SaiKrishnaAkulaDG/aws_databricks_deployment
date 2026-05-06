# INVARIANT_CATALOGUE.md — Quick Reference Guide

**PBVI Phase:** 8 — Discovery
**Document Type:** Invariant reference and enforcement index
**Created:** Phase 8 completion
**Status:** FINAL

---

## Overview

This catalogue provides a condensed reference for all 38 invariants from INVARIANTS.md, organized by layer and with enforcement points marked for quick navigation.

**Key:**
- **Embedded in:** Where the invariant constraint appears in the codebase (Claude.md, task prompts, model files)
- **Test:** DuckDB query or bash check to verify the invariant
- **Category:** Data correctness vs. Operational
- **Scope:** GLOBAL (all tasks) or TASK-SCOPED (specific components)

---

## GLOBAL INVARIANTS (2)

These apply to every task without exception.

| ID | Invariant | Category | Scope | Embedded In | Test |
|---|---|---|---|---|---|
| **INV-01** | Source files are read-only | Data | GLOBAL | Claude.md § 2 | `git status source/` → no modifications |
| **INV-22** | All Silver/Gold records have traceable `_pipeline_run_id` | Data | GLOBAL | Claude.md § 2 | `SELECT DISTINCT _pipeline_run_id FROM silver/**/*.parquet EXCEPT SELECT run_id FROM pipeline/run_log.parquet WHERE status='SUCCESS'` → 0 rows |

---

## BRONZE LAYER INVARIANTS (4)

| ID | Invariant | Category | Enforcement Point | Test |
|---|---|---|---|---|
| **INV-02** | Idempotency via partition existence check | Data | Bronze loader logic | Partition does not exist → read CSV; partition exists → skip |
| **INV-03** | Partitions immutable after first write | Data | File system + monitoring | Bronze file mtime must not change on re-run |
| **INV-04** | Audit columns non-null | Data | Bronze write | `SELECT COUNT(*) FROM bronze/**/*.parquet WHERE _source_file IS NULL OR _ingested_at IS NULL OR _pipeline_run_id IS NULL` → 0 |
| **INV-40** | Partition writes are atomic | Data | Bronze writer (temp + rename) | Partial write detected via mtime check; full re-write on re-run |

---

## SILVER TRANSACTIONS INVARIANTS (8)

| ID | Invariant | Category | Enforcement Point | Test |
|---|---|---|---|---|
| **INV-05** | Total partition accounting (Bronze = Silver + Quarantine) | Data | Silver model logic + verification | `SELECT bronze_count - (silver_count + quarantine_count) FROM ...` → 0 per date |
| **INV-06** | Global deduplication on `transaction_id` | Data | dbt window function + dbt test | `SELECT transaction_id, COUNT(*) FROM silver/transactions/**/*.parquet GROUP BY transaction_id HAVING COUNT(*) > 1` → 0 rows |
| **INV-08** | Sign assignment from transaction codes only | Data | Silver dbt join to Silver TC | Query: verify all DR codes have positive `_signed_amount`, all CR codes have negative |
| **INV-09** | Invalid transaction codes → quarantine | Data | Silver dbt LEFT JOIN + filter | `SELECT COUNT(*) FROM silver/transactions WHERE transaction_code NOT IN (SELECT transaction_code FROM silver/transaction_codes)` → 0 |
| **INV-10** | Unresolvable `account_id` → flag, not quarantine | Data | Silver dbt LEFT JOIN + `_is_resolvable` flag | `SELECT COUNT(*) FROM silver/quarantine WHERE _rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID'` → 0 |
| **INV-26** | Quarantine rejection codes exhaustive | Data | dbt model validation | `SELECT DISTINCT _rejection_reason FROM silver/quarantine/**/*.parquet` → only values in {NULL_REQUIRED_FIELD, INVALID_AMOUNT, DUPLICATE_TRANSACTION_ID, INVALID_TRANSACTION_CODE, INVALID_CHANNEL, INVALID_ACCOUNT_STATUS} |
| **INV-37** | Silver reads only Silver transaction codes | Data | dbt model source reference | `grep -r "bronze/transaction_codes" dbt/models/silver/` → no results |
| **INV-45** | Missing merchant name flagged (non-blocking) | Data | Silver dbt computed column | `SELECT COUNT(*) FROM silver/transactions WHERE transaction_type='PURCHASE' AND merchant_name IS NULL AND _missing_merchant_name = FALSE` → 0 |

---

## SILVER ACCOUNTS INVARIANTS (2)

| ID | Invariant | Category | Enforcement Point | Test |
|---|---|---|---|---|
| **INV-07** | One record per `account_id` (no duplicates) | Data | Silver dbt upsert | `SELECT account_id, COUNT(*) FROM silver/accounts/data.parquet GROUP BY account_id HAVING COUNT(*) > 1` → 0 rows |
| **INV-36** | Upsert replaces existing record | Data | dbt merge logic | After delta load, query latest account state → matches delta file, not prior state |

---

## GOLD DAILY SUMMARY INVARIANTS (2)

| ID | Invariant | Category | Enforcement Point | Test |
|---|---|---|---|---|
| **INV-13** | One row per processed `transaction_date` | Data | dbt model cardinality | `SELECT transaction_date, COUNT(*) FROM gold/daily_summary GROUP BY transaction_date HAVING COUNT(*) > 1` → 0 rows |
| **INV-44** | Row exists for every date, even if zero resolvable txns | Data | dbt UNION with all dates | For any fully-quarantined date, `SELECT * FROM gold/daily_summary WHERE transaction_date = 'X'` → 1 row with `total_transactions = 0` |

---

## GOLD WEEKLY ACCOUNT SUMMARY INVARIANTS (3)

| ID | Invariant | Category | Enforcement Point | Test |
|---|---|---|---|---|
| **INV-14** | One row per (account_id, week_start_date); immutable after first compute | Data | `pipeline.py` control gate + dbt materialisation | After two runs covering same week: `SELECT week_start_date, account_id, COUNT(*) FROM gold/weekly_account_summary GROUP BY week_start_date, account_id HAVING COUNT(*) > 1` → 0 rows |
| **INV-38** | Row exists only for accounts with ≥1 resolvable Silver transactions | Data | dbt LEFT JOIN + filter | `SELECT g.* FROM gold/weekly_account_summary g WHERE NOT EXISTS (SELECT 1 FROM silver/transactions WHERE account_id=g.account_id AND DATE_TRUNC('week', transaction_date)=g.week_start_date AND _is_resolvable=true)` → 0 rows |
| **INV-39** | Every weekly account has at least one daily entry in same week | Data | dbt cross-file consistency check | `SELECT w.* FROM gold/weekly_account_summary w WHERE NOT EXISTS (SELECT 1 FROM gold/daily_summary d WHERE d.transaction_date >= w.week_start_date AND d.transaction_date <= w.week_end_date AND d.total_transactions > 0)` → 0 rows |

---

## GOLD GENERAL INVARIANTS (2)

| ID | Invariant | Category | Enforcement Point | Test |
|---|---|---|---|---|
| **INV-11** | Gold uses only `_is_resolvable = true` for aggregates (except unresolvable columns) | Data | dbt computed column filter | `SELECT * FROM gold/daily_summary WHERE total_signed_amount != (SELECT SUM(_signed_amount) FROM silver/transactions WHERE transaction_date=gold.transaction_date AND _is_resolvable=true)` → 0 rows |
| **INV-12** | Gold reads only from Silver (no Bronze, no CSV) | Data | dbt model source reference | `grep -r "bronze/" dbt/models/gold/ ; grep -r "source/" dbt/models/gold/` → no results |

---

## WATERMARK AND CONTROL-PLANE INVARIANTS (9)

| ID | Invariant | Category | Enforcement Point | Test |
|---|---|---|---|---|
| **INV-15** | Watermark advances only on full success | Operational | `pipeline.py` sequential orchestration | If Silver fails: watermark stays at prior date; re-run target date = prior + 1 |
| **INV-17** | Incremental pipeline processes exactly one date | Operational | `pipeline.py` date calculation | Watermark = 2024-01-02 → next run processes 2024-01-03 only, not 2024-01-04 or 2024-01-02 |
| **INV-18** | Missing source file is a no-op | Operational | Bronze loader file check | Source file absent → no Bronze write, no Silver, no Gold, watermark unchanged |
| **INV-19** | Run log append-only | Operational | `pipeline.py` buffer flush logic | `SELECT COUNT(*) FROM pipeline/run_log.parquet` must be monotonically non-decreasing across runs |
| **INV-20A** | Successful runs produce one SUCCESS row per executed model | Operational | `pipeline.py` event parser + buffer | For each executed model: exactly one run_log row with status=SUCCESS for that run_id |
| **INV-20B** | Failed runs produce at least one FAILED row | Operational | `pipeline.py` error handler | On compile failure: `model_name=DBT_COMPILE, layer=ORCHESTRATION, status=FAILED` row written |
| **INV-20C** | Run log flush failure recovery via UNLOGGED_RUN | Operational | `.jsonl` fallback + recovery logic | After simulated flush failure: next successful run writes UNLOGGED_RUN synthetic row before proceeding |
| **INV-31** | `gold_weekly_control` written before watermark advances | Operational | `pipeline.py` sequential writes | Control table entry exists for every week in Gold output |
| **INV-32** | Absent control files are valid initial state | Operational | `pipeline.py` initialisation | First run with no `pipeline/` files succeeds; both control tables created |

---

## ORCHESTRATION INVARIANTS (5)

| ID | Invariant | Category | Enforcement Point | Test |
|---|---|---|---|---|
| **INV-24** | Intra-pipeline processing order enforced | Operational | `pipeline.py` sequential calls | Accounts Bronze → Accounts Silver → Transactions Bronze → Transactions Silver (per date) |
| **INV-33** | All Gold files fully written before watermark advances | Operational | dbt completion event + fsync | Gold partition files exist and are non-zero size at watermark write time |
| **INV-34** | Run log records_* fields from Parquet query, not dbt metadata | Data | DuckDB post-model query | `records_written = SELECT COUNT(*) FROM output_parquet`, not dbt row_count |
| **INV-35** | Non-executed models produce SKIPPED rows | Operational | `pipeline.py` on subprocess exit | After Silver failure: Gold models have run_log rows with status=SKIPPED |
| **INV-41** | Gold writes atomic (table materialisation, no incremental) | Data | dbt_project.yml config | `grep -r "materialized='incremental'" dbt/models/gold/` → no results; `materialized='table'` required |

---

## CONTROL-PLANE ROBUSTNESS INVARIANTS (2)

| ID | Invariant | Category | Enforcement Point | Test |
|---|---|---|---|---|
| **INV-42** | Gold overwrite required if control entry absent | Operational | dbt table materialisation | If Gold has data for week but control table has no entry: re-run must overwrite, not append |
| **INV-43** | Corrupt control files halt pipeline | Operational | `pipeline.py` read validation | Unreadable control table → FAILED run log entry, no silent fallback |

---

## IMPLEMENTATION GUIDANCE (Reclassified Invariants)

These are design principles and enforcement procedures, not independently testable invariants. Recorded for reference.

| ID | Principle | Embedding Target |
|---|---|---|
| **IG-01** | Watermark value is exact and authoritative | `pipeline.py` control table reader |
| **IG-02** | Run log buffer must deduplicate on (run_id, model_name) | Run log buffer implementation |
| **IG-03** | DAG-derived execution order from `dbt compile` | `pipeline.py` manifest loader |
| **IG-04** | Control-plane files are sole state source | `pipeline.py` state reader (no filesystem inference) |
| **IG-05** | Idempotency is system-wide design principle | Claude.md § 2 (design principle, not testable) |
| **IG-06** | Post-run accounting check is mandatory | Verification script (operationalisation of INV-05) |
| **IG-07** | Gold weekly model must only run through `pipeline.py` | Model file warning comment + procedure |
| **IG-08** | dbt version pin + JSON schema verification | requirements.txt pin + upgrade procedure |
| **IG-09** | Gold models must use `table` materialisation | `dbt_project.yml` + model config |
| **IG-10** | Out-of-sequence source files silently ignored | Bronze loader date filter |

---

## Verification Command Index

### Quick Verification: All GLOBAL Invariants
```bash
# INV-01: Source files modified?
git status source/

# INV-22: Silver/Gold run_id traceability
duckdb << EOF
SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
EXCEPT
SELECT COUNT(*) FROM read_parquet('/app/data/pipeline/run_log.parquet')
WHERE status='SUCCESS';
EOF
```

### Quick Verification: Bronze Layer (INV-02 through INV-04, INV-40)
```bash
bash verification/verify_bronze.sh
```

### Quick Verification: Silver Layer (INV-05 through INV-10, INV-26, INV-37)
```bash
bash verification/verify_silver_transactions.sh
bash verification/verify_silver_accounts.sh
```

### Quick Verification: Gold Layer (INV-11 through INV-14, INV-38, INV-39, INV-41, INV-42)
```bash
bash verification/verify_gold.sh
```

### Quick Verification: Control Plane (INV-15 through INV-20C, INV-31 through INV-35, INV-43)
```bash
bash verification/verify_audit_trail.sh
```

### Complete System Verification
```bash
# Portable regression suite covering all REGRESSION-RELEVANT invariants
bash verification/REGRESSION_SUITE.sh
```

### Full Section 10 Verification (all requirements brief checks)
```bash
bash verification/verify_section10.sh
```

---

## Invariant Violation Diagnosis Guide

### Symptom: "Gold total doesn't match source CSV sum"
**Likely violation:** INV-11 (unresolvable records in aggregates) or INV-08 (sign assignment error)
**Diagnosis:**
```sql
-- Check if unresolvable records leaked into Gold
SELECT COUNT(*) FROM silver/transactions WHERE _is_resolvable = false AND transaction_date = 'X';
-- Check sign assignment correctness
SELECT transaction_code, debit_credit_indicator, COUNT(*) FROM silver/transactions
GROUP BY transaction_code, debit_credit_indicator;
```

### Symptom: "Duplicate transaction_id in Silver"
**Likely violation:** INV-06 (deduplication failed)
**Diagnosis:**
```sql
SELECT transaction_id, COUNT(*) FROM silver/transactions/**/*.parquet
GROUP BY transaction_id HAVING COUNT(*) > 1;
```

### Symptom: "Bronze = Silver + Quarantine accounting fails"
**Likely violation:** INV-05 (silent data loss in Silver promotion)
**Diagnosis:**
```bash
bash verification/verify_section10.sh | grep "ACCOUNTING"
```

### Symptom: "Same week computed twice with different closing_balance"
**Likely violation:** INV-14 (control gate bypassed or incremental materialisation used)
**Diagnosis:**
```sql
-- Check for duplicate (account_id, week_start_date)
SELECT week_start_date, account_id, COUNT(*) FROM gold/weekly_account_summary
GROUP BY week_start_date, account_id HAVING COUNT(*) > 1;
-- Check materialization
grep -r "incremental" dbt/models/gold/
-- Check control table has entry for week
SELECT * FROM pipeline/gold_weekly_control.parquet WHERE week_start_date = '2024-01-01';
```

### Symptom: "Pipeline failed but run_log has no entry for this date"
**Likely violation:** INV-20B (failed run not logged) or INV-20C (flush failure not recovered)
**Diagnosis:**
```bash
# Check for .jsonl fallback file
ls -la data/pipeline/pipeline_runlog_fallback.jsonl
# If exists, next run should write UNLOGGED_RUN synthetic row
duckdb -c "SELECT * FROM read_parquet('data/pipeline/run_log.parquet') WHERE model_name='UNLOGGED_RUN';"
```

### Symptom: "Watermark didn't advance but data was written"
**Likely violation:** INV-15 (watermark advanced despite partial failure) or split failure between data write and control update
**Diagnosis:**
```sql
-- Check watermark timestamp
SELECT last_processed_date, updated_at, updated_by_run_id FROM read_parquet('data/pipeline/control.parquet');
-- Check run_log for that run_id
SELECT * FROM read_parquet('data/pipeline/run_log.parquet') WHERE run_id = 'VALUE_FROM_CONTROL';
```

---

## Coverage Matrix: Invariants → Verification Tasks

This matrix shows which invariants are covered by which verification script/phase task.

| Section | Invariants | Coverage | Task |
|---|---|---|---|
| Bronze | INV-02–04, INV-40 | Complete | S2 task prompts + verify_bronze.sh |
| Silver TX | INV-05–06, INV-08–10, INV-26, INV-37 | Complete | S4 task prompts + verify_silver_transactions.sh |
| Silver Accts | INV-07, INV-36 | Complete | S3 task prompts + verify_silver_accounts.sh |
| Gold Daily | INV-13, INV-44 | Complete | S5 task prompts + verify_gold.sh |
| Gold Weekly | INV-14, INV-38–39 | Complete | S5/S6 task prompts + verify_gold.sh |
| Gold General | INV-11–12, INV-41 | Complete | S5 task prompts + verify_gold.sh |
| Orchestration | INV-24, INV-33–35, INV-43 | Partial (idempotency manual) | S6 task prompts + verify_audit_trail.sh |
| Watermark | INV-15–18, INV-32 | Partial (no-op manual) | S6 task prompts + verify_audit_trail.sh |
| Control Plane | INV-20A–C, INV-31, INV-42 | Complete | S6/S7 task prompts + verify_audit_trail.sh |
| Global | INV-01, INV-22 | Complete | REGRESSION_SUITE.sh |

---

*Invariant Catalogue provides Phase 8 discovery reference for all system constraints and their enforcement points.*
