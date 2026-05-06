# Session Execution Prompt — S05: Gold Layer

## Execution Mode
Autonomous

## Agent Identity
You are Claude Code executing build Session 5 for the Credit Card Financial Transactions Lake project. You follow the PBVI methodology. Execute each CC prompt from EXECUTION_PLAN.md exactly as written.

## Repository Context
Branch: `session/05-gold`
Create this branch from main (after S04 PR is merged) before executing any task.

METHODOLOGY_VERSION check: read PROJECT_MANIFEST.md, locate METHODOLOGY_VERSION. Expected: v4.3. If absent or different, output a METHODOLOGY VERSION WARNING block, then continue.

## What Has Already Been Built
Sessions 1–4 are complete. `data/silver/transactions/date=YYYY-MM-DD/data.parquet` exists for all 7 dates (2024-01-01 through 2024-01-07). No `transaction_id` appears more than once globally across all Silver transaction partitions. All `_signed_amount` values are non-null and correctly signed per `debit_credit_indicator` (DR = positive, CR = negative). All rejected records are in `data/silver/quarantine/` with valid reason codes from the exhaustive list in Section 5 of the brief. `_is_resolvable` and `_missing_merchant_name` flags are correctly set for all Silver transaction records. Bronze count equals Silver count plus quarantine count for every date (integration test passes). S01–S04 branches are merged to main.

## Planning Artifacts
- docs/ARCHITECTURE.md — DECIDED
- docs/INVARIANTS.md — SIGNED (Pratham, 15/04/2026)
- docs/EXECUTION_PLAN.md — AMENDED (Phase 4 gate applied, 16/04/2026)
- docs/Claude.md — FROZEN v1.0 (16/04/2026)

## Scope Boundary
This session creates or modifies: `dbt/models/gold/gold_daily_summary.sql`, `dbt/models/gold/gold_weekly_account_summary.sql`, `verification/verify_gold.sh`.
No other files may be created or modified (except session log and verification record).

CRITICAL: `incremental` materialisation is prohibited in `dbt/models/gold/`. Any use constitutes a scope violation — stop immediately and report.

## Task Prompt Immutability
Execute each CC prompt exactly as written in EXECUTION_PLAN.md. Do not extend scope, add functionality not specified, or fix adjacent issues.

## Session Tasks
Execute all three tasks in order:
- Task 5.1 — Gold Model: Daily Transaction Summary
- Task 5.2 — Gold Model: Weekly Account Transaction Aggregates
- Task 5.3 — Gold Layer Verification Script

## Artifact Paths
- Session Log: sessions/S05_session_log.md
- Verification Record: sessions/S05_verification_record.md

## Stop Conditions
SESSION BLOCKED, SCOPE VIOLATION, CHALLENGE FINDINGS — same handling as S01.
