# Session Execution Prompt — S03: Silver — Reference Data and Accounts

## Execution Mode
Autonomous

## Agent Identity
You are Claude Code executing build Session 3 for the Credit Card Financial Transactions Lake project. You follow the PBVI methodology. Execute each CC prompt from EXECUTION_PLAN.md exactly as written.

## Repository Context
Branch: `session/03-silver-ref`
Create this branch from main (after S02 PR is merged) before executing any task.

METHODOLOGY_VERSION check: read PROJECT_MANIFEST.md, locate METHODOLOGY_VERSION. Expected: v4.3. If absent or different, output a METHODOLOGY VERSION WARNING block, then continue.

## What Has Already Been Built
Sessions 1 and 2 are complete. All three Bronze loaders are implemented and verified. Running any Bronze loader twice against the same source file produces identical row counts — no duplicates. All Bronze audit columns (`_source_file`, `_ingested_at`, `_pipeline_run_id`) are non-null for every record. Bronze partitions for transactions and accounts exist for 2024-01-01 through 2024-01-07 at `data/bronze/transactions/date=YYYY-MM-DD/data.parquet` and `data/bronze/accounts/date=YYYY-MM-DD/data.parquet`. Bronze transaction codes are in `data/bronze/transaction_codes/data.parquet`. Source files in `source/` are unmodified (`git status source/` is clean). S01 and S02 branches are merged to main.

## Planning Artifacts
- docs/ARCHITECTURE.md — DECIDED
- docs/INVARIANTS.md — SIGNED (Pratham, 15/04/2026)
- docs/EXECUTION_PLAN.md — AMENDED (Phase 4 gate applied, 16/04/2026)
- docs/Claude.md — FROZEN v1.0 (16/04/2026)

## Scope Boundary
This session creates or modifies: `dbt/models/silver/silver_transaction_codes.sql`, `dbt/models/silver/silver_accounts.sql`, `dbt/dbt_project.yml` (materialisation config only), `verification/verify_silver_ref.sh`.
No other files may be created or modified (except session log and verification record).

## Task Prompt Immutability
Execute each CC prompt exactly as written in EXECUTION_PLAN.md. Do not extend scope, add functionality not specified, or fix adjacent issues.

## Session Tasks
Execute all three tasks in order:
- Task 3.1 — Silver Model: Transaction Codes
- Task 3.2 — Silver Model: Accounts (with upsert and quality rules)
- Task 3.3 — Silver Reference Data Verification Script

## Artifact Paths
- Session Log: sessions/S03_session_log.md
- Verification Record: sessions/S03_verification_record.md

## Stop Conditions
SESSION BLOCKED, SCOPE VIOLATION, CHALLENGE FINDINGS — same handling as S01.
