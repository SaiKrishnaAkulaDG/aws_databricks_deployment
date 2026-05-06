# Session Execution Prompt — S04: Silver — Transactions

## Execution Mode
Autonomous

## Agent Identity
You are Claude Code executing build Session 4 for the Credit Card Financial Transactions Lake project. You follow the PBVI methodology. Execute each CC prompt from EXECUTION_PLAN.md exactly as written.

## Repository Context
Branch: `session/04-silver-transactions`
Create this branch from main (after S03 PR is merged) before executing any task.

METHODOLOGY_VERSION check: read PROJECT_MANIFEST.md, locate METHODOLOGY_VERSION. Expected: v4.3. If absent or different, output a METHODOLOGY VERSION WARNING block, then continue.

## What Has Already Been Built
Sessions 1–3 are complete. `data/silver/transaction_codes/data.parquet` contains all records from Bronze transaction codes with correct audit columns. `data/silver/accounts/data.parquet` contains exactly one record per `account_id` after processing all 7 account delta files — upsert on `account_id` is verified. All rejected account records are in `data/silver/quarantine/` with valid rejection reason codes. dbt runs cleanly for `silver_transaction_codes` and `silver_accounts` models. S01–S03 branches are merged to main.

## Planning Artifacts
- docs/ARCHITECTURE.md — DECIDED
- docs/INVARIANTS.md — SIGNED (Pratham, 15/04/2026)
- docs/EXECUTION_PLAN.md — AMENDED (Phase 4 gate applied, 16/04/2026)
- docs/Claude.md — FROZEN v1.0 (16/04/2026)

## Scope Boundary
This session creates or modifies: `dbt/models/silver/silver_transactions.sql`, `verification/verify_silver_transactions.sh`, `verification/verify_silver_integration.sh`.
No other files may be created or modified (except session log and verification record).

## Task Prompt Immutability
Execute each CC prompt exactly as written in EXECUTION_PLAN.md. Do not extend scope, add functionality not specified, or fix adjacent issues.

## Session Tasks
Execute all three tasks in order:
- Task 4.1 — Silver Model: Transactions with Full Quality Rules
- Task 4.2 — Silver Transactions Verification Script
- Task 4.3 — Silver Layer Integration Test

## Artifact Paths
- Session Log: sessions/S04_session_log.md
- Verification Record: sessions/S04_verification_record.md

## Stop Conditions
SESSION BLOCKED, SCOPE VIOLATION, CHALLENGE FINDINGS — same handling as S01.
