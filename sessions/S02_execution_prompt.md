# Session Execution Prompt — S02: Bronze Layer

## Execution Mode
Autonomous

## Agent Identity
You are Claude Code executing build Session 2 for the Credit Card Financial Transactions Lake project. You follow the PBVI methodology. Every task has a CC prompt, verification command, and test cases. Execute each exactly as written.

## Repository Context
Branch: `session/02-bronze`
Create this branch from main (after S01 PR is merged) before executing any task.

METHODOLOGY_VERSION check: read PROJECT_MANIFEST.md, locate METHODOLOGY_VERSION. Expected: v4.3. If absent or different, output a METHODOLOGY VERSION WARNING block, then continue.

## What Has Already Been Built
Session 1 is complete. The full directory structure exists. Docker Compose starts without error (`docker compose up`). `dbt debug` passes inside the container. `python pipeline/pipeline.py --help` exits 0. `PROJECT_MANIFEST.md` exists at repo root with all five planning artifacts registered as PRESENT. The scaffold is committed on branch `session/01-scaffold`, merged to main.

**Pre-session requirement (engineer action):** Source CSV files have been manually converted from Excel and placed in `source/` before this session begins. The pipeline has no knowledge of the original Excel format. Files expected:
- `source/transactions_2024-01-01.csv` through `source/transactions_2024-01-07.csv`
- `source/accounts_2024-01-01.csv` through `source/accounts_2024-01-07.csv`
- `source/transaction_codes.csv`

## Planning Artifacts
- docs/ARCHITECTURE.md — DECIDED
- docs/INVARIANTS.md — SIGNED (Pratham, 15/04/2026)
- docs/EXECUTION_PLAN.md — AMENDED (Phase 4 gate applied, 16/04/2026)
- docs/Claude.md — FROZEN v1.0 (16/04/2026)

## Scope Boundary
This session creates files: `pipeline/bronze_transactions.py`, `pipeline/bronze_accounts.py`, `pipeline/bronze_transaction_codes.py`, `verification/verify_bronze.sh`.
No other files may be created or modified (except session log and verification record).

## Task Prompt Immutability
Execute each CC prompt exactly as written in EXECUTION_PLAN.md. Do not extend scope, add functionality not specified, or fix adjacent issues.

## Session Tasks
Execute all four tasks in order:
- Task 2.1 — Bronze Loader: Transactions
- Task 2.2 — Bronze Loader: Accounts
- Task 2.3 — Bronze Loader: Transaction Codes
- Task 2.4 — Bronze Layer Verification Script

## Artifact Paths
- Session Log: sessions/S02_session_log.md
- Verification Record: sessions/S02_verification_record.md

## Stop Conditions
SESSION BLOCKED, SCOPE VIOLATION, CHALLENGE FINDINGS — same handling as S01.
