# Session Execution Prompt — S06: Pipeline Orchestration

## Execution Mode
Autonomous

## Agent Identity
You are Claude Code executing build Session 6 for the Credit Card Financial Transactions Lake project. You follow the PBVI methodology. Execute each CC prompt from EXECUTION_PLAN.md exactly as written.

## Repository Context
Branch: `session/06-orchestration`
Create this branch from main (after S05 PR is merged) before executing any task.

METHODOLOGY_VERSION check: read PROJECT_MANIFEST.md, locate METHODOLOGY_VERSION. Expected: v4.3. If absent or different, output a METHODOLOGY VERSION WARNING block, then continue.

## What Has Already Been Built
Sessions 1–5 are complete. Both Gold dbt models are implemented and verified. `data/gold/daily_summary/data.parquet` produces exactly one row per transaction date (no duplicates). `data/gold/weekly_account_summary/data.parquet` produces exactly one row per `(account_id, week_start_date)` for accounts with at least one resolvable transaction. All Gold aggregations match Silver resolvable-only totals per date. All five dbt models (`silver_transaction_codes`, `silver_accounts`, `silver_transactions`, `gold_daily_summary`, `gold_weekly_account_summary`) pass `dbt run` individually. S01–S05 branches are merged to main.

`pipeline/pipeline.py` currently exists as a stub only — it accepts `--mode`, `--start-date`, `--end-date` CLI arguments, generates a `run_id`, initialises the control plane, and prints a TODO message. It does not yet wire any Bronze loaders or dbt invocations.

## Planning Artifacts
- docs/ARCHITECTURE.md — DECIDED
- docs/INVARIANTS.md — SIGNED (Pratham, 15/04/2026)
- docs/EXECUTION_PLAN.md — AMENDED (Phase 4 gate applied, 16/04/2026)
- docs/Claude.md — FROZEN v1.0 (16/04/2026)

## Scope Boundary
This session creates or modifies: `pipeline/dbt_runner.py`, `pipeline/run_log.py`, `pipeline/watermark.py`, `pipeline/weekly_control.py`, `pipeline/pipeline.py` (full orchestration wiring — replaces stub implementation).
No other files may be created or modified (except session log and verification record).

## Task Prompt Immutability
Execute each CC prompt exactly as written in EXECUTION_PLAN.md. Do not extend scope, add functionality not specified, or fix adjacent issues.

## Session Tasks
Execute all five tasks in order:
- Task 6.1 — DAG Derivation and dbt JSON Log Streaming
- Task 6.2 — Run Log Writer with Async Buffer
- Task 6.3 — Watermark and Weekly Control Helpers
- Task 6.4 — Historical Orchestration Mode
- Task 6.5 — Incremental Orchestration Mode

## Artifact Paths
- Session Log: sessions/S06_session_log.md
- Verification Record: sessions/S06_verification_record.md

## Stop Conditions
SESSION BLOCKED, SCOPE VIOLATION, CHALLENGE FINDINGS — same handling as S01.
