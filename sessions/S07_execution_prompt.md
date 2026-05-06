# Session Execution Prompt — S07: End-to-End Verification

## Execution Mode
Autonomous

## Agent Identity
You are Claude Code executing build Session 7 for the Credit Card Financial Transactions Lake project. You follow the PBVI methodology. Execute each CC prompt from EXECUTION_PLAN.md exactly as written.

## Repository Context
Branch: `session/07-verification`
Create this branch from main (after S06 PR is merged) before executing any task.

METHODOLOGY_VERSION check: read PROJECT_MANIFEST.md, locate METHODOLOGY_VERSION. Expected: v4.3. If absent or different, output a METHODOLOGY VERSION WARNING block, then continue.

## What Has Already Been Built
Sessions 1–6 are complete. `pipeline.py` fully orchestrates both historical and incremental modes. The DAG is derived from `dbt compile` at runtime — no hardcoded model list. JSON log streaming produces real-time per-model run log entries via `NodeStart` and `NodeFinished` dbt events. The watermark advances only on full success (Bronze + Silver + Gold all pass). `gold_weekly_control.parquet` is updated before the watermark advances. SKIPPED rows are written for non-executed models on failure. The async run log buffer flushes to `data/pipeline/run_log.parquet` after watermark advancement. All individual session verification scripts exist in `verification/`. S01–S06 branches are merged to main.

**Pre-session requirement (engineer action):** Run `rm -rf data/` before starting this session so the pipeline executes from a fully clean state with no prior Parquet outputs.

## Planning Artifacts
- docs/ARCHITECTURE.md — DECIDED
- docs/INVARIANTS.md — SIGNED (Pratham, 15/04/2026)
- docs/EXECUTION_PLAN.md — AMENDED (Phase 4 gate applied, 16/04/2026)
- docs/Claude.md — FROZEN v1.0 (16/04/2026)

## Scope Boundary
This session creates or modifies: `verification/verify_idempotency.sh`, `verification/verify_audit_trail.sh`, `verification/REGRESSION_SUITE.sh`.
No other files may be created or modified (except session log and verification record).

## Task Prompt Immutability
Execute each CC prompt exactly as written in EXECUTION_PLAN.md. Do not extend scope, add functionality not specified, or fix adjacent issues.

## Session Tasks
Execute all four tasks in order:
- Task 7.1 — Full Historical Pipeline Run and Section 10 Sign-Off
- Task 7.2 — Idempotency Verification
- Task 7.3 — Audit Trail Verification
- Task 7.4 — Regression Suite Assembly

## Artifact Paths
- Session Log: sessions/S07_session_log.md
- Verification Record: sessions/S07_verification_record.md

## Stop Conditions
SESSION BLOCKED, SCOPE VIOLATION, CHALLENGE FINDINGS — same handling as S01.

SESSION 7 COMPLETION GATE (mandatory before this session may be closed):
All Section 10 verification commands from EXECUTION_PLAN.md pass. `verification/REGRESSION_SUITE.sh` is assembled and committed. Engineer signs off on Phase 8 entry criteria in sessions/S07_session_log.md before the PR is raised. Phase 8 may not begin until the engineer's written sign-off is present.
