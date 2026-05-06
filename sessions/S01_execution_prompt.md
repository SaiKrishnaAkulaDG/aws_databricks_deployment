# Session Execution Prompt — S01: Project Scaffold and Infrastructure

## Execution Mode
Autonomous

## Agent Identity
You are Claude Code executing build Session 1 for the Credit Card Financial Transactions Lake project. You follow the PBVI methodology. Every task in this session has a task ID, a CC prompt, a verification command, and a set of test cases. You execute each exactly as written.

## Repository Context
Branch: `session/01-scaffold`
Create this branch before executing any task.

METHODOLOGY_VERSION check: read PROJECT_MANIFEST.md, locate METHODOLOGY_VERSION. Expected: v4.3. If absent or different, output a METHODOLOGY VERSION WARNING block, then continue.

## What Has Already Been Built
This is the first session — repository scaffolded, no prior state.

## Planning Artifacts
- docs/ARCHITECTURE.md — DECIDED
- docs/INVARIANTS.md — SIGNED (Pratham, 15/04/2026)
- docs/EXECUTION_PLAN.md — AMENDED (Phase 4 gate applied, 16/04/2026)
- docs/Claude.md — FROZEN v1.0 (16/04/2026)

## Scope Boundary
This session creates files in: directory structure under `source/`, `data/`, `dbt/`, `pipeline/`, `tools/`, `docs/`, `sessions/`, `verification/`, and the files `PROJECT_MANIFEST.md`, `.gitignore`, `README.md`, `docker-compose.yml`, `Dockerfile`, `requirements.txt`, `dbt/dbt_project.yml`, `dbt/profiles.yml`, `pipeline/pipeline.py`, `pipeline/__init__.py`.
No files outside this boundary may be created or modified.

## Task Prompt Immutability
Execute each CC prompt exactly as written in EXECUTION_PLAN.md. Do not extend scope, add functionality not specified, or fix adjacent issues. Out of Scope Observations are the release valve for anything noticed outside task boundaries.

## Session Tasks
Execute all five tasks in order:
- Task 1.1 — Repository Directory Structure and PROJECT_MANIFEST.md
- Task 1.2 — Docker and Environment Configuration
- Task 1.3 — dbt Project Skeleton
- Task 1.4 — pipeline.py Stub with CLI and Control-Plane Initialisation
- Task 1.5 — Git Initialisation and First Commit

## Artifact Paths
- Session Log: sessions/S01_session_log.md
- Verification Record: sessions/S01_verification_record.md

## Stop Conditions

SESSION BLOCKED: any verification command fails. Record Status = BLOCKED in session log. Write full verification output verbatim into the Verification Record. Write a one-line failure classification: ENVIRONMENTAL | SCOPE GAP | UNKNOWN. Output SESSION BLOCKED summary (session number, task ID, verification output, classification). Stop. Do not retry.

SCOPE VIOLATION: any file outside the declared scope boundary is created or modified. Record SCOPE VIOLATION in session log. Output SCOPE VIOLATION summary. Stop. Wait for engineer disposition: ACCEPT or REVERT.

CHALLENGE FINDINGS: after each task's verification and scope checks pass, run the independent challenge agent against evidence only (no session context). CLEAN verdict proceeds to commit. FINDINGS verdict outputs CHALLENGE FINDINGS summary and stops. Wait for engineer to disposition each finding as ACCEPT (with rationale) or TEST (with a test case to run immediately — must pass before session continues).
