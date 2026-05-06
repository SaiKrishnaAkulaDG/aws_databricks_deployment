# EXECUTION_PLAN.md
## Credit Card Financial Transactions Lake
**PBVI Phase:** 3 — Execution Planning
**Brief Version:** 1.0 + REQUIREMENTS_GAPS_COVER.md (all gaps closed)
**Architecture Version:** ARCHITECTURE.md — DECIDED
**Invariants Version:** INVARIANTS.md — SIGNED (Pratham, 15/04/2026)
**Status:** AMENDED — Phase 4 Design Gate findings applied (16/04/2026); ready for Phase 5

---

## Resolved Decisions Table

| OQ | Question | Resolution |
|---|---|---|
| OQ-1 | `gold_weekly_control` schema | `week_start_date` (DATE, PK), `week_end_date` (DATE), `computed_at` (TIMESTAMP), `computed_by_run_id` (STRING). Week-grain only — no `account_id`. Defined in INVARIANTS.md Section 3.3. |
| OQ-2 | Run log sentinel values for ORCHESTRATION rows | `model_name = 'DBT_COMPILE'`, `layer = 'ORCHESTRATION'`, `status = 'FAILED'`. Not a dbt model — must not be interpreted as one. Defined in INVARIANTS.md Section 3.4. |
| OQ-3 | Verification coverage for orchestrator behaviours | Closed by INV-15, INV-20C, INV-31, INV-35. Verification commands embedded in S6 tasks. |
| OQ-4 | Date range of the historical load | Runtime parameter. User supplies `--start-date` and `--end-date` as CLI arguments. No hardcoded default. Pipeline validates source file existence for the range before processing. Concrete seed data range: 2024-01-01 to 2024-01-07. |

**Pre-build assumptions recorded:**
- Source files are provided as `.csv` in `source/` before Session 2 begins. Conversion from Excel is a manual pre-build step performed by the engineer. The pipeline has no knowledge of the original Excel format.
- Day 1 accounts file (`accounts_2024-01-01.csv`) is treated as a full snapshot. Days 2–7 are true deltas.
- `transaction_codes.csv` is static. Loaded once during historical pipeline initialisation.

---

## Session Overview Table

| # | Session | Goal | Tasks | Est. Duration |
|---|---|---|---|---|
| S1 | Project Scaffold and Infrastructure | Runnable Docker environment, dbt project skeleton, `pipeline.py` stub, `PROJECT_MANIFEST.md` | 5 | 2–3 hrs |
| S2 | Bronze Layer | All three Bronze loaders complete with idempotency, audit columns, and atomic writes | 4 | 2–3 hrs |
| S3 | Silver — Reference Data and Accounts | Transaction codes and accounts Silver promotion with quality rules and upsert | 3 | 2 hrs |
| S4 | Silver — Transactions | Transactions Silver promotion: sign assignment, global deduplication, quarantine, flags | 3 | 2–3 hrs |
| S5 | Gold Layer | Both Gold models complete with correct filters, aggregations, and atomic writes | 3 | 2–3 hrs |
| S6 | Pipeline Orchestration | `pipeline.py` fully wired: DAG derivation, JSON log streaming, run log buffer, watermark, `gold_weekly_control` | 5 | 3–4 hrs |
| S7 | End-to-End Verification | Full historical pipeline run, idempotency proof, audit trail verification, Section 10 sign-off commands | 4 | 2–3 hrs |

**Total tasks: 27**

---

## Session 1 — Project Scaffold and Infrastructure

**Session goal:** `docker compose up` starts the environment without error. `dbt debug` passes. `python pipeline/pipeline.py --help` runs without error. `PROJECT_MANIFEST.md` exists at repo root with all planning artifacts registered. The scaffold is committed and all registered paths exist.

**Integration check:**
```bash
docker compose run --rm pipeline python pipeline/pipeline.py --help
docker compose run --rm pipeline dbt debug --project-dir /app/dbt
```

---

### Task 1.1 — Repository Directory Structure and PROJECT_MANIFEST.md

**Description:**
Creates the full repository directory structure and `PROJECT_MANIFEST.md`. All directories required by the architecture must exist and be registered. Planning artifacts already produced (brief, ARCHITECTURE.md, INVARIANTS.md, REQUIREMENTS_GAPS_COVER.md) are registered as PRESENT. All remaining artifacts are registered as PENDING with their target phase and owner.

**Inputs:** None (greenfield)
**Outputs:** Full directory tree, `PROJECT_MANIFEST.md`, `.gitkeep` files to preserve empty directories

**CC Prompt:**
```
Create the full directory structure for the Credit Card Financial Transactions Lake project.

DIRECTORIES TO CREATE (add .gitkeep to each empty directory):
  source/
  data/bronze/transactions/
  data/bronze/accounts/
  data/bronze/transaction_codes/
  data/silver/transactions/
  data/silver/accounts/
  data/silver/transaction_codes/
  data/silver/quarantine/
  data/gold/daily_summary/
  data/gold/weekly_account_summary/
  data/pipeline/
  dbt/models/silver/
  dbt/models/gold/
  dbt/tests/
  dbt/macros/
  pipeline/
  tools/
  docs/
  sessions/
  verification/

Then create PROJECT_MANIFEST.md at repo root with these sections:

## Project
- Name: Credit Card Financial Transactions Lake
- METHODOLOGY_VERSION: v4.3
- Phase: 3 — Execution Planning

## Core Documents
| File | Status | Phase | Owner | Description |
Register these as PRESENT:
  - docs/REQUIREMENTS_BRIEF.md | PRESENT | Phase 1 | Engineer | Requirements brief v1.0
  - docs/REQUIREMENTS_GAPS_COVER.md | PRESENT | Phase 1 | Engineer | Gap resolution record
  - docs/ARCHITECTURE.md | PRESENT | Phase 1 | Engineer | Architecture decision record
  - docs/INVARIANTS.md | PRESENT | Phase 2 | Engineer | Invariant set — signed
  - docs/EXECUTION_PLAN.md | PRESENT | Phase 3 | Engineer | Execution plan

Register these as PENDING:
  - docs/PHASE4_GATE_RECORD.md | PENDING | Phase 4 | Engineer | Design gate record
  - Claude.md | PENDING | Phase 5 | Engineer | Frozen execution contract
  - verification/REGRESSION_SUITE.sh | PENDING | Phase 8 | Engineer | Regression suite

## Session Logs
(populated as sessions run)

## Verification Records
(populated as sessions run)

## Verification Checklists
(populated at Phase 8)

## Discovery Artifacts
Register all seven as PENDING with Phase 8 ownership:
  - discovery/INTAKE_SUMMARY.md
  - discovery/TOPOLOGY.md
  - discovery/MODULE_CONTRACTS.md
  - discovery/INTEGRATION_CONTRACTS.md
  - discovery/INVARIANT_CATALOGUE.md
  - discovery/RISK_REGISTER.md
  - discovery/ANNOTATION_CHECKLIST.md

## Structural Exceptions
  - README.md | repo root | Universal repo convention
  - PROJECT_MANIFEST.md | repo root | Registry cannot register itself

Do not create any files not listed above. Do not create subdirectories not listed above.
```

**Test cases:**
- All directories exist with `.gitkeep` files
- `PROJECT_MANIFEST.md` exists at repo root with METHODOLOGY_VERSION = v4.3
- All five planning docs registered as PRESENT
- All PENDING artifacts listed with correct phase ownership

**Verification command:**
```bash
find . -type d | sort
cat PROJECT_MANIFEST.md
```

**Invariant enforcement:** None task-specific — GLOBAL invariants apply.

**Regression classification:** NOT-REGRESSION-RELEVANT — directory structure failure is immediately visible; no silent regression risk.

---

### Task 1.2 — Docker Environment

**Description:**
Creates `Dockerfile`, `docker-compose.yml`, `requirements.txt`, and `.env.example`. The Docker environment must start with a single command and make the pipeline and dbt available inside the container. `source/` is bind-mounted read-only. `data/` is bind-mounted read-write.

**Inputs:** Directory structure from Task 1.1
**Outputs:** `Dockerfile`, `docker-compose.yml`, `requirements.txt`, `.env.example`

**CC Prompt:**
```
Create the Docker environment for the Credit Card Financial Transactions Lake.

Dockerfile:
  Base image: python:3.11-slim
  Working directory: /app
  COPY requirements.txt .
  RUN pip install --no-cache-dir -r requirements.txt
  COPY . .

docker-compose.yml:
  Service name: pipeline
  Build: from local Dockerfile
  Working directory: /app
  Volumes:
    - ./source:/app/source:ro       (read-only)
    - ./data:/app/data:rw           (read-write)
  env_file: .env
  Command: python pipeline/pipeline.py

requirements.txt — pin these exact versions:
  dbt-core==1.7.9
  dbt-duckdb==1.7.4
  duckdb==0.10.0
  pandas==2.2.0

.env.example:
  PIPELINE_ENV=dev

INVARIANT ENFORCEMENT:
- INV-01: source/ is mounted read-only (:ro). No pipeline component may write to
  /app/source. This mount flag is the structural enforcement mechanism.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- `docker compose build` completes without error
- `docker compose run --rm pipeline python --version` returns Python 3.11.x
- `docker compose run --rm pipeline dbt --version` returns dbt-core 1.7.9

**Verification command:**
```bash
docker compose build
docker compose run --rm pipeline python --version
docker compose run --rm pipeline dbt --version
```

**Invariant enforcement:** INV-01

**Regression classification:** NOT-REGRESSION-RELEVANT — Docker environment failure is immediately visible.

---

### Task 1.3 — dbt Project Skeleton

**Description:**
Creates the complete dbt project: `dbt_project.yml`, `profiles.yml`, `sources.yml`, model stub files for all five Silver and Gold models, and `schema.yml`. All Gold models declared with `materialized='table'`. Silver models declared with `materialized='table'`. Tags applied per layer. Warning comment added to `gold_weekly_account_summary.sql`.

**Inputs:** Directory structure from Task 1.1
**Outputs:** `dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/models/silver/*.sql`, `dbt/models/gold/*.sql`, `dbt/models/schema.yml`

**CC Prompt:**
```
Create the dbt project skeleton for the Credit Card Financial Transactions Lake.

dbt/dbt_project.yml:
  name: credit_card_lake
  version: 1.0.0
  config-version: 2
  profile: credit_card_lake
  model-paths: ["models"]
  test-paths: ["tests"]
  macro-paths: ["macros"]
  models:
    credit_card_lake:
      silver:
        +materialized: table
        +tags: ["silver"]
      gold:
        +materialized: table
        +tags: ["gold"]

dbt/profiles.yml:
  credit_card_lake:
    target: dev
    outputs:
      dev:
        type: duckdb
        path: /app/data/lake.duckdb
        extensions: [parquet]

Create stub SQL files — each contains only a single-line comment:
  dbt/models/silver/silver_transaction_codes.sql  -- TODO: implement silver_transaction_codes
  dbt/models/silver/silver_accounts.sql           -- TODO: implement silver_accounts
  dbt/models/silver/silver_transactions.sql       -- TODO: implement silver_transactions
  dbt/models/gold/gold_daily_summary.sql          -- TODO: implement gold_daily_summary
  dbt/models/gold/gold_weekly_account_summary.sql

For gold_weekly_account_summary.sql, the stub must contain this warning comment:
-- WARNING: This model must only be executed through pipeline.py.
-- Direct invocation via `dbt run --select gold_weekly_account_summary` bypasses
-- pipeline.py and the pipeline/gold_weekly_control.parquet enforcement gate,
-- causing all weeks to be recomputed and overwriting closing_balance values.
-- There is no structural enforcement for this — it is a named procedural control.
-- TODO: implement gold_weekly_account_summary

dbt/models/schema.yml — declare all five models with empty column lists:
  models:
    - name: silver_transaction_codes
    - name: silver_accounts
    - name: silver_transactions
    - name: gold_daily_summary
    - name: gold_weekly_account_summary

INVARIANT ENFORCEMENT:
- INV-41: Gold models must use materialized='table'. incremental is prohibited.
  Verify: grep -r "incremental" dbt/models/gold/ must return no results.
- IG-07: Warning comment in gold_weekly_account_summary.sql is a named procedural
  control. It must be present in the stub and preserved in the final implementation.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- `dbt debug` passes inside the container
- `dbt compile` produces `target/manifest.json`
- `grep -r "incremental" dbt/models/gold/` returns no results
- Warning comment present in `gold_weekly_account_summary.sql`

**Verification command:**
```bash
docker compose run --rm pipeline dbt debug --project-dir /app/dbt
docker compose run --rm pipeline dbt compile --project-dir /app/dbt
grep -r "incremental" dbt/models/gold/
```

**Invariant enforcement:** INV-41, IG-07

**Regression classification:** REGRESSION-RELEVANT — incremental materialisation check is portable:
```bash
grep -r "incremental" dbt/models/gold/
```
Expected: no output.

---

### Task 1.4 — pipeline.py Stub with CLI and Control-Plane Initialisation

**Description:**
Creates `pipeline/pipeline.py` with full argparse CLI, a unique `run_id` generator, and control-plane initialisation logic. On first run, absent control-plane files are created with correct empty schema. If files exist but are unreadable or corrupt, the pipeline halts with a FAILED fallback log entry and does not reinitialise silently.

**Inputs:** Directory structure from Task 1.1
**Outputs:** `pipeline/pipeline.py`, `pipeline/__init__.py`

**CC Prompt:**
```
Create pipeline/pipeline.py for the Credit Card Financial Transactions Lake.

SECTION 1 — CLI (argparse):
  --mode         required  choices: historical, incremental
  --start-date   required for historical  format: YYYY-MM-DD
  --end-date     required for historical  format: YYYY-MM-DD
  --help         must work and exit 0

SECTION 2 — run_id generator:
  Function generate_run_id() -> str
  Implementation:
    import uuid
    return str(uuid.uuid4())
  UUID4 provides 122 bits of randomness — uniqueness is guaranteed across any
  realistic number of pipeline runs. Called once per pipeline invocation.
  Do NOT use timestamps, sequential integers, or any deterministic scheme.
  run_id is the connective tissue for the entire audit trail (INV-22 GLOBAL) —
  the uniqueness guarantee takes priority over human readability.

SECTION 3 — Control-plane initialisation:
  Function initialise_control_plane(data_dir: str) -> None

  For data/pipeline/control.parquet:
    If absent: create with schema (last_processed_date DATE, updated_at TIMESTAMP,
    updated_by_run_id STRING) and zero rows. Log "Initialised control.parquet."
    If present: attempt read with duckdb. If read fails (corrupt/invalid):
      write FAILED entry to data/pipeline/pipeline_runlog_fallback.jsonl
      with error_message explaining the corruption. Raise SystemExit(1).
      Do NOT reinitialise the file silently.
    If present and readable: proceed silently.

  For data/pipeline/gold_weekly_control.parquet:
    Same pattern as above with schema:
    (week_start_date DATE, week_end_date DATE, computed_at TIMESTAMP,
    computed_by_run_id STRING)

SECTION 4 — Main stub:
  if __name__ == "__main__":
      args = parse_args()
      run_id = generate_run_id()
      initialise_control_plane("/app/data")
      print(f"Pipeline stub ready — mode={args.mode}, run_id={run_id}")
      print("TODO: wire orchestration")

Create pipeline/__init__.py as an empty file.

INVARIANT ENFORCEMENT:
- INV-32: Absent control-plane files must be treated as valid initial state.
  Create with correct empty schema. Never raise unhandled exception on absent files.
- INV-43: Corrupt or unreadable control-plane files must halt immediately with a
  FAILED fallback log entry. Must not default to assumed safe state or reinitialise silently.
- INV-01: No path referencing /app/source may appear in any write operation anywhere
  in this file.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- `python pipeline/pipeline.py --help` exits 0
- First run creates both control-plane Parquet files with correct schema and zero rows
- Second run on already-initialised files proceeds without error
- Injecting a corrupt `data/pipeline/control.parquet` halts with fallback `.jsonl` entry

**Verification command:**
```bash
docker compose run --rm pipeline python pipeline/pipeline.py --help
docker compose run --rm pipeline python pipeline/pipeline.py --mode historical --start-date 2024-01-01 --end-date 2024-01-07
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print(conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/pipeline/control.parquet')\").fetchone())
print(conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/pipeline/gold_weekly_control.parquet')\").fetchone())
"
```

**Invariant enforcement:** INV-32, INV-43, INV-01

**Regression classification:** REGRESSION-RELEVANT — control-plane initialisation correctness is a silent failure mode on first run:
```bash
docker compose run --rm pipeline python pipeline/pipeline.py --help
```
Expected: exits 0.

---

### Task 1.5 — Git Initialisation and First Commit

**Description:**
Initialises the git repository, creates `.gitignore` and `README.md`, commits the scaffold. `.gitignore` must exclude `data/`, `dbt/target/`, `.env`, and Python cache files.

**Inputs:** All outputs from Tasks 1.1–1.4
**Outputs:** `.git/`, `.gitignore`, `README.md`, first commit

**CC Prompt:**
```
Initialise the git repository and create the first scaffold commit.

.gitignore:
  data/
  dbt/target/
  dbt/dbt_packages/
  .env
  __pycache__/
  *.pyc
  *.pyo
  .DS_Store

README.md at repo root:
  # Credit Card Financial Transactions Lake
  **Status:** Phase 3 complete. Build not yet started.

  ## What This Is
  A Medallion architecture (Bronze → Silver → Gold) data lake that ingests daily
  credit card transaction CSV extracts, enforces quality rules at each layer boundary,
  and produces Gold-layer aggregations queryable via DuckDB.

  ## Stack
  Python 3.11 · dbt-core 1.7.9 · dbt-duckdb 1.7.4 · DuckDB 0.10.0 · Docker Compose

  ## How to Run
  1. Copy converted CSV files to source/
  2. cp .env.example .env
  3. docker compose run --rm pipeline python pipeline/pipeline.py \
       --mode historical --start-date 2024-01-01 --end-date 2024-01-07

  ## Docs
  All planning artifacts are in docs/.

Then run:
  git init
  git add .
  git commit -m "chore: PBVI project initialisation — Credit Card Financial Transactions Lake scaffold"

Output the commit hash.
```

**Test cases:**
- `git log --oneline` shows exactly one commit
- `git status` shows clean working tree
- `data/` is not tracked (excluded by .gitignore)
- `.env` is not tracked

**Verification command:**
```bash
git log --oneline
git status
git ls-files data/
```

**Invariant enforcement:** None task-specific.

**Regression classification:** NOT-REGRESSION-RELEVANT — git state is not a runtime regression risk.

---

## Session 2 — Bronze Layer

**Session goal:** All three Bronze loaders are complete. Running any Bronze loader twice against the same source file produces identical row counts — no duplicates. All Bronze audit columns are non-null for every record. Bronze partitions are immutable after first write. Source files are not modified by any pipeline operation.

**Pre-session requirement:** Place converted CSV files in `source/` before starting this session.

**Integration check:**
```bash
docker compose run --rm pipeline python -c "from pipeline.bronze_transactions import load_bronze_transactions; print(load_bronze_transactions('2024-01-01', 'TEST-RUN-001'))"
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('TX count:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/date=2024-01-01/data.parquet')\").fetchone())
print('Null audit:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/date=2024-01-01/data.parquet') WHERE _pipeline_run_id IS NULL\").fetchone())
"
# Second run — must skip and return identical count
docker compose run --rm pipeline python -c "from pipeline.bronze_transactions import load_bronze_transactions; result=load_bronze_transactions('2024-01-01', 'TEST-RUN-002'); print('Skipped:', result['skipped'])"
```

---

### Task 2.1 — Bronze Loader: Transactions

**Description:**
Creates `pipeline/bronze_transactions.py`. Reads `source/transactions_YYYY-MM-DD.csv`, adds three audit columns, writes to `data/bronze/transactions/date=YYYY-MM-DD/data.parquet`. Partition existence check enforces idempotency. Atomic write via temp file + rename. Source file is never written to.

**Inputs:** `source/transactions_YYYY-MM-DD.csv`
**Outputs:** `data/bronze/transactions/date=YYYY-MM-DD/data.parquet`

**CC Prompt:**
```
Create pipeline/bronze_transactions.py — the Bronze loader for transactions.

Function: load_bronze_transactions(date_str: str, run_id: str) -> dict
  Returns: {"records_written": int, "skipped": bool, "source_file": str}

Logic (implement in exactly this order):

  1. source_path = f"/app/source/transactions_{date_str}.csv"
  2. target_dir  = f"/app/data/bronze/transactions/date={date_str}"
  3. target_path = f"{target_dir}/data.parquet"
  4. temp_path   = f"{target_dir}/.data.parquet.tmp"

  IDEMPOTENCY GATE:
  5. If os.path.exists(target_path):  # check FILE exists, not just the directory
     return {"records_written": 0, "skipped": True, "source_file": source_path}
     Stop here. Do not read source file. Do not write anything.
     NOTE: An existing directory WITHOUT a data.parquet file is an incomplete prior
     write — do NOT treat it as already ingested. Proceed with ingestion in that case.

  6. Create target_dir (os.makedirs, exist_ok=True).

  ATOMIC WRITE:
  7. Open DuckDB connection.
  8. Execute: COPY (SELECT *, '{basename}' AS _source_file, now() AS _ingested_at,
     '{run_id}' AS _pipeline_run_id FROM read_csv_auto('{source_path}'))
     TO '{temp_path}' (FORMAT PARQUET)
  9. On success: os.rename(temp_path, target_path)
  10. On any exception: delete temp_path if it exists. Re-raise exception.

  11. row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{target_path}')").fetchone()[0]
  12. Return {"records_written": row_count, "skipped": False, "source_file": source_path}

INVARIANT ENFORCEMENT:
- INV-01: No write operation may target any path beginning with /app/source.
  Assert this by inspection — source_path is only passed to read_csv_auto(), never to COPY TO.
- INV-02: Idempotency gate in step 5 — check target_path FILE existence before reading
  source file. If data.parquet exists, skip entirely. An empty target_dir is not a skip.
- INV-03: Partition is never overwritten — the file existence check in step 5 is the
  enforcement mechanism. Only a fully written data.parquet causes skip.
- INV-04: All three audit columns (_source_file, _ingested_at, _pipeline_run_id)
  are added at read time in step 8 and must be non-null for every record.
- INV-40: Atomic write — temp file written first, renamed to final path only on success.
  A failed write leaves no data.parquet — the next run will correctly re-ingest.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.

Do not implement Silver logic. This loader handles Bronze only.
```

**Test cases:**
- Happy path: partition created, `records_written` matches source CSV row count minus header
- Idempotency: second call returns `skipped=True`, partition mtime unchanged
- Null audit columns: `SELECT COUNT(*) WHERE _pipeline_run_id IS NULL` = 0
- Source unchanged: `git status source/` shows no modifications
- Atomic write: if `.tmp` file exists from a simulated crash, next run cleans it and succeeds

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Row count:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/date=2024-01-01/data.parquet')\").fetchone())
print('Null run_id:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/date=2024-01-01/data.parquet') WHERE _pipeline_run_id IS NULL\").fetchone())
"
git status source/
```

**Invariant enforcement:** INV-01, INV-02, INV-03, INV-04, INV-40

**Regression classification:** REGRESSION-RELEVANT:
```bash
docker compose run --rm pipeline python -c "import duckdb; conn=duckdb.connect(); print(conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/**/*.parquet') WHERE _pipeline_run_id IS NULL\").fetchone())"
```
Expected: `(0,)`

---

### Task 2.2 — Bronze Loader: Accounts

**Description:**
Creates `pipeline/bronze_accounts.py`. Same pattern as Task 2.1 but for `source/accounts_YYYY-MM-DD.csv` → `data/bronze/accounts/date=YYYY-MM-DD/data.parquet`.

**Inputs:** `source/accounts_YYYY-MM-DD.csv`
**Outputs:** `data/bronze/accounts/date=YYYY-MM-DD/data.parquet`

**CC Prompt:**
```
Create pipeline/bronze_accounts.py — the Bronze loader for accounts.

Function: load_bronze_accounts(date_str: str, run_id: str) -> dict
  Returns: {"records_written": int, "skipped": bool, "source_file": str}

Implement identical logic to bronze_transactions.py with these substitutions:
  source_path = f"/app/source/accounts_{date_str}.csv"
  target_dir  = f"/app/data/bronze/accounts/date={date_str}"
  target_path = f"{target_dir}/data.parquet"
  temp_path   = f"{target_dir}/.data.parquet.tmp"

All other logic is identical — idempotency gate checks os.path.exists(target_path)
(FILE existence, not directory existence), audit columns, atomic write, return dict.

INVARIANT ENFORCEMENT:
- INV-01, INV-02, INV-03, INV-04, INV-40 — identical to bronze_transactions.py.
- INV-02 clarification: idempotency gate must check target_path file exists,
  not target_dir directory exists. An empty directory is an incomplete prior write.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.

Do not implement Silver upsert logic here. Bronze is exact copy of source plus audit columns.
```

**Test cases:** Identical to Task 2.1 but for accounts partitions.

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Row count:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/accounts/date=2024-01-01/data.parquet')\").fetchone())
print('Null run_id:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/accounts/date=2024-01-01/data.parquet') WHERE _pipeline_run_id IS NULL\").fetchone())
"
```

**Invariant enforcement:** INV-01, INV-02, INV-03, INV-04, INV-40

**Regression classification:** REGRESSION-RELEVANT:
```bash
docker compose run --rm pipeline python -c "import duckdb; conn=duckdb.connect(); print(conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/accounts/**/*.parquet') WHERE _pipeline_run_id IS NULL\").fetchone())"
```
Expected: `(0,)`

---

### Task 2.3 — Bronze Loader: Transaction Codes

**Description:**
Creates `pipeline/bronze_transaction_codes.py`. Reads `source/transaction_codes.csv`, adds three audit columns, writes to `data/bronze/transaction_codes/data.parquet`. Single reference file — not date-partitioned. Idempotency gate checks file existence, not partition directory.

**Inputs:** `source/transaction_codes.csv`
**Outputs:** `data/bronze/transaction_codes/data.parquet`

**CC Prompt:**
```
Create pipeline/bronze_transaction_codes.py — the Bronze loader for transaction codes.

Function: load_bronze_transaction_codes(run_id: str) -> dict
  Returns: {"records_written": int, "skipped": bool, "source_file": str}

Logic:
  1. source_path = "/app/source/transaction_codes.csv"
  2. target_path = "/app/data/bronze/transaction_codes/data.parquet"
  3. temp_path   = "/app/data/bronze/transaction_codes/.data.parquet.tmp"

  IDEMPOTENCY GATE:
  4. If target_path already exists as a file: return
     {"records_written": 0, "skipped": True, "source_file": source_path}

  5. Create parent directory if needed.

  ATOMIC WRITE:
  6. COPY (SELECT *, 'transaction_codes.csv' AS _source_file, now() AS _ingested_at,
     '{run_id}' AS _pipeline_run_id FROM read_csv_auto('{source_path}'))
     TO '{temp_path}' (FORMAT PARQUET)
  7. On success: os.rename(temp_path, target_path)
  8. On failure: delete temp_path. Re-raise.

  9. row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{target_path}')").fetchone()[0]
  10. Return {"records_written": row_count, "skipped": False, "source_file": source_path}

INVARIANT ENFORCEMENT:
- INV-01, INV-02, INV-03, INV-04, INV-40 — same as bronze_transactions.py.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- Happy path: file created, row count matches source CSV minus header
- Idempotency: second call returns `skipped=True`, file mtime unchanged
- No null `_pipeline_run_id`

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Row count:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transaction_codes/data.parquet')\").fetchone())
print('Null run_id:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transaction_codes/data.parquet') WHERE _pipeline_run_id IS NULL\").fetchone())
"
```

**Invariant enforcement:** INV-01, INV-02, INV-03, INV-04, INV-40

**Regression classification:** REGRESSION-RELEVANT:
```bash
docker compose run --rm pipeline python -c "import duckdb; conn=duckdb.connect(); print(conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transaction_codes/data.parquet') WHERE _pipeline_run_id IS NULL\").fetchone())"
```
Expected: `(0,)`

---

### Task 2.4 — Bronze Completeness Verification Script

**Description:**
Creates `verification/verify_bronze.sh` — a portable shell script that runs all Section 10.1 Bronze completeness checks. This is the session integration check and a regression suite candidate.

**Inputs:** Loaded Bronze Parquet files, source CSV files
**Outputs:** `verification/verify_bronze.sh`

**CC Prompt:**
```
Create verification/verify_bronze.sh — a portable shell script that verifies Bronze
layer completeness per Section 10.1 of the requirements brief.

Run from repo root: bash verification/verify_bronze.sh

PASS/FAIL checks to implement:

1. BRONZE_TRANSACTIONS_COMPLETENESS
   For each date in 2024-01-01 through 2024-01-07:
   source_count = line count of source/transactions_{date}.csv minus 1 (header)
   bronze_count = SELECT COUNT(*) FROM read_parquet('data/bronze/transactions/date={date}/data.parquet')
   PASS if equal, FAIL with counts if not.

2. BRONZE_ACCOUNTS_COMPLETENESS
   Same pattern for accounts files and partitions.

3. BRONZE_TRANSACTION_CODES_COMPLETENESS
   source_count = line count of source/transaction_codes.csv minus 1
   bronze_count = SELECT COUNT(*) FROM read_parquet('data/bronze/transaction_codes/data.parquet')
   PASS if equal.

4. BRONZE_AUDIT_COLUMNS_NOT_NULL
   SELECT COUNT(*) FROM read_parquet('data/bronze/transactions/**/*.parquet')
   WHERE _pipeline_run_id IS NULL — PASS if 0.
   Same check for accounts and transaction_codes.

5. BRONZE_SOURCE_FILES_UNMODIFIED
   Print modification timestamps for all source/ CSV files.
   Label: MANUAL CHECK — engineer confirms no mtime changes between runs.

Print final summary: N checks passed, M checks failed.
```

**Test cases:**
- All checks PASS after successful Bronze load of all 7 dates
- Any partition with missing rows prints FAIL with actual vs expected counts

**Verification command:**
```bash
bash verification/verify_bronze.sh
```

**Invariant enforcement:** INV-02, INV-03, INV-04

**Regression classification:** REGRESSION-RELEVANT — direct candidate for `verification/REGRESSION_SUITE.sh`. Portable from repo root.

---

## Session 3 — Silver: Reference Data and Accounts

**Session goal:** `silver/transaction_codes/data.parquet` contains all records from Bronze transaction codes with correct audit columns. `silver/accounts/data.parquet` contains exactly one record per `account_id` after processing all 7 account delta files. All rejected account records are in quarantine with valid rejection reason codes.

**Integration check:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt --select silver_transaction_codes silver_accounts --vars '{"target_date": "2024-01-01", "run_id": "TEST-RUN-001"}'
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('TC count:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transaction_codes/data.parquet')\").fetchone())
print('Accounts dupes:', conn.execute(\"SELECT COUNT(*) FROM (SELECT account_id, COUNT(*) c FROM read_parquet('/app/data/silver/accounts/data.parquet') GROUP BY account_id HAVING c > 1)\").fetchone())
"
```

---

### Task 3.1 — Silver Model: Transaction Codes

**Description:**
Implements `dbt/models/silver/silver_transaction_codes.sql`. Reads from Bronze transaction codes, carries forward audit columns, adds Silver promotion columns. Writes to `data/silver/transaction_codes/data.parquet`. Reads exclusively from the Bronze Parquet file.

**Inputs:** `data/bronze/transaction_codes/data.parquet`
**Outputs:** `data/silver/transaction_codes/data.parquet`

**CC Prompt:**
```
Implement dbt/models/silver/silver_transaction_codes.sql.

This model promotes all Bronze transaction code records to Silver.
No quality rules apply — transaction codes are a governed reference file.

SOURCE:
  SELECT * FROM read_parquet('/app/data/bronze/transaction_codes/data.parquet')

OUTPUT COLUMNS (all source columns plus):
  _source_file         STRING    carried from Bronze _source_file
  _bronze_ingested_at  TIMESTAMP carried from Bronze _ingested_at
  _pipeline_run_id     STRING    carried from Bronze _pipeline_run_id
  _promoted_at         TIMESTAMP current_timestamp

Write to: /app/data/silver/transaction_codes/data.parquet
Materialisation: table (set in dbt_project.yml)

INVARIANT ENFORCEMENT:
- INV-37: This model reads from bronze/transaction_codes/data.parquet only.
  No reads from source CSV or any other path.
- INV-22: _pipeline_run_id must be non-null for every record — carried from Bronze.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- Silver TC row count equals Bronze TC row count
- All records have non-null `_pipeline_run_id`
- `_promoted_at` is non-null for all records
- No reads from `source/` or `bronze/` in any Gold model (verified separately)

**Verification command:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt --select silver_transaction_codes --vars '{"run_id": "TEST-RUN-001"}'
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Silver TC count:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transaction_codes/data.parquet')\").fetchone())
print('Null run_id:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transaction_codes/data.parquet') WHERE _pipeline_run_id IS NULL\").fetchone())
"
```

**Invariant enforcement:** INV-37, INV-22

**Regression classification:** REGRESSION-RELEVANT:
```bash
docker compose run --rm pipeline python -c "import duckdb; conn=duckdb.connect(); print(conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transaction_codes/data.parquet') WHERE _pipeline_run_id IS NULL\").fetchone())"
```
Expected: `(0,)`

---

### Task 3.2 — Silver Model: Accounts with Upsert and Quality Rules

**Description:**
Implements `dbt/models/silver/silver_accounts.sql`. Applies quality rules (`NULL_REQUIRED_FIELD`, `INVALID_ACCOUNT_STATUS`), writes rejected records to quarantine, upserts passing records into `data/silver/accounts/data.parquet` on `account_id`. Exactly one record per `account_id` at all times. Re-running for an already-processed date produces identical output.

**Inputs:** `data/bronze/accounts/date=YYYY-MM-DD/data.parquet`, existing `data/silver/accounts/data.parquet`
**Outputs:** `data/silver/accounts/data.parquet` (upserted), `data/silver/quarantine/date=YYYY-MM-DD/rejected.parquet`

**CC Prompt:**
```
Implement dbt/models/silver/silver_accounts.sql.

Receives dbt vars: target_date (YYYY-MM-DD), run_id (STRING)

SOURCE: read_parquet('/app/data/bronze/accounts/date={{ var("target_date") }}/data.parquet')

STEP 1 — QUALITY CHECKS (first failing rule wins per record):

  Rule NULL_REQUIRED_FIELD:
    account_id, open_date, credit_limit, current_balance, billing_cycle_start,
    billing_cycle_end, or account_status is null or empty string.
    -> quarantine with _rejection_reason = 'NULL_REQUIRED_FIELD'

  Rule INVALID_ACCOUNT_STATUS:
    account_status NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
    -> quarantine with _rejection_reason = 'INVALID_ACCOUNT_STATUS'

STEP 2 — QUARANTINE WRITE (rejected records):
  Path: /app/data/silver/quarantine/date={{ var("target_date") }}/rejected.parquet
  Columns: all source columns plus:
    _source_file      from Bronze _source_file
    _pipeline_run_id  '{{ var("run_id") }}'
    _rejected_at      current_timestamp
    _rejection_reason code from Step 1

STEP 3 — UPSERT (passing records):
  existing = read_parquet('/app/data/silver/accounts/data.parquet') IF EXISTS ELSE empty
  incoming = passing records from Step 1 with Silver audit columns:
    _source_file         from Bronze _source_file
    _bronze_ingested_at  from Bronze _ingested_at
    _pipeline_run_id     '{{ var("run_id") }}'
    _record_valid_from   current_timestamp

  Upsert key: account_id
  Logic: incoming record REPLACES existing record for same account_id.
         Existing records not in incoming delta are retained unchanged.
  Write: full result set to /app/data/silver/accounts/data.parquet (overwrite single file)

  Before writing: assert COUNT(*) = COUNT(DISTINCT account_id) in result set.
  If assertion fails: raise exception, do not write.

INVARIANT ENFORCEMENT:
- INV-07: Exactly one record per account_id at all times. Assert before write.
- INV-36: Incoming delta REPLACES existing record. Retaining stale or duplicating = violation.
- INV-26: Every quarantine record has non-null _rejection_reason from exhaustive list.
- INV-22: _pipeline_run_id non-null for all Silver and quarantine records.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- After processing all 7 dates: no `account_id` appears more than once in Silver accounts
- Delta record for existing account replaces stale record — current_balance reflects delta value
- Record with null `account_id` goes to quarantine with `NULL_REQUIRED_FIELD`
- Record with invalid status goes to quarantine with `INVALID_ACCOUNT_STATUS`
- Re-running for an already-processed date produces identical output

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Dupes:', conn.execute(\"SELECT COUNT(*) FROM (SELECT account_id, COUNT(*) c FROM read_parquet('/app/data/silver/accounts/data.parquet') GROUP BY account_id HAVING c > 1)\").fetchone())
print('Null run_id:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/accounts/data.parquet') WHERE _pipeline_run_id IS NULL\").fetchone())
print('Bad rejection reason:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/quarantine/**/*.parquet') WHERE _rejection_reason NOT IN ('NULL_REQUIRED_FIELD','INVALID_AMOUNT','DUPLICATE_TRANSACTION_ID','INVALID_TRANSACTION_CODE','INVALID_CHANNEL','INVALID_ACCOUNT_STATUS') OR _rejection_reason IS NULL\").fetchone())
"
```

**Invariant enforcement:** INV-07, INV-36, INV-26, INV-22

**Regression classification:** REGRESSION-RELEVANT:
```bash
docker compose run --rm pipeline python -c "import duckdb; conn=duckdb.connect(); print(conn.execute(\"SELECT COUNT(*) FROM (SELECT account_id, COUNT(*) c FROM read_parquet('/app/data/silver/accounts/data.parquet') GROUP BY account_id HAVING c > 1)\").fetchone())"
```
Expected: `(0,)`

---

### Task 3.3 — Silver Accounts Verification Script

**Description:**
Creates `verification/verify_silver_accounts.sh` covering Silver accounts quality checks.

**Inputs:** `data/silver/accounts/data.parquet`, `data/silver/quarantine/`
**Outputs:** `verification/verify_silver_accounts.sh`

**CC Prompt:**
```
Create verification/verify_silver_accounts.sh.

PASS/FAIL checks:

1. SILVER_ACCOUNTS_NO_DUPLICATES
   SELECT account_id, COUNT(*) c ... HAVING c > 1 — expect 0 rows

2. SILVER_ACCOUNTS_NO_NULL_RUN_ID
   SELECT COUNT(*) WHERE _pipeline_run_id IS NULL — expect 0

3. SILVER_QUARANTINE_VALID_REJECTION_REASONS
   All quarantine records have _rejection_reason in:
   (NULL_REQUIRED_FIELD, INVALID_AMOUNT, DUPLICATE_TRANSACTION_ID,
   INVALID_TRANSACTION_CODE, INVALID_CHANNEL, INVALID_ACCOUNT_STATUS)
   expect 0 violations

4. SILVER_ACCOUNTS_UPSERT_CORRECTNESS
   Print current_balance for account_id values that appear in multiple daily delta files.
   Label: MANUAL CHECK — engineer confirms Silver reflects most recent delta value.

Final summary: N passed, M failed.
```

**Verification command:**
```bash
bash verification/verify_silver_accounts.sh
```

**Invariant enforcement:** INV-07, INV-36, INV-26

**Regression classification:** REGRESSION-RELEVANT — portable from repo root.

---

## Session 4 — Silver: Transactions

**Session goal:** `silver/transactions/date=YYYY-MM-DD/data.parquet` exists for all 7 dates. No `transaction_id` appears more than once globally. All `_signed_amount` values are non-null and correctly signed per `debit_credit_indicator`. All rejected records are in quarantine with valid reason codes. `_is_resolvable` and `_missing_merchant_name` flags are correctly set. Bronze count = Silver count + quarantine count for every date.

**Integration check:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Global dupes:', conn.execute(\"SELECT COUNT(*) FROM (SELECT transaction_id, COUNT(*) c FROM read_parquet('/app/data/silver/transactions/**/*.parquet') GROUP BY transaction_id HAVING c > 1)\").fetchone())
print('Null signed amount:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet') WHERE _signed_amount IS NULL\").fetchone())
print('DR negative:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet') t JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc ON t.transaction_code = tc.transaction_code WHERE tc.debit_credit_indicator = 'DR' AND t._signed_amount < 0\").fetchone())
"
```

---

### Task 4.1 — Silver Model: Transactions with Full Quality Rules

**Description:**
Implements `dbt/models/silver/silver_transactions.sql`. Applies all six quality rules from Section 5.1. Signs amounts via `debit_credit_indicator` from Silver transaction codes exclusively. Sets `_is_resolvable` and `_missing_merchant_name` derived flags. Enforces global deduplication on `transaction_id` across all existing Silver partitions. Writes Silver and quarantine records with correct audit columns.

**Inputs:** `data/bronze/transactions/date=YYYY-MM-DD/data.parquet`, `data/silver/transaction_codes/data.parquet`, `data/silver/accounts/data.parquet`
**Outputs:** `data/silver/transactions/date=YYYY-MM-DD/data.parquet`, `data/silver/quarantine/date=YYYY-MM-DD/rejected.parquet`

**CC Prompt:**
```
Implement dbt/models/silver/silver_transactions.sql.

Receives dbt vars: target_date (YYYY-MM-DD), run_id (STRING)

SOURCE: read_parquet('/app/data/bronze/transactions/date={{ var("target_date") }}/data.parquet')

STEP 1 — QUALITY CHECKS (apply in this exact order; first failing rule wins):

  Rule NULL_REQUIRED_FIELD:
    transaction_id, account_id, transaction_date, amount, transaction_code,
    or channel is null or empty string.
    -> quarantine with _rejection_reason = 'NULL_REQUIRED_FIELD'

  Rule INVALID_AMOUNT:
    amount is zero, negative, or non-numeric.
    -> quarantine with _rejection_reason = 'INVALID_AMOUNT'

  Rule DUPLICATE_TRANSACTION_ID:
    transaction_id already exists in ANY existing Silver transactions partition.
    Check: SELECT transaction_id FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
    This is GLOBAL — not limited to current date. Includes ALL existing partitions.
    -> quarantine with _rejection_reason = 'DUPLICATE_TRANSACTION_ID'

  Rule INVALID_TRANSACTION_CODE:
    transaction_code NOT IN (SELECT transaction_code FROM
    read_parquet('/app/data/silver/transaction_codes/data.parquet'))
    -> quarantine with _rejection_reason = 'INVALID_TRANSACTION_CODE'

  Rule INVALID_CHANNEL:
    channel NOT IN ('ONLINE', 'IN_STORE')
    -> quarantine with _rejection_reason = 'INVALID_CHANNEL'

  Rule UNRESOLVABLE_ACCOUNT_ID:
    account_id NOT IN (SELECT account_id FROM read_parquet('/app/data/silver/accounts/data.parquet'))
    -> DO NOT quarantine. Write to Silver with _is_resolvable = FALSE.
    This is the ONLY rule that produces a Silver record rather than a quarantine record.

STEP 2 — SIGN ASSIGNMENT (records passing all hard rules above):
  JOIN to read_parquet('/app/data/silver/transaction_codes/data.parquet') on transaction_code.
  _signed_amount = CASE
    WHEN debit_credit_indicator = 'DR' THEN amount          -- positive
    WHEN debit_credit_indicator = 'CR' THEN -1.0 * amount   -- negative
  END
  Source amounts are always positive. Sign is assigned here exclusively — no other logic.

STEP 3 — DERIVED FLAGS (for all Silver records including unresolvable):
  _is_resolvable: TRUE if account_id resolved, FALSE if UNRESOLVABLE_ACCOUNT_ID
  _missing_merchant_name: TRUE when transaction_type = 'PURCHASE' AND merchant_name IS NULL
                          FALSE in all other cases

STEP 4 — QUARANTINE WRITE:
  Path: /app/data/silver/quarantine/date={{ var("target_date") }}/rejected.parquet
  All source columns plus: _source_file, _pipeline_run_id, _rejected_at, _rejection_reason

STEP 5 — SILVER WRITE:
  Path: /app/data/silver/transactions/date={{ var("target_date") }}/data.parquet
  All source columns plus:
    _source_file           from Bronze _source_file
    _bronze_ingested_at    from Bronze _ingested_at
    _pipeline_run_id       '{{ var("run_id") }}'
    _promoted_at           current_timestamp
    _is_resolvable         BOOLEAN per Step 3
    _signed_amount         DECIMAL per Step 2
    _missing_merchant_name BOOLEAN per Step 3

POST-WRITE ASSERTION:
  bronze_count = SELECT COUNT(*) FROM bronze source
  silver_count = SELECT COUNT(*) FROM silver partition just written
  quarantine_count = SELECT COUNT(*) FROM quarantine partition just written
  Assert: bronze_count = silver_count + quarantine_count
  If assertion fails: raise exception.

INVARIANT ENFORCEMENT:
- INV-05: Every Bronze record exits into exactly one destination. Assert after write.
- INV-06: Global deduplication on transaction_id. Check ALL existing Silver partitions.
- INV-08: Sign assignment uses debit_credit_indicator from Silver transaction codes only.
  DR = positive. CR = negative. No custom logic. No inference.
- INV-09: INVALID_TRANSACTION_CODE -> quarantine, not Silver.
- INV-10: UNRESOLVABLE_ACCOUNT_ID -> Silver with _is_resolvable=FALSE. Never quarantine.
- INV-26: Quarantine rejection reasons from exhaustive list only.
  UNRESOLVABLE_ACCOUNT_ID is NOT a valid quarantine rejection reason.
- INV-37: transaction_code validation uses silver/transaction_codes only — not bronze, not source CSV.
- INV-22: _pipeline_run_id non-null for every Silver and quarantine record.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- No `transaction_id` appears more than once across all Silver partitions (global)
- All DR-coded transactions have positive `_signed_amount`
- All CR-coded transactions have negative `_signed_amount`
- No `_signed_amount` is null
- `UNRESOLVABLE_ACCOUNT_ID` records appear in Silver with `_is_resolvable = false` and NOT in quarantine
- Bronze count = Silver count + quarantine count for every date
- PURCHASE + null `merchant_name` → `_missing_merchant_name = true`

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Global dupes:', conn.execute(\"SELECT COUNT(*) FROM (SELECT transaction_id, COUNT(*) c FROM read_parquet('/app/data/silver/transactions/**/*.parquet') GROUP BY transaction_id HAVING c > 1)\").fetchone())
print('DR negative:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet') t JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc ON t.transaction_code = tc.transaction_code WHERE tc.debit_credit_indicator = 'DR' AND t._signed_amount < 0\").fetchone())
print('CR positive:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet') t JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc ON t.transaction_code = tc.transaction_code WHERE tc.debit_credit_indicator = 'CR' AND t._signed_amount > 0\").fetchone())
print('Unresolvable in quarantine:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/quarantine/**/*.parquet') WHERE _rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID'\").fetchone())
print('Null signed amount:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet') WHERE _signed_amount IS NULL\").fetchone())
"
```

**Invariant enforcement:** INV-05, INV-06, INV-08, INV-09, INV-10, INV-26, INV-37, INV-22

**Regression classification:** REGRESSION-RELEVANT:
```bash
docker compose run --rm pipeline python -c "import duckdb; conn=duckdb.connect(); print(conn.execute(\"SELECT COUNT(*) FROM (SELECT transaction_id, COUNT(*) c FROM read_parquet('/app/data/silver/transactions/**/*.parquet') GROUP BY transaction_id HAVING c > 1)\").fetchone())"
docker compose run --rm pipeline python -c "import duckdb; conn=duckdb.connect(); print(conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet') WHERE _signed_amount IS NULL\").fetchone())"
```
Both expected: `(0,)`

---

### Task 4.2 — Silver Transactions Verification Script

**Description:**
Creates `verification/verify_silver_transactions.sh` covering all Section 10.2 Silver quality checks.

**Inputs:** All Silver layer Parquet files
**Outputs:** `verification/verify_silver_transactions.sh`

**CC Prompt:**
```
Create verification/verify_silver_transactions.sh — Section 10.2 checks.

PASS/FAIL checks:

1. SILVER_BRONZE_ACCOUNTING
   For each date 2024-01-01 through 2024-01-07:
   bronze_count = silver_count + quarantine_count — expect 0 discrepancy per date.

2. SILVER_NO_DUPLICATE_TRANSACTION_ID
   No transaction_id > once across all Silver partitions — expect 0 rows.

3. SILVER_VALID_TRANSACTION_CODES
   Every Silver transaction has a transaction_code present in Silver transaction_codes.
   LEFT JOIN — expect 0 null matches.

4. SILVER_NO_NULL_SIGNED_AMOUNT
   No _signed_amount IS NULL — expect 0.

5. SILVER_QUARANTINE_VALID_REJECTION_REASONS
   All quarantine records have _rejection_reason in exhaustive list — expect 0 violations.

6. SILVER_SIGN_DR_POSITIVE
   All DR-coded transactions have _signed_amount > 0 — expect 0 violations.

7. SILVER_SIGN_CR_NEGATIVE
   All CR-coded transactions have _signed_amount < 0 — expect 0 violations.

8. SILVER_UNRESOLVABLE_NOT_IN_QUARANTINE
   No quarantine record has _rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID' — expect 0.

Final summary: N passed, M failed.
```

**Verification command:**
```bash
bash verification/verify_silver_transactions.sh
```

**Invariant enforcement:** INV-05, INV-06, INV-08, INV-09, INV-10, INV-26

**Regression classification:** REGRESSION-RELEVANT — portable from repo root. Full regression suite candidate.

---

### Task 4.3 — Silver Layer Integration Test

**Description:**
Creates `verification/verify_silver_integration.sh` — the total Bronze/Silver/quarantine accounting check across all dates combined. Operationalises IG-06.

**Inputs:** All Bronze and Silver layer Parquet files
**Outputs:** `verification/verify_silver_integration.sh`

**CC Prompt:**
```
Create verification/verify_silver_integration.sh.

Single check: SILVER_TOTAL_ACCOUNTING

  total_bronze    = SELECT COUNT(*) FROM read_parquet('data/bronze/transactions/**/*.parquet')
  total_silver    = SELECT COUNT(*) FROM read_parquet('data/silver/transactions/**/*.parquet')
  total_quarantine = SELECT COUNT(*) FROM read_parquet('data/silver/quarantine/**/*.parquet')

  Assert: total_bronze = total_silver + total_quarantine
  Print: PASS or FAIL with all three counts.

This is the mandatory post-run accounting check. It must pass for any pipeline run
to be considered correct.
```

**Verification command:**
```bash
bash verification/verify_silver_integration.sh
```

**Invariant enforcement:** INV-05

**Regression classification:** REGRESSION-RELEVANT — portable from repo root. Must be in regression suite.

---

## Session 5 — Gold Layer

**Session goal:** `gold/daily_summary/data.parquet` contains exactly one row per transaction date, including dates where all transactions were quarantined. `gold/weekly_account_summary/data.parquet` contains exactly one row per `(account_id, week_start_date)` for accounts with at least one resolvable transaction. All aggregations match Silver resolvable-only totals. Unresolvable exposure columns correct.

**Integration check:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Daily dupes:', conn.execute(\"SELECT COUNT(*) FROM (SELECT transaction_date, COUNT(*) c FROM read_parquet('/app/data/gold/daily_summary/data.parquet') GROUP BY transaction_date HAVING c > 1)\").fetchone())
print('Weekly dupes:', conn.execute(\"SELECT COUNT(*) FROM (SELECT account_id, week_start_date, COUNT(*) c FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet') GROUP BY account_id, week_start_date HAVING c > 1)\").fetchone())
print('Amount mismatch:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/gold/daily_summary/data.parquet') g LEFT JOIN (SELECT transaction_date, SUM(_signed_amount) s FROM read_parquet('/app/data/silver/transactions/**/*.parquet') WHERE _is_resolvable=true GROUP BY transaction_date) sv ON g.transaction_date=sv.transaction_date WHERE g.total_signed_amount != COALESCE(sv.s,0)\").fetchone())
"
```

---

### Task 5.1 — Gold Model: Daily Transaction Summary

**Description:**
Implements `dbt/models/gold/gold_daily_summary.sql`. Aggregates Silver transactions by `transaction_date`. Primary aggregations use `_is_resolvable = true` only. Adds `total_unresolvable_transactions` and `total_unresolvable_amount` exposure columns. Produces exactly one row per processed date including zero-transaction dates. Reads exclusively from Silver.

**Inputs:** `data/silver/transactions/**/*.parquet`, `data/silver/transaction_codes/data.parquet`
**Outputs:** `data/gold/daily_summary/data.parquet`

**CC Prompt:**
```
Implement dbt/models/gold/gold_daily_summary.sql.

Receives dbt var: run_id (STRING)

SOURCE: read_parquet('/app/data/silver/transactions/**/*.parquet')
REFERENCE: read_parquet('/app/data/silver/transaction_codes/data.parquet') for transaction_type

OUTPUT: one row per distinct transaction_date in Silver — including dates where
all transactions have _is_resolvable=false or all were quarantined (zero Silver records).
Use a date spine from MIN to MAX transaction_date in Silver and LEFT JOIN aggregations.

OUTPUT COLUMNS:
  transaction_date                DATE      calendar date
  total_transactions              INTEGER   COUNT(*) WHERE _is_resolvable = true
  total_signed_amount             DECIMAL   SUM(_signed_amount) WHERE _is_resolvable = true
  transactions_by_type            STRUCT    Per transaction_type: {count, sum_signed_amount}
                                            WHERE _is_resolvable = true
  online_transactions             INTEGER   COUNT WHERE channel='ONLINE' AND _is_resolvable=true
  instore_transactions            INTEGER   COUNT WHERE channel='IN_STORE' AND _is_resolvable=true
  total_unresolvable_transactions INTEGER   COUNT WHERE _is_resolvable = false
  total_unresolvable_amount       DECIMAL   SUM(_signed_amount) WHERE _is_resolvable = false
  _computed_at                    TIMESTAMP current_timestamp
  _pipeline_run_id                STRING    '{{ var("run_id") }}'
  _source_period_start            DATE      MIN(transaction_date) across all Silver
  _source_period_end              DATE      MAX(transaction_date) across all Silver

For dates with zero Silver records: total_transactions=0, total_signed_amount=0,
total_unresolvable_transactions=0, total_unresolvable_amount=0.

INVARIANT ENFORCEMENT:
- INV-11: Primary aggregations use _is_resolvable=true exclusively.
  Unresolvable columns use _is_resolvable=false exclusively. Do not mix populations.
- INV-12: Reads from Silver only. No reads from bronze/ or source/.
- INV-13: Exactly one row per transaction_date — including zero-transaction dates.
- INV-44: Dates with all transactions quarantined must appear with zero counts.
- INV-41: materialized='table' (set in dbt_project.yml). No incremental.
- INV-22: _pipeline_run_id non-null for every Gold record.

-- WARNING: This model must only be executed through pipeline.py.
-- Direct dbt invocation bypasses pipeline.py control plane.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- Exactly one row per `transaction_date` in output
- `total_signed_amount` matches `SUM(_signed_amount)` from Silver where `_is_resolvable=true`
- A fully-quarantined date appears with `total_transactions = 0`
- `total_unresolvable_transactions` matches COUNT of `_is_resolvable=false` Silver records
- No reads from `bronze/` or `source/` in SQL

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Daily dupes:', conn.execute(\"SELECT COUNT(*) FROM (SELECT transaction_date, COUNT(*) c FROM read_parquet('/app/data/gold/daily_summary/data.parquet') GROUP BY transaction_date HAVING c > 1)\").fetchone())
print('Amount mismatch:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/gold/daily_summary/data.parquet') g LEFT JOIN (SELECT transaction_date, SUM(_signed_amount) s FROM read_parquet('/app/data/silver/transactions/**/*.parquet') WHERE _is_resolvable=true GROUP BY transaction_date) sv ON g.transaction_date=sv.transaction_date WHERE g.total_signed_amount != COALESCE(sv.s,0)\").fetchone())
"
grep -r "bronze/" dbt/models/gold/
```

**Invariant enforcement:** INV-11, INV-12, INV-13, INV-41, INV-44, INV-22

**Regression classification:** REGRESSION-RELEVANT — amount mismatch and population mixing are silent analytical errors. Portable commands included above.

---

### Task 5.2 — Gold Model: Weekly Account Transaction Aggregates

**Description:**
Implements `dbt/models/gold/gold_weekly_account_summary.sql`. Aggregates by `(account_id, ISO week)` for `_is_resolvable = true` records only. Only accounts with at least one resolvable transaction in the week are included. `closing_balance` sourced from Silver accounts at model execution time. Receives only uncomputed weeks via `target_weeks` dbt var — does not read `gold_weekly_control`. Appends new week rows to existing Gold file.

**Inputs:** `data/silver/transactions/**/*.parquet`, `data/silver/accounts/data.parquet`
**Outputs:** `data/gold/weekly_account_summary/data.parquet`

**CC Prompt:**
```
Implement dbt/models/gold/gold_weekly_account_summary.sql.

-- WARNING: This model must only be executed through pipeline.py.
-- Direct invocation via `dbt run --select gold_weekly_account_summary` bypasses
-- pipeline.py and the pipeline/gold_weekly_control.parquet enforcement gate,
-- causing all weeks to be recomputed and overwriting closing_balance values.
-- There is no structural enforcement for this — it is a named procedural control.

Receives dbt vars: run_id (STRING), target_weeks (JSON string — array of
{"week_start": "YYYY-MM-DD", "week_end": "YYYY-MM-DD"})

Parse target_weeks from JSON. If target_weeks is empty: write no rows and exit.

SOURCE:
  transactions: read_parquet('/app/data/silver/transactions/**/*.parquet')
    Filter: _is_resolvable = true
    Filter: transaction_date within any week in target_weeks
  accounts: read_parquet('/app/data/silver/accounts/data.parquet')

AGGREGATION per (account_id, week_start_date):
  Include ONLY accounts with at least one resolvable transaction in that week.

OUTPUT COLUMNS:
  week_start_date      DATE      Monday of ISO week (from target_weeks)
  week_end_date        DATE      Sunday of ISO week
  account_id           STRING
  total_purchases      INTEGER   COUNT of transaction_type='PURCHASE'
  avg_purchase_amount  DECIMAL   AVG(_signed_amount) for PURCHASE (NULL if 0 purchases)
  total_payments       DECIMAL   SUM(_signed_amount) for transaction_type='PAYMENT'
  total_fees           DECIMAL   SUM(_signed_amount) for transaction_type='FEE'
  total_interest       DECIMAL   SUM(_signed_amount) for transaction_type='INTEREST'
  closing_balance      DECIMAL   current_balance from Silver accounts at execution time
  _computed_at         TIMESTAMP current_timestamp
  _pipeline_run_id     STRING    '{{ var("run_id") }}'

WRITE BEHAVIOUR:
  If data/gold/weekly_account_summary/data.parquet exists: read existing rows,
  UNION with new week rows, write complete file. Past weeks are immutable — never rewritten.
  If file does not exist: write new week rows as new file.

INVARIANT ENFORCEMENT:
- INV-11: Only _is_resolvable=true records in aggregations.
- INV-12: Reads from Silver only.
- INV-14: Model receives only uncomputed weeks in target_weeks. It must not query
  gold_weekly_control itself — pipeline.py has already filtered.
- INV-38: Exactly one row per (account_id, week_start_date).
  Accounts with zero resolvable transactions in a week excluded entirely.
- INV-41: materialized='table'.
- INV-22: _pipeline_run_id non-null.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- No duplicate `(account_id, week_start_date)` pairs
- Every account in output has at least one resolvable Silver transaction in that week
- `closing_balance` matches `current_balance` in Silver accounts
- Empty `target_weeks` produces no new rows and no error
- No reads from `bronze/` or `source/`

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Weekly dupes:', conn.execute(\"SELECT COUNT(*) FROM (SELECT account_id, week_start_date, COUNT(*) c FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet') GROUP BY account_id, week_start_date HAVING c > 1)\").fetchone())
print('Zero-tx accounts:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet') g LEFT JOIN (SELECT account_id, DATE_TRUNC('week', transaction_date) wk FROM read_parquet('/app/data/silver/transactions/**/*.parquet') WHERE _is_resolvable=true) s ON g.account_id=s.account_id AND g.week_start_date=s.wk WHERE s.account_id IS NULL\").fetchone())
"
```

**Invariant enforcement:** INV-11, INV-12, INV-14, INV-38, INV-41, INV-22

**Regression classification:** REGRESSION-RELEVANT — portable commands included above.

---

### Task 5.3 — Gold Verification Script

**Description:**
Creates `verification/verify_gold.sh` covering all Section 10.3 Gold correctness checks and cross-Gold consistency (INV-39).

**Inputs:** Gold and Silver layer Parquet files
**Outputs:** `verification/verify_gold.sh`

**CC Prompt:**
```
Create verification/verify_gold.sh — Section 10.3 checks.

PASS/FAIL checks:

1. GOLD_DAILY_ONE_ROW_PER_DATE
   No duplicate transaction_date in daily summary — expect 0 duplicates.

2. GOLD_DAILY_AMOUNT_MATCHES_SILVER
   For each date: Gold total_signed_amount = SUM(_signed_amount) from Silver
   where _is_resolvable=true. Expect 0 mismatches.

3. GOLD_WEEKLY_ONE_ROW_PER_ACCOUNT_WEEK
   No duplicate (account_id, week_start_date) — expect 0 duplicates.

4. GOLD_WEEKLY_PURCHASES_MATCH_SILVER
   For week starting 2024-01-01: Gold total_purchases = COUNT(*) from Silver
   PURCHASE transactions with _is_resolvable=true. Expect match.

5. GOLD_CROSS_CONSISTENCY (INV-39)
   Every account in Gold weekly has at least one Gold daily row in same week
   with total_transactions > 0. Expect 0 violations.

6. GOLD_NO_NULL_RUN_ID
   No null _pipeline_run_id in daily_summary or weekly_account_summary. Expect 0.

7. GOLD_NO_BRONZE_READS
   grep -r "bronze/" dbt/models/gold/ — expect no output.

Final summary: N passed, M failed.
```

**Verification command:**
```bash
bash verification/verify_gold.sh
```

**Invariant enforcement:** INV-11, INV-13, INV-38, INV-39, INV-22

**Regression classification:** REGRESSION-RELEVANT — portable from repo root.

---

## Session 6 — Pipeline Orchestration

**Session goal:** `pipeline.py` fully orchestrates both historical and incremental modes. DAG derived from `dbt compile`. JSON log streaming produces real-time per-model run log entries. Watermark advances only on full success. `gold_weekly_control` written before watermark advances. SKIPPED rows written for non-executed models on failure. Async run log buffer flushes after watermark advancement.

**Integration check:**
```bash
docker compose run --rm pipeline python pipeline/pipeline.py --mode historical --start-date 2024-01-01 --end-date 2024-01-07
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Watermark:', conn.execute(\"SELECT last_processed_date FROM read_parquet('/app/data/pipeline/control.parquet')\").fetchone())
print('Run log rows:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/pipeline/run_log.parquet')\").fetchone())
print('Weekly control:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/pipeline/gold_weekly_control.parquet')\").fetchone())
"
```

---

### Task 6.1 — DAG Derivation and dbt JSON Log Streaming

**Description:**
Creates `pipeline/dbt_runner.py` with `derive_execution_order()` (reads `target/manifest.json` via topological sort) and `stream_dbt_layer()` (runs dbt with `--log-format json`, streams `NodeStart` and `NodeFinished` events in real time). Compile failure raises a typed exception that the caller converts to an ORCHESTRATION run log row.

**Inputs:** dbt project with `dbt_project.yml` and `target/manifest.json`
**Outputs:** `pipeline/dbt_runner.py`

**CC Prompt:**
```
Create pipeline/dbt_runner.py.

CLASS: CompileError(Exception) — raised when dbt compile fails.

FUNCTION 1: derive_execution_order() -> dict
  Purpose: derive model execution order from the dbt DAG. Single stateable purpose.

  Steps:
    1. subprocess.run(['dbt', 'compile', '--project-dir', '/app/dbt',
       '--profiles-dir', '/app/dbt'], capture_output=True, text=True)
    2. If returncode != 0: raise CompileError(stderr)
    3. Read /app/dbt/target/manifest.json
    4. Extract model nodes (keys starting with 'model.')
    5. Topological sort using node['depends_on']['nodes'] edges
    6. Return {"silver": [names with tag 'silver' in topo order],
               "gold": [names with tag 'gold' in topo order]}

FUNCTION 2: stream_dbt_layer(tag: str, run_id: str, model_vars: dict) -> generator
  Purpose: run dbt for one layer and yield per-model events as they arrive. Single purpose.

  Steps:
    1. cmd = ['dbt', 'run', '--project-dir', '/app/dbt', '--profiles-dir', '/app/dbt',
              '--select', f'tag:{tag}', '--vars', json.dumps(model_vars),
              '--log-format', 'json']
    2. proc = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, text=True)
    3. For each stdout line: parse JSON.
       If event type is 'NodeStart': yield {"event": "start", "model": name, "started_at": ts}
       If event type is 'NodeFinished': yield {"event": "finish", "model": name,
         "status": status, "started_at": ts, "completed_at": ts}
    4. proc.wait()
    5. yield {"event": "exit", "returncode": proc.returncode}

INVARIANT ENFORCEMENT:
- IG-03: derive_execution_order uses topological sort from manifest.json.
  A hardcoded model list is prohibited.
- IG-08: dbt-core is pinned to 1.7.9 in requirements.txt. NodeStart and NodeFinished
  field paths are version-specific. Any dbt upgrade requires re-verifying these paths
  before deployment.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- `derive_execution_order()` returns silver models before gold, `silver_transactions` after `silver_accounts`
- Broken `ref()` in any model causes `CompileError`
- `stream_dbt_layer` yields at least one `NodeFinished` event per model
- Exit event always yielded regardless of model success or failure

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from pipeline.dbt_runner import derive_execution_order
order = derive_execution_order()
print('Silver order:', order['silver'])
print('Gold order:', order['gold'])
"
```

**Invariant enforcement:** IG-03, IG-08

**Regression classification:** REGRESSION-RELEVANT — DAG-derived order is a silent failure when new models are added.

---

### Task 6.2 — Run Log Writer with Async Buffer

**Description:**
Creates `pipeline/run_log.py` with `RunLogBuffer` class. Accumulates entries in memory throughout execution. Deduplicates on `(run_id, model_name)`. Flushes to `data/pipeline/run_log.parquet` as single append after watermark advancement. Fallback `.jsonl` on flush failure. Detects and writes synthetic `UNLOGGED_RUN` row for prior run with no log entries.

**Inputs:** Run log entries from dbt stream and orchestrator events
**Outputs:** `pipeline/run_log.py`

**CC Prompt:**
```
Create pipeline/run_log.py.

CLASS: RunLogBuffer

  __init__(self, run_id: str, pipeline_type: str):
    self._buffer = []
    self._run_id = run_id
    self._pipeline_type = pipeline_type  # 'HISTORICAL' or 'INCREMENTAL'

  add_entry(self, model_name, layer, started_at, completed_at, status,
            records_processed=None, records_written=None, records_rejected=None,
            error_message=None):
    Build entry dict. Deduplicate: if entry for same (run_id, model_name) exists
    in buffer, REPLACE it. Then append.

  add_skipped(self, model_name: str, layer: str):
    add_entry with status='SKIPPED', all counts None, error_message=None.

  add_orchestration_failure(self, error_message: str):
    add_entry with model_name='DBT_COMPILE', layer='ORCHESTRATION',
    status='FAILED', all counts None, error_message=error_message.

  flush(self, target_path: str):
    If target_path exists: read existing Parquet, concat with buffer DataFrame, write back.
    If not exists: write buffer as new Parquet.
    On any exception:
      Write buffer as JSON lines to /app/data/pipeline/pipeline_runlog_fallback.jsonl (append).
      Raise the original exception so caller knows flush failed.

  check_unlogged_run(self, control_path: str, run_log_path: str) -> str | None:
    Read updated_by_run_id from control_path.
    If run_log_path does not exist: return updated_by_run_id (it is unlogged).
    Check: SELECT COUNT(*) FROM run_log WHERE run_id = updated_by_run_id.
    If count = 0: return updated_by_run_id.
    Else: return None.

  write_unlogged_run_row(self, unlogged_run_id: str):
    add_entry: model_name='UNLOGGED_RUN', layer='ORCHESTRATION', status='FAILED',
    error_message=f'Run log flush failed for prior run {unlogged_run_id}. Data was
    written successfully. See pipeline_runlog_fallback.jsonl for run detail.'

INVARIANT ENFORCEMENT:
- INV-19: Run log append-only. flush() reads existing rows and appends — never truncates.
- INV-20A: One SUCCESS row per model per run — deduplicate in add_entry before append.
- INV-20B: Every failed run has at least one traceable run log row — enforced by callers
  using add_orchestration_failure or add_entry(status='FAILED').
- INV-20C: check_unlogged_run + write_unlogged_run_row implement the recovery mechanism.
- IG-02: Deduplicate on (run_id, model_name) in add_entry before appending to buffer.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- Buffer accumulates correctly; flush appends without overwriting existing rows
- Deduplication: two calls to `add_entry` for same `(run_id, model_name)` result in one buffer entry
- Flush failure writes to fallback `.jsonl` and raises
- `check_unlogged_run` detects prior unlogged run_id from control.parquet

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Run log rows:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/pipeline/run_log.parquet')\").fetchone())
print('Dupes:', conn.execute(\"SELECT COUNT(*) FROM (SELECT run_id, model_name, COUNT(*) c FROM read_parquet('/app/data/pipeline/run_log.parquet') GROUP BY run_id, model_name HAVING c > 1)\").fetchone())
"
```

**Invariant enforcement:** INV-19, INV-20A, INV-20B, INV-20C, IG-02

**Regression classification:** REGRESSION-RELEVANT — append-only violation is a silent audit integrity failure.

---

### Task 6.3 — Watermark and gold_weekly_control Manager

**Description:**
Creates `pipeline/control_plane.py` with functions for reading/advancing the watermark and reading/updating `gold_weekly_control`. Enforces call ordering via documented contract — `record_computed_weeks` must be called before `advance_watermark` by `pipeline.py`.

**Inputs:** `data/pipeline/control.parquet`, `data/pipeline/gold_weekly_control.parquet`
**Outputs:** `pipeline/control_plane.py`

**CC Prompt:**
```
Create pipeline/control_plane.py.

FUNCTION 1: get_watermark(control_path: str) -> date | None
  Read last_processed_date from control_path Parquet. Return date or None if zero rows.

FUNCTION 2: advance_watermark(control_path: str, new_date: date, run_id: str) -> None
  Write single row: last_processed_date=new_date, updated_at=now(), updated_by_run_id=run_id.
  ATOMIC WRITE — do NOT overwrite control_path directly:
    temp_path = control_path + '.tmp'
    Write the new single-row Parquet to temp_path.
    Then: os.replace(temp_path, control_path)
    os.replace() is atomic on POSIX — either the full file is swapped or the old file
    remains intact. A crash before the rename leaves the old control.parquet untouched.
    A crash after the rename means the new file is fully written. No partial-write window.
  PRECONDITION: must only be called AFTER record_computed_weeks succeeds for same run.
  Add docstring stating this precondition explicitly.

FUNCTION 3: get_computed_weeks(weekly_control_path: str) -> set
  Return set of week_start_date values from gold_weekly_control Parquet.
  Return empty set if zero rows.

FUNCTION 4: record_computed_weeks(weekly_control_path: str, weeks: list, run_id: str) -> None
  weeks: list of dicts with keys week_start_date (date), week_end_date (date).
  Append new rows to weekly_control Parquet with computed_at=now(), computed_by_run_id=run_id.
  Read existing file first if it exists, concat, write back.
  Must be called BEFORE advance_watermark. Docstring states this.

FUNCTION 5: get_uncomputed_weeks(silver_path: str, weekly_control_path: str) -> list
  Read all distinct ISO weeks (Monday to Sunday) from Silver transactions
  where _is_resolvable=true.
  Exclude weeks already in gold_weekly_control.
  Return list of dicts: [{"week_start_date": date, "week_end_date": date}, ...]

INVARIANT ENFORCEMENT:
- INV-15: advance_watermark is only called after all layers succeed. This module
  provides the function — pipeline.py is responsible for calling it at the right time.
- INV-31: record_computed_weeks must be called before advance_watermark.
  Enforced via call order in pipeline.py. Docstrings state the precondition.
- INV-32: This module raises if files are absent. pipeline.py initialises files first.
- IG-04: State is read exclusively from control Parquet files. No filesystem inference.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- `get_watermark` returns None on empty file, correct date after advance
- `advance_watermark` overwrites single row correctly
- `get_computed_weeks` returns empty set on empty file
- `record_computed_weeks` appends without overwriting existing rows
- `get_uncomputed_weeks` excludes already-computed weeks

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Watermark:', conn.execute(\"SELECT last_processed_date FROM read_parquet('/app/data/pipeline/control.parquet')\").fetchone())
print('Computed weeks:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/pipeline/gold_weekly_control.parquet')\").fetchone())
"
```

**Invariant enforcement:** INV-15, INV-31, INV-32, IG-04

**Regression classification:** REGRESSION-RELEVANT — watermark advancing past a failed date is unrecoverable.

---

### Task 6.4 — Historical Pipeline Mode

**Description:**
Wires the historical pipeline mode in `pipeline/pipeline.py`. Processes transaction_codes first (Bronze + Silver), then for each date in range: accounts Bronze → accounts Silver → transactions Bronze → transactions Silver (enforcing INV-24 intra-date order). Then Gold models. Then `record_computed_weeks`. Then `advance_watermark`. Then run log buffer flush.

**Inputs:** All Bronze loaders, `dbt_runner.py`, `run_log.py`, `control_plane.py`
**Outputs:** Fully wired historical mode in `pipeline.py`

**CC Prompt:**
```
Wire the historical pipeline mode into pipeline/pipeline.py.

When --mode historical:

PRE-CHECKS:
  Validate --start-date and --end-date present and parse as dates. start <= end.
  For each date in range: check source/transactions_{date}.csv and
  source/accounts_{date}.csv exist. Check source/transaction_codes.csv exists.
  If any file missing: log which files are absent. SystemExit(1). Do not process.

UNLOGGED RUN DETECTION:
  run_log_buffer.check_unlogged_run(control_path, run_log_path)
  If returns a run_id: call write_unlogged_run_row(unlogged_run_id)

STEP 0 — Transaction Codes (once):
  load_bronze_transaction_codes(run_id) -> log entry
  dbt silver_transaction_codes via stream_dbt_layer -> log entry

FOR EACH DATE in start_date to end_date (in ascending order):
  1. load_bronze_accounts(date, run_id) -> log entry
  2. stream_dbt_layer('silver', run_id, {'target_date': date, 'run_id': run_id})
     Process NodeFinished for silver_accounts:
       Query records_written from Silver accounts, records_rejected from quarantine.
       Add log entry.
  3. load_bronze_transactions(date, run_id) -> log entry
  4. stream_dbt_layer for silver_transactions:
       Query records_written from Silver transactions, records_rejected from quarantine.
       Add log entry.
  On ANY step failure:
    Add FAILED log entry for failing step.
    Add SKIPPED entries for all remaining models.
    flush buffer to fallback jsonl.
    SystemExit(1). Watermark does NOT advance.

GOLD:
  uncomputed_weeks = get_uncomputed_weeks(silver_path, weekly_control_path)
  stream_dbt_layer('gold', run_id, {'run_id': run_id,
    'target_weeks': json.dumps([{'week_start': str(w['week_start_date']),
    'week_end': str(w['week_end_date'])} for w in uncomputed_weeks])})
  Log entries for gold_daily_summary and gold_weekly_account_summary.
  On failure: FAILED + SKIPPED entries, flush to fallback, SystemExit(1).

POST-GOLD — SUCCESS PATH ONLY:
  record_computed_weeks(weekly_control_path, uncomputed_weeks, run_id)  # BEFORE watermark
  advance_watermark(control_path, end_date, run_id)                    # AFTER weekly_control
  run_log_buffer.flush(run_log_path)

INVARIANT ENFORCEMENT:
- INV-24: Intra-date order — accounts Bronze, accounts Silver, transactions Bronze,
  transactions Silver. This exact sequence. No deviation.
- INV-15: Watermark advances only after all steps complete without error.
- INV-31: record_computed_weeks called before advance_watermark.
- INV-33: Gold Parquet files fully written before watermark advances.
- INV-35: SKIPPED rows written for all non-executed models on failure.
- INV-20B: At least one run log row for every invocation including failures.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- Full run: watermark set to `2024-01-07`, run log has entries for all models
- Simulated Silver failure on day 3: watermark unchanged, FAILED + SKIPPED entries in log
- Re-run: Bronze skips loaded partitions, Silver picks up
- `gold_weekly_control` has entry for every ISO week in the date range

**Verification command:**
```bash
docker compose run --rm pipeline python pipeline/pipeline.py --mode historical --start-date 2024-01-01 --end-date 2024-01-07
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Watermark:', conn.execute(\"SELECT last_processed_date FROM read_parquet('/app/data/pipeline/control.parquet')\").fetchone())
print('Run log rows:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/pipeline/run_log.parquet')\").fetchone())
print('Weekly control rows:', conn.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/pipeline/gold_weekly_control.parquet')\").fetchone())
"
```

**Invariant enforcement:** INV-24, INV-15, INV-31, INV-33, INV-35, INV-20B

**Regression classification:** REGRESSION-RELEVANT — watermark advancing past a failed date is unrecoverable within this exercise.

---

### Task 6.5 — Incremental Pipeline Mode

**Description:**
Wires the incremental pipeline mode in `pipeline/pipeline.py`. Reads watermark, determines `next_date = watermark + 1 day`, checks for source file existence (no-op exit if absent), processes exactly that one date, advances watermark. Processes exactly one date per invocation.

**Inputs:** `data/pipeline/control.parquet`, source files for next date
**Outputs:** Fully wired incremental mode in `pipeline.py`

**CC Prompt:**
```
Wire the incremental pipeline mode into pipeline/pipeline.py.

When --mode incremental:

STEP 1 — Read watermark:
  watermark = get_watermark(control_path)
  If watermark is None: log error "Historical pipeline must be run first." SystemExit(1).
  next_date = watermark + timedelta(days=1)

STEP 2 — Source file check:
  If source/transactions_{next_date}.csv OR source/accounts_{next_date}.csv absent:
    Log: f"No source files for {next_date} — no-op exit."
    SystemExit(0). No Bronze writes. No run log rows. No watermark change.

STEP 3 — Process next_date (same intra-date sequence as historical):
  1. load_bronze_accounts(next_date, run_id)
  2. stream_dbt_layer silver_accounts for next_date
  3. load_bronze_transactions(next_date, run_id)
  4. stream_dbt_layer silver_transactions for next_date
  On failure: FAILED + SKIPPED entries, flush to fallback, SystemExit(1).

STEP 4 — Gold:
  uncomputed_weeks = get_uncomputed_weeks(silver_path, weekly_control_path)
  Run gold_daily_summary and gold_weekly_account_summary.
  On failure: FAILED + SKIPPED, flush to fallback, SystemExit(1).

STEP 5 — Success path only:
  record_computed_weeks(uncomputed_weeks, run_id)
  advance_watermark(next_date, run_id)
  run_log_buffer.flush(run_log_path)

INVARIANT ENFORCEMENT:
- INV-17: Process exactly next_date (watermark + 1). No other date.
- INV-18: Missing source file = no-op clean exit. Not a failure. No run log rows.
- INV-15: Watermark advances only on full success.
- INV-24: Same intra-date ordering as historical mode.
- INV-35: SKIPPED rows for non-executed models on failure.

Each function must have a single stateable purpose. Conditional nesting must not exceed
two levels. This is never negotiable.
```

**Test cases:**
- No source file for next date: exits 0, watermark unchanged, no run log rows written
- Valid source file: watermark advances by exactly 1 day
- Running twice with no new file: both runs are no-op, watermark unchanged
- Silver failure: watermark stays at prior value

**Verification command:**
```bash
docker compose run --rm pipeline python pipeline/pipeline.py --mode incremental
docker compose run --rm pipeline python -c "
import duckdb; conn=duckdb.connect()
print('Watermark after no-op:', conn.execute(\"SELECT last_processed_date FROM read_parquet('/app/data/pipeline/control.parquet')\").fetchone())
"
```

**Invariant enforcement:** INV-17, INV-18, INV-15, INV-24, INV-35

**Regression classification:** REGRESSION-RELEVANT — watermark advancing on no-op is unrecoverable.

---

## Session 7 — End-to-End Verification

**Session goal:** Full historical pipeline runs cleanly from a clean state. All Section 10 verification commands pass. Idempotency proof passes across all layers. Audit trail is complete and traceable. `verification/REGRESSION_SUITE.sh` is assembled and passes. Engineer signs off on Phase 8 entry criteria.

**Pre-session requirement:** Remove all Parquet outputs (`rm -rf data/`) so the pipeline runs from scratch.

**Integration check:**
```bash
rm -rf data/
docker compose run --rm pipeline python pipeline/pipeline.py --mode historical --start-date 2024-01-01 --end-date 2024-01-07
bash verification/verify_bronze.sh
bash verification/verify_silver_transactions.sh
bash verification/verify_silver_accounts.sh
bash verification/verify_gold.sh
bash verification/verify_section10.sh
```

---

### Task 7.1 — Section 10 Verification Commands

**Description:**
Creates `verification/verify_section10.sh` — exact DuckDB queries implementing every verification condition from Section 10 of the requirements brief. This is the formal Phase 8 sign-off evidence.

**Inputs:** All layer Parquet files after successful historical pipeline run
**Outputs:** `verification/verify_section10.sh`

**CC Prompt:**
```
Create verification/verify_section10.sh — every verification condition from
Section 10 of the requirements brief as exact DuckDB CLI queries.
Run from repo root: bash verification/verify_section10.sh

Section 10.1 — Bronze Completeness:
  B1: Total rows in bronze/transactions across all 7 partitions = total rows
      across all 7 source CSV files. Print actual counts. PASS/FAIL.
  B2: Same for bronze/accounts.
  B3: bronze/transaction_codes row count = transaction_codes.csv row count.

Section 10.2 — Silver Quality:
  S1: silver transactions + quarantine = bronze transactions. PASS/FAIL.
  S2: No transaction_id > once in Silver. PASS/FAIL.
  S3: Every Silver transaction has valid transaction_code in Silver TC. PASS/FAIL.
  S4: No null _signed_amount in Silver transactions. PASS/FAIL.
  S5: Every quarantine record has non-null _rejection_reason from exhaustive list. PASS/FAIL.

Section 10.3 — Gold Correctness:
  G1: Gold daily_summary has exactly one row per distinct transaction_date in Silver
      where _is_resolvable=true. PASS/FAIL.
  G2: Gold weekly total_purchases for week 2024-01-01 = COUNT(*) from Silver
      PURCHASE transactions with _is_resolvable=true for that week. PASS/FAIL.
  G3: Gold total_signed_amount per date = SUM(_signed_amount) from Silver
      where _is_resolvable=true. PASS/FAIL.

Section 10.4 — Idempotency:
  I1-I4: Run pipeline a second time. Assert identical row counts per layer. PASS/FAIL.
  I5: Run incremental with no new file. Assert no layer changes. PASS/FAIL.

Section 10.5 — Audit Trail:
  A1: No null _pipeline_run_id in Bronze. PASS/FAIL.
  A2: No null _pipeline_run_id in Silver. PASS/FAIL.
  A3: No null _pipeline_run_id in Gold. PASS/FAIL.
  A4: Every Silver _pipeline_run_id has a corresponding run_log row with status=SUCCESS.
      SELECT DISTINCT _pipeline_run_id FROM silver EXCEPT SELECT run_id FROM run_log
      WHERE status='SUCCESS' — expect 0 rows. PASS/FAIL.

Final summary: N checks passed, M checks failed.
```

**Test cases:**
- All checks PASS after clean historical pipeline run
- Any deliberate data corruption causes the relevant check to FAIL with diagnostic output

**Verification command:**
```bash
bash verification/verify_section10.sh
```

**Invariant enforcement:** All invariants — this is the complete system verification.

**Regression classification:** REGRESSION-RELEVANT — full candidate for regression suite foundation.

---

### Task 7.2 — Idempotency Proof

**Description:**
Creates `verification/verify_idempotency.sh`. Runs the full pipeline twice and asserts identical row counts and content. Runs incremental pipeline with no new source file and asserts no changes. Checks Bronze partition mtimes do not change on re-run.

**Inputs:** Clean state + source CSVs
**Outputs:** `verification/verify_idempotency.sh`

**CC Prompt:**
```
Create verification/verify_idempotency.sh — three idempotency tests.

TEST 1 — FULL_PIPELINE_RERUN:
  Record row counts for all layers after first run.
  Run: docker compose run --rm pipeline python pipeline/pipeline.py --mode historical
       --start-date 2024-01-01 --end-date 2024-01-07
  Record row counts again.
  Assert counts identical per layer:
    bronze/transactions, bronze/accounts, bronze/transaction_codes,
    silver/transactions, silver/accounts, silver/quarantine,
    gold/daily_summary, gold/weekly_account_summary
  PASS if all identical. FAIL with layer name and counts if any differ.

TEST 2 — INCREMENTAL_NOOP:
  Confirm no source file exists for 2024-01-08.
  Run incremental pipeline.
  Assert watermark unchanged (still 2024-01-07).
  Assert all layer row counts unchanged.
  PASS/FAIL.

TEST 3 — BRONZE_PARTITION_IMMUTABILITY:
  Record mtime for all Bronze partition files (stat -c %Y or stat -f %m).
  Run full historical pipeline again.
  Assert no mtime changed.
  PASS/FAIL per partition. Label any changed files as FAIL with old and new mtime.

Final summary: N tests passed, M tests failed.
```

**Verification command:**
```bash
bash verification/verify_idempotency.sh
```

**Invariant enforcement:** INV-02, INV-03, INV-06, INV-14, INV-17, INV-18, INV-41

**Regression classification:** REGRESSION-RELEVANT — full regression suite candidate.

---

### Task 7.3 — Audit Trail Verification

**Description:**
Creates `verification/verify_audit_trail.sh`. Verifies complete audit trail from Gold → Silver → Bronze via `_pipeline_run_id`. Every `_pipeline_run_id` in Silver must trace to a SUCCESS row in the run log.

**Inputs:** All layer Parquet files, `data/pipeline/run_log.parquet`
**Outputs:** `verification/verify_audit_trail.sh`

**CC Prompt:**
```
Create verification/verify_audit_trail.sh — Section 10.5 audit trail verification.

PASS/FAIL checks:

AT1: BRONZE_RUN_ID_COMPLETE
  No null _pipeline_run_id across bronze/transactions, bronze/accounts,
  bronze/transaction_codes — expect 0 per entity.

AT2: SILVER_RUN_ID_COMPLETE
  No null _pipeline_run_id across silver/transactions, silver/accounts,
  silver/transaction_codes, silver/quarantine — expect 0 per entity.

AT3: GOLD_RUN_ID_COMPLETE
  No null _pipeline_run_id in gold/daily_summary, gold/weekly_account_summary.

AT4: SILVER_RUN_ID_TRACEABLE
  SELECT DISTINCT _pipeline_run_id FROM silver/transactions/**/*.parquet
  EXCEPT
  SELECT run_id FROM pipeline/run_log.parquet WHERE status='SUCCESS'
  Expect 0 rows — every Silver run_id must appear in run_log with SUCCESS.

AT5: RUN_LOG_APPEND_ONLY
  Print current run_log row count.
  Label: MANUAL CHECK — engineer confirms count is non-decreasing across runs.

AT6: WATERMARK_RUN_ID_IN_LOG
  updated_by_run_id from control.parquet must appear in run_log.parquet.
  Expect 1 matching row.

Final summary: N passed, M failed.
```

**Verification command:**
```bash
bash verification/verify_audit_trail.sh
```

**Invariant enforcement:** INV-22, INV-19, INV-20A, INV-20B

**Regression classification:** REGRESSION-RELEVANT — AT4 is the core lineage guarantee.

---

### Task 7.4 — Regression Suite Assembly

**Description:**
Assembles `verification/REGRESSION_SUITE.sh` from all REGRESSION-RELEVANT portable verification commands identified across Sessions 1–7. This is the Phase 8 required output per PBVI v4.1 (CQ-002). Non-portable commands are noted with reason, not silently omitted.

**Inputs:** All verification scripts and portable commands from REGRESSION-RELEVANT tasks
**Outputs:** `verification/REGRESSION_SUITE.sh`

**CC Prompt:**
```
Create verification/REGRESSION_SUITE.sh — consolidated regression suite.
Runnable from repo root with no session-specific setup.

Assemble portable PASS/FAIL checks in this order:

## SECTION: ENVIRONMENT
  - dbt gold models have no incremental materialisation: grep -r "incremental" dbt/models/gold/
    PASS if no output.

## SECTION: BRONZE LAYER
  - No null _pipeline_run_id in bronze/transactions
  - No null _pipeline_run_id in bronze/accounts
  - No null _pipeline_run_id in bronze/transaction_codes
  - Bronze transaction row count per date matches source CSV line count

## SECTION: SILVER LAYER
  - No duplicate transaction_id across all Silver partitions
  - No null _signed_amount in Silver transactions
  - No null _pipeline_run_id in Silver transactions
  - No UNRESOLVABLE_ACCOUNT_ID in quarantine
  - Bronze = Silver + quarantine (total across all dates)
  - No duplicate account_id in Silver accounts
  - All quarantine rejection reasons from exhaustive list

## SECTION: GOLD LAYER
  - No duplicate transaction_date in daily summary
  - Gold total_signed_amount matches Silver resolvable-only sum per date
  - No duplicate (account_id, week_start_date) in weekly summary
  - No accounts in Gold weekly with zero resolvable Silver transactions

## SECTION: AUDIT TRAIL
  - Silver run_ids traceable to run_log SUCCESS rows
  - No null _pipeline_run_id in Gold daily summary
  - No null _pipeline_run_id in Gold weekly summary

## SECTION: CONTROL PLANE
  - gold_weekly_control has entry for every week_start_date in Gold weekly summary
  - Watermark exists and is non-null in control.parquet

## SECTION: IDEMPOTENCY (MANUAL — note reason)
  - Full pipeline re-run row count comparison: MANUAL — requires running pipeline twice
  - Bronze mtime check: MANUAL — requires comparing timestamps across runs

Each automated check prints PASS or FAIL with label.
Non-portable manual checks are listed under a MANUAL CHECKS section at the end
with one-line reason why they cannot be automated.

Final summary: N automated checks passed, M failed. K manual checks require engineer.
```

**Verification command:**
```bash
bash verification/REGRESSION_SUITE.sh
```

**Invariant enforcement:** All invariants — this is the complete regression guard.

**Regression classification:** REGRESSION-RELEVANT — this script is itself the regression suite. Must be committed to `verification/REGRESSION_SUITE.sh`.

---

## Engineer Sign-Off

I confirm that:
- All open questions from ARCHITECTURE.md are resolved with concrete decisions
- All 27 tasks produce discrete, independently verifiable outputs
- Every TASK-SCOPED invariant from INVARIANTS.md is embedded in at least one task CC prompt with full condition text
- GLOBAL invariants (INV-01, INV-22) are enforced in Claude.md (Phase 5) and noted in relevant tasks
- Regression classification is assigned to every task
- Portable verification commands are provided for all REGRESSION-RELEVANT tasks
- Session 1 begins with a scaffolding task (Task 1.1)
- `verification/REGRESSION_SUITE.sh` is a named output of Task 7.4

**Signed:** Pratham
**Date:** 16/04/2026

*Ready for Phase 4 — Design Gate*
