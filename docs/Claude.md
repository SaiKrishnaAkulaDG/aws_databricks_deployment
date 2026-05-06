# Claude.md — v1.0 · FROZEN · 16/04/2026

## Changelog
| Version | Date | Author | Change |
|---|---|---|---|
| v1.0 | 16/04/2026 | Pratham | Greenfield — Initial |

---

## 1. System Intent

This system ingests daily credit card transaction CSV extracts, enforces data quality rules at each layer boundary through a Medallion architecture (Bronze → Silver → Gold), and produces Gold-layer aggregations queryable via DuckDB with a complete, traceable audit trail from raw file to Gold aggregate. It does not perform risk computation, credit decisioning, or any modification of source system records. Success is a pipeline that is fully re-runnable without producing duplicates or incorrect aggregations, and where every Gold record is traceable back to a Bronze source row via `_pipeline_run_id`.

---

## 2. Hard Invariants

INVARIANT: Each function, method, or handler must have a single stateable purpose. Conditional nesting exceeding two levels is a structural violation — refactor before proceeding. This is never negotiable.

INVARIANT: Source CSV files in `source/` must never be modified, overwritten, deleted, or renamed by any pipeline component. No write operation may target any path beginning with `/app/source`. This is never negotiable.

INVARIANT: Every record in Silver (transactions, accounts, transaction codes, quarantine) and Gold (daily summary, weekly account summary) must have a non-null `_pipeline_run_id` that is traceable to a corresponding row in `pipeline/run_log.parquet` with `status = SUCCESS` for the same `run_id` and `model_name`. This is never negotiable.

---

## 3. Scope Boundary

CC may create or modify files only within these paths:

```
pipeline/
dbt/models/
dbt/tests/
dbt/macros/
dbt/dbt_project.yml
dbt/profiles.yml
dbt/packages.yml
docker-compose.yml
Dockerfile
requirements.txt
.gitignore
README.md
verification/
sessions/
tools/
```

CC must not:
- Write to or read from `source/` as a write target
- Modify `docs/` (planning artifacts are frozen)
- Modify `data/` directly (runtime outputs only — pipeline writes these)
- Modify `PROJECT_MANIFEST.md` without being explicitly instructed to do so

If a task prompt conflicts with an invariant: the invariant wins — flag it, never resolve silently.

---

## 4. Fixed Stack

| Component | Version / Value |
|---|---|
| Python | 3.11 |
| dbt-core | 1.7.9 |
| dbt-duckdb | 1.7.4 |
| DuckDB | 0.10.0 |
| Docker Compose | v2 |
| Container name | `pipeline` |
| dbt project dir | `/app/dbt` |
| dbt profiles dir | `/app/dbt` |
| Source CSV dir | `/app/source/` |
| Bronze data dir | `/app/data/bronze/` |
| Silver data dir | `/app/data/silver/` |
| Gold data dir | `/app/data/gold/` |
| Control dir | `/app/data/pipeline/` |
| Run log path | `/app/data/pipeline/run_log.parquet` |
| Control path | `/app/data/pipeline/control.parquet` |
| Weekly control path | `/app/data/pipeline/gold_weekly_control.parquet` |
| Fallback log path | `/app/data/pipeline/pipeline_runlog_fallback.jsonl` |
| dbt log format | `--log-format json` |
| Run ID scheme | `uuid.uuid4()` — never timestamps or sequential integers |
| Gold model materialisation | `table` — `incremental` is prohibited in `dbt/models/gold/` |
| ORCHESTRATION sentinel — model_name | `DBT_COMPILE` |
| ORCHESTRATION sentinel — layer | `ORCHESTRATION` |

Environment variables (defined in `.env`, sourced by Docker Compose):
- `DBT_PROFILES_DIR` — `/app/dbt`

---

## 5. Rules

Rule 1: All file references use full paths from repo root — never bare filenames.

Rule 2: All files inside any enhancement package carry their ENH-NNN prefix — no exceptions.

Rule 3: Any file not in the mandatory set for its directory and not registered in PROJECT_MANIFEST.md must not be read by CC as authoritative input. CC flags unregistered files and reports them to the engineer before proceeding.
