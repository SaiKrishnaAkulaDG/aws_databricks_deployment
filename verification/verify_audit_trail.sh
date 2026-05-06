#!/bin/bash
set -uo pipefail
cd "$(dirname "$0")/.."

PASSED=0
FAILED=0

pass() { echo "  PASS: $1"; PASSED=$((PASSED + 1)); }
fail() { echo "  FAIL: $1"; FAILED=$((FAILED + 1)); }

echo "=== Audit Trail Verification — Section 10.5 ==="
echo ""

# AT1: AUDIT_TRAIL_SILVER_TRANSACTIONS_RUN_ID
echo "AT1: AUDIT_TRAIL_SILVER_TRANSACTIONS_RUN_ID"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
    WHERE _pipeline_run_id IS NULL
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "All Silver transactions have non-null _pipeline_run_id (0 nulls)"
else
  fail "$COUNT Silver transaction record(s) with null _pipeline_run_id"
fi
echo ""

# AT2: AUDIT_TRAIL_SILVER_ACCOUNTS_RUN_ID
echo "AT2: AUDIT_TRAIL_SILVER_ACCOUNTS_RUN_ID"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/silver/accounts/**/*.parquet')
    WHERE _pipeline_run_id IS NULL
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "All Silver accounts have non-null _pipeline_run_id (0 nulls)"
else
  fail "$COUNT Silver account record(s) with null _pipeline_run_id"
fi
echo ""

# AT3: AUDIT_TRAIL_GOLD_RUN_ID
echo "AT3: AUDIT_TRAIL_GOLD_RUN_ID"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()

# Check daily summary
daily_nulls = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/gold/daily_summary/data.parquet')
    WHERE _pipeline_run_id IS NULL
""").fetchone()[0]

# Check weekly summary
weekly_nulls = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet')
    WHERE _pipeline_run_id IS NULL
""").fetchone()[0]

print(daily_nulls + weekly_nulls)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "All Gold records have non-null _pipeline_run_id (0 nulls)"
else
  fail "$COUNT Gold record(s) with null _pipeline_run_id"
fi
echo ""

# AT4: AUDIT_TRAIL_SILVER_RUN_ID_TRACEABLE
echo "AT4: AUDIT_TRAIL_SILVER_RUN_ID_TRACEABLE"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()

# Find all distinct run_ids in Silver
silver_run_ids = set(
    r[0] for r in conn.execute("""
        SELECT DISTINCT _pipeline_run_id FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
        WHERE _pipeline_run_id IS NOT NULL
        UNION ALL
        SELECT DISTINCT _pipeline_run_id FROM read_parquet('/app/data/silver/accounts/**/*.parquet')
        WHERE _pipeline_run_id IS NOT NULL
    """).fetchall()
)

# Find all SUCCESS run_ids in run_log
success_run_ids = set(
    r[0] for r in conn.execute("""
        SELECT DISTINCT run_id FROM read_parquet('/app/data/pipeline/run_log.parquet')
        WHERE status = 'SUCCESS'
    """).fetchall()
)

# Check if all Silver run_ids are in run_log SUCCESS
untraceable = silver_run_ids - success_run_ids
print(len(untraceable))
for rid in sorted(untraceable):
    print(f"  UNTRACEABLE: {rid}")
PYEOF
)
COUNT=$(echo "$RESULT" | head -1)
if [ "$COUNT" = "0" ]; then
  pass "All Silver _pipeline_run_id values trace to SUCCESS run_log entries"
else
  echo "$RESULT" | tail -n +2
  fail "$COUNT Silver _pipeline_run_id value(s) not found in run_log SUCCESS entries"
fi
echo ""

# AT5: AUDIT_TRAIL_GOLD_RUN_ID_TRACEABLE
echo "AT5: AUDIT_TRAIL_GOLD_RUN_ID_TRACEABLE"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()

# Find all distinct run_ids in Gold
gold_run_ids = set(
    r[0] for r in conn.execute("""
        SELECT DISTINCT _pipeline_run_id FROM read_parquet('/app/data/gold/daily_summary/data.parquet')
        WHERE _pipeline_run_id IS NOT NULL
        UNION ALL
        SELECT DISTINCT _pipeline_run_id FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet')
        WHERE _pipeline_run_id IS NOT NULL
    """).fetchall()
)

# Find all SUCCESS run_ids in run_log
success_run_ids = set(
    r[0] for r in conn.execute("""
        SELECT DISTINCT run_id FROM read_parquet('/app/data/pipeline/run_log.parquet')
        WHERE status = 'SUCCESS'
    """).fetchall()
)

# Check if all Gold run_ids are in run_log SUCCESS
untraceable = gold_run_ids - success_run_ids
print(len(untraceable))
for rid in sorted(untraceable):
    print(f"  UNTRACEABLE: {rid}")
PYEOF
)
COUNT=$(echo "$RESULT" | head -1)
if [ "$COUNT" = "0" ]; then
  pass "All Gold _pipeline_run_id values trace to SUCCESS run_log entries"
else
  echo "$RESULT" | tail -n +2
  fail "$COUNT Gold _pipeline_run_id value(s) not found in run_log SUCCESS entries"
fi
echo ""

# AT6: AUDIT_TRAIL_RUN_LOG_NO_DUPLICATES
echo "AT6: AUDIT_TRAIL_RUN_LOG_NO_DUPLICATES (INV-20A)"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM (
        SELECT run_id, model_name, target_date, COUNT(*) c
        FROM read_parquet('/app/data/pipeline/run_log.parquet')
        GROUP BY run_id, model_name, target_date
        HAVING c > 1
    )
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "No duplicate (run_id, model_name, target_date) entries in run_log (audit log integrity)"
else
  fail "$COUNT duplicate (run_id, model_name, target_date) tuple(s) found in run_log"
fi
echo ""

# SUMMARY
echo "==================================="
echo "SUMMARY: $PASSED passed, $FAILED failed"
echo "==================================="
if [ "$FAILED" -eq 0 ]; then
  echo "ALL AUDIT TRAIL CHECKS PASSED"
  exit 0
else
  echo "SOME AUDIT TRAIL CHECKS FAILED"
  exit 1
fi
