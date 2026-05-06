#!/bin/bash
set -uo pipefail
cd "$(dirname "$0")/.."

PASSED=0
FAILED=0

pass() { echo "  PASS: $1"; PASSED=$((PASSED + 1)); }
fail() { echo "  FAIL: $1"; FAILED=$((FAILED + 1)); }

echo "=== Idempotency Verification — Section 10.4 ==="
echo ""

# TEST 1: IDEMPOTENCY_FULL_PIPELINE_RERUN
echo "TEST 1: IDEMPOTENCY_FULL_PIPELINE_RERUN"
echo "  Capturing initial counts..."

INITIAL_COUNTS=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()

# Count records before rerun
bronze_tx_before = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/**/*.parquet')").fetchone()[0]
bronze_acc_before = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/bronze/accounts/**/*.parquet')").fetchone()[0]
silver_tx_before = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet')").fetchone()[0]
silver_acc_before = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/silver/accounts/**/*.parquet')").fetchone()[0]
gold_daily_before = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/gold/daily_summary/data.parquet')").fetchone()[0]
gold_weekly_before = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet')").fetchone()[0]

print(f"{bronze_tx_before} {bronze_acc_before} {silver_tx_before} {silver_acc_before} {gold_daily_before} {gold_weekly_before}")
PYEOF
)

echo "  Running full pipeline rerun (same 7 dates)..."
python3 pipeline/pipeline.py --start-date 2024-01-01 --end-date 2024-01-07 2>&1 | tail -5

echo "  Capturing post-rerun counts..."

POST_COUNTS=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()

# Count records after rerun
bronze_tx_after = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/**/*.parquet')").fetchone()[0]
bronze_acc_after = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/bronze/accounts/**/*.parquet')").fetchone()[0]
silver_tx_after = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet')").fetchone()[0]
silver_acc_after = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/silver/accounts/**/*.parquet')").fetchone()[0]
gold_daily_after = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/gold/daily_summary/data.parquet')").fetchone()[0]
gold_weekly_after = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet')").fetchone()[0]

print(f"{bronze_tx_after} {bronze_acc_after} {silver_tx_after} {silver_acc_after} {gold_daily_after} {gold_weekly_after}")
PYEOF
)

# Compare counts
INIT_ARRAY=($INITIAL_COUNTS)
POST_ARRAY=($POST_COUNTS)

MATCH=true
for i in {0..5}; do
  if [ "${INIT_ARRAY[$i]}" != "${POST_ARRAY[$i]}" ]; then
    MATCH=false
    break
  fi
done

if [ "$MATCH" = true ]; then
  pass "Full pipeline rerun produces identical row counts: bronze_tx=${INIT_ARRAY[0]}, bronze_acc=${INIT_ARRAY[1]}, silver_tx=${INIT_ARRAY[2]}, silver_acc=${INIT_ARRAY[3]}, gold_daily=${INIT_ARRAY[4]}, gold_weekly=${INIT_ARRAY[5]}"
else
  fail "Row counts differ after rerun: before=$INITIAL_COUNTS after=$POST_COUNTS"
fi
echo ""

# TEST 2: IDEMPOTENCY_INCREMENTAL_NOOP
echo "TEST 2: IDEMPOTENCY_INCREMENTAL_NOOP (Single date re-run)"

echo "  Capturing single-date counts before incremental rerun..."

SINGLE_BEFORE=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
import os
conn = duckdb.connect()

try:
    silver_tx_2401_before = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/date=2024-01-01/data.parquet')").fetchone()[0]
except:
    silver_tx_2401_before = 0

try:
    silver_acc_2401_before = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/silver/accounts/date=2024-01-01/data.parquet')").fetchone()[0]
except:
    silver_acc_2401_before = 0

print(f"{silver_tx_2401_before} {silver_acc_2401_before}")
PYEOF
)

echo "  Running pipeline for single date (2024-01-01 only)..."
python3 pipeline/pipeline.py --start-date 2024-01-01 --end-date 2024-01-01 2>&1 | tail -3

echo "  Capturing single-date counts after incremental rerun..."

SINGLE_AFTER=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
import os
conn = duckdb.connect()

try:
    silver_tx_2401_after = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/date=2024-01-01/data.parquet')").fetchone()[0]
except:
    silver_tx_2401_after = 0

try:
    silver_acc_2401_after = conn.execute("SELECT COUNT(*) FROM read_parquet('/app/data/silver/accounts/date=2024-01-01/data.parquet')").fetchone()[0]
except:
    silver_acc_2401_after = 0

print(f"{silver_tx_2401_after} {silver_acc_2401_after}")
PYEOF
)

BEFORE_ARRAY=($SINGLE_BEFORE)
AFTER_ARRAY=($SINGLE_AFTER)

if [ -n "$BEFORE_ARRAY" ] && [ -n "$AFTER_ARRAY" ]; then
  if [ "${BEFORE_ARRAY[0]}" = "${AFTER_ARRAY[0]}" ] && [ "${BEFORE_ARRAY[1]}" = "${AFTER_ARRAY[1]}" ]; then
    pass "Incremental single-date rerun produces no change: silver_tx=${BEFORE_ARRAY[0]}, silver_acc=${BEFORE_ARRAY[1]}"
  else
    fail "Row counts changed on single-date rerun: before=$SINGLE_BEFORE after=$SINGLE_AFTER"
  fi
else
  fail "Unable to capture counts for 2024-01-01: before='$SINGLE_BEFORE' after='$SINGLE_AFTER'"
fi
echo ""

# TEST 3: IDEMPOTENCY_BRONZE_PARTITION_IMMUTABILITY
echo "TEST 3: IDEMPOTENCY_BRONZE_PARTITION_IMMUTABILITY"
echo "  Capturing Bronze partition mtimes before rerun..."

BRONZE_MTIMES_BEFORE=$(ls -l data/bronze/transactions/date=*/data.parquet 2>/dev/null | awk '{print $6, $7, $8}' | sort)
BRONZE_SIZE_BEFORE=$(find data/bronze/transactions/ -name "data.parquet" -exec stat -c "%s" {} \; 2>/dev/null | sort -n)

echo "  Running pipeline rerun..."
python3 pipeline/pipeline.py --start-date 2024-01-01 --end-date 2024-01-07 2>&1 | tail -3

echo "  Capturing Bronze partition mtimes after rerun..."

BRONZE_MTIMES_AFTER=$(ls -l data/bronze/transactions/date=*/data.parquet 2>/dev/null | awk '{print $6, $7, $8}' | sort)
BRONZE_SIZE_AFTER=$(find data/bronze/transactions/ -name "data.parquet" -exec stat -c "%s" {} \; 2>/dev/null | sort -n)

if [ "$BRONZE_MTIMES_BEFORE" = "$BRONZE_MTIMES_AFTER" ] && [ "$BRONZE_SIZE_BEFORE" = "$BRONZE_SIZE_AFTER" ]; then
  pass "Bronze partitions immutable: mtimes and sizes unchanged after rerun"
else
  fail "Bronze partitions modified after rerun (INV-03 violation)"
fi
echo ""

# SUMMARY
echo "==================================="
echo "SUMMARY: $PASSED passed, $FAILED failed"
echo "==================================="
if [ "$FAILED" -eq 0 ]; then
  echo "ALL IDEMPOTENCY CHECKS PASSED"
  exit 0
else
  echo "SOME IDEMPOTENCY CHECKS FAILED"
  exit 1
fi
