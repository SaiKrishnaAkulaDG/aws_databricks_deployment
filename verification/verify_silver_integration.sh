#!/usr/bin/env bash
# verification/verify_silver_integration.sh
# INV-05 global accounting: total_bronze = total_silver + total_quarantine
# Run from repo root: bash verification/verify_silver_integration.sh

set -uo pipefail
cd "$(dirname "$0")/.."

PASSED=0
FAILED=0

pass() { echo "  PASS: $1"; PASSED=$((PASSED + 1)); }
fail() { echo "  FAIL: $1"; FAILED=$((FAILED + 1)); }

echo "=== Silver Layer Integration Test — INV-05 ==="
echo ""

# CHECK 1: SILVER_TOTAL_ACCOUNTING
echo "CHECK 1: SILVER_TOTAL_ACCOUNTING"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
total_bronze = conn.execute(
    "SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/**/*.parquet')"
).fetchone()[0]
total_silver = conn.execute(
    "SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet')"
).fetchone()[0]
try:
    total_quarantine = conn.execute(
        "SELECT COUNT(*) FROM read_parquet('/app/data/silver/quarantine/**/*.parquet')"
    ).fetchone()[0]
except Exception:
    total_quarantine = 0
if total_bronze == total_silver + total_quarantine:
    print(f"PASS {total_bronze} {total_silver} {total_quarantine}")
else:
    print(f"FAIL {total_bronze} {total_silver} {total_quarantine}")
PYEOF
)
LINE=$(echo "$RESULT" | tail -1)
OUTCOME=$(echo "$LINE" | awk '{print $1}')
BRONZE=$(echo "$LINE" | awk '{print $2}')
SILVER=$(echo "$LINE" | awk '{print $3}')
QUARANTINE=$(echo "$LINE" | awk '{print $4}')
if [ "$OUTCOME" = "PASS" ]; then
  pass "total_bronze($BRONZE) = total_silver($SILVER) + total_quarantine($QUARANTINE)"
else
  fail "ACCOUNTING MISMATCH: total_bronze=$BRONZE total_silver=$SILVER total_quarantine=$QUARANTINE"
fi
echo ""

# SUMMARY
echo "==================================="
echo "SUMMARY: $PASSED passed, $FAILED failed"
echo "==================================="
if [ "$FAILED" -eq 0 ]; then
  echo "ALL CHECKS PASSED"
  exit 0
else
  echo "SOME CHECKS FAILED"
  exit 1
fi
