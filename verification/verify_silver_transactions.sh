#!/usr/bin/env bash
# verification/verify_silver_transactions.sh
# Section 10.2 — Silver transactions quality checks
# Run from repo root: bash verification/verify_silver_transactions.sh

set -uo pipefail
cd "$(dirname "$0")/.."

PASSED=0
FAILED=0

pass() { echo "  PASS: $1"; PASSED=$((PASSED + 1)); }
fail() { echo "  FAIL: $1"; FAILED=$((FAILED + 1)); }

echo "=== Silver Transactions Verification — Section 10.2 ==="
echo ""

# CHECK 1: SILVER_BRONZE_ACCOUNTING
echo "CHECK 1: SILVER_BRONZE_ACCOUNTING"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
discrepancies = 0
for d in ['2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-05','2024-01-06','2024-01-07']:
    bronze = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/date={d}/data.parquet')"
    ).fetchone()[0]
    silver = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/date={d}/data.parquet')"
    ).fetchone()[0]
    try:
        quarantine = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('/app/data/silver/quarantine/date={d}/rejected.parquet')"
        ).fetchone()[0]
    except Exception:
        quarantine = 0
    if bronze != silver + quarantine:
        discrepancies += 1
        print(f"  DISCREPANCY {d}: bronze={bronze} silver={silver} quarantine={quarantine}")
print(discrepancies)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "All 7 dates: bronze_count = silver_count + quarantine_count (0 discrepancies)"
else
  echo "$RESULT" | head -n -1
  fail "$COUNT date(s) with accounting discrepancy"
fi
echo ""

# CHECK 2: SILVER_NO_DUPLICATE_TRANSACTION_ID
echo "CHECK 2: SILVER_NO_DUPLICATE_TRANSACTION_ID"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM (
        SELECT transaction_id, COUNT(*) c
        FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
        GROUP BY transaction_id
        HAVING c > 1
    )
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "No duplicate transaction_id across all Silver partitions"
else
  fail "$COUNT duplicate transaction_id(s) found"
fi
echo ""

# CHECK 3: SILVER_VALID_TRANSACTION_CODES
echo "CHECK 3: SILVER_VALID_TRANSACTION_CODES"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet') t
    LEFT JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
        ON t.transaction_code = tc.transaction_code
    WHERE tc.transaction_code IS NULL
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "All Silver transactions have a matching transaction_code in Silver TC"
else
  fail "$COUNT transaction(s) with unmatched transaction_code"
fi
echo ""

# CHECK 4: SILVER_NO_NULL_SIGNED_AMOUNT
echo "CHECK 4: SILVER_NO_NULL_SIGNED_AMOUNT"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
    WHERE _signed_amount IS NULL
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "No null _signed_amount in Silver"
else
  fail "$COUNT record(s) with null _signed_amount"
fi
echo ""

# CHECK 5: SILVER_QUARANTINE_VALID_REJECTION_REASONS
echo "CHECK 5: SILVER_QUARANTINE_VALID_REJECTION_REASONS"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/silver/quarantine/**/*.parquet')
    WHERE _rejection_reason NOT IN (
        'NULL_REQUIRED_FIELD',
        'INVALID_AMOUNT',
        'DUPLICATE_TRANSACTION_ID',
        'INVALID_TRANSACTION_CODE',
        'INVALID_CHANNEL',
        'INVALID_ACCOUNT_STATUS'
    ) OR _rejection_reason IS NULL
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "All quarantine records have valid rejection reasons"
else
  fail "$COUNT record(s) with invalid or null rejection reason"
fi
echo ""

# CHECK 6: SILVER_SIGN_DR_POSITIVE
echo "CHECK 6: SILVER_SIGN_DR_POSITIVE"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet') t
    JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
        ON t.transaction_code = tc.transaction_code
    WHERE tc.debit_credit_indicator = 'DR' AND t._signed_amount <= 0
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "All DR-coded transactions have _signed_amount > 0"
else
  fail "$COUNT DR transaction(s) with non-positive _signed_amount"
fi
echo ""

# CHECK 7: SILVER_SIGN_CR_NEGATIVE
echo "CHECK 7: SILVER_SIGN_CR_NEGATIVE"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet') t
    JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
        ON t.transaction_code = tc.transaction_code
    WHERE tc.debit_credit_indicator = 'CR' AND t._signed_amount >= 0
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "All CR-coded transactions have _signed_amount < 0"
else
  fail "$COUNT CR transaction(s) with non-negative _signed_amount"
fi
echo ""

# CHECK 8: SILVER_UNRESOLVABLE_NOT_IN_QUARANTINE
echo "CHECK 8: SILVER_UNRESOLVABLE_NOT_IN_QUARANTINE"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/silver/quarantine/**/*.parquet')
    WHERE _rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID'
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "No quarantine records with UNRESOLVABLE_ACCOUNT_ID rejection reason"
else
  fail "$COUNT quarantine record(s) incorrectly carry UNRESOLVABLE_ACCOUNT_ID"
fi
echo ""

# CHECK 9: SILVER_NO_NULL_RUN_ID
echo "CHECK 9: SILVER_NO_NULL_RUN_ID"
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
  pass "No null _pipeline_run_id in Silver transactions (INV-22)"
else
  fail "$COUNT record(s) with null _pipeline_run_id"
fi
echo ""

# CHECK 10: SILVER_TC_SOURCE_PATH_CORRECT (INV-37)
echo "CHECK 10: SILVER_TC_SOURCE_PATH_CORRECT"
COUNT=$( (grep -E "bronze/transaction_codes|source/" dbt/models/silver/silver_transactions.sql 2>/dev/null || true) | wc -l | tr -d ' ')
if [ "$COUNT" = "0" ]; then
  pass "silver_transactions.sql contains no bronze/transaction_codes or source/ references (INV-37)"
else
  fail "$COUNT line(s) in silver_transactions.sql reference bronze/transaction_codes or source/"
fi
echo ""

# CHECK 11: SILVER_UNRESOLVABLE_IN_SILVER
echo "CHECK 11: SILVER_UNRESOLVABLE_IN_SILVER"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
    WHERE _is_resolvable = false
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" != "0" ]; then
  pass "Silver contains $COUNT record(s) with _is_resolvable = false (INV-10 positive path exercised)"
else
  fail "No records with _is_resolvable = false — unresolvable account path not exercised"
fi
echo ""

# CHECK 12: SILVER_QUARANTINE_NO_NULL_RUN_ID
echo "CHECK 12: SILVER_QUARANTINE_NO_NULL_RUN_ID"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF'
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/silver/quarantine/**/*.parquet')
    WHERE _pipeline_run_id IS NULL
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "No null _pipeline_run_id in quarantine records (INV-22)"
else
  fail "$COUNT quarantine record(s) with null _pipeline_run_id"
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
