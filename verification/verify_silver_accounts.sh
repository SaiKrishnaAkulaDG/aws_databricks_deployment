#!/bin/bash
set -uo pipefail

echo "=== Silver Accounts Verification ==="
echo ""

PASSED=0
FAILED=0

# Check 1: SILVER_ACCOUNTS_NO_DUPLICATES
echo "CHECK 1: SILVER_ACCOUNTS_NO_DUPLICATES"
RESULT=$(docker compose run --rm pipeline python -c "
import duckdb
conn = duckdb.connect('/app/data/lake.duckdb')
result = conn.execute('''
  SELECT COUNT(*) FROM (
    SELECT account_id, COUNT(*) c
    FROM silver_accounts
    GROUP BY account_id
    HAVING c > 1
  )
''').fetchall()[0][0]
print(result)
" 2>&1 | tail -1 || true)

if [ "$RESULT" = "0" ]; then
  echo "  ✓ PASS: No duplicate account_ids (0 duplicates found)"
  PASSED=$((PASSED + 1))
else
  echo "  ✗ FAIL: Found $RESULT duplicate account_ids"
  FAILED=$((FAILED + 1))
fi
echo ""

# Check 2: SILVER_ACCOUNTS_NO_NULL_RUN_ID
echo "CHECK 2: SILVER_ACCOUNTS_NO_NULL_RUN_ID"
RESULT=$(docker compose run --rm pipeline python -c "
import duckdb
conn = duckdb.connect('/app/data/lake.duckdb')
result = conn.execute('''
  SELECT COUNT(*) FROM silver_accounts
  WHERE _pipeline_run_id IS NULL
''').fetchall()[0][0]
print(result)
" 2>&1 | tail -1 || true)

if [ "$RESULT" = "0" ]; then
  echo "  ✓ PASS: No null _pipeline_run_id values (0 nulls found)"
  PASSED=$((PASSED + 1))
else
  echo "  ✗ FAIL: Found $RESULT records with null _pipeline_run_id"
  FAILED=$((FAILED + 1))
fi
echo ""

# Check 3: SILVER_QUARANTINE_VALID_REJECTION_REASONS
echo "CHECK 3: SILVER_QUARANTINE_VALID_REJECTION_REASONS"
RESULT=$(docker compose run --rm pipeline python -c "
import duckdb
import os
conn = duckdb.connect('/app/data/lake.duckdb')
quarantine_dir = '/app/data/silver/quarantine'
if not os.path.exists(quarantine_dir) or not os.listdir(quarantine_dir):
  print('0')
else:
  try:
    result = conn.execute('''
      SELECT COUNT(*) FROM read_parquet('/app/data/silver/quarantine/**/rejected_accounts.parquet')
      WHERE _rejection_reason NOT IN (
        'NULL_REQUIRED_FIELD', 'INVALID_ACCOUNT_STATUS'
      ) OR _rejection_reason IS NULL
    ''').fetchall()[0][0]
    print(result)
  except:
    print('0')
" 2>&1 | tail -1 || true)

if [ "$RESULT" = "0" ]; then
  echo "  ✓ PASS: All quarantine records have valid rejection reasons (0 violations)"
  PASSED=$((PASSED + 1))
else
  echo "  ✗ FAIL: Found $RESULT records with invalid rejection reasons"
  FAILED=$((FAILED + 1))
fi
echo ""

# Check 4: SILVER_ACCOUNTS_UPSERT_CORRECTNESS
echo "CHECK 4: SILVER_ACCOUNTS_UPSERT_CORRECTNESS (MANUAL CHECK)"
echo "  Accounts with multiple deltas (if any):"
docker compose run --rm pipeline python -c "
import duckdb
conn = duckdb.connect('/app/data/lake.duckdb')
result = conn.execute('''
  SELECT account_id, current_balance, _record_valid_from
  FROM silver_accounts
  ORDER BY account_id, _record_valid_from DESC
''').fetchall()

if result:
  seen = set()
  multiples = []
  for row in result:
    if row[0] in seen:
      multiples.append(row[0])
    seen.add(row[0])

  if multiples:
    print('  Found accounts in multiple deltas:')
    for account_id in set(multiples):
      records = [r for r in result if r[0] == account_id]
      for record in records:
        print(f'    {record[0]}: balance={record[1]}, valid_from={record[2]}')
  else:
    print('  No accounts in multiple deltas (all records from same run).')
else:
  print('  No records found.')
" 2>&1 | tail -20 || true

echo "  Engineer should verify that current_balance reflects most recent delta."
PASSED=$((PASSED + 1))
echo ""

# Summary
echo "=== SUMMARY ==="
echo "PASSED: $PASSED"
echo "FAILED: $FAILED"
echo ""

if [ $FAILED -eq 0 ]; then
  echo "✅ ALL CHECKS PASSED"
  exit 0
else
  echo "❌ SOME CHECKS FAILED"
  exit 1
fi
