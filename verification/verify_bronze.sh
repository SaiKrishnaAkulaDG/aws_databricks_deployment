#!/bin/bash
set -uo pipefail

PASSED=0
FAILED=0

echo "=== Bronze Layer Completeness Verification ==="
echo ""

# Helper function to get bronze count
get_bronze_count() {
    local path="$1"
    docker compose run --rm pipeline python -c "
import duckdb
conn = duckdb.connect()
print(conn.execute(\"SELECT COUNT(*) FROM read_parquet('$path')\").fetchone()[0])
" 2>/dev/null || true
}

# 1. BRONZE_TRANSACTIONS_COMPLETENESS
echo "1. BRONZE_TRANSACTIONS_COMPLETENESS"
for date in 2024-01-01 2024-01-02 2024-01-03 2024-01-04 2024-01-05 2024-01-06 2024-01-07; do
    source_count=$(($(wc -l < "source/transactions_${date}.csv") - 1))
    bronze_count=$(get_bronze_count "/app/data/bronze/transactions/date=${date}/data.parquet")

    if [ "$source_count" -eq "$bronze_count" ]; then
        echo "   ✓ PASS: $date ($source_count rows)"
        ((PASSED++))
    else
        echo "   ✗ FAIL: $date (source: $source_count, bronze: $bronze_count)"
        ((FAILED++))
    fi
done
echo ""

# 2. BRONZE_ACCOUNTS_COMPLETENESS
echo "2. BRONZE_ACCOUNTS_COMPLETENESS"
for date in 2024-01-01 2024-01-02 2024-01-03 2024-01-04 2024-01-05 2024-01-06 2024-01-07; do
    source_count=$(($(wc -l < "source/accounts_${date}.csv") - 1))
    bronze_count=$(get_bronze_count "/app/data/bronze/accounts/date=${date}/data.parquet")

    if [ "$source_count" -eq "$bronze_count" ]; then
        echo "   ✓ PASS: $date ($source_count rows)"
        ((PASSED++))
    else
        echo "   ✗ FAIL: $date (source: $source_count, bronze: $bronze_count)"
        ((FAILED++))
    fi
done
echo ""

# 3. BRONZE_TRANSACTION_CODES_COMPLETENESS
echo "3. BRONZE_TRANSACTION_CODES_COMPLETENESS"
source_count=$(($(wc -l < "source/transaction_codes.csv") - 1))
bronze_count=$(get_bronze_count "/app/data/bronze/transaction_codes/data.parquet")

if [ "$source_count" -eq "$bronze_count" ]; then
    echo "   ✓ PASS: transaction_codes ($source_count rows)"
    ((PASSED++))
else
    echo "   ✗ FAIL: transaction_codes (source: $source_count, bronze: $bronze_count)"
    ((FAILED++))
fi
echo ""

# Helper function to get null count
get_null_count() {
    local path="$1"
    docker compose run --rm pipeline python -c "
import duckdb
conn = duckdb.connect()
print(conn.execute(\"SELECT COUNT(*) FROM read_parquet('$path') WHERE _pipeline_run_id IS NULL\").fetchone()[0])
" 2>/dev/null || true
}

# 4. BRONZE_AUDIT_COLUMNS_NOT_NULL
echo "4. BRONZE_AUDIT_COLUMNS_NOT_NULL"

tx_nulls=$(get_null_count "/app/data/bronze/transactions/**/*.parquet")
if [ "$tx_nulls" -eq 0 ]; then
    echo "   ✓ PASS: Transactions (0 null _pipeline_run_id)"
    ((PASSED++))
else
    echo "   ✗ FAIL: Transactions ($tx_nulls null _pipeline_run_id)"
    ((FAILED++))
fi

acc_nulls=$(get_null_count "/app/data/bronze/accounts/**/*.parquet")
if [ "$acc_nulls" -eq 0 ]; then
    echo "   ✓ PASS: Accounts (0 null _pipeline_run_id)"
    ((PASSED++))
else
    echo "   ✗ FAIL: Accounts ($acc_nulls null _pipeline_run_id)"
    ((FAILED++))
fi

tc_nulls=$(get_null_count "/app/data/bronze/transaction_codes/data.parquet")
if [ "$tc_nulls" -eq 0 ]; then
    echo "   ✓ PASS: Transaction Codes (0 null _pipeline_run_id)"
    ((PASSED++))
else
    echo "   ✗ FAIL: Transaction Codes ($tc_nulls null _pipeline_run_id)"
    ((FAILED++))
fi
echo ""

# 5. BRONZE_SOURCE_FILES_UNMODIFIED
echo "5. BRONZE_SOURCE_FILES_UNMODIFIED (MANUAL CHECK)"
echo "   Source file modification times:"
ls -lh source/*.csv | awk '{print "   " $6, $7, $8, $9}'
echo "   → Engineer must verify no mtime changes between runs"
echo ""

echo "=== Summary ==="
echo "Checks passed: $PASSED"
echo "Checks failed: $FAILED"

if [ "$FAILED" -eq 0 ]; then
    exit 0
else
    exit 1
fi
