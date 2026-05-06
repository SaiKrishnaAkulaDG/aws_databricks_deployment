#!/usr/bin/env python3
"""
verification/check_weekly_aggregates.py
INV-11 verification: gold_weekly_account_summary financial aggregates vs Silver.
Checks total_purchases, total_payments, total_fees, total_interest
for week starting 2024-01-01.
Run via: python3 /app/verification/check_weekly_aggregates.py
"""
import sys
import duckdb

conn = duckdb.connect()

WEEK_START = '2024-01-01'
WEEK_END   = '2024-01-07'

PASSED = 0
FAILED = 0


def check(name, mismatches):
    global PASSED, FAILED
    if mismatches == 0:
        print(f"  PASS: {name}")
        PASSED += 1
    else:
        print(f"  FAIL: {name} — {mismatches} account(s) with mismatch")
        FAILED += 1


print("=== Weekly Account Aggregates — INV-11 Verification ===")
print(f"Week: {WEEK_START} to {WEEK_END}")
print()

# CHECK 1: total_purchases = Silver COUNT(PURCHASE) per account
print("CHECK 1: TOTAL_PURCHASES_MATCHES_SILVER")
result = conn.execute("""
    SELECT COUNT(*)
    FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet') g
    LEFT JOIN (
        SELECT st.account_id, COUNT(*) AS silver_count
        FROM read_parquet('/app/data/silver/transactions/**/*.parquet') st
        JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
            ON st.transaction_code = tc.transaction_code
        WHERE st._is_resolvable = true
          AND st.transaction_date >= DATE '2024-01-01'
          AND st.transaction_date <= DATE '2024-01-07'
          AND tc.transaction_type = 'PURCHASE'
        GROUP BY st.account_id
    ) sv ON g.account_id = sv.account_id
    WHERE g.week_start_date = DATE '2024-01-01'
      AND g.total_purchases != COALESCE(sv.silver_count, 0)
""").fetchone()[0]
check("total_purchases = Silver COUNT(PURCHASE) per account", result)
print()

# CHECK 2: total_payments = Silver SUM(_signed_amount for PAYMENT) per account
print("CHECK 2: TOTAL_PAYMENTS_MATCHES_SILVER")
result = conn.execute("""
    SELECT COUNT(*)
    FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet') g
    LEFT JOIN (
        SELECT st.account_id, COALESCE(SUM(st._signed_amount), 0.0) AS silver_sum
        FROM read_parquet('/app/data/silver/transactions/**/*.parquet') st
        JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
            ON st.transaction_code = tc.transaction_code
        WHERE st._is_resolvable = true
          AND st.transaction_date >= DATE '2024-01-01'
          AND st.transaction_date <= DATE '2024-01-07'
          AND tc.transaction_type = 'PAYMENT'
        GROUP BY st.account_id
    ) sv ON g.account_id = sv.account_id
    WHERE g.week_start_date = DATE '2024-01-01'
      AND g.total_payments != COALESCE(sv.silver_sum, 0.0)
""").fetchone()[0]
check("total_payments = Silver SUM(_signed_amount) for PAYMENT per account", result)
print()

# CHECK 3: total_fees = Silver SUM(_signed_amount for FEE) per account
print("CHECK 3: TOTAL_FEES_MATCHES_SILVER")
result = conn.execute("""
    SELECT COUNT(*)
    FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet') g
    LEFT JOIN (
        SELECT st.account_id, COALESCE(SUM(st._signed_amount), 0.0) AS silver_sum
        FROM read_parquet('/app/data/silver/transactions/**/*.parquet') st
        JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
            ON st.transaction_code = tc.transaction_code
        WHERE st._is_resolvable = true
          AND st.transaction_date >= DATE '2024-01-01'
          AND st.transaction_date <= DATE '2024-01-07'
          AND tc.transaction_type = 'FEE'
        GROUP BY st.account_id
    ) sv ON g.account_id = sv.account_id
    WHERE g.week_start_date = DATE '2024-01-01'
      AND g.total_fees != COALESCE(sv.silver_sum, 0.0)
""").fetchone()[0]
check("total_fees = Silver SUM(_signed_amount) for FEE per account", result)
print()

# CHECK 4: total_interest = Silver SUM(_signed_amount for INTEREST) per account
print("CHECK 4: TOTAL_INTEREST_MATCHES_SILVER")
result = conn.execute("""
    SELECT COUNT(*)
    FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet') g
    LEFT JOIN (
        SELECT st.account_id, COALESCE(SUM(st._signed_amount), 0.0) AS silver_sum
        FROM read_parquet('/app/data/silver/transactions/**/*.parquet') st
        JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
            ON st.transaction_code = tc.transaction_code
        WHERE st._is_resolvable = true
          AND st.transaction_date >= DATE '2024-01-01'
          AND st.transaction_date <= DATE '2024-01-07'
          AND tc.transaction_type = 'INTEREST'
        GROUP BY st.account_id
    ) sv ON g.account_id = sv.account_id
    WHERE g.week_start_date = DATE '2024-01-01'
      AND g.total_interest != COALESCE(sv.silver_sum, 0.0)
""").fetchone()[0]
check("total_interest = Silver SUM(_signed_amount) for INTEREST per account", result)
print()

print("===================================")
print(f"SUMMARY: {PASSED} passed, {FAILED} failed")
print("===================================")
if FAILED > 0:
    sys.exit(1)
