#!/usr/bin/env python3
"""Verification script for silver_accounts deduplication after multiple runs."""

import duckdb
import sys

conn = duckdb.connect('/app/data/lake.duckdb')

print("=== Verification: silver_accounts Deduplication ===\n")

try:
    # TEST 1: No duplicate account_ids
    print("TEST 1: No duplicate account_ids (INV-07)")
    result = conn.execute("""
        SELECT account_id, COUNT(*) as cnt
        FROM silver_accounts
        GROUP BY account_id
        HAVING cnt > 1
    """).fetchall()
    assert len(result) == 0, f"Found duplicate account_ids: {result}"
    print("  ✓ All account_ids unique\n")

    # TEST 2: Record count
    print("TEST 2: Record count")
    count = conn.execute("SELECT COUNT(*) FROM silver_accounts").fetchall()[0][0]
    print(f"  Total records: {count}\n")

    # TEST 3: Verify COUNT(*) = COUNT(DISTINCT account_id)
    print("TEST 3: COUNT(*) = COUNT(DISTINCT account_id)")
    total = conn.execute("SELECT COUNT(*) FROM silver_accounts").fetchall()[0][0]
    distinct = conn.execute("SELECT COUNT(DISTINCT account_id) FROM silver_accounts").fetchall()[0][0]
    assert total == distinct, f"Count mismatch: {total} total vs {distinct} distinct"
    print(f"  ✓ Count consistency: {total} = {distinct}\n")

    # TEST 4: All audit columns non-null
    print("TEST 4: All audit columns are non-null")
    result = conn.execute("""
        SELECT COUNT(*) FROM silver_accounts
        WHERE _source_file IS NULL OR _bronze_ingested_at IS NULL OR
              _pipeline_run_id IS NULL OR _record_valid_from IS NULL
    """).fetchall()
    assert result[0][0] == 0, f"Found {result[0][0]} records with null audit columns"
    print("  ✓ All audit columns non-null\n")

    # TEST 5: No NULL required fields
    print("TEST 5: No NULL required fields")
    result = conn.execute("""
        SELECT COUNT(*) FROM silver_accounts
        WHERE account_id IS NULL OR account_id = '' OR
              open_date IS NULL OR credit_limit IS NULL OR
              current_balance IS NULL OR billing_cycle_start IS NULL OR
              billing_cycle_end IS NULL OR account_status IS NULL OR
              account_status = ''
    """).fetchall()
    assert result[0][0] == 0, f"Found {result[0][0]} records with null required fields"
    print("  ✓ No NULL in required fields\n")

    # TEST 6: Valid account_status values
    print("TEST 6: Valid account_status values")
    result = conn.execute("""
        SELECT COUNT(*) FROM silver_accounts
        WHERE account_status NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
    """).fetchall()
    assert result[0][0] == 0, f"Found {result[0][0]} records with invalid status"
    print("  ✓ All status values valid\n")

    print("=== ALL TESTS PASSED ===\n")
    sys.exit(0)

except AssertionError as e:
    print(f"  ✗ FAILED: {e}\n")
    sys.exit(1)
except Exception as e:
    print(f"  ✗ ERROR: {e}\n")
    sys.exit(1)
