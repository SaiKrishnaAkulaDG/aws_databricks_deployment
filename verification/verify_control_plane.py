#!/usr/bin/env python3
"""
verification/verify_control_plane.py
Task 6.3: Watermark and gold_weekly_control Manager verification tests
"""

import sys
import os
import shutil
import tempfile
from datetime import date, timedelta
sys.path.insert(0, '/app')

from pipeline.control_plane import (
    get_watermark,
    advance_watermark,
    get_computed_weeks,
    record_computed_weeks,
    get_uncomputed_weeks
)
import duckdb
import pandas as pd

test_dir = tempfile.mkdtemp()
control_path = os.path.join(test_dir, "control.parquet")
weekly_control_path = os.path.join(test_dir, "gold_weekly_control.parquet")
silver_path = os.path.join(test_dir, "silver")

try:
    print("=== Control Plane Verification ===\n")

    # TEST 1: get_watermark returns None on missing file
    print("TEST 1: get_watermark() returns None on missing file")
    result = get_watermark(control_path)
    if result is None:
        print("  ✓ Missing control.parquet returns None\n")
    else:
        print(f"  ✗ Expected None, got {result}\n")
        sys.exit(1)

    # TEST 2: advance_watermark writes single row
    print("TEST 2: advance_watermark() writes single row atomically")
    test_date = date(2026, 4, 22)
    advance_watermark(control_path, test_date, "run-001")

    conn = duckdb.connect()
    rows = conn.execute(f"SELECT * FROM read_parquet('{control_path}')").fetchall()
    conn.close()

    if len(rows) == 1 and rows[0][0] == test_date and rows[0][2] == "run-001":
        print("  ✓ Single row written with correct fields\n")
    else:
        print(f"  ✗ Expected 1 row, got {len(rows)}\n")
        sys.exit(1)

    # TEST 3: get_watermark returns correct date after advance
    print("TEST 3: get_watermark() returns correct date after advance")
    result = get_watermark(control_path)
    if result == test_date:
        print(f"  ✓ Retrieved watermark: {result}\n")
    else:
        print(f"  ✗ Expected {test_date}, got {result}\n")
        sys.exit(1)

    # TEST 4: advance_watermark overwrites (not appends)
    print("TEST 4: advance_watermark() overwrites on second call (not appends)")
    new_date = date(2026, 4, 23)
    advance_watermark(control_path, new_date, "run-002")

    conn = duckdb.connect()
    count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{control_path}')").fetchone()[0]
    result = conn.execute(f"SELECT last_processed_date FROM read_parquet('{control_path}')").fetchone()[0]
    conn.close()

    if count == 1 and result == new_date:
        print("  ✓ Overwrite successful: single row with new date\n")
    else:
        print(f"  ✗ Expected 1 row with {new_date}, got {count} rows with {result}\n")
        sys.exit(1)

    # TEST 5: get_computed_weeks returns empty set on missing file
    print("TEST 5: get_computed_weeks() returns empty set on missing file")
    result = get_computed_weeks(weekly_control_path)
    if isinstance(result, set) and len(result) == 0:
        print("  ✓ Missing gold_weekly_control returns empty set\n")
    else:
        print(f"  ✗ Expected empty set, got {result}\n")
        sys.exit(1)

    # TEST 6: record_computed_weeks appends to empty file
    print("TEST 6: record_computed_weeks() appends to empty file")
    week1 = date(2026, 4, 20)  # Monday
    weeks = [
        {'week_start_date': week1, 'week_end_date': week1 + timedelta(days=6)},
        {'week_start_date': week1 + timedelta(days=7), 'week_end_date': week1 + timedelta(days=13)}
    ]
    record_computed_weeks(weekly_control_path, weeks, "run-001")

    conn = duckdb.connect()
    count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{weekly_control_path}')").fetchone()[0]
    conn.close()

    if count == 2:
        print("  ✓ 2 weeks recorded in gold_weekly_control\n")
    else:
        print(f"  ✗ Expected 2 rows, got {count}\n")
        sys.exit(1)

    # TEST 7: record_computed_weeks appends to existing file (not overwrites)
    print("TEST 7: record_computed_weeks() appends to existing file")
    week3 = week1 + timedelta(days=14)
    new_weeks = [{'week_start_date': week3, 'week_end_date': week3 + timedelta(days=6)}]
    record_computed_weeks(weekly_control_path, new_weeks, "run-002")

    conn = duckdb.connect()
    count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{weekly_control_path}')").fetchone()[0]
    conn.close()

    if count == 3:
        print("  ✓ Third week appended: total 3 rows\n")
    else:
        print(f"  ✗ Expected 3 rows, got {count}\n")
        sys.exit(1)

    # TEST 8: get_computed_weeks returns correct set after records
    print("TEST 8: get_computed_weeks() returns correct set of dates")
    result = get_computed_weeks(weekly_control_path)
    if len(result) == 3 and week1 in result:
        print(f"  ✓ Retrieved {len(result)} computed weeks\n")
    else:
        print(f"  ✗ Expected 3 weeks including {week1}, got {result}\n")
        sys.exit(1)

    # TEST 9: get_uncomputed_weeks returns empty list when no Silver transactions
    print("TEST 9: get_uncomputed_weeks() returns empty list with no transactions")
    os.makedirs(silver_path, exist_ok=True)
    empty_silver_data = {
        'transaction_date': [],
        'transaction_id': [],
        '_is_resolvable': [],
        '_pipeline_run_id': []
    }
    df_empty = pd.DataFrame(empty_silver_data)
    conn = duckdb.connect()
    conn.execute(f"COPY df_empty TO '{silver_path}/empty.parquet' (FORMAT PARQUET)")
    conn.close()

    result = get_uncomputed_weeks(silver_path, weekly_control_path)
    if isinstance(result, list) and len(result) == 0:
        print("  ✓ No transactions → empty uncomputed weeks\n")
    else:
        print(f"  ✗ Expected empty list, got {result}\n")
        sys.exit(1)

    # TEST 10: get_uncomputed_weeks excludes already-computed weeks
    print("TEST 10: get_uncomputed_weeks() excludes already-computed weeks")
    # Create Silver with transaction from a second week (2024-01-08)
    # Weekly control already has week1 (2024-01-22 to 2024-01-28) computed
    silver_path_10 = os.path.join(test_dir, "silver10")
    os.makedirs(silver_path_10, exist_ok=True)

    conn = duckdb.connect()
    conn.execute(f"""
        COPY (SELECT '2024-01-08'::DATE AS transaction_date,
                     'tx1' AS transaction_id,
                     true AS _is_resolvable,
                     'run-001' AS _pipeline_run_id)
        TO '{silver_path_10}/transactions.parquet' (FORMAT PARQUET)
    """)
    conn.close()

    # Weekly control already has 3 weeks computed (from test 8)
    result = get_uncomputed_weeks(silver_path_10, weekly_control_path)

    # Result should have exactly 1 week for 2024-01-08
    # 2024-01-08 is a Monday, so week_start_date should be 2024-01-08
    if len(result) == 1 and result[0]['week_start_date'] == date(2024, 1, 8):
        print(f"  ✓ 1 uncomputed week returned with correct Monday date\n")
    else:
        print(f"  ✗ Expected 1 uncomputed week starting 2024-01-08, got {len(result)} weeks: {result}\n")
        sys.exit(1)

    # TEST 11: get_uncomputed_weeks respects _is_resolvable filter
    print("TEST 11: get_uncomputed_weeks() filters on _is_resolvable=true")
    # Create Silver with two transactions: one resolvable (2024-01-08), one unresolvable (2024-01-15)
    # Create empty weekly_control
    silver_path_11 = os.path.join(test_dir, "silver11")
    os.makedirs(silver_path_11, exist_ok=True)
    weekly_path_11 = os.path.join(test_dir, "weekly11.parquet")

    conn = duckdb.connect()
    # Create Silver with mixed resolvability
    conn.execute(f"""
        COPY (SELECT '2024-01-08'::DATE AS transaction_date,
                     'tx1' AS transaction_id,
                     true AS _is_resolvable,
                     'run-001' AS _pipeline_run_id
              UNION ALL
              SELECT '2024-01-15'::DATE AS transaction_date,
                     'tx2' AS transaction_id,
                     false AS _is_resolvable,
                     'run-001' AS _pipeline_run_id)
        TO '{silver_path_11}/transactions.parquet' (FORMAT PARQUET)
    """)
    conn.close()

    result = get_uncomputed_weeks(silver_path_11, weekly_path_11)
    # Result should have exactly 1 week (2024-01-08, resolvable)
    # The unresolvable transaction on 2024-01-15 should be filtered out
    if len(result) == 1 and result[0]['week_start_date'] == date(2024, 1, 8):
        print(f"  ✓ Only resolvable transaction dates considered\n")
    else:
        print(f"  ✗ Expected 1 uncomputed week (only resolvable), got {len(result)}\n")
        sys.exit(1)

    # TEST 12: record_computed_weeks with empty list is a no-op
    print("TEST 12: record_computed_weeks() with empty list is no-op")
    empty_weekly_path = os.path.join(test_dir, "weekly_empty_test.parquet")

    record_computed_weeks(empty_weekly_path, [], "run-test")

    if not os.path.exists(empty_weekly_path):
        print("  ✓ Empty weeks list: no file created\n")
    else:
        print(f"  ✗ Empty weeks list should not create file\n")
        sys.exit(1)

    # TEST 13: get_uncomputed_weeks with missing silver_path returns empty list
    print("TEST 13: get_uncomputed_weeks() with missing silver_path returns []")
    missing_silver_path = os.path.join(test_dir, "nonexistent_silver")
    empty_weekly_path_13 = os.path.join(test_dir, "weekly_empty_13.parquet")

    result = get_uncomputed_weeks(missing_silver_path, empty_weekly_path_13)

    if isinstance(result, list) and len(result) == 0:
        print("  ✓ Missing silver_path returns empty list\n")
    else:
        print(f"  ✗ Expected empty list, got {result}\n")
        sys.exit(1)

    print("=== All control plane tests passed ===")

except Exception as e:
    print(f"✗ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
