#!/usr/bin/env python3
"""
verification/verify_task62_runlog.py
Task 6.2 verification: RunLogBuffer accumulation, deduplication, flush, recovery.
"""

import sys
import os
import shutil
import tempfile
sys.path.insert(0, '/app')

from pipeline.run_log import RunLogBuffer
import duckdb
import pandas as pd

test_dir = tempfile.mkdtemp()
run_log_path = os.path.join(test_dir, "run_log.parquet")
control_path = os.path.join(test_dir, "control.parquet")
fallback_path = "/app/data/pipeline/pipeline_runlog_fallback.jsonl"

try:
    print("=== RunLogBuffer Verification ===\n")

    # Test 1: Buffer accumulation
    print("TEST 1: Buffer accumulation")
    buffer = RunLogBuffer("test-run-001", "HISTORICAL")
    buffer.add_entry("silver_accounts", "SILVER", "2026-04-22T10:00:00Z", "2026-04-22T10:00:05Z",
                     "SUCCESS", records_processed=100, records_written=100)
    buffer.add_entry("silver_transactions", "SILVER", "2026-04-22T10:00:05Z", "2026-04-22T10:00:10Z",
                     "SUCCESS", records_processed=500, records_written=500)

    if len(buffer._buffer) == 2:
        print("  ✓ Buffer accumulated 2 entries\n")
    else:
        print(f"  ✗ Expected 2 entries, got {len(buffer._buffer)}\n")
        sys.exit(1)

    # Test 2: Deduplication
    print("TEST 2: Deduplication on (run_id, model_name)")
    buffer.add_entry("silver_accounts", "SILVER", "2026-04-22T10:00:00Z", "2026-04-22T10:00:06Z",
                     "SUCCESS", records_processed=101, records_written=101)

    if len(buffer._buffer) == 2:
        print("  ✓ Deduplication: replacement occurred, buffer still has 2 entries")
        if buffer._buffer[0]["records_written"] == 101:
            print("  ✓ Entry was replaced (records_written updated to 101)\n")
        else:
            print("  ✗ Entry was not replaced\n")
            sys.exit(1)
    else:
        print(f"  ✗ Expected 2 entries after dedup, got {len(buffer._buffer)}\n")
        sys.exit(1)

    # Test 3: Flush creates new parquet
    print("TEST 3: flush() creates new parquet file")
    buffer.flush(run_log_path)

    if os.path.exists(run_log_path):
        conn = duckdb.connect()
        count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{run_log_path}')").fetchone()[0]
        conn.close()
        if count == 2:
            print(f"  ✓ Parquet file created with {count} rows\n")
        else:
            print(f"  ✗ Expected 2 rows, got {count}\n")
            sys.exit(1)
    else:
        print("  ✗ Parquet file not created\n")
        sys.exit(1)

    # Test 4: Flush appends without overwriting
    print("TEST 4: flush() appends without overwriting existing rows")
    buffer2 = RunLogBuffer("test-run-002", "HISTORICAL")
    buffer2.add_entry("gold_daily_summary", "GOLD", "2026-04-22T10:00:10Z", "2026-04-22T10:00:15Z",
                      "SUCCESS", records_processed=7, records_written=7)
    buffer2.flush(run_log_path)

    conn = duckdb.connect()
    count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{run_log_path}')").fetchone()[0]
    conn.close()

    if count == 3:
        print(f"  ✓ After second flush, parquet has {count} rows (append successful)\n")
    else:
        print(f"  ✗ Expected 3 rows after append, got {count}\n")
        sys.exit(1)

    # Test 5: add_skipped
    print("TEST 5: add_skipped() creates SKIPPED entry")
    buffer3 = RunLogBuffer("test-run-003", "INCREMENTAL")
    buffer3.add_skipped("gold_weekly_account_summary", "GOLD")
    buffer3.flush(run_log_path)

    conn = duckdb.connect()
    skipped = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{run_log_path}') WHERE status = 'SKIPPED'"
    ).fetchone()[0]
    conn.close()

    if skipped >= 1:
        print(f"  ✓ SKIPPED entry created and flushed\n")
    else:
        print("  ✗ SKIPPED entry not found\n")
        sys.exit(1)

    # Test 6: add_orchestration_failure
    print("TEST 6: add_orchestration_failure() creates orchestration failure entry")
    buffer4 = RunLogBuffer("test-run-004", "HISTORICAL")
    buffer4.add_orchestration_failure("dbt compile failed: syntax error in model X")
    buffer4.flush(run_log_path)

    conn = duckdb.connect()
    orch_fail = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{run_log_path}') WHERE model_name = 'DBT_COMPILE' AND layer = 'ORCHESTRATION'"
    ).fetchone()[0]
    conn.close()

    if orch_fail >= 1:
        print(f"  ✓ ORCHESTRATION failure entry created\n")
    else:
        print("  ✗ ORCHESTRATION failure entry not found\n")
        sys.exit(1)

    # Test 7: Multi-date deduplication on (run_id, model_name, target_date)
    print("TEST 7: Multi-date deduplication preserves per-date entries")
    buffer6 = RunLogBuffer("test-run-006", "HISTORICAL")
    # Add same model for different dates
    buffer6.add_entry("bronze_accounts", "BRONZE", "2026-04-22T10:00:00Z", "2026-04-22T10:00:05Z",
                      "SUCCESS", records_written=100, target_date="2026-04-22")
    buffer6.add_entry("bronze_accounts", "BRONZE", "2026-04-23T10:00:00Z", "2026-04-23T10:00:05Z",
                      "SUCCESS", records_written=95, target_date="2026-04-23")
    buffer6.add_entry("bronze_accounts", "BRONZE", "2026-04-24T10:00:00Z", "2026-04-24T10:00:05Z",
                      "SUCCESS", records_written=110, target_date="2026-04-24")

    if len(buffer6._buffer) == 3:
        print("  ✓ Multi-date entries preserved (not deduplicated)\n")
        # Verify each entry has correct target_date and records_written
        entries_by_date = {e["target_date"]: e["records_written"] for e in buffer6._buffer}
        if entries_by_date.get("2026-04-22") == 100 and entries_by_date.get("2026-04-23") == 95 and entries_by_date.get("2026-04-24") == 110:
            print("  ✓ Each date's metrics preserved correctly\n")
        else:
            print(f"  ✗ Entries not as expected: {entries_by_date}\n")
            sys.exit(1)
    else:
        print(f"  ✗ Expected 3 entries, got {len(buffer6._buffer)}\n")
        sys.exit(1)

    # Test 8: Same (run_id, model_name, target_date) is deduplicated
    print("TEST 8: Same (run_id, model_name, target_date) is deduplicated (replaced)")
    buffer7 = RunLogBuffer("test-run-007", "HISTORICAL")
    buffer7.add_entry("silver_accounts", "SILVER", "2026-04-22T10:00:00Z", "2026-04-22T10:00:05Z",
                      "SUCCESS", records_written=100, target_date="2026-04-22")
    buffer7.add_entry("silver_accounts", "SILVER", "2026-04-22T10:00:10Z", "2026-04-22T10:00:15Z",
                      "SUCCESS", records_written=105, target_date="2026-04-22")

    if len(buffer7._buffer) == 1 and buffer7._buffer[0]["records_written"] == 105:
        print("  ✓ Duplicate (run_id, model_name, target_date) was replaced\n")
    else:
        print(f"  ✗ Expected 1 entry with records_written=105, got {len(buffer7._buffer)} entries\n")
        sys.exit(1)

    # Test 9: check_unlogged_run (no prior run in control.parquet)
    print("TEST 9: check_unlogged_run() returns None when control.parquet is empty")
    # Create an empty control.parquet (no rows)
    empty_control_data = {
        "last_processed_date": [],
        "updated_at": [],
        "updated_by_run_id": []
    }
    df_empty_control = pd.DataFrame(empty_control_data)
    conn = duckdb.connect()
    conn.execute(f"COPY df_empty_control TO '{control_path}' (FORMAT PARQUET)")
    conn.close()

    buffer5 = RunLogBuffer("test-run-005", "HISTORICAL")
    result = buffer5.check_unlogged_run(control_path, run_log_path)

    if result is None:
        print("  ✓ check_unlogged_run returned None (empty control.parquet)\n")
    else:
        print(f"  ✗ Expected None, got {result}\n")
        sys.exit(1)

    print("=== All verification tests passed ===")

except Exception as e:
    print(f"✗ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
