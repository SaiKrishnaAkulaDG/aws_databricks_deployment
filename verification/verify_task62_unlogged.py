#!/usr/bin/env python3
"""
verification/verify_task62_unlogged.py
Task 6.2 Finding 3: Verify check_unlogged_run() and write_unlogged_run_row()
for INV-20C unlogged run recovery.
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

try:
    print("=== Unlogged Run Detection and Recovery ===\n")

    # Setup: Create control.parquet with a prior run_id that has no log entries
    print("SETUP: Creating control.parquet with prior unlogged run")
    control_data = {
        "last_processed_date": ["2026-04-21"],
        "updated_at": ["2026-04-22T10:00:00Z"],
        "updated_by_run_id": ["unlogged-run-999"]
    }
    df_control = pd.DataFrame(control_data)
    conn = duckdb.connect()
    conn.execute(f"COPY df_control TO '{control_path}' (FORMAT PARQUET)")
    conn.close()
    print(f"  ✓ control.parquet created with updated_by_run_id='unlogged-run-999'\n")

    # Setup: Create run_log.parquet with entries for a different run_id
    print("SETUP: Creating run_log.parquet with entries for a different run")
    runlog_data = {
        "run_id": ["logged-run-001", "logged-run-001"],
        "model_name": ["silver_accounts", "silver_transactions"],
        "layer": ["SILVER", "SILVER"],
        "started_at": ["2026-04-22T10:00:00Z", "2026-04-22T10:00:05Z"],
        "completed_at": ["2026-04-22T10:00:05Z", "2026-04-22T10:00:10Z"],
        "status": ["SUCCESS", "SUCCESS"],
        "records_processed": [100, 500],
        "records_written": [100, 500],
        "records_rejected": [None, None],
        "error_message": [None, None]
    }
    df_runlog = pd.DataFrame(runlog_data)
    conn = duckdb.connect()
    conn.execute(f"COPY df_runlog TO '{run_log_path}' (FORMAT PARQUET)")
    conn.close()
    print(f"  ✓ run_log.parquet created with entries only for 'logged-run-001'\n")

    # Test 1: check_unlogged_run detects prior unlogged run_id
    print("TEST 1: check_unlogged_run() detects unlogged run")
    buffer = RunLogBuffer("current-run-001", "HISTORICAL")
    unlogged_run_id = buffer.check_unlogged_run(control_path, run_log_path)

    if unlogged_run_id == "unlogged-run-999":
        print(f"  ✓ Detected unlogged run_id: {unlogged_run_id}\n")
    else:
        print(f"  ✗ Expected 'unlogged-run-999', got {unlogged_run_id}\n")
        sys.exit(1)

    # Test 2: write_unlogged_run_row creates UNLOGGED_RUN entry
    print("TEST 2: write_unlogged_run_row() creates recovery entry")
    buffer.write_unlogged_run_row(unlogged_run_id)

    if len(buffer._buffer) == 1:
        entry = buffer._buffer[0]
        if entry["model_name"] == "UNLOGGED_RUN" and entry["layer"] == "ORCHESTRATION" and entry["status"] == "FAILED":
            print(f"  ✓ UNLOGGED_RUN entry created with correct fields")
            print(f"    model_name: {entry['model_name']}")
            print(f"    layer: {entry['layer']}")
            print(f"    status: {entry['status']}")
            if "unlogged-run-999" in entry["error_message"]:
                print(f"    error_message contains unlogged run_id: ✓\n")
            else:
                print(f"    ✗ error_message doesn't reference unlogged run_id\n")
                sys.exit(1)
        else:
            print(f"  ✗ Entry fields incorrect\n")
            sys.exit(1)
    else:
        print(f"  ✗ Expected 1 entry in buffer, got {len(buffer._buffer)}\n")
        sys.exit(1)

    # Test 3: Flush the UNLOGGED_RUN entry and verify it's in parquet
    print("TEST 3: UNLOGGED_RUN entry is persisted after flush")
    buffer.flush(run_log_path)

    conn = duckdb.connect()
    count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{run_log_path}')").fetchone()[0]
    unlogged_count = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{run_log_path}') WHERE model_name = 'UNLOGGED_RUN'"
    ).fetchone()[0]
    conn.close()

    if unlogged_count == 1:
        print(f"  ✓ UNLOGGED_RUN entry persisted to parquet")
        print(f"    Total rows in run_log: {count}")
        print(f"    UNLOGGED_RUN rows: {unlogged_count}\n")
    else:
        print(f"  ✗ UNLOGGED_RUN entry not found in parquet\n")
        sys.exit(1)

    # Test 4: Direct recovery via adding unlogged run_id entry to parquet
    print("TEST 4: After adding entry with unlogged run_id, check_unlogged_run returns None")
    # To properly recover, we need to add an entry with run_id="unlogged-run-999" to the run_log
    recovery_buffer = RunLogBuffer("unlogged-run-999", "HISTORICAL")
    recovery_buffer.add_orchestration_failure("Recovered from prior unlogged run")
    recovery_buffer.flush(run_log_path)

    buffer3 = RunLogBuffer("current-run-003", "HISTORICAL")
    result = buffer3.check_unlogged_run(control_path, run_log_path)

    if result is None:
        print(f"  ✓ check_unlogged_run returned None (entry with unlogged run_id now present)\n")
    else:
        print(f"  ✗ Expected None, got {result}\n")
        sys.exit(1)

    # Test 5: First-run scenario — missing control.parquet returns None
    print("TEST 5: First-run scenario — missing control.parquet returns None")
    missing_control_path = os.path.join(test_dir, "control_missing.parquet")
    buffer6 = RunLogBuffer("first-run-001", "HISTORICAL")
    result = buffer6.check_unlogged_run(missing_control_path, run_log_path)

    if result is None:
        print(f"  ✓ check_unlogged_run returned None (control.parquet missing on first run)\n")
    else:
        print(f"  ✗ Expected None for missing control.parquet, got {result}\n")
        sys.exit(1)

    # Test 6: NULL updated_by_run_id in control.parquet raises ValueError (INV-43)
    print("TEST 6: NULL updated_by_run_id in control.parquet raises ValueError (INV-43)")
    null_control_path = os.path.join(test_dir, "control_null.parquet")
    null_control_data = {
        "last_processed_date": ["2026-04-21"],
        "updated_at": ["2026-04-22T10:00:00Z"],
        "updated_by_run_id": [None]  # NULL value violates schema
    }
    df_null_control = pd.DataFrame(null_control_data)
    conn = duckdb.connect()
    conn.execute(f"COPY df_null_control TO '{null_control_path}' (FORMAT PARQUET)")
    conn.close()

    buffer7 = RunLogBuffer("current-run-007", "HISTORICAL")
    try:
        result = buffer7.check_unlogged_run(null_control_path, run_log_path)
        print(f"  ✗ Expected ValueError, but got result: {result}\n")
        sys.exit(1)
    except ValueError as e:
        if "NULL" in str(e) and "INV-43" in str(e):
            print(f"  ✓ ValueError raised correctly: {e}\n")
        else:
            print(f"  ✗ ValueError raised but message incorrect: {e}\n")
            sys.exit(1)
    except Exception as e:
        print(f"  ✗ Wrong exception type: {type(e).__name__}: {e}\n")
        sys.exit(1)

    # Test 7: Calling flush() twice on same buffer instance — no duplicates (IG-02)
    print("TEST 7: Calling flush() twice on same buffer — no duplicates")
    dup_test_path = os.path.join(test_dir, "dup_test.parquet")
    buffer_dup = RunLogBuffer("dup-run-001", "HISTORICAL")
    buffer_dup.add_entry("model_a", "SILVER", "2026-04-22T10:00:00Z", "2026-04-22T10:00:05Z",
                         "SUCCESS", records_processed=50, records_written=50)

    # First flush
    buffer_dup.flush(dup_test_path)

    # Second flush on same instance
    buffer_dup.flush(dup_test_path)

    # Read parquet and count entries for model_a
    conn = duckdb.connect()
    count = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{dup_test_path}') WHERE run_id = 'dup-run-001'"
    ).fetchone()[0]
    conn.close()

    if count == 1:
        print(f"  ✓ Second flush() did not duplicate entries (count = {count})\n")
    else:
        print(f"  ✗ Expected 1 entry after two flush() calls, got {count}\n")
        sys.exit(1)

    # Test 8: Multiple rows in control.parquet raises ValueError (INV-43)
    print("TEST 8: Multiple rows in control.parquet raises ValueError (INV-43)")
    multi_control_path = os.path.join(test_dir, "control_multi.parquet")
    multi_control_data = {
        "last_processed_date": ["2026-04-21", "2026-04-20"],
        "updated_at": ["2026-04-22T10:00:00Z", "2026-04-21T10:00:00Z"],
        "updated_by_run_id": ["run-001", "run-000"]  # 2 rows instead of 1
    }
    df_multi_control = pd.DataFrame(multi_control_data)
    conn = duckdb.connect()
    conn.execute(f"COPY df_multi_control TO '{multi_control_path}' (FORMAT PARQUET)")
    conn.close()

    buffer8 = RunLogBuffer("current-run-008", "HISTORICAL")
    try:
        result = buffer8.check_unlogged_run(multi_control_path, run_log_path)
        print(f"  ✗ Expected ValueError, but got result: {result}\n")
        sys.exit(1)
    except ValueError as e:
        if "2 rows" in str(e) and "INV-43" in str(e):
            print(f"  ✓ ValueError raised correctly: {e}\n")
        else:
            print(f"  ✗ ValueError raised but message incorrect: {e}\n")
            sys.exit(1)
    except Exception as e:
        print(f"  ✗ Wrong exception type: {type(e).__name__}: {e}\n")
        sys.exit(1)

    # Test 9: flush() exception path clears buffer — no duplicates on retry
    print("TEST 9: flush() exception path clears buffer on retry")
    retry_test_path = os.path.join(test_dir, "retry_test.parquet")
    buffer_retry = RunLogBuffer("retry-run-001", "HISTORICAL")
    buffer_retry.add_entry("model_x", "SILVER", "2026-04-22T10:00:00Z", "2026-04-22T10:00:05Z",
                           "SUCCESS", records_processed=75, records_written=75)

    # Record fallback .jsonl line count before first flush
    fallback_path = "/app/data/pipeline/pipeline_runlog_fallback.jsonl"
    fallback_count_before = 0
    if os.path.exists(fallback_path):
        with open(fallback_path, "r") as f:
            fallback_count_before = len(f.readlines())

    # First flush with invalid path to trigger exception
    invalid_path = "/invalid/nonexistent/path/parquet_file.parquet"
    try:
        buffer_retry.flush(invalid_path)
        print(f"  ✗ Expected flush() to raise exception with invalid path\n")
        sys.exit(1)
    except Exception:
        pass  # Expected

    # Now add a new entry and flush to valid path (buffer was cleared by exception, only new entry in it)
    buffer_retry.add_entry("model_y", "SILVER", "2026-04-22T10:00:05Z", "2026-04-22T10:00:10Z",
                           "SUCCESS", records_processed=80, records_written=80)
    buffer_retry.flush(retry_test_path)

    # Verify parquet contains only 1 entry (model_y, NOT duplicated model_x from failed flush)
    conn = duckdb.connect()
    count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{retry_test_path}')").fetchone()[0]
    model_y_count = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{retry_test_path}') WHERE model_name = 'model_y'"
    ).fetchone()[0]
    conn.close()

    if count == 1 and model_y_count == 1:
        print(f"  ✓ After exception retry, parquet has only 1 entry (model_y, not duplicated from failed flush)\n")
    else:
        print(f"  ✗ Expected 1 entry total (model_y), got {count} total and {model_y_count} model_y\n")
        sys.exit(1)

    # Verify fallback .jsonl has the entry from failed flush but NOT duplicated on retry
    if os.path.exists(fallback_path):
        with open(fallback_path, "r") as f:
            fallback_lines = f.readlines()
        fallback_count_after = len(fallback_lines)
        # Should have exactly 1 more line (model_x from failed flush)
        if fallback_count_after == fallback_count_before + 1:
            print(f"  ✓ Fallback .jsonl has no duplicates (1 entry added from failed flush, none from retry)\n")
        else:
            print(f"  ✗ Fallback .jsonl should have {fallback_count_before + 1} entries, got {fallback_count_after}\n")
            sys.exit(1)
    else:
        print(f"  ✗ Expected fallback .jsonl to exist from first flush failure\n")
        sys.exit(1)

    # Test 10: Empty control.parquet (zero rows) returns None
    print("TEST 10: Empty control.parquet (zero rows) returns None")
    empty_control_path = os.path.join(test_dir, "control_empty.parquet")
    empty_control_data = {
        "last_processed_date": [],
        "updated_at": [],
        "updated_by_run_id": []
    }
    df_empty = pd.DataFrame(empty_control_data)
    conn = duckdb.connect()
    conn.execute(f"COPY df_empty TO '{empty_control_path}' (FORMAT PARQUET)")
    conn.close()

    buffer10 = RunLogBuffer("current-run-010", "HISTORICAL")
    result = buffer10.check_unlogged_run(empty_control_path, run_log_path)

    if result is None:
        print(f"  ✓ Empty control.parquet returns None (no prior run to recover)\n")
    else:
        print(f"  ✗ Expected None for empty control.parquet, got {result}\n")
        sys.exit(1)

    print("=== All unlogged run recovery tests passed ===")

except Exception as e:
    print(f"✗ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
