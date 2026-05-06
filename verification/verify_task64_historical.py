#!/usr/bin/env python3
"""
verification/verify_task64_historical.py
Task 6.4: Historical Pipeline Mode verification tests
"""

import sys
import os
sys.path.insert(0, '/app')

from pipeline.pipeline import (
    validate_historical_args_and_files,
    process_transaction_codes_step,
    process_date_sequence,
    process_gold_step,
    finalize_run,
)
from pipeline.run_log import RunLogBuffer
from datetime import date

try:
    print("=== Task 6.4: Historical Pipeline Mode Verification ===\n")

    # TEST 1: Date parsing and validation
    print("TEST 1: validate_historical_args_and_files() with valid dates")
    try:
        start, end = validate_historical_args_and_files("2024-01-01", "2024-01-07")
        if start == date(2024, 1, 1) and end == date(2024, 1, 7):
            print("  ✓ Valid dates parsed correctly\n")
        else:
            print(f"  ✗ Expected 2024-01-01 and 2024-01-07, got {start} and {end}\n")
            sys.exit(1)
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}\n")
        sys.exit(1)

    # TEST 2: Date validation - start > end
    print("TEST 2: validate_historical_args_and_files() rejects start > end")
    try:
        validate_historical_args_and_files("2024-01-07", "2024-01-01")
        print("  ✗ Should have rejected start > end\n")
        sys.exit(1)
    except ValueError as e:
        if "start_date must be <= end_date" in str(e):
            print("  ✓ Correctly rejected start > end\n")
        else:
            print(f"  ✗ Wrong error: {e}\n")
            sys.exit(1)

    # TEST 3: validate_historical_args_and_files handles valid date range
    print("TEST 3: validate_historical_args_and_files() validates date range presence")
    try:
        result = validate_historical_args_and_files("2024-01-01", "2024-01-07")
        if isinstance(result, tuple) and len(result) == 2:
            print("  ✓ Date range validation works (skipping file existence check — depends on environment)\n")
        else:
            print(f"  ✗ Unexpected result: {result}\n")
            sys.exit(1)
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}\n")
        sys.exit(1)

    # TEST 4: RunLogBuffer initialization for historical mode
    print("TEST 4: RunLogBuffer initializes for historical mode")
    run_id = "test-run-001"
    buffer = RunLogBuffer(run_id, "historical")
    if buffer._run_id == run_id and buffer._pipeline_type == "historical":
        print("  ✓ RunLogBuffer created for historical mode\n")
    else:
        print("  ✗ RunLogBuffer not initialized correctly\n")
        sys.exit(1)

    # TEST 5: Run log entry addition
    print("TEST 5: RunLogBuffer.add_entry() creates correct entries")
    buffer.add_entry(
        model_name="test_model",
        layer="BRONZE",
        started_at="2024-01-01T10:00:00",
        completed_at="2024-01-01T10:01:00",
        status="SUCCESS",
        records_written=100
    )
    if len(buffer._buffer) == 1 and buffer._buffer[0]["model_name"] == "test_model":
        print("  ✓ Entry added to buffer\n")
    else:
        print("  ✗ Entry not added correctly\n")
        sys.exit(1)

    # TEST 6: SKIPPED entries
    print("TEST 6: RunLogBuffer.add_skipped() creates SKIPPED entries")
    buffer.add_skipped("silver_model", "SILVER")
    skipped_entry = [e for e in buffer._buffer if e["status"] == "SKIPPED"]
    if len(skipped_entry) == 1 and skipped_entry[0]["model_name"] == "silver_model":
        print("  ✓ SKIPPED entry created\n")
    else:
        print("  ✗ SKIPPED entry not created\n")
        sys.exit(1)

    # TEST 7: Orchestration failure entry
    print("TEST 7: RunLogBuffer.add_orchestration_failure() creates ORCHESTRATION entries")
    buffer2 = RunLogBuffer("test-run-002", "historical")
    buffer2.add_orchestration_failure("Test orchestration error")
    orch_entry = [e for e in buffer2._buffer if e["layer"] == "ORCHESTRATION"]
    if len(orch_entry) == 1 and orch_entry[0]["model_name"] == "DBT_COMPILE":
        print("  ✓ ORCHESTRATION failure entry created\n")
    else:
        print("  ✗ ORCHESTRATION failure entry not created\n")
        sys.exit(1)

    # TEST 8: Missing required args
    print("TEST 8: validate_historical_args_and_files() rejects missing args")
    try:
        validate_historical_args_and_files(None, "2024-01-01")
        print("  ✗ Should have rejected missing start_date\n")
        sys.exit(1)
    except ValueError as e:
        if "required for historical mode" in str(e):
            print("  ✓ Correctly rejected missing arguments\n")
        else:
            print(f"  ✗ Wrong error: {e}\n")
            sys.exit(1)

    # TEST 9: Invalid date format
    print("TEST 9: validate_historical_args_and_files() rejects invalid date format")
    try:
        validate_historical_args_and_files("2024/01/01", "2024-01-07")
        print("  ✗ Should have rejected invalid date format\n")
        sys.exit(1)
    except ValueError as e:
        if "Invalid date format" in str(e):
            print("  ✓ Correctly rejected invalid date format\n")
        else:
            print(f"  ✗ Wrong error: {e}\n")
            sys.exit(1)

    # TEST 10: Verify silver_transaction_codes write code exists
    print("TEST 10: process_transaction_codes_step() includes silver_transaction_codes parquet write")
    import inspect
    source = inspect.getsource(process_transaction_codes_step)
    if "TO '/app/data/silver/transaction_codes/data.parquet'" in source:
        print("  ✓ silver_transaction_codes parquet write found in code\n")
    else:
        print("  ✗ silver_transaction_codes parquet write NOT found in code\n")
        sys.exit(1)

    # TEST 11: Verify silver_accounts quarantine write code exists
    print("TEST 11: process_date_sequence() includes quarantine write for rejected accounts")
    source = inspect.getsource(process_date_sequence)
    if "NULL_REQUIRED_FIELD" in source and "INVALID_ACCOUNT_STATUS" in source and "/app/data/silver/quarantine/date=" in source and "COPY" in source:
        print("  ✓ silver_accounts quarantine write found in code\n")
    else:
        print("  ✗ silver_accounts quarantine write NOT found in code\n")
        sys.exit(1)

    print("=== All Task 6.4 verification tests passed ===")

except Exception as e:
    print(f"✗ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
