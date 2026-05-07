#!/usr/bin/env python3
"""
pipeline/control_plane.py
Watermark and gold_weekly_control Manager

Functions for reading/advancing watermark state and managing computed weeks.
Enforces call ordering via documented preconditions — record_computed_weeks
must be called before advance_watermark (INV-31).
"""

from datetime import datetime, date, timedelta
from typing import Optional, Set
import duckdb
import pandas as pd
from pipeline.s3_utils import configure_duckdb_s3, s3_key_exists, atomic_parquet_put, parse_s3_uri


def get_watermark(control_path: str) -> Optional[date]:
    """
    Read last_processed_date from control.parquet.

    Returns date if one row exists, None if zero rows or file missing.
    Missing file is valid initial state (first run — INV-32).
    """
    try:
        with duckdb.connect() as conn:
            configure_duckdb_s3(conn)
            rows = conn.execute(f"SELECT last_processed_date FROM read_parquet('{control_path}')").fetchall()
    except Exception as e:
        if "No files found" in str(e) or isinstance(e, FileNotFoundError):
            return None
        raise

    if len(rows) == 0:
        return None

    return rows[0][0]


def advance_watermark(control_path: str, new_date: date, run_id: str) -> None:
    """
    Write single-row watermark state atomically via S3 PutObject.

    PRECONDITION: Must be called AFTER record_computed_weeks succeeds for same run (INV-31).

    Uses atomic_parquet_put: serialise to bytes in memory then single boto3 put_object call.
    S3 PutObject is atomic — either the full new object is visible or the prior one is.
    """
    bucket, key = parse_s3_uri(control_path)
    new_row = {
        'last_processed_date': [new_date],
        'updated_at': [datetime.utcnow()],
        'updated_by_run_id': [run_id]
    }
    df_new = pd.DataFrame(new_row)
    atomic_parquet_put(bucket, key, df_new)


def get_computed_weeks(weekly_control_path: str) -> Set[date]:
    """
    Return set of computed week_start_date values from gold_weekly_control.parquet.

    Returns empty set if zero rows or file missing.
    Missing file is valid initial state (first run — INV-32).
    """
    try:
        with duckdb.connect() as conn:
            configure_duckdb_s3(conn)
            rows = conn.execute(f"SELECT week_start_date FROM read_parquet('{weekly_control_path}')").fetchall()
    except Exception as e:
        if "No files found" in str(e) or isinstance(e, FileNotFoundError):
            return set()
        raise

    return {row[0].date() if hasattr(row[0], 'date') else row[0] for row in rows}


def record_computed_weeks(weekly_control_path: str, weeks: list, run_id: str) -> None:
    """
    Append new week entries to gold_weekly_control.parquet.

    PRECONDITION: Must be called BEFORE advance_watermark (INV-31).

    Append-only semantics: never updates or deletes existing rows (INV-14).
    Returns immediately if weeks list is empty (no-op).
    """
    if not weeks:
        return

    new_rows = {
        'week_start_date': [w['week_start_date'] for w in weeks],
        'week_end_date': [w['week_end_date'] for w in weeks],
        'computed_at': [datetime.utcnow()] * len(weeks),
        'computed_by_run_id': [run_id] * len(weeks)
    }
    df_new = pd.DataFrame(new_rows)

    bucket, key = parse_s3_uri(weekly_control_path)
    if s3_key_exists(bucket, key):
        with duckdb.connect() as conn:
            configure_duckdb_s3(conn)
            df_existing = conn.execute(f"SELECT * FROM read_parquet('{weekly_control_path}')").df()
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    atomic_parquet_put(bucket, key, df_combined)


def get_uncomputed_weeks(silver_path: str, weekly_control_path: str) -> list:
    """
    Return list of uncomputed ISO weeks with resolvable Silver transactions.

    Reads all distinct transaction_date values from Silver where _is_resolvable=true,
    converts to ISO weeks (Monday-Sunday), excludes weeks already in gold_weekly_control,
    and returns sorted list of uncomputed weeks.

    Returns empty list if silver_path does not exist (first-run valid state per INV-32).
    State sourced exclusively from control Parquets, never filesystem inference.
    """
    try:
        with duckdb.connect() as conn:
            configure_duckdb_s3(conn)
            dates_result = conn.execute(f"""
                SELECT DISTINCT transaction_date
                FROM read_parquet('{silver_path}/**/*.parquet')
                WHERE _is_resolvable = true
                ORDER BY transaction_date
            """).fetchall()
    except Exception as e:
        if "No files found" in str(e) or isinstance(e, FileNotFoundError):
            return []
        raise

    if not dates_result:
        return []

    week_set = set()
    for (txn_date,) in dates_result:
        if isinstance(txn_date, str):
            txn_date_obj = datetime.strptime(txn_date, '%Y-%m-%d').date()
        elif hasattr(txn_date, 'date'):
            txn_date_obj = txn_date.date()
        else:
            txn_date_obj = txn_date
        week_start = txn_date_obj - timedelta(days=txn_date_obj.weekday())
        week_set.add(week_start)

    computed_weeks = get_computed_weeks(weekly_control_path)

    uncomputed = sorted([w for w in week_set if w not in computed_weeks])

    return [
        {
            'week_start_date': week_start,
            'week_end_date': week_start + timedelta(days=6)
        }
        for week_start in uncomputed
    ]
