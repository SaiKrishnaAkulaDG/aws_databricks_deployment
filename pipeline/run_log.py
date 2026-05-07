#!/usr/bin/env python3
"""
pipeline/run_log.py
Run Log Writer with Async Buffer

Accumulates run log entries in memory and flushes to parquet with deduplication.
Implements INV-19 (append-only), INV-20A (deduplication), INV-20C (recovery).
"""

import json
import os
from datetime import datetime
from typing import Optional
import duckdb
import pandas as pd
import numpy as np
from pipeline.s3_utils import configure_duckdb_s3, atomic_parquet_put, parse_s3_uri


class RunLogBuffer:
    """Accumulates run log entries and flushes to parquet with deduplication."""

    def __init__(self, run_id: str, pipeline_type: str):
        """Initialize buffer for a run session."""
        self._buffer = []
        self._run_id = run_id
        self._pipeline_type = pipeline_type  # 'historical' or 'incremental' — used by Tasks 6.4/6.5 orchestration modes

    def add_entry(self, model_name: str, layer: str, started_at: str = None, completed_at: str = None,
                  status: str = None, records_processed: Optional[int] = None,
                  records_written: Optional[int] = None, records_rejected: Optional[int] = None,
                  error_message: Optional[str] = None, target_date: Optional[str] = None):
        """
        Add or replace entry in buffer. Deduplicates on (run_id, model_name, target_date).
        """
        entry = {
            "run_id": self._run_id,
            "model_name": model_name,
            "target_date": target_date,
            "layer": layer,
            "started_at": started_at,
            "completed_at": completed_at,
            "status": status,
            "records_processed": records_processed,
            "records_written": records_written,
            "records_rejected": records_rejected,
            "error_message": error_message
        }

        idx = next((i for i, e in enumerate(self._buffer)
                   if e["run_id"] == self._run_id and e["model_name"] == model_name and e.get("target_date") == target_date), None)
        if idx is not None:
            self._buffer[idx] = entry
        else:
            self._buffer.append(entry)

    def add_skipped(self, model_name: str, layer: str, target_date: Optional[str] = None):
        """Add a SKIPPED entry."""
        self.add_entry(model_name, layer, None, None, "SKIPPED", target_date=target_date)

    def add_orchestration_failure(self, error_message: str):
        """Add an ORCHESTRATION layer failure entry."""
        self.add_entry("DBT_COMPILE", "ORCHESTRATION", None, None, "FAILED",
                       error_message=error_message)

    def flush(self, target_path: str):
        """
        Flush buffer to parquet on S3. Appends without overwriting (INV-19).
        On exception, writes buffer to local fallback .jsonl and re-raises.
        Clears buffer after flush (success or failure) to prevent re-append on subsequent calls.
        """
        try:
            df_buffer = pd.DataFrame(self._buffer)

            bucket, key = parse_s3_uri(target_path)

            try:
                with duckdb.connect() as conn:
                    configure_duckdb_s3(conn)
                    df_existing = conn.execute(f"SELECT * FROM read_parquet('{target_path}')").df()
                df_combined = pd.concat([df_existing, df_buffer], ignore_index=True)
            except Exception as e:
                if "No files found" in str(e) or isinstance(e, FileNotFoundError):
                    df_combined = df_buffer
                else:
                    raise

            atomic_parquet_put(bucket, key, df_combined)

            self._buffer = []

        except Exception as e:
            fallback_path = "/tmp/pipeline_runlog_fallback.jsonl"
            with open(fallback_path, "a") as f:
                for entry in self._buffer:
                    f.write(json.dumps(entry) + "\n")
            self._buffer = []
            raise e

    def check_unlogged_run(self, control_path: str, run_log_path: str) -> Optional[str]:
        """
        Check if a prior run has no log entries.
        Returns unlogged run_id if found, None otherwise.
        Returns None if control.parquet does not exist (first run).
        Raises exception if control.parquet or run_log.parquet is corrupted.
        """
        try:
            with duckdb.connect() as conn:
                configure_duckdb_s3(conn)
                rows = conn.execute(f"SELECT updated_by_run_id FROM read_parquet('{control_path}')").fetchall()
        except Exception as e:
            if "No files found" in str(e) or isinstance(e, FileNotFoundError):
                return None
            raise

        if len(rows) == 0:
            return None

        if len(rows) > 1:
            raise ValueError(f"control.parquet has {len(rows)} rows — expected exactly 1, INV-43 halt required")

        prior_run_id = rows[0][0]

        if pd.isna(prior_run_id):
            raise ValueError("control.parquet: updated_by_run_id is NULL — schema violation, INV-43 halt required")

        try:
            with duckdb.connect() as conn:
                configure_duckdb_s3(conn)
                count = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{run_log_path}') WHERE run_id = '{prior_run_id}'"
                ).fetchone()[0]
        except Exception as e:
            if "No files found" in str(e) or isinstance(e, FileNotFoundError):
                return prior_run_id
            raise

        if count == 0:
            return prior_run_id
        return None

    def write_unlogged_run_row(self, unlogged_run_id: str):
        """Add a synthetic UNLOGGED_RUN row for recovery."""
        self.add_entry(
            "UNLOGGED_RUN",
            "ORCHESTRATION",
            None,
            None,
            "FAILED",
            error_message=f"Run log flush failed for prior run {unlogged_run_id}. Data was written successfully. See pipeline_runlog_fallback.jsonl for run detail."
        )
