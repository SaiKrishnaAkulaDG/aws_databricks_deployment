import os
import duckdb
from pathlib import Path


def load_bronze_transactions(date_str: str, run_id: str) -> dict:
    """Load transactions CSV to Bronze layer with idempotency and audit columns."""

    source_path = f"/app/source/transactions_{date_str}.csv"
    target_dir = f"/app/data/bronze/transactions/date={date_str}"
    target_path = f"{target_dir}/data.parquet"
    temp_path = f"{target_dir}/.data.parquet.tmp"

    # Idempotency gate: check if target file exists
    if os.path.exists(target_path):
        return {"records_written": 0, "skipped": True, "source_file": source_path}

    # Create target directory
    os.makedirs(target_dir, exist_ok=True)

    # Atomic write using DuckDB
    basename = os.path.basename(source_path)
    conn = duckdb.connect()

    try:
        conn.execute(f"""
            COPY (
                SELECT *,
                    '{basename}' AS _source_file,
                    now() AS _ingested_at,
                    '{run_id}' AS _pipeline_run_id
                FROM read_csv_auto('{source_path}')
            )
            TO '{temp_path}' (FORMAT PARQUET)
        """)
        os.rename(temp_path, target_path)
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise e
    finally:
        conn.close()

    # Get row count
    conn = duckdb.connect()
    row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{target_path}')").fetchone()[0]
    conn.close()

    return {"records_written": row_count, "skipped": False, "source_file": source_path}
