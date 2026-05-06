import os
import duckdb


def load_bronze_transaction_codes(run_id: str) -> dict:
    """Load transaction codes CSV to Bronze layer with idempotency and audit columns."""

    source_path = "/app/source/transaction_codes.csv"
    target_path = "/app/data/bronze/transaction_codes/data.parquet"
    temp_path = "/app/data/bronze/transaction_codes/.data.parquet.tmp"

    # Idempotency gate: check if target file exists
    if os.path.exists(target_path):
        return {"records_written": 0, "skipped": True, "source_file": source_path}

    # Create parent directory if needed
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    # Atomic write using DuckDB
    conn = duckdb.connect()

    try:
        conn.execute(f"""
            COPY (
                SELECT *,
                    'transaction_codes.csv' AS _source_file,
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
