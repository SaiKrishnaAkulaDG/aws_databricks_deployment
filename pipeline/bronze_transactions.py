import os
import duckdb
from pipeline.s3_utils import configure_duckdb_s3, s3_key_exists


def load_bronze_transactions(date_str: str, run_id: str) -> dict:
    """Load transactions CSV to Bronze layer with idempotency and audit columns."""
    bucket = os.environ["S3_BUCKET"]
    source_path = f"/app/source/transactions_{date_str}.csv"
    s3_key = f"bronze/transactions/date={date_str}/data.parquet"
    target_path = f"s3://{bucket}/{s3_key}"

    if s3_key_exists(bucket, s3_key):
        return {"records_written": 0, "skipped": True, "source_file": source_path}

    basename = os.path.basename(source_path)
    with duckdb.connect() as conn:
        configure_duckdb_s3(conn)
        conn.execute(f"""
            COPY (
                SELECT *,
                    '{basename}' AS _source_file,
                    now() AS _ingested_at,
                    '{run_id}' AS _pipeline_run_id
                FROM read_csv_auto('{source_path}')
            )
            TO '{target_path}' (FORMAT PARQUET)
        """)

    with duckdb.connect() as conn:
        configure_duckdb_s3(conn)
        row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{target_path}')").fetchone()[0]

    return {"records_written": row_count, "skipped": False, "source_file": source_path}
