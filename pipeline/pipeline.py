import argparse
import os
import sys
import uuid
import json
import duckdb
import pandas as pd
from datetime import datetime, date, timedelta
from pipeline.bronze_accounts import load_bronze_accounts
from pipeline.bronze_transactions import load_bronze_transactions
from pipeline.bronze_transaction_codes import load_bronze_transaction_codes
from pipeline.dbt_runner import stream_dbt_layer, derive_execution_order
from pipeline.run_log import RunLogBuffer
from pipeline.control_plane import (
    get_watermark,
    advance_watermark,
    get_computed_weeks,
    record_computed_weeks,
    get_uncomputed_weeks,
)
from pipeline.s3_utils import configure_duckdb_s3, s3_key_exists, atomic_parquet_put


def generate_run_id() -> str:
    return str(uuid.uuid4())


def initialise_control_plane(s3_bucket: str) -> None:
    control_path = f"s3://{s3_bucket}/pipeline/control.parquet"
    gold_weekly_path = f"s3://{s3_bucket}/pipeline/gold_weekly_control.parquet"
    fallback_path = "/tmp/pipeline_runlog_fallback.jsonl"

    control_schema = {
        'last_processed_date': pd.Series(dtype='object'),
        'updated_at': pd.Series(dtype='datetime64[ns]'),
        'updated_by_run_id': pd.Series(dtype='str'),
    }
    gold_weekly_schema = {
        'week_start_date': pd.Series(dtype='object'),
        'week_end_date': pd.Series(dtype='object'),
        'computed_at': pd.Series(dtype='datetime64[ns]'),
        'computed_by_run_id': pd.Series(dtype='str'),
    }

    for s3_key, schema_dict, label in [
        (f"pipeline/control.parquet", control_schema, "control.parquet"),
        (f"pipeline/gold_weekly_control.parquet", gold_weekly_schema, "gold_weekly_control.parquet"),
    ]:
        if not s3_key_exists(s3_bucket, s3_key):
            try:
                atomic_parquet_put(s3_bucket, s3_key, pd.DataFrame(schema_dict))
                print(f"Initialised {label}.")
            except Exception as e:
                with open(fallback_path, "a") as f:
                    f.write(f'{{"timestamp": "{datetime.utcnow().isoformat()}", "run_id": "N/A", "model_name": "DBT_COMPILE", "layer": "ORCHESTRATION", "status": "FAILED", "error_message": "{str(e)}"}}\n')
                print(f"Error initialising {label}: {e}")
                raise SystemExit(1)
        else:
            try:
                full_path = f"s3://{s3_bucket}/{s3_key}"
                with duckdb.connect() as conn:
                    configure_duckdb_s3(conn)
                    conn.execute(f"SELECT COUNT(*) FROM read_parquet('{full_path}')")
            except Exception as e:
                with open(fallback_path, "a") as f:
                    f.write(f'{{"timestamp": "{datetime.utcnow().isoformat()}", "run_id": "N/A", "model_name": "DBT_COMPILE", "layer": "ORCHESTRATION", "status": "FAILED", "error_message": "Corrupt {label}: {str(e)}"}}\n')
                print(f"Error reading {label}: {e}")
                raise SystemExit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="Credit Card Financial Transactions Lake Pipeline")
    parser.add_argument("--mode", required=True, choices=["historical", "incremental"],
                        help="Pipeline mode")
    parser.add_argument("--start-date", required=False, default=None,
                        help="Start date for historical mode (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=False, default=None,
                        help="End date for historical mode (YYYY-MM-DD)")
    return parser.parse_args()


def validate_historical_args_and_files(start_date_str: str, end_date_str: str) -> tuple:
    if not start_date_str or not end_date_str:
        raise ValueError("--start-date and --end-date required for historical mode")

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date format (YYYY-MM-DD): {e}")

    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")

    missing_files = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        accounts_file = f"/app/source/accounts_{date_str}.csv"
        transactions_file = f"/app/source/transactions_{date_str}.csv"
        if not os.path.exists(accounts_file):
            missing_files.append(accounts_file)
        if not os.path.exists(transactions_file):
            missing_files.append(transactions_file)
        current += timedelta(days=1)

    if not os.path.exists("/app/source/transaction_codes.csv"):
        missing_files.append("/app/source/transaction_codes.csv")

    if missing_files:
        print("ERROR: Missing source files:")
        for f in missing_files:
            print(f"  {f}")
        raise ValueError("Required source files missing")

    return start_date, end_date


def process_transaction_codes_step(run_id: str, run_log_buffer: RunLogBuffer, fallback_path: str) -> bool:
    s3_bucket = os.environ["S3_BUCKET"]

    try:
        started = datetime.utcnow().isoformat()
        result = load_bronze_transaction_codes(run_id)
        completed = datetime.utcnow().isoformat()

        run_log_buffer.add_entry(
            model_name="bronze_transaction_codes",
            layer="BRONZE",
            started_at=started,
            completed_at=completed,
            status="SUCCESS",
            records_written=result["records_written"]
        )
        print(f"Loaded {result['records_written']} transaction codes to Bronze")

        silver_started = None
        silver_completed = None
        for event in stream_dbt_layer("silver_transaction_codes", run_id, {"run_id": run_id, "s3_bucket": s3_bucket}):
            if event.get("event") == "start" and event.get("model") == "silver_transaction_codes":
                silver_started = event.get("started_at")
            elif event.get("event") == "finish" and event.get("model") == "silver_transaction_codes":
                if event.get("status") == "success":
                    silver_completed = event.get("completed_at")
                else:
                    raise Exception(f"silver_transaction_codes failed with status {event.get('status')}")
            elif event.get("event") == "exit":
                if event.get("returncode") != 0:
                    raise Exception(f"dbt silver_transaction_codes failed with code {event.get('returncode')}")

        silver_tc_path = f"s3://{s3_bucket}/silver/transaction_codes/data.parquet"

        with duckdb.connect() as conn:
            configure_duckdb_s3(conn)
            conn.execute(f"""
                COPY (
                    SELECT
                        transaction_code,
                        description,
                        debit_credit_indicator,
                        transaction_type,
                        affects_balance,
                        _source_file,
                        _ingested_at AS _bronze_ingested_at,
                        _pipeline_run_id,
                        CURRENT_TIMESTAMP AS _promoted_at
                    FROM read_parquet(
                        's3://{s3_bucket}/bronze/transaction_codes/data.parquet'
                    )
                ) TO '{silver_tc_path}'
                (FORMAT PARQUET)
            """)

        with duckdb.connect() as conn:
            configure_duckdb_s3(conn)
            records = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{silver_tc_path}')"
            ).fetchone()[0]

        run_log_buffer.add_entry(
            model_name="silver_transaction_codes",
            layer="SILVER",
            started_at=silver_started,
            completed_at=silver_completed,
            status="SUCCESS",
            records_written=records
        )
        return True

    except Exception as e:
        run_log_buffer.add_entry(
            model_name="silver_transaction_codes",
            layer="SILVER",
            status="FAILED",
            error_message=str(e)
        )
        run_log_buffer.add_skipped("silver_accounts", "SILVER")
        run_log_buffer.add_skipped("bronze_transactions", "BRONZE")
        run_log_buffer.add_skipped("silver_transactions", "SILVER")
        run_log_buffer.add_skipped("gold_daily_summary", "GOLD")
        run_log_buffer.add_skipped("gold_weekly_account_summary", "GOLD")
        run_log_buffer.flush(fallback_path)
        print(f"ERROR in transaction codes step: {e}")
        return False


def process_date_sequence(start_date: date, end_date: date, run_id: str, run_log_buffer: RunLogBuffer, fallback_path: str) -> bool:
    s3_bucket = os.environ["S3_BUCKET"]
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")

        try:
            bronze_started = datetime.utcnow().isoformat()
            result = load_bronze_accounts(date_str, run_id)
            bronze_completed = datetime.utcnow().isoformat()

            run_log_buffer.add_entry(
                model_name="bronze_accounts",
                layer="BRONZE",
                started_at=bronze_started,
                completed_at=bronze_completed,
                status="SUCCESS",
                records_written=result["records_written"],
                target_date=date_str
            )

        except Exception as e:
            run_log_buffer.add_entry(
                model_name="bronze_accounts",
                layer="BRONZE",
                status="FAILED",
                error_message=str(e),
                target_date=date_str
            )
            run_log_buffer.add_skipped("silver_accounts", "SILVER", target_date=date_str)
            run_log_buffer.add_skipped("bronze_transactions", "BRONZE", target_date=date_str)
            run_log_buffer.add_skipped("silver_transactions", "SILVER", target_date=date_str)
            temp = current_date + timedelta(days=1)
            while temp <= end_date:
                temp_str = temp.strftime("%Y-%m-%d")
                run_log_buffer.add_skipped("bronze_accounts", "BRONZE", target_date=temp_str)
                run_log_buffer.add_skipped("silver_accounts", "SILVER", target_date=temp_str)
                run_log_buffer.add_skipped("bronze_transactions", "BRONZE", target_date=temp_str)
                run_log_buffer.add_skipped("silver_transactions", "SILVER", target_date=temp_str)
                temp += timedelta(days=1)
            run_log_buffer.add_skipped("gold_daily_summary", "GOLD")
            run_log_buffer.add_skipped("gold_weekly_account_summary", "GOLD")
            run_log_buffer.flush(fallback_path)
            print(f"ERROR on {date_str} (bronze_accounts): {e}")
            return False

        try:
            # Create a schema-only placeholder on S3 so the glob() check in silver_accounts.sql
            # succeeds on the first run before any real silver accounts data exists.
            placeholder_key = "silver/accounts/data.parquet"
            if not s3_key_exists(s3_bucket, placeholder_key):
                try:
                    placeholder_df = pd.DataFrame({
                        'account_id': pd.Series(dtype='str'),
                        'customer_name': pd.Series(dtype='str'),
                        'account_status': pd.Series(dtype='str'),
                        'credit_limit': pd.Series(dtype='float64'),
                        'current_balance': pd.Series(dtype='float64'),
                        'open_date': pd.Series(dtype='object'),
                        'billing_cycle_start': pd.Series(dtype='object'),
                        'billing_cycle_end': pd.Series(dtype='object'),
                        '_source_file': pd.Series(dtype='str'),
                        '_bronze_ingested_at': pd.Series(dtype='datetime64[ns]'),
                        '_pipeline_run_id': pd.Series(dtype='str'),
                        '_record_valid_from': pd.Series(dtype='datetime64[ns]'),
                    })
                    atomic_parquet_put(s3_bucket, placeholder_key, placeholder_df)
                except Exception as e:
                    run_log_buffer.add_entry(
                        model_name="placeholder_creation",
                        layer="SILVER",
                        status="FAILED",
                        error_message=f"Failed to create placeholder file: {str(e)}",
                        target_date=date_str
                    )
                    raise

            silver_started = None
            silver_completed = None
            silver_finished = False
            for event in stream_dbt_layer("silver_accounts", run_id, {"target_date": date_str, "run_id": run_id, "s3_bucket": s3_bucket}):
                if event.get("event") == "start" and event.get("model") == "silver_accounts":
                    silver_started = event.get("started_at")
                elif event.get("event") == "finish" and event.get("model") == "silver_accounts":
                    if event.get("status") == "success":
                        silver_completed = event.get("completed_at")
                        silver_finished = True
                    else:
                        raise Exception(f"silver_accounts failed")
                elif event.get("event") == "exit":
                    if event.get("returncode") != 0:
                        raise Exception(f"dbt failed on silver_accounts")

            if not silver_finished:
                raise Exception(f"dbt silver_accounts: missing finish event")

            silver_accounts_path = f"s3://{s3_bucket}/silver/accounts/data.parquet"
            with duckdb.connect() as conn:
                configure_duckdb_s3(conn)
                records_written = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{silver_accounts_path}')"
                ).fetchone()[0]

            with duckdb.connect() as conn:
                configure_duckdb_s3(conn)
                rejected = conn.execute(f"""
                    SELECT
                        account_id, customer_name, account_status,
                        credit_limit, current_balance, open_date,
                        billing_cycle_start, billing_cycle_end,
                        _source_file, _ingested_at, _pipeline_run_id,
                        CURRENT_TIMESTAMP AS _rejected_at,
                        CASE
                            WHEN account_id IS NULL OR account_id = ''
                                 OR open_date IS NULL
                                 OR credit_limit IS NULL
                                 OR current_balance IS NULL
                                 OR billing_cycle_start IS NULL
                                 OR billing_cycle_end IS NULL
                                 OR account_status IS NULL
                                 OR account_status = ''
                                THEN 'NULL_REQUIRED_FIELD'
                            WHEN account_status NOT IN (
                                'ACTIVE', 'SUSPENDED', 'CLOSED')
                                THEN 'INVALID_ACCOUNT_STATUS'
                        END AS _rejection_reason
                    FROM read_parquet(
                        's3://{s3_bucket}/bronze/accounts/date={date_str}/data.parquet'
                    )
                    WHERE account_id IS NULL OR account_id = ''
                       OR open_date IS NULL OR credit_limit IS NULL
                       OR current_balance IS NULL
                       OR billing_cycle_start IS NULL
                       OR billing_cycle_end IS NULL
                       OR account_status IS NULL OR account_status = ''
                       OR account_status NOT IN (
                           'ACTIVE', 'SUSPENDED', 'CLOSED')
                """).fetchdf()

            if len(rejected) > 0:
                quarantine_path = f"s3://{s3_bucket}/silver/quarantine/date={date_str}/rejected_accounts.parquet"
                with duckdb.connect() as conn:
                    configure_duckdb_s3(conn)
                    conn.register('rejected_df', rejected)
                    conn.execute(f"""
                        COPY (SELECT * FROM rejected_df)
                        TO '{quarantine_path}'
                        (FORMAT PARQUET)
                    """)
                records_rejected = len(rejected)
            else:
                records_rejected = 0

            run_log_buffer.add_entry(
                model_name="silver_accounts",
                layer="SILVER",
                started_at=silver_started,
                completed_at=silver_completed,
                status="SUCCESS",
                records_written=records_written,
                records_rejected=records_rejected,
                target_date=date_str
            )

        except Exception as e:
            run_log_buffer.add_entry(
                model_name="silver_accounts",
                layer="SILVER",
                status="FAILED",
                error_message=str(e),
                target_date=date_str
            )
            run_log_buffer.add_skipped("bronze_transactions", "BRONZE", target_date=date_str)
            run_log_buffer.add_skipped("silver_transactions", "SILVER", target_date=date_str)
            temp = current_date + timedelta(days=1)
            while temp <= end_date:
                temp_str = temp.strftime("%Y-%m-%d")
                run_log_buffer.add_skipped("bronze_accounts", "BRONZE", target_date=temp_str)
                run_log_buffer.add_skipped("silver_accounts", "SILVER", target_date=temp_str)
                run_log_buffer.add_skipped("bronze_transactions", "BRONZE", target_date=temp_str)
                run_log_buffer.add_skipped("silver_transactions", "SILVER", target_date=temp_str)
                temp += timedelta(days=1)
            run_log_buffer.add_skipped("gold_daily_summary", "GOLD")
            run_log_buffer.add_skipped("gold_weekly_account_summary", "GOLD")
            run_log_buffer.flush(fallback_path)
            print(f"ERROR on {date_str} (silver_accounts): {e}")
            return False

        try:
            bronze_started = datetime.utcnow().isoformat()
            result = load_bronze_transactions(date_str, run_id)
            bronze_completed = datetime.utcnow().isoformat()

            run_log_buffer.add_entry(
                model_name="bronze_transactions",
                layer="BRONZE",
                started_at=bronze_started,
                completed_at=bronze_completed,
                status="SUCCESS",
                records_written=result["records_written"],
                target_date=date_str
            )

        except Exception as e:
            run_log_buffer.add_entry(
                model_name="bronze_transactions",
                layer="BRONZE",
                status="FAILED",
                error_message=str(e),
                target_date=date_str
            )
            run_log_buffer.add_skipped("silver_transactions", "SILVER", target_date=date_str)
            temp = current_date + timedelta(days=1)
            while temp <= end_date:
                temp_str = temp.strftime("%Y-%m-%d")
                run_log_buffer.add_skipped("bronze_accounts", "BRONZE", target_date=temp_str)
                run_log_buffer.add_skipped("silver_accounts", "SILVER", target_date=temp_str)
                run_log_buffer.add_skipped("bronze_transactions", "BRONZE", target_date=temp_str)
                run_log_buffer.add_skipped("silver_transactions", "SILVER", target_date=temp_str)
                temp += timedelta(days=1)
            run_log_buffer.add_skipped("gold_daily_summary", "GOLD")
            run_log_buffer.add_skipped("gold_weekly_account_summary", "GOLD")
            run_log_buffer.flush(fallback_path)
            print(f"ERROR on {date_str} (bronze_transactions): {e}")
            return False

        try:
            silver_started = None
            silver_completed = None
            silver_finished = False
            for event in stream_dbt_layer("silver_transactions", run_id, {"target_date": date_str, "run_id": run_id, "s3_bucket": s3_bucket}):
                if event.get("event") == "start" and event.get("model") == "silver_transactions":
                    silver_started = event.get("started_at")
                elif event.get("event") == "finish" and event.get("model") == "silver_transactions":
                    if event.get("status") == "success":
                        silver_completed = event.get("completed_at")
                        silver_finished = True
                    else:
                        raise Exception(f"silver_transactions failed")
                elif event.get("event") == "exit":
                    if event.get("returncode") != 0:
                        raise Exception(f"dbt failed on silver_transactions")

            if not silver_finished:
                raise Exception(f"dbt silver_transactions: missing finish event")

            silver_txn_path = f"s3://{s3_bucket}/silver/transactions/date={date_str}/*.parquet"
            with duckdb.connect() as conn:
                configure_duckdb_s3(conn)
                records_written = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{silver_txn_path}')"
                ).fetchone()[0]

            quarantine_path = f"s3://{s3_bucket}/silver/quarantine/date={date_str}/rejected_transactions.parquet"
            try:
                with duckdb.connect() as conn:
                    configure_duckdb_s3(conn)
                    records_rejected = conn.execute(
                        f"SELECT COUNT(*) FROM read_parquet('{quarantine_path}')"
                    ).fetchone()[0]
            except Exception:
                records_rejected = 0

            run_log_buffer.add_entry(
                model_name="silver_transactions",
                layer="SILVER",
                started_at=silver_started,
                completed_at=silver_completed,
                status="SUCCESS",
                records_written=records_written,
                records_rejected=records_rejected,
                target_date=date_str
            )

        except Exception as e:
            run_log_buffer.add_entry(
                model_name="silver_transactions",
                layer="SILVER",
                status="FAILED",
                error_message=str(e),
                target_date=date_str
            )
            temp = current_date + timedelta(days=1)
            while temp <= end_date:
                temp_str = temp.strftime("%Y-%m-%d")
                run_log_buffer.add_skipped("bronze_accounts", "BRONZE", target_date=temp_str)
                run_log_buffer.add_skipped("silver_accounts", "SILVER", target_date=temp_str)
                run_log_buffer.add_skipped("bronze_transactions", "BRONZE", target_date=temp_str)
                run_log_buffer.add_skipped("silver_transactions", "SILVER", target_date=temp_str)
                temp += timedelta(days=1)
            run_log_buffer.add_skipped("gold_daily_summary", "GOLD")
            run_log_buffer.add_skipped("gold_weekly_account_summary", "GOLD")
            run_log_buffer.flush(fallback_path)
            print(f"ERROR on {date_str} (silver_transactions): {e}")
            return False

        current_date += timedelta(days=1)

    return True


def process_gold_step(uncomputed_weeks: list, run_id: str, run_log_buffer: RunLogBuffer, fallback_path: str) -> bool:
    s3_bucket = os.environ["S3_BUCKET"]

    try:
        target_weeks_json = json.dumps([
            {
                "week_start": str(w["week_start_date"]),
                "week_end": str(w["week_end_date"])
            }
            for w in uncomputed_weeks
        ])

        daily_started = None
        daily_completed = None
        daily_finished = False
        weekly_started = None
        weekly_completed = None
        weekly_finished = False

        # Create schema-only placeholder on S3 so glob() checks in gold models succeed on first run.
        daily_key = "gold/daily_summary/data.parquet"
        if not s3_key_exists(s3_bucket, daily_key):
            atomic_parquet_put(s3_bucket, daily_key, pd.DataFrame({
                'transaction_date': pd.Series(dtype='object'),
                'total_transactions': pd.Series(dtype='int64'),
                'total_signed_amount': pd.Series(dtype='float64'),
                'transactions_by_type': pd.Series(dtype='object'),
                'online_transactions': pd.Series(dtype='int64'),
                'instore_transactions': pd.Series(dtype='int64'),
                'total_unresolvable_transactions': pd.Series(dtype='int64'),
                'total_unresolvable_amount': pd.Series(dtype='float64'),
                '_computed_at': pd.Series(dtype='datetime64[ns]'),
                '_pipeline_run_id': pd.Series(dtype='str'),
                '_source_period_start': pd.Series(dtype='object'),
                '_source_period_end': pd.Series(dtype='object'),
            }))

        weekly_key = "gold/weekly_account_summary/data.parquet"
        if not s3_key_exists(s3_bucket, weekly_key):
            atomic_parquet_put(s3_bucket, weekly_key, pd.DataFrame({
                'week_start_date': pd.Series(dtype='object'),
                'week_end_date': pd.Series(dtype='object'),
                'account_id': pd.Series(dtype='str'),
                'total_purchases': pd.Series(dtype='int64'),
                'avg_purchase_amount': pd.Series(dtype='float64'),
                'total_payments': pd.Series(dtype='float64'),
                'total_fees': pd.Series(dtype='float64'),
                'total_interest': pd.Series(dtype='float64'),
                'closing_balance': pd.Series(dtype='float64'),
                '_computed_at': pd.Series(dtype='datetime64[ns]'),
                '_pipeline_run_id': pd.Series(dtype='str'),
            }))

        for event in stream_dbt_layer("tag:gold", run_id, {"run_id": run_id, "target_weeks": target_weeks_json, "s3_bucket": s3_bucket}):
            if event.get("event") == "start" and event.get("model") == "gold_daily_summary":
                daily_started = event.get("started_at")
            elif event.get("event") == "finish" and event.get("model") == "gold_daily_summary":
                if event.get("status") == "success":
                    daily_completed = event.get("completed_at")
                    daily_finished = True
                else:
                    raise Exception(f"gold_daily_summary failed")
            elif event.get("event") == "start" and event.get("model") == "gold_weekly_account_summary":
                weekly_started = event.get("started_at")
            elif event.get("event") == "finish" and event.get("model") == "gold_weekly_account_summary":
                if event.get("status") == "success":
                    weekly_completed = event.get("completed_at")
                    weekly_finished = True
                else:
                    raise Exception(f"gold_weekly_account_summary failed")
            elif event.get("event") == "exit":
                if event.get("returncode") != 0:
                    raise Exception(f"dbt gold layer failed with code {event.get('returncode')}")

        if not daily_finished or not weekly_finished:
            raise Exception(f"dbt gold layer: missing finish events for daily={daily_finished}, weekly={weekly_finished}")

        with duckdb.connect() as conn:
            configure_duckdb_s3(conn)
            daily_records = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('s3://{s3_bucket}/gold/daily_summary/*.parquet')"
            ).fetchone()[0]
            weekly_records = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('s3://{s3_bucket}/gold/weekly_account_summary/*.parquet')"
            ).fetchone()[0]

        run_log_buffer.add_entry(
            model_name="gold_daily_summary",
            layer="GOLD",
            started_at=daily_started,
            completed_at=daily_completed,
            status="SUCCESS",
            records_written=daily_records
        )

        run_log_buffer.add_entry(
            model_name="gold_weekly_account_summary",
            layer="GOLD",
            started_at=weekly_started,
            completed_at=weekly_completed,
            status="SUCCESS",
            records_written=weekly_records
        )

        return True

    except Exception as e:
        run_log_buffer.add_entry(
            model_name="gold_daily_summary",
            layer="GOLD",
            status="FAILED",
            error_message=str(e)
        )
        run_log_buffer.add_skipped("gold_weekly_account_summary", "GOLD")
        run_log_buffer.flush(fallback_path)
        print(f"ERROR in gold step: {e}")
        return False


def finalize_run(uncomputed_weeks: list, end_date: date, run_id: str, run_log_buffer: RunLogBuffer,
                 control_path: str, weekly_control_path: str, run_log_path: str, fallback_path: str,
                 mode: str = "historical") -> None:
    try:
        record_computed_weeks(weekly_control_path, uncomputed_weeks, run_id)
    except Exception as e:
        run_log_buffer.add_entry(
            model_name="DBT_COMPILE",
            layer="ORCHESTRATION",
            status="FAILED",
            error_message=f"record_computed_weeks failed: {e}"
        )
        run_log_buffer.flush(fallback_path)
        print(f"FINALIZE ERROR (weeks not recorded): {e}")
        raise

    try:
        advance_watermark(control_path, end_date, run_id)
    except Exception as e:
        run_log_buffer.add_entry(
            model_name="DBT_COMPILE",
            layer="ORCHESTRATION",
            status="FAILED",
            error_message=f"advance_watermark failed: {e}"
        )
        run_log_buffer.flush(fallback_path)
        print(f"FINALIZE ERROR (watermark not advanced): {e}")
        raise

    run_log_buffer.flush(run_log_path)
    print(f"{mode.capitalize()} pipeline completed successfully.")


if __name__ == "__main__":
    args = parse_args()
    run_id = generate_run_id()

    S3_BUCKET = os.environ["S3_BUCKET"]
    initialise_control_plane(S3_BUCKET)

    CONTROL_PATH        = f"s3://{S3_BUCKET}/pipeline/control.parquet"
    WEEKLY_CONTROL_PATH = f"s3://{S3_BUCKET}/pipeline/gold_weekly_control.parquet"
    RUN_LOG_PATH        = f"s3://{S3_BUCKET}/pipeline/run_log.parquet"
    FALLBACK_PATH       = "/tmp/pipeline_runlog_fallback.jsonl"
    SILVER_PATH         = f"s3://{S3_BUCKET}/silver/transactions"

    if args.mode == "historical":
        try:
            start_date, end_date = validate_historical_args_and_files(args.start_date, args.end_date)
            run_log_buffer = RunLogBuffer(run_id, "historical")

            try:
                unlogged_run_id = run_log_buffer.check_unlogged_run(CONTROL_PATH, RUN_LOG_PATH)
                if unlogged_run_id:
                    run_log_buffer.write_unlogged_run_row(unlogged_run_id)
                    print(f"Recovered unlogged run: {unlogged_run_id}")
            except Exception as e:
                run_log_buffer.add_entry(
                    model_name="DBT_COMPILE",
                    layer="ORCHESTRATION",
                    status="FAILED",
                    error_message=f"Control plane read failed: {e}"
                )
                run_log_buffer.flush(FALLBACK_PATH)
                print(f"CONTROL PLANE ERROR: {e}")
                sys.exit(1)

            if not process_transaction_codes_step(run_id, run_log_buffer, FALLBACK_PATH):
                sys.exit(1)

            if not process_date_sequence(start_date, end_date, run_id, run_log_buffer, FALLBACK_PATH):
                sys.exit(1)

            try:
                uncomputed_weeks = get_uncomputed_weeks(SILVER_PATH, WEEKLY_CONTROL_PATH)
                uncomputed_weeks = [w for w in uncomputed_weeks if w['week_end_date'] <= end_date]
            except Exception as e:
                run_log_buffer.add_entry(
                    model_name="DBT_COMPILE",
                    layer="ORCHESTRATION",
                    status="FAILED",
                    error_message=f"Control plane read failed: {e}"
                )
                run_log_buffer.flush(FALLBACK_PATH)
                print(f"CONTROL PLANE ERROR: {e}")
                sys.exit(1)

            if not process_gold_step(uncomputed_weeks, run_id, run_log_buffer, FALLBACK_PATH):
                sys.exit(1)

            finalize_run(uncomputed_weeks, end_date, run_id, run_log_buffer,
                         CONTROL_PATH, WEEKLY_CONTROL_PATH, RUN_LOG_PATH, FALLBACK_PATH, mode="historical")

        except SystemExit:
            raise
        except Exception as e:
            print(f"FATAL: {e}")
            sys.exit(1)

    elif args.mode == "incremental":
        try:
            run_log_buffer = RunLogBuffer(run_id, "incremental")

            try:
                unlogged_run_id = run_log_buffer.check_unlogged_run(CONTROL_PATH, RUN_LOG_PATH)
                if unlogged_run_id:
                    run_log_buffer.write_unlogged_run_row(unlogged_run_id)
                    print(f"Recovered unlogged run: {unlogged_run_id}")
            except Exception as e:
                run_log_buffer.add_entry(
                    model_name="DBT_COMPILE",
                    layer="ORCHESTRATION",
                    status="FAILED",
                    error_message=f"Control plane read failed: {e}"
                )
                run_log_buffer.flush(FALLBACK_PATH)
                print(f"CONTROL PLANE ERROR: {e}")
                sys.exit(1)

            try:
                watermark = get_watermark(CONTROL_PATH)
                if watermark is None:
                    print("ERROR: No watermark found. Run historical mode first.")
                    sys.exit(1)
                next_date = watermark + timedelta(days=1)
                end_date = next_date
            except Exception as e:
                run_log_buffer.add_entry(
                    model_name="DBT_COMPILE",
                    layer="ORCHESTRATION",
                    status="FAILED",
                    error_message=f"Watermark read failed: {e}"
                )
                run_log_buffer.flush(FALLBACK_PATH)
                print(f"WATERMARK ERROR: {e}")
                sys.exit(1)

            print(f"Incremental run for date: {next_date}")

            if not process_transaction_codes_step(run_id, run_log_buffer, FALLBACK_PATH):
                sys.exit(1)

            if not process_date_sequence(next_date, end_date, run_id, run_log_buffer, FALLBACK_PATH):
                sys.exit(1)

            try:
                uncomputed_weeks = get_uncomputed_weeks(SILVER_PATH, WEEKLY_CONTROL_PATH)
                uncomputed_weeks = [w for w in uncomputed_weeks if w['week_end_date'] <= end_date]
            except Exception as e:
                run_log_buffer.add_entry(
                    model_name="DBT_COMPILE",
                    layer="ORCHESTRATION",
                    status="FAILED",
                    error_message=f"Control plane read failed: {e}"
                )
                run_log_buffer.flush(FALLBACK_PATH)
                print(f"CONTROL PLANE ERROR: {e}")
                sys.exit(1)

            if not process_gold_step(uncomputed_weeks, run_id, run_log_buffer, FALLBACK_PATH):
                sys.exit(1)

            finalize_run(uncomputed_weeks, end_date, run_id, run_log_buffer,
                         CONTROL_PATH, WEEKLY_CONTROL_PATH, RUN_LOG_PATH, FALLBACK_PATH, mode="incremental")

        except SystemExit:
            raise
        except Exception as e:
            print(f"FATAL: {e}")
            sys.exit(1)
