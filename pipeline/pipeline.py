import argparse
import os
import sys
import uuid
import json
import duckdb
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


def generate_run_id() -> str:
    return str(uuid.uuid4())


def initialise_control_plane(data_dir: str) -> None:
    control_path = f"{data_dir}/pipeline/control.parquet"
    gold_weekly_path = f"{data_dir}/pipeline/gold_weekly_control.parquet"
    fallback_path = f"{data_dir}/pipeline/pipeline_runlog_fallback.jsonl"

    os.makedirs(f"{data_dir}/pipeline", exist_ok=True)

    control_schema = "(last_processed_date DATE, updated_at TIMESTAMP, updated_by_run_id STRING)"
    gold_weekly_schema = "(week_start_date DATE, week_end_date DATE, computed_at TIMESTAMP, computed_by_run_id STRING)"

    for target_path, schema, label in [
        (control_path, control_schema, "control.parquet"),
        (gold_weekly_path, gold_weekly_schema, "gold_weekly_control.parquet"),
    ]:
        if not os.path.exists(target_path):
            try:
                conn = duckdb.connect()
                conn.execute(f"CREATE TABLE temp_init {schema}")
                conn.execute(f"COPY (SELECT * FROM temp_init) TO '{target_path}' (FORMAT PARQUET)")
                conn.close()
                print(f"Initialised {label}.")
            except Exception as e:
                with open(fallback_path, "a") as f:
                    f.write(f'{{"timestamp": "{datetime.utcnow().isoformat()}", "run_id": "N/A", "model_name": "DBT_COMPILE", "layer": "ORCHESTRATION", "status": "FAILED", "error_message": "{str(e)}"}}\n')
                print(f"Error initialising {label}: {e}")
                raise SystemExit(1)
        else:
            try:
                conn = duckdb.connect()
                conn.execute(f"SELECT COUNT(*) FROM read_parquet('{target_path}')")
                conn.close()
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
        for event in stream_dbt_layer("silver_transaction_codes", run_id, {"run_id": run_id}):
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

        os.makedirs('/app/data/silver/transaction_codes', exist_ok=True)

        with duckdb.connect() as conn:
            conn.execute("""
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
                        '/app/data/bronze/transaction_codes/data.parquet'
                    )
                ) TO '/app/data/silver/transaction_codes/data.parquet'
                (FORMAT PARQUET)
            """)

        with duckdb.connect() as conn:
            records = conn.execute(
                "SELECT COUNT(*) FROM read_parquet('/app/data/silver/transaction_codes/data.parquet')"
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
            os.makedirs('/app/data/silver/accounts', exist_ok=True)
            os.makedirs(f"/app/data/silver/transactions/date={date_str}", exist_ok=True)
            os.makedirs(f"/app/data/silver/quarantine/date={date_str}", exist_ok=True)

            # Create placeholder parquet file with correct schema if it doesn't exist.
            # This allows Jinja2's glob() check in silver_accounts.sql to succeed on first run.
            placeholder_path = '/app/data/silver/accounts/data.parquet'
            if not os.path.exists(placeholder_path):
                try:
                    with duckdb.connect() as conn:
                        conn.execute(f"""
                            CREATE TABLE placeholder_schema (
                                account_id VARCHAR,
                                customer_name VARCHAR,
                                account_status VARCHAR,
                                credit_limit DECIMAL,
                                current_balance DECIMAL,
                                open_date DATE,
                                billing_cycle_start DATE,
                                billing_cycle_end DATE,
                                _source_file VARCHAR,
                                _bronze_ingested_at TIMESTAMP,
                                _pipeline_run_id VARCHAR,
                                _record_valid_from TIMESTAMP
                            )
                        """)
                        conn.execute(f"COPY (SELECT * FROM placeholder_schema) TO '{placeholder_path}' (FORMAT PARQUET)")
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
            for event in stream_dbt_layer("silver_accounts", run_id, {"target_date": date_str, "run_id": run_id}):
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

            with duckdb.connect() as conn:
                records_written = conn.execute(
                    "SELECT COUNT(*) FROM read_parquet('/app/data/silver/accounts/data.parquet')"
                ).fetchone()[0]

            with duckdb.connect() as conn:
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
                        '/app/data/bronze/accounts/date={date_str}/data.parquet'
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
                os.makedirs(
                    f'/app/data/silver/quarantine/date={date_str}',
                    exist_ok=True
                )
                quarantine_path = (
                    f'/app/data/silver/quarantine/date={date_str}'
                    f'/rejected_accounts.parquet'
                )
                with duckdb.connect() as conn:
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
            for event in stream_dbt_layer("silver_transactions", run_id, {"target_date": date_str, "run_id": run_id}):
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

            with duckdb.connect() as conn:
                records_written = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/date={date_str}/*.parquet')"
                ).fetchone()[0]

            quarantine_path = f'/app/data/silver/quarantine/date={date_str}/rejected_transactions.parquet'
            if os.path.exists(quarantine_path):
                with duckdb.connect() as conn:
                    records_rejected = conn.execute(
                        f"SELECT COUNT(*) FROM read_parquet('{quarantine_path}')"
                    ).fetchone()[0]
            else:
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

        os.makedirs('/app/data/gold/daily_summary', exist_ok=True)
        os.makedirs('/app/data/gold/weekly_account_summary', exist_ok=True)

        daily_placeholder = '/app/data/gold/daily_summary/data.parquet'
        if not os.path.exists(daily_placeholder):
            with duckdb.connect() as conn:
                conn.execute("""CREATE TABLE gd1 (
                    transaction_date DATE,
                    total_transactions BIGINT,
                    total_signed_amount DOUBLE,
                    transactions_by_type JSON,
                    online_transactions BIGINT,
                    instore_transactions BIGINT,
                    total_unresolvable_transactions BIGINT,
                    total_unresolvable_amount DOUBLE,
                    _computed_at TIMESTAMP,
                    _pipeline_run_id VARCHAR,
                    _source_period_start DATE,
                    _source_period_end DATE
                )""")
                conn.execute(f"COPY (SELECT * FROM gd1) TO '{daily_placeholder}' (FORMAT PARQUET)")

        weekly_placeholder = '/app/data/gold/weekly_account_summary/data.parquet'
        if not os.path.exists(weekly_placeholder):
            with duckdb.connect() as conn:
                conn.execute("""CREATE TABLE gw1 (
                    week_start_date DATE,
                    week_end_date DATE,
                    account_id VARCHAR,
                    total_purchases BIGINT,
                    avg_purchase_amount DOUBLE,
                    total_payments DOUBLE,
                    total_fees DOUBLE,
                    total_interest DOUBLE,
                    closing_balance DOUBLE,
                    _computed_at TIMESTAMP,
                    _pipeline_run_id VARCHAR
                )""")
                conn.execute(f"COPY (SELECT * FROM gw1) TO '{weekly_placeholder}' (FORMAT PARQUET)")

        for event in stream_dbt_layer("tag:gold", run_id, {"run_id": run_id, "target_weeks": target_weeks_json}):
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
            daily_records = conn.execute(
                "SELECT COUNT(*) FROM read_parquet('/app/data/gold/daily_summary/*.parquet')"
            ).fetchone()[0]
            weekly_records = conn.execute(
                "SELECT COUNT(*) FROM read_parquet('/app/data/gold/weekly_account_summary/*.parquet')"
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
    initialise_control_plane("/app/data")

    CONTROL_PATH = "/app/data/pipeline/control.parquet"
    WEEKLY_CONTROL_PATH = "/app/data/pipeline/gold_weekly_control.parquet"
    RUN_LOG_PATH = "/app/data/pipeline/run_log.parquet"
    FALLBACK_PATH = "/app/data/pipeline/pipeline_runlog_fallback.jsonl"
    SILVER_PATH = "/app/data/silver/transactions"

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

            finalize_run(uncomputed_weeks, end_date, run_id, run_log_buffer, CONTROL_PATH, WEEKLY_CONTROL_PATH, RUN_LOG_PATH, FALLBACK_PATH, mode="historical")

        except SystemExit:
            raise
        except Exception as e:
            print(f"ORCHESTRATION ERROR: {e}")
            try:
                run_log_buffer.add_orchestration_failure(str(e))
                run_log_buffer.flush(FALLBACK_PATH)
            except:
                pass
            sys.exit(1)

    elif args.mode == "incremental":
        run_log_buffer = None
        try:
            watermark = get_watermark(CONTROL_PATH)
            if watermark is None:
                print("ERROR: Historical pipeline must be run first.")
                sys.exit(1)

            next_date = watermark + timedelta(days=1)
            next_date_str = next_date.strftime("%Y-%m-%d")

            accounts_file = f"/app/source/accounts_{next_date_str}.csv"
            transactions_file = f"/app/source/transactions_{next_date_str}.csv"

            if not os.path.exists(accounts_file) or not os.path.exists(transactions_file):
                print(f"No source files for {next_date_str} — no-op exit.")
                sys.exit(0)

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

            if not process_date_sequence(next_date, next_date, run_id, run_log_buffer, FALLBACK_PATH):
                run_log_buffer.add_skipped("gold_daily_summary", "GOLD", target_date=next_date_str)
                run_log_buffer.add_skipped("gold_weekly_account_summary", "GOLD", target_date=next_date_str)
                sys.exit(1)

            try:
                uncomputed_weeks = get_uncomputed_weeks(SILVER_PATH, WEEKLY_CONTROL_PATH)
                uncomputed_weeks = [w for w in uncomputed_weeks if w['week_end_date'] <= next_date]
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

            finalize_run(uncomputed_weeks, next_date, run_id, run_log_buffer, CONTROL_PATH, WEEKLY_CONTROL_PATH, RUN_LOG_PATH, FALLBACK_PATH, mode="incremental")

        except SystemExit:
            raise
        except Exception as e:
            print(f"ORCHESTRATION ERROR: {e}")
            if run_log_buffer is not None:
                try:
                    run_log_buffer.add_orchestration_failure(str(e))
                    run_log_buffer.flush(FALLBACK_PATH)
                except Exception as log_err:
                    print(f"WARNING: Could not flush run log: {log_err}")
            sys.exit(1)
