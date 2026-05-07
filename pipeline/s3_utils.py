#!/usr/bin/env python3
import io
import os
import boto3
from botocore.exceptions import ClientError
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def configure_duckdb_s3(conn: duckdb.DuckDBPyConnection) -> None:
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    try:
        conn.execute("LOAD httpfs;")
    except Exception:
        conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_region='{region}';")

    # Inject credentials from boto3's chain (env vars → ~/.aws → EC2 IMDS).
    # DuckDB 0.10.0 httpfs does not reliably resolve IMDS inside Docker; boto3 does.
    session = boto3.Session(region_name=region)
    creds = session.get_credentials()
    if creds:
        frozen = creds.get_frozen_credentials()
        conn.execute(f"SET s3_access_key_id='{frozen.access_key}';")
        conn.execute(f"SET s3_secret_access_key='{frozen.secret_key}';")
        if frozen.token:
            conn.execute(f"SET s3_session_token='{frozen.token}';")


def parse_s3_uri(uri: str) -> tuple:
    """Parse 's3://bucket/key' -> (bucket, key)."""
    assert uri.startswith("s3://"), f"Not an S3 URI: {uri}"
    without_scheme = uri[5:]
    slash_pos = without_scheme.find("/")
    if slash_pos == -1:
        return without_scheme, ""
    return without_scheme[:slash_pos], without_scheme[slash_pos + 1:]


def s3_key_exists(bucket: str, key: str) -> bool:
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name=region)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def atomic_parquet_put(bucket: str, key: str, df: pd.DataFrame) -> None:
    """Serialize DataFrame to Parquet bytes and write to S3 in a single PutObject call."""
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name=region)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
