"""
Snowpark WIDF Lambda Loader
===========================
AWS Lambda function that loads data from S3 to Snowflake using
Workload Identity Federation (WIDF) for secure, KEYLESS authentication.

🔑 The Key Point: NO secrets, NO passwords, NO key pairs!
   Just IAM role trust between AWS and Snowflake.

Reference: https://docs.snowflake.com/en/user-guide/workload-identity-federation
"""

from __future__ import annotations

import json
import logging
import os
import urllib.parse
from typing import Any, TypedDict

import boto3
import pandas as pd
from snowflake.snowpark import Session


class S3ObjectInfo(TypedDict):
    """S3 object details from event."""

    bucket: str
    key: str


class FileResult(TypedDict, total=False):
    """Result of processing a single file."""

    file: str
    records: int
    status: str
    error: str


class LambdaResponse(TypedDict):
    """Lambda function response."""

    statusCode: int
    body: str


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_snowpark_session() -> Session:
    """
    Create a Snowpark session using Workload Identity Federation.

    🔑 THE MAGIC: The 'WORKLOAD_IDENTITY' authenticator tells Snowpark
    to use the Lambda's IAM role for authentication with Snowflake.

    No passwords. No secrets. No key pairs. Just IAM trust!
    """
    connection_params: dict[str, str | int] = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "authenticator": "WORKLOAD_IDENTITY",  # 🔑 WIDF - Keyless auth!
        "workload_identity_provider": "AWS",  # 🔑 Using AWS IAM for identity
        "user": os.environ.get("SNOWFLAKE_USER", "LAMBDA_LOADER_BOT"),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "WIDF_DEMO_DB"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "role": os.environ.get("SNOWFLAKE_ROLE", "WIDF_DEMO_ROLE"),
    }

    logger.info("🔑 Connecting to Snowflake using WORKLOAD_IDENTITY (keyless!)")
    logger.info(f"   Account: {connection_params['account']}")
    logger.info(f"   User: {connection_params['user']}")

    return Session.builder.configs(connection_params).create()


def parse_s3_event(event: dict[str, Any]) -> list[S3ObjectInfo]:
    """Parse S3 event notification to extract bucket and object details."""
    records: list[S3ObjectInfo] = []

    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            continue

        s3_info: dict[str, Any] = record.get("s3", {})
        bucket: str | None = s3_info.get("bucket", {}).get("name")
        key: str = urllib.parse.unquote_plus(s3_info.get("object", {}).get("key", ""))

        if bucket and key:
            records.append(S3ObjectInfo(bucket=bucket, key=key))
            logger.info(f"📦 Found S3 object: s3://{bucket}/{key}")

    return records


def read_json_from_s3(bucket: str, key: str) -> list[dict[str, Any]]:
    """Read JSON file from S3 and return as list of records."""
    s3_client = boto3.client("s3")

    logger.info(f"📖 Reading: s3://{bucket}/{key}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content: str = response["Body"].read().decode("utf-8")

    # Support both JSON array and newline-delimited JSON
    try:
        data: Any = json.loads(content)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        return [
            json.loads(line) for line in content.strip().split("\n") if line.strip()
        ]


def load_to_snowflake(
    session: Session, records: list[dict[str, Any]], source_file: str
) -> int:
    """
    Load records into Snowflake RAW_DATA table using bulk insert.

    Uses write_pandas() for efficient bulk loading as recommended in:
    https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas
    """
    # Ensure table exists
    session.sql("""
        CREATE TABLE IF NOT EXISTS RAW_DATA (
            id              INTEGER AUTOINCREMENT,
            source_file     VARCHAR,
            payload         VARIANT,
            loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            loaded_by       VARCHAR DEFAULT CURRENT_USER()
        )
    """).collect()

    # Prepare data as pandas DataFrame for bulk insert
    df = pd.DataFrame(
        {
            "SOURCE_FILE": [source_file] * len(records),
            "PAYLOAD": [json.dumps(record) for record in records],
        }
    )

    # Use write_pandas for efficient bulk loading
    # This is much faster than row-by-row inserts
    session.write_pandas(
        df,
        table_name="RAW_DATA_STAGING",
        auto_create_table=True,
        overwrite=True,
    )

    # Insert from staging with PARSE_JSON for VARIANT conversion
    session.sql("""
        INSERT INTO RAW_DATA (source_file, payload)
        SELECT SOURCE_FILE, PARSE_JSON(PAYLOAD)
        FROM RAW_DATA_STAGING
    """).collect()

    # Clean up staging table
    session.sql("DROP TABLE IF EXISTS RAW_DATA_STAGING").collect()

    logger.info(f"✅ Loaded {len(records)} records from {source_file}")
    return len(records)


def lambda_handler(event: dict[str, Any], context: Any) -> LambdaResponse:
    """
    AWS Lambda handler for S3 event-triggered data loading.

    This demonstrates Snowflake WIDF (Workload Identity Federation):
    - Lambda assumes an IAM role
    - Snowflake trusts that IAM role
    - No secrets stored anywhere!
    """
    logger.info("=" * 60)
    logger.info("🚀 Snowpark WIDF Lambda Loader")
    logger.info("   Keyless ETL: S3 → Snowflake")
    logger.info("=" * 60)

    # Parse S3 event
    s3_objects: list[S3ObjectInfo] = parse_s3_event(event)

    if not s3_objects:
        return LambdaResponse(
            statusCode=200,
            body=json.dumps({"message": "No files to process"}),
        )

    session: Session | None = None
    total_records: int = 0
    results: list[FileResult] = []

    try:
        # 🔑 Connect using WIDF - no secrets!
        session = get_snowpark_session()

        # Verify connection
        current_user: str = str(session.sql("SELECT CURRENT_USER()").collect()[0][0])
        current_role: str = str(session.sql("SELECT CURRENT_ROLE()").collect()[0][0])
        logger.info(f"✅ Connected as: {current_user} (role: {current_role})")
        logger.info("🔑 Authentication: WORKLOAD_IDENTITY (keyless!)")

        # Process each file
        for s3_obj in s3_objects:
            source: str = f"s3://{s3_obj['bucket']}/{s3_obj['key']}"
            try:
                records: list[dict[str, Any]] = read_json_from_s3(
                    s3_obj["bucket"], s3_obj["key"]
                )
                count: int = load_to_snowflake(session, records, source)
                total_records += count
                results.append(
                    FileResult(file=s3_obj["key"], records=count, status="success")
                )
            except Exception as e:
                logger.error(f"❌ Failed: {s3_obj['key']} - {e}")
                results.append(
                    FileResult(file=s3_obj["key"], error=str(e), status="failed")
                )

        return LambdaResponse(
            statusCode=200,
            body=json.dumps(
                {
                    "message": "Data loaded via WIDF (keyless!)",
                    "authenticated_as": current_user,
                    "total_records": total_records,
                    "files": results,
                }
            ),
        )

    except Exception as e:
        logger.error(f"❌ Lambda failed: {e}")
        return LambdaResponse(statusCode=500, body=json.dumps({"error": str(e)}))

    finally:
        if session:
            session.close()


def main() -> None:
    """Local testing."""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║           Snowpark WIDF Lambda Loader                            ║
║           Keyless ETL: AWS Lambda → Snowflake                    ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  🔑 THE KEY POINT:                                               ║
║                                                                  ║
║  This Lambda authenticates to Snowflake using                    ║
║  WORKLOAD IDENTITY FEDERATION (WIDF)                             ║
║                                                                  ║
║  • NO passwords                                                  ║
║  • NO secret keys                                                ║
║  • NO key pairs                                                  ║
║  • NO Secrets Manager                                            ║
║                                                                  ║
║  Just IAM role trust between AWS and Snowflake!                  ║
║                                                                  ║
╠══════════════════════════════════════════════════════════════════╣
║  Connection Config:                                              ║
║                                                                  ║
║    authenticator = "WORKLOAD_IDENTITY"  ← The magic!             ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    main()
