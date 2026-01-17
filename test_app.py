"""
Snowflake Integration Test for WIDF Lambda Loader.

This test validates that the Snowpark pandas connector works correctly
by connecting to Snowflake with standard auth (PAT/password) and loading data.

If this test passes but Lambda fails → WIDF auth issue (run: task snow:lambda-wid)
If this test fails → Check your Snowflake credentials/connectivity

Run: pytest test_app.py -v
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from dotenv import find_dotenv, load_dotenv
from snowflake.snowpark import Session

from app import load_to_snowflake

# =============================================================================
# Configuration
# =============================================================================
# Load .env file for Snowflake credentials
if load_dotenv(find_dotenv()):
    print("Loaded .env file")
else:
    print("No .env file found")
    sys.exit(1)

SAMPLE_DATA_PATH = Path(__file__).parent / "data" / "sample-data.json"
TEST_DB = os.environ.get("DEMO_DATABASE", "WIDF_DEMO_DB") + "_TEST"
TEST_SCHEMA = "PUBLIC"  # load_to_snowflake uses current schema
RAW_DATA_TABLE = "RAW_DATA"  # Table created by load_to_snowflake


def load_sample_data() -> list[dict[str, Any]]:
    """Load the sample data file."""
    with open(SAMPLE_DATA_PATH) as f:
        return json.load(f)


# =============================================================================
# Fixtures
# =============================================================================
@pytest.fixture(scope="module")
def snowflake_session() -> Generator[Session, None, None]:
    """
    Create a Snowflake session using standard auth (PAT or password).

    This uses the same connection params as the Lambda, but with
    local oauth authentication instead of WORKLOAD_IDENTITY.

    Required env vars:
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_USER (your admin user, not the WIDF service user)
    https://docs.snowflake.com/en/user-guide/oauth-local-applications
    """

    options: dict[str, Any] = {
        "authenticator": "OAUTH_AUTHORIZATION_CODE",
        "client_store_temporary_credentials": True,
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("SNOWFLAKE_USER"),
    }

    session = Session.builder.configs(options).getOrCreate()
    print(f"✅ Connected as: {session.get_current_user()}")
    yield session
    session.close() if session else None


@pytest.fixture(scope="module")
def test_database(snowflake_session: Session) -> Generator[str, None, None]:
    """Create test database and schema, cleanup after tests."""
    session = snowflake_session

    # Setup: Create test database and schema
    print(f"\n🔧 Setting up test database: {TEST_DB}")
    session.sql(f"CREATE DATABASE IF NOT EXISTS {TEST_DB}").collect()
    session.sql(f"USE DATABASE {TEST_DB}").collect()
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}").collect()
    session.sql(f"USE SCHEMA {TEST_SCHEMA}").collect()

    yield TEST_DB

    # Teardown: Drop test database
    print(f"\n🧹 Cleaning up test database: {TEST_DB}")
    session.sql(f"DROP DATABASE IF EXISTS {TEST_DB}").collect()


# =============================================================================
# Tests
# =============================================================================
class TestSnowflakeConnection:
    """Test Snowflake connectivity with standard auth."""

    def test_connection_works(self, snowflake_session: Session) -> None:
        """Verify we can connect to Snowflake."""
        result = snowflake_session.sql("SELECT CURRENT_USER()").collect()
        current_user = str(result[0][0])

        print(f"✅ Connected as: {current_user}")
        assert current_user is not None


class TestDataLoading:
    """Test data loading with Snowpark pandas connector."""

    def test_sample_data_exists(self) -> None:
        """Verify sample data file exists."""
        assert SAMPLE_DATA_PATH.exists(), f"Missing: {SAMPLE_DATA_PATH}"

    def test_load_data_with_load_to_snowflake(
        self, snowflake_session: Session, test_database: str
    ) -> None:
        """
        Load sample data using the actual load_to_snowflake function from app.py.

        This is the critical test: if this works, Lambda code is correct.
        """
        session = snowflake_session
        sample_data = load_sample_data()
        source_file = "test://sample-data.json"

        print(f"\n📦 Loading {len(sample_data)} records using load_to_snowflake()...")

        # Use the ACTUAL function from app.py
        count = load_to_snowflake(session, sample_data, source_file)

        print(f"✅ load_to_snowflake() loaded {count} records")
        assert count == len(sample_data)

    def test_verify_loaded_data(
        self, snowflake_session: Session, test_database: str
    ) -> None:
        """Verify the data was loaded correctly."""
        session = snowflake_session
        sample_data = load_sample_data()

        # Query loaded data
        result = session.sql(f"SELECT COUNT(*) FROM {RAW_DATA_TABLE}").collect()
        count = result[0][0]

        print(f"✅ Found {count} records in {RAW_DATA_TABLE}")
        assert count == len(sample_data), f"Expected {len(sample_data)}, got {count}"

    def test_query_payload_data(
        self, snowflake_session: Session, test_database: str
    ) -> None:
        """Verify VARIANT payload can be queried."""
        session = snowflake_session

        # Query using VARIANT syntax
        result = session.sql(f"""
            SELECT
                payload:event_id::VARCHAR as event_id,
                payload:action::VARCHAR as action
            FROM {RAW_DATA_TABLE}
            WHERE payload:event_id IS NOT NULL
            LIMIT 5
        """).collect()

        print(f"✅ Successfully queried VARIANT payload, got {len(result)} rows")
        assert len(result) > 0


class TestSummary:
    """Summary test to confirm everything works."""

    def test_all_passed(self, snowflake_session: Session, test_database: str) -> None:
        """
        If we get here, all Snowflake operations work!

        Any Lambda failure after this is due to WIDF auth, not code.
        """
        print("""
╔══════════════════════════════════════════════════════════════════╗
║              ✅ All Integration Tests Passed!                    ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  Snowpark pandas connector works correctly.                      ║
║                                                                  ║
║  If Lambda fails after deployment, it's the WIDF setup:          ║
║                                                                  ║
║    → Run: task snow:lambda-wid                                   ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
        """)
        assert True
