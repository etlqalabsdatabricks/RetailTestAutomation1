import sys
import os
sys.dont_write_bytecode = True

import pytest
import logging
from pyspark.sql import SparkSession

# Import configuration constants (table names, CSV path, etc.)
from test_configuration.config import RAW_CSV_PATH

# Ensure directories exist
os.makedirs("logs", exist_ok=True)
os.makedirs("reports", exist_ok=True)
os.makedirs("differences", exist_ok=True)

# Configure shared logger — all test files and utility functions write to this file.
logging.basicConfig(
    filename="logs/test_results.log",
    filemode='w',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Log test results automatically after each test."""
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        logger.info(f"{report.outcome.upper()} - {item.name}")


@pytest.fixture(scope="session")
def spark():
    """Provides a SparkSession for pytest tests."""
    logger.info("Creating SparkSession")
    spark_session = SparkSession.getActiveSession()
    if spark_session is None:
        from databricks.connect import DatabricksSession
        builder = DatabricksSession.builder
        host = os.environ.get("DATABRICKS_HOST")
        token = os.environ.get("DATABRICKS_TOKEN")
        if host and token:
            builder = builder.host(host).token(token).serverless(True)
        spark_session = builder.getOrCreate()
    logger.info("SparkSession created successfully")
    return spark_session


@pytest.fixture(scope="session")
def raw_csv_df(spark):
    """
    Load the raw CSV file once per test session.
    Uses header=True (first row is column names) and inferSchema=True.
    Shared across all test files that need the raw source data.
    """
    logger.info(f"Loading raw CSV from: {RAW_CSV_PATH}")
    df = spark.read.csv(RAW_CSV_PATH, header=True, inferSchema=True)
    logger.info(f"Raw CSV loaded: {df.count()} rows, {len(df.columns)} columns")
    return df