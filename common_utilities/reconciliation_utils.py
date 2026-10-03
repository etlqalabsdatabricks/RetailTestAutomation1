# --------------------------------------------------------------------------------------
# reconciliation_utils.py
# --------------------------------------------------------------------------------------
# Shared utility functions for data reconciliation tests across the retail pipeline.
# These functions encapsulate common patterns (row counts, column checks, aggregate
# comparisons, distinct-value matching, no-data-loss checks) so test files stay
# concise and focused on *what* they verify rather than *how*.
#
# Usage:  from common_utilities.reconciliation_utils import *
# --------------------------------------------------------------------------------------

import logging

# Shared logger — conftest.py configures the root logger with basicConfig,
# so every module that calls getLogger with the same name writes to the same log file.
logger = logging.getLogger("reconciliation")


# ======================================================================================
# LOGGING HELPERS
# ======================================================================================

def log_start(test_name):
    """Log the start of a test case."""
    logger.info(f"Test case: {test_name} started")


def log_end(test_name):
    """Log the completion of a test case."""
    logger.info(f"Test case: {test_name} completed")


# ======================================================================================
# ROW COUNT HELPERS
# ======================================================================================

def get_count(spark, table_name):
    """Return the row count of a table, logging the result."""
    count = spark.table(table_name).count()
    logger.info(f"{table_name} row count: {count}")
    return count


def assert_not_empty(spark, table_name, label):
    """Assert a table has more than 0 rows."""
    count = get_count(spark, table_name)
    assert count > 0, f"{label} table is empty (0 rows)"


def assert_counts_match(spark, table_a, table_b, label_a, label_b):
    """Assert two tables have the same row count."""
    count_a = get_count(spark, table_a)
    count_b = get_count(spark, table_b)
    logger.info(f"{label_a} count: {count_a}, {label_b} count: {count_b}")
    assert count_a == count_b, (
        f"Row count mismatch: {label_a}={count_a}, {label_b}={count_b}"
    )


# ======================================================================================
# SCHEMA / COLUMN HELPERS
# ======================================================================================

def assert_columns_exist(spark, table_name, expected_columns):
    """
    Assert every column in *expected_columns* is present in the table schema.
    *expected_columns* is a list of column name strings.
    """
    actual = set(spark.table(table_name).columns)
    logger.info(f"{table_name} columns: {sorted(actual)}")
    for col in expected_columns:
        assert col in actual, f"Expected column '{col}' not found in {table_name}"


def assert_columns_exist_in_df(df, expected_columns):
    """
    Assert every column in *expected_columns* is present in the DataFrame.
    Used for DataFrames that are not backed by a named table (e.g., raw CSV).
    """
    actual = set(df.columns)
    for col in expected_columns:
        assert col in actual, f"Expected column '{col}' not found in DataFrame (columns: {sorted(actual)})"


# ======================================================================================
# AGGREGATE HELPERS
# ======================================================================================

def get_sum(spark, table_name, column):
    """
    Return COALESCE(SUM(column), 0) from a table, logging the result.
    COALESCE handles empty tables (returns 0 instead of NULL).
    """
    result = spark.sql(
        f"SELECT COALESCE(SUM({column}), 0) AS total FROM {table_name}"
    ).collect()[0]["total"]
    logger.info(f"{table_name} SUM({column}) = {result}")
    return result


def assert_sums_match(spark, table_a, col_a, table_b, col_b, label_a, label_b, tolerance=0.0):
    """
    Assert SUM(col_a) from table_a equals SUM(col_b) from table_b.
    If *tolerance* > 0, uses abs(a - b) < tolerance for floating-point comparisons.
    """
    sum_a = get_sum(spark, table_a, col_a)
    sum_b = get_sum(spark, table_b, col_b)
    logger.info(f"{label_a} total {col_a}: {sum_a}, {label_b} total {col_b}: {sum_b}")
    if tolerance > 0:
        assert abs(sum_a - sum_b) < tolerance, (
            f"Total {col_a} mismatch: {label_a}={sum_a}, {label_b}={sum_b} (tolerance={tolerance})"
        )
    else:
        assert sum_a == sum_b, (
            f"Total {col_a} mismatch: {label_a}={sum_a}, {label_b}={sum_b}"
        )


def assert_sum_df_vs_table(raw_df, raw_col, spark, table_name, table_col, label_raw, label_table, tolerance=0.0):
    """
    Assert SUM(raw_col) from a DataFrame equals SUM(table_col) from a table.
    Used when one source is a raw CSV DataFrame (not a registered table).
    If *tolerance* > 0, uses abs(a - b) < tolerance for floating-point comparisons.
    """
    sum_raw = raw_df.agg({raw_col: "sum"}).collect()[0][0]
    sum_table = get_sum(spark, table_name, table_col)
    logger.info(f"{label_raw} total {raw_col}: {sum_raw}, {label_table} total {table_col}: {sum_table}")
    if tolerance > 0:
        assert abs(sum_raw - sum_table) < tolerance, (
            f"Total {raw_col} mismatch: {label_raw}={sum_raw}, {label_table}={sum_table} (tolerance={tolerance})"
        )
    else:
        assert sum_raw == sum_table, (
            f"Total {raw_col} mismatch: {label_raw}={sum_raw}, {label_table}={sum_table}"
        )


# ======================================================================================
# RECONCILIATION HELPERS (exceptAll-based)
# ======================================================================================

def assert_distinct_values_match(df_a, df_b, label_a, label_b):
    """
    Assert that every distinct value in df_a also exists in df_b.
    Both DataFrames must have the same column name(s) for the comparison.
    Uses exceptAll (SQL EXCEPT ALL equivalent) which preserves duplicates.
    """
    missing = df_a.exceptAll(df_b).count()
    logger.info(f"Distinct values in {label_a} missing from {label_b}: {missing}")
    assert missing == 0, (
        f"Value mismatch: {missing} values in {label_a} not found in {label_b}"
    )


def assert_no_data_loss(df_source, df_target, label_source, label_target):
    """
    Assert no rows were lost from source to target.
    Returns rows in df_source that do NOT appear in df_target.
    Both DataFrames must have identical schemas (same columns, same order).
    """
    missing = df_source.exceptAll(df_target).count()
    logger.info(f"Rows in {label_source} missing from {label_target}: {missing}")
    assert missing == 0, (
        f"Data loss detected: {missing} rows in {label_source} not found in {label_target}"
    )


def assert_no_extra_rows(df_source, df_target, label_source, label_target):
    """
    Assert target has no rows that are absent from source.
    Returns rows in df_target that do NOT appear in df_source.
    Both DataFrames must have identical schemas (same columns, same order).
    """
    extra = df_target.exceptAll(df_source).count()
    logger.info(f"Extra rows in {label_target} not in {label_source}: {extra}")
    assert extra == 0, (
        f"Extra rows detected: {extra} rows in {label_target} not found in {label_source}"
    )


# ======================================================================================
# DATA QUALITY HELPERS
# ======================================================================================

def assert_no_nulls(spark, table_name, column, label):
    """Assert a column has no NULL values in the table."""
    null_count = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {table_name} WHERE {column} IS NULL"
    ).collect()[0]["cnt"]
    logger.info(f"{label} NULL {column} count: {null_count}")
    assert null_count == 0, f"{column} is NULL for {null_count} rows in {label}"


def get_mismatch_count(spark, sql_query):
    """
    Execute a SQL query that returns a single 'cnt' column and return the integer.
    Used for mismatch-counting queries (e.g., WHERE calculation is wrong).
    The query must return exactly one row with a column named 'cnt'.
    """
    count = spark.sql(sql_query).collect()[0]["cnt"]
    logger.info(f"Mismatch count: {count}")
    return count