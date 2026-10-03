# --------------------------------------------------------------------------------------
# Test: Raw CSV vs Bronze Table Reconciliation
# --------------------------------------------------------------------------------------
# Verifies that the bronze table accurately reflects the raw CSV source file.
# CSV column names with spaces ("Ship Mode", "Postal Code") are renamed to
# underscores in bronze ("Ship_Mode", "Postal_Code") per RAW_TO_BRONZE_COLUMN_MAPPING.
# Bronze also adds metadata columns (_ingestion_timestamp, _source_file).
# --------------------------------------------------------------------------------------

import pytest
from test_configuration.config import *
from common_utilities.reconciliation_utils import *


@pytest.mark.usefixtures("spark")
class TestRawCsvBronzeReconciliation:
    """Reconciliation tests between raw CSV source and bronze table."""

    # ==================================================================================
    # SECTION 1: Sanity Checks
    # ==================================================================================

    # Smoke: raw CSV must contain data
    @pytest.mark.smoke
    @pytest.mark.count_check
    def test_raw_csv_not_empty(self, raw_csv_df):
        """Raw CSV file is not empty."""
        log_start("test_raw_csv_not_empty")
        raw_count = raw_csv_df.count()
        assert raw_count > 0, "Raw CSV file is empty"
        log_end("test_raw_csv_not_empty")

    # Smoke: bronze table must contain data
    @pytest.mark.smoke
    @pytest.mark.count_check
    def test_bronze_table_not_empty(self, spark):
        """Bronze table is not empty."""
        log_start("test_bronze_table_not_empty")
        assert_not_empty(spark, BRONZE_ORDERS, "bronze")
        log_end("test_bronze_table_not_empty")

    # Smoke: row counts must match between raw CSV and bronze
    @pytest.mark.smoke
    @pytest.mark.count_check
    def test_row_count_match(self, spark, raw_csv_df):
        """Raw CSV and bronze table have the same row count."""
        log_start("test_row_count_match")
        raw_count = raw_csv_df.count()
        bronze_count = get_count(spark, BRONZE_ORDERS)
        assert raw_count == bronze_count, (
            f"Row count mismatch: raw_csv={raw_count}, bronze={bronze_count}"
        )
        log_end("test_row_count_match")

    # ==================================================================================
    # SECTION 2: Schema Verification
    # ==================================================================================

    # Regression: all raw CSV columns (renamed) must exist in bronze
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_bronze_has_all_raw_columns(self, spark, raw_csv_df):
        """Every raw CSV column (with name mapping) exists in bronze."""
        log_start("test_bronze_has_all_raw_columns")
        bronze_columns = set(spark.table(BRONZE_ORDERS).columns)
        for raw_col in raw_csv_df.columns:
            bronze_col = RAW_TO_BRONZE_COLUMN_MAPPING.get(raw_col, raw_col)
            assert bronze_col in bronze_columns, (
                f"Column '{bronze_col}' (from raw CSV '{raw_col}') not found in bronze"
            )
        log_end("test_bronze_has_all_raw_columns")

    # Regression: bronze must have metadata columns added during ingestion
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_bronze_has_metadata_columns(self, spark):
        """Bronze table has _ingestion_timestamp and _source_file columns."""
        log_start("test_bronze_has_metadata_columns")
        assert_columns_exist(spark, BRONZE_ORDERS, BRONZE_METADATA_COLUMNS)
        log_end("test_bronze_has_metadata_columns")

    # Regression: _ingestion_timestamp must not be NULL
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_ingestion_timestamp_not_null(self, spark):
        """_ingestion_timestamp is populated for all rows."""
        log_start("test_ingestion_timestamp_not_null")
        assert_no_nulls(spark, BRONZE_ORDERS, "_ingestion_timestamp", "bronze")
        log_end("test_ingestion_timestamp_not_null")

    # Regression: _source_file must not be NULL
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_source_file_not_null(self, spark):
        """_source_file is populated for all rows."""
        log_start("test_source_file_not_null")
        assert_no_nulls(spark, BRONZE_ORDERS, "_source_file", "bronze")
        log_end("test_source_file_not_null")

    # Regression: _source_file must contain the expected CSV filename
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_source_file_value_correct(self, spark):
        """_source_file contains the expected raw CSV path."""
        log_start("test_source_file_value_correct")
        expected_filename = RAW_CSV_PATH.split("/")[-1]
        wrong_count = get_mismatch_count(spark, f"""
            SELECT COUNT(*) AS cnt
            FROM {BRONZE_ORDERS}
            WHERE _source_file NOT LIKE '%{expected_filename}'
        """)
        assert wrong_count == 0, f"{wrong_count} rows have unexpected _source_file value"
        log_end("test_source_file_value_correct")

    # ==================================================================================
    # SECTION 3: Row-Level Reconciliation
    # ==================================================================================

    # Regression: no data lost from raw CSV to bronze
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_no_data_loss_raw_to_bronze(self, spark, raw_csv_df):
        """Every raw CSV row exists in bronze (no data loss during ingestion)."""
        log_start("test_no_data_loss_raw_to_bronze")
        # Rename CSV columns to match bronze schema
        raw_renamed = raw_csv_df
        for csv_col, bronze_col in RAW_TO_BRONZE_COLUMN_MAPPING.items():
            raw_renamed = raw_renamed.withColumnRenamed(csv_col, bronze_col)
        # Compare only data columns (exclude metadata)
        data_columns = raw_renamed.columns
        bronze_data = spark.table(BRONZE_ORDERS).select(*data_columns)
        assert_no_data_loss(raw_renamed, bronze_data, "raw_csv", "bronze")
        log_end("test_no_data_loss_raw_to_bronze")

    # Regression: no extra rows in bronze beyond raw CSV
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_no_extra_rows_in_bronze(self, spark, raw_csv_df):
        """Bronze has no data rows absent from raw CSV."""
        log_start("test_no_extra_rows_in_bronze")
        raw_renamed = raw_csv_df
        for csv_col, bronze_col in RAW_TO_BRONZE_COLUMN_MAPPING.items():
            raw_renamed = raw_renamed.withColumnRenamed(csv_col, bronze_col)
        data_columns = raw_renamed.columns
        bronze_data = spark.table(BRONZE_ORDERS).select(*data_columns)
        assert_no_extra_rows(raw_renamed, bronze_data, "raw_csv", "bronze")
        log_end("test_no_extra_rows_in_bronze")

    # ==================================================================================
    # SECTION 4: Aggregate Comparisons
    # ==================================================================================

    # Regression: total Sales must match (tolerance for floating-point)
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_total_sales_match(self, spark, raw_csv_df):
        """SUM(Sales) matches between raw CSV and bronze."""
        log_start("test_total_sales_match")
        assert_sum_df_vs_table(raw_csv_df, "Sales", spark, BRONZE_ORDERS, "Sales",
                                "raw_csv", "bronze", tolerance=0.01)
        log_end("test_total_sales_match")

    # Regression: total Quantity must match (integer, exact)
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_total_quantity_match(self, spark, raw_csv_df):
        """SUM(Quantity) matches between raw CSV and bronze."""
        log_start("test_total_quantity_match")
        assert_sum_df_vs_table(raw_csv_df, "Quantity", spark, BRONZE_ORDERS, "Quantity",
                                "raw_csv", "bronze")
        log_end("test_total_quantity_match")

    # Regression: total Profit must match (tolerance for floating-point)
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_total_profit_match(self, spark, raw_csv_df):
        """SUM(Profit) matches between raw CSV and bronze."""
        log_start("test_total_profit_match")
        assert_sum_df_vs_table(raw_csv_df, "Profit", spark, BRONZE_ORDERS, "Profit",
                                "raw_csv", "bronze", tolerance=0.01)
        log_end("test_total_profit_match")

    # ==================================================================================
    # SECTION 5: Distinct Value Comparisons
    # ==================================================================================

    # Regression: distinct Ship Mode values must match
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_distinct_ship_modes_match(self, spark, raw_csv_df):
        """Distinct Ship Mode values match between raw CSV and bronze."""
        log_start("test_distinct_ship_modes_match")
        raw_modes = raw_csv_df.selectExpr("`Ship Mode` AS Ship_Mode").distinct()
        bronze_modes = spark.sql(f"SELECT DISTINCT Ship_Mode FROM {BRONZE_ORDERS}")
        assert_distinct_values_match(raw_modes, bronze_modes, "raw_csv", "bronze")
        log_end("test_distinct_ship_modes_match")

    # Regression: distinct Segment values must match
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_distinct_segments_match(self, spark, raw_csv_df):
        """Distinct Segment values match between raw CSV and bronze."""
        log_start("test_distinct_segments_match")
        raw_segments = raw_csv_df.select("Segment").distinct()
        bronze_segments = spark.sql(f"SELECT DISTINCT Segment FROM {BRONZE_ORDERS}")
        assert_distinct_values_match(raw_segments, bronze_segments, "raw_csv", "bronze")
        log_end("test_distinct_segments_match")

    # Regression: distinct Category values must match
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_distinct_categories_match(self, spark, raw_csv_df):
        """Distinct Category values match between raw CSV and bronze."""
        log_start("test_distinct_categories_match")
        raw_cats = raw_csv_df.select("Category").distinct()
        bronze_cats = spark.sql(f"SELECT DISTINCT Category FROM {BRONZE_ORDERS}")
        assert_distinct_values_match(raw_cats, bronze_cats, "raw_csv", "bronze")
        log_end("test_distinct_categories_match")

    # Regression: distinct Sub-Category values must match
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_distinct_sub_categories_match(self, spark, raw_csv_df):
        """Distinct Sub-Category values match between raw CSV and bronze."""
        log_start("test_distinct_sub_categories_match")
        raw_subs = raw_csv_df.select("`Sub-Category`").distinct()
        bronze_subs = spark.sql(f"SELECT DISTINCT `Sub-Category` FROM {BRONZE_ORDERS}")
        assert_distinct_values_match(raw_subs, bronze_subs, "raw_csv", "bronze")
        log_end("test_distinct_sub_categories_match")

    # Regression: distinct Region values must match
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_distinct_regions_match(self, spark, raw_csv_df):
        """Distinct Region values match between raw CSV and bronze."""
        log_start("test_distinct_regions_match")
        raw_regions = raw_csv_df.select("Region").distinct()
        bronze_regions = spark.sql(f"SELECT DISTINCT Region FROM {BRONZE_ORDERS}")
        assert_distinct_values_match(raw_regions, bronze_regions, "raw_csv", "bronze")
        log_end("test_distinct_regions_match")

    # Regression: distinct State values must match
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_distinct_states_match(self, spark, raw_csv_df):
        """Distinct State values match between raw CSV and bronze."""
        log_start("test_distinct_states_match")
        raw_states = raw_csv_df.select("State").distinct()
        bronze_states = spark.sql(f"SELECT DISTINCT State FROM {BRONZE_ORDERS}")
        assert_distinct_values_match(raw_states, bronze_states, "raw_csv", "bronze")
        log_end("test_distinct_states_match")

    # Regression: distinct Postal Code values must match
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_distinct_postal_codes_match(self, spark, raw_csv_df):
        """Distinct Postal Code values match between raw CSV and bronze."""
        log_start("test_distinct_postal_codes_match")
        raw_postal = raw_csv_df.selectExpr("`Postal Code` AS Postal_Code").distinct()
        bronze_postal = spark.sql(f"SELECT DISTINCT Postal_Code FROM {BRONZE_ORDERS}")
        assert_distinct_values_match(raw_postal, bronze_postal, "raw_csv", "bronze")
        log_end("test_distinct_postal_codes_match")