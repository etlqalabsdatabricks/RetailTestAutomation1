# --------------------------------------------------------------------------------------
# Test: Bronze vs Silver Table Reconciliation
# --------------------------------------------------------------------------------------
# Verifies that the silver transformation preserved all data from bronze.
# Silver renames Sub-Category (hyphen) to Sub_Category (underscore) and adds
# derived columns (order_id, discount_amount, net_sales, profit_margin_pct,
# is_profitable, silver_processed_timestamp).
# --------------------------------------------------------------------------------------

import pytest
from test_configuration.config import *
from common_utilities.reconciliation_utils import *


@pytest.mark.usefixtures("spark")
class TestBronzeSilverReconciliation:
    """Reconciliation tests between bronze and silver tables."""

    # ==================================================================================
    # SECTION 1: Row Count Checks
    # ==================================================================================

    # Smoke: bronze and silver must have the same row count
    @pytest.mark.smoke
    @pytest.mark.count_check
    def test_row_count_match(self, spark):
        """Bronze and silver tables have the same row count."""
        log_start("test_row_count_match")
        assert_counts_match(spark, BRONZE_ORDERS, SILVER_ORDERS, "bronze", "silver")
        log_end("test_row_count_match")

    # Regression: bronze must not be empty
    @pytest.mark.regression
    @pytest.mark.count_check
    def test_bronze_count_not_zero(self, spark):
        """Bronze table is not empty."""
        log_start("test_bronze_count_not_zero")
        assert_not_empty(spark, BRONZE_ORDERS, "bronze")
        log_end("test_bronze_count_not_zero")

    # Regression: silver must not be empty
    @pytest.mark.regression
    @pytest.mark.count_check
    def test_silver_count_not_zero(self, spark):
        """Silver table is not empty."""
        log_start("test_silver_count_not_zero")
        assert_not_empty(spark, SILVER_ORDERS, "silver")
        log_end("test_silver_count_not_zero")

    # ==================================================================================
    # SECTION 2: Row-Level Reconciliation
    # ==================================================================================

    # Regression: no data loss from bronze to silver
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_no_data_loss_bronze_to_silver(self, spark):
        """Every bronze row exists in silver (no data loss during transformation)."""
        log_start("test_no_data_loss_bronze_to_silver")
        # Select common columns, renaming bronze Sub-Category to match silver Sub_Category
        bronze_df = spark.sql(f"""
            SELECT Ship_Mode, Segment, Country, City, State, Postal_Code,
                   Region, Category, `Sub-Category` AS Sub_Category,
                   Sales, Quantity, Discount, Profit,
                   _ingestion_timestamp, _source_file
            FROM {BRONZE_ORDERS}
        """)
        silver_df = spark.sql(f"""
            SELECT Ship_Mode, Segment, Country, City, State, Postal_Code,
                   Region, Category, Sub_Category,
                   Sales, Quantity, Discount, Profit,
                   _ingestion_timestamp, _source_file
            FROM {SILVER_ORDERS}
        """)
        assert_no_data_loss(bronze_df, silver_df, "bronze", "silver")
        log_end("test_no_data_loss_bronze_to_silver")

    # Regression: no extra rows in silver beyond bronze
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_no_extra_rows_in_silver(self, spark):
        """Silver has no data rows absent from bronze."""
        log_start("test_no_extra_rows_in_silver")
        bronze_df = spark.sql(f"""
            SELECT Ship_Mode, Segment, Country, City, State, Postal_Code,
                   Region, Category, `Sub-Category` AS Sub_Category,
                   Sales, Quantity, Discount, Profit,
                   _ingestion_timestamp, _source_file
            FROM {BRONZE_ORDERS}
        """)
        silver_df = spark.sql(f"""
            SELECT Ship_Mode, Segment, Country, City, State, Postal_Code,
                   Region, Category, Sub_Category,
                   Sales, Quantity, Discount, Profit,
                   _ingestion_timestamp, _source_file
            FROM {SILVER_ORDERS}
        """)
        assert_no_extra_rows(bronze_df, silver_df, "bronze", "silver")
        log_end("test_no_extra_rows_in_silver")

    # ==================================================================================
    # SECTION 3: Aggregate Comparisons
    # ==================================================================================

    # Regression: total Sales must match between bronze and silver
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_total_sales_match(self, spark):
        """SUM(Sales) matches between bronze and silver."""
        log_start("test_total_sales_match")
        assert_sums_match(spark, BRONZE_ORDERS, "Sales", SILVER_ORDERS, "Sales",
                           "bronze", "silver")
        log_end("test_total_sales_match")

    # Regression: total Quantity must match between bronze and silver
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_total_quantity_match(self, spark):
        """SUM(Quantity) matches between bronze and silver."""
        log_start("test_total_quantity_match")
        assert_sums_match(spark, BRONZE_ORDERS, "Quantity", SILVER_ORDERS, "Quantity",
                           "bronze", "silver")
        log_end("test_total_quantity_match")

    # Regression: total Profit must match between bronze and silver
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_total_profit_match(self, spark):
        """SUM(Profit) matches between bronze and silver."""
        log_start("test_total_profit_match")
        assert_sums_match(spark, BRONZE_ORDERS, "Profit", SILVER_ORDERS, "Profit",
                           "bronze", "silver")
        log_end("test_total_profit_match")

    # ==================================================================================
    # SECTION 4: Distinct Value Comparisons
    # ==================================================================================

    # Regression: Sub-Category values in bronze must exist in silver (as Sub_Category)
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_sub_category_values_match(self, spark):
        """Sub-Category (bronze) values match Sub_Category (silver)."""
        log_start("test_sub_category_values_match")
        bronze_sub = spark.sql(f"SELECT DISTINCT `Sub-Category` AS Sub_Category FROM {BRONZE_ORDERS}")
        silver_sub = spark.sql(f"SELECT DISTINCT Sub_Category FROM {SILVER_ORDERS}")
        assert_distinct_values_match(bronze_sub, silver_sub, "bronze", "silver")
        log_end("test_sub_category_values_match")

    # Regression: distinct Category values must match
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_distinct_categories_match(self, spark):
        """Distinct Category values match between bronze and silver."""
        log_start("test_distinct_categories_match")
        bronze_cats = spark.sql(f"SELECT DISTINCT Category FROM {BRONZE_ORDERS}")
        silver_cats = spark.sql(f"SELECT DISTINCT Category FROM {SILVER_ORDERS}")
        assert_distinct_values_match(bronze_cats, silver_cats, "bronze", "silver")
        log_end("test_distinct_categories_match")

    # ==================================================================================
    # SECTION 5: Silver Derived Column Verification
    # ==================================================================================

    # Regression: silver must have all expected derived columns
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_silver_has_derived_columns(self, spark):
        """Silver table has all expected derived columns."""
        log_start("test_silver_has_derived_columns")
        assert_columns_exist(spark, SILVER_ORDERS, SILVER_DERIVED_COLUMNS)
        log_end("test_silver_has_derived_columns")

    # Regression: net_sales = Sales - discount_amount for all rows
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_net_sales_calculation(self, spark):
        """net_sales = Sales - discount_amount in silver."""
        log_start("test_net_sales_calculation")
        mismatch = get_mismatch_count(spark, f"""
            SELECT COUNT(*) AS cnt FROM {SILVER_ORDERS}
            WHERE ABS(net_sales - (Sales - discount_amount)) > 0.01
        """)
        assert mismatch == 0, f"net_sales error: {mismatch} rows where net_sales != Sales - discount_amount"
        log_end("test_net_sales_calculation")

    # Regression: discount_amount = Sales * Discount for all rows
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_discount_amount_calculation(self, spark):
        """discount_amount = Sales * Discount in silver."""
        log_start("test_discount_amount_calculation")
        mismatch = get_mismatch_count(spark, f"""
            SELECT COUNT(*) AS cnt FROM {SILVER_ORDERS}
            WHERE ABS(discount_amount - (Sales * Discount)) > 0.01
        """)
        assert mismatch == 0, f"discount_amount error: {mismatch} rows where discount_amount != Sales * Discount"
        log_end("test_discount_amount_calculation")

    # Regression: profit_margin_pct = ROUND((Profit / Sales) * 100, 2) for all rows where Sales > 0
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_profit_margin_pct_calculation(self, spark):
        """profit_margin_pct = ROUND((Profit / Sales) * 100, 2) in silver."""
        log_start("test_profit_margin_pct_calculation")
        mismatch = get_mismatch_count(spark, f"""
            SELECT COUNT(*) AS cnt FROM {SILVER_ORDERS}
            WHERE Sales > 0
            AND ABS(profit_margin_pct - ROUND((Profit / Sales) * 100, 2)) > 0.01
        """)
        assert mismatch == 0, f"profit_margin_pct error: {mismatch} rows with incorrect calculation"
        log_end("test_profit_margin_pct_calculation")

    # Regression: is_profitable must be True when Profit > 0, False otherwise
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_is_profitable_flag(self, spark):
        """is_profitable flag is correct for all rows in silver."""
        log_start("test_is_profitable_flag")
        mismatch = get_mismatch_count(spark, f"""
            SELECT COUNT(*) AS cnt FROM {SILVER_ORDERS}
            WHERE (Profit > 0 AND is_profitable = false)
               OR (Profit <= 0 AND is_profitable = true)
        """)
        assert mismatch == 0, f"is_profitable flag error: {mismatch} rows with incorrect flag"
        log_end("test_is_profitable_flag")

    # Regression: order_id must be sequential from 0 with no duplicates
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_order_id_sequence(self, spark):
        """order_id is sequential starting from 0 with no duplicates."""
        log_start("test_order_id_sequence")
        result = spark.sql(f"""
            SELECT MIN(order_id) AS min_id, MAX(order_id) AS max_id,
                   COUNT(DISTINCT order_id) AS distinct_ids
            FROM {SILVER_ORDERS}
        """).collect()[0]
        row_count = get_count(spark, SILVER_ORDERS)
        assert result["min_id"] == 0, f"order_id should start from 0, actual min: {result['min_id']}"
        assert result["distinct_ids"] == row_count, (
            f"order_id has duplicates: {row_count} rows but only {result['distinct_ids']} distinct ids"
        )
        log_end("test_order_id_sequence")