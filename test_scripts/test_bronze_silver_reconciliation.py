import pytest
import logging
from test_configuration.config import *

logging.basicConfig(
    filename="logs/test_results.log",
    filemode='a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("spark")
class TestBronzeSilverReconciliation:
    """Test cases to verify data reconciliation between bronze and silver tables."""

    @pytest.mark.smoke
    @pytest.mark.count_check
    def test_row_count_match(self, spark):
        """Verify bronze and silver tables have the same row count."""
        logger.info("Test case: test_row_count_match started")
        bronze_count = spark.table(BRONZE_ORDERS).count()
        silver_count = spark.table(SILVER_ORDERS).count()
        logger.info(f"Bronze count: {bronze_count}, Silver count: {silver_count}")
        assert bronze_count == silver_count, (
            f"Row count mismatch: bronze={bronze_count}, silver={silver_count}"
        )
        logger.info("Test case: test_row_count_match completed")

    @pytest.mark.regression
    @pytest.mark.count_check
    def test_bronze_count_not_zero(self, spark):
        """Verify bronze table is not empty."""
        logger.info("Test case: test_bronze_count_not_zero started")
        bronze_count = spark.table(BRONZE_ORDERS).count()
        assert bronze_count > 0, "Bronze table is empty"
        logger.info(f"Bronze table has {bronze_count} rows")
        logger.info("Test case: test_bronze_count_not_zero completed")

    @pytest.mark.regression
    @pytest.mark.count_check
    def test_silver_count_not_zero(self, spark):
        """Verify silver table is not empty."""
        logger.info("Test case: test_silver_count_not_zero started")
        silver_count = spark.table(SILVER_ORDERS).count()
        assert silver_count > 0, "Silver table is empty"
        logger.info(f"Silver table has {silver_count} rows")
        logger.info("Test case: test_silver_count_not_zero completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_no_data_loss_bronze_to_silver(self, spark):
        """Verify all bronze rows exist in silver (no data loss during transformation)."""
        logger.info("Test case: test_no_data_loss_bronze_to_silver started")
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
        missing_in_silver = bronze_df.exceptAll(silver_df).count()
        logger.info(f"Rows in bronze missing from silver: {missing_in_silver}")
        assert missing_in_silver == 0, (
            f"Data loss detected: {missing_in_silver} rows in bronze not found in silver"
        )
        logger.info("Test case: test_no_data_loss_bronze_to_silver completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_no_extra_rows_in_silver(self, spark):
        """Verify silver does not have extra rows not present in bronze."""
        logger.info("Test case: test_no_extra_rows_in_silver started")
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
        extra_in_silver = silver_df.exceptAll(bronze_df).count()
        logger.info(f"Extra rows in silver not in bronze: {extra_in_silver}")
        assert extra_in_silver == 0, (
            f"Extra rows detected: {extra_in_silver} rows in silver not found in bronze"
        )
        logger.info("Test case: test_no_extra_rows_in_silver completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_total_sales_match(self, spark):
        """Verify total Sales amount matches between bronze and silver."""
        logger.info("Test case: test_total_sales_match started")
        bronze_sales = spark.sql(f"SELECT COALESCE(SUM(Sales), 0) AS total FROM {BRONZE_ORDERS}").collect()[0]["total"]
        silver_sales = spark.sql(f"SELECT COALESCE(SUM(Sales), 0) AS total FROM {SILVER_ORDERS}").collect()[0]["total"]
        logger.info(f"Bronze total sales: {bronze_sales}, Silver total sales: {silver_sales}")
        assert bronze_sales == silver_sales, (
            f"Total Sales mismatch: bronze={bronze_sales}, silver={silver_sales}"
        )
        logger.info("Test case: test_total_sales_match completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_total_quantity_match(self, spark):
        """Verify total Quantity matches between bronze and silver."""
        logger.info("Test case: test_total_quantity_match started")
        bronze_qty = spark.sql(f"SELECT COALESCE(SUM(Quantity), 0) AS total FROM {BRONZE_ORDERS}").collect()[0]["total"]
        silver_qty = spark.sql(f"SELECT COALESCE(SUM(Quantity), 0) AS total FROM {SILVER_ORDERS}").collect()[0]["total"]
        logger.info(f"Bronze total quantity: {bronze_qty}, Silver total quantity: {silver_qty}")
        assert bronze_qty == silver_qty, (
            f"Total Quantity mismatch: bronze={bronze_qty}, silver={silver_qty}"
        )
        logger.info("Test case: test_total_quantity_match completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_total_profit_match(self, spark):
        """Verify total Profit matches between bronze and silver."""
        logger.info("Test case: test_total_profit_match started")
        bronze_profit = spark.sql(f"SELECT COALESCE(SUM(Profit), 0) AS total FROM {BRONZE_ORDERS}").collect()[0]["total"]
        silver_profit = spark.sql(f"SELECT COALESCE(SUM(Profit), 0) AS total FROM {SILVER_ORDERS}").collect()[0]["total"]
        logger.info(f"Bronze total profit: {bronze_profit}, Silver total profit: {silver_profit}")
        assert bronze_profit == silver_profit, (
            f"Total Profit mismatch: bronze={bronze_profit}, silver={silver_profit}"
        )
        logger.info("Test case: test_total_profit_match completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_sub_category_values_match(self, spark):
        """Verify Sub-Category in bronze matches Sub_Category in silver."""
        logger.info("Test case: test_sub_category_values_match started")
        bronze_sub = spark.sql(f"SELECT DISTINCT `Sub-Category` AS Sub_Category FROM {BRONZE_ORDERS}")
        silver_sub = spark.sql(f"SELECT DISTINCT Sub_Category FROM {SILVER_ORDERS}")
        missing = bronze_sub.exceptAll(silver_sub).count()
        assert missing == 0, (
            f"Sub-Category values mismatch: {missing} categories in bronze not found in silver"
        )
        logger.info("Test case: test_sub_category_values_match completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_distinct_categories_match(self, spark):
        """Verify distinct Category values match between bronze and silver."""
        logger.info("Test case: test_distinct_categories_match started")
        bronze_cats = spark.sql(f"SELECT DISTINCT Category FROM {BRONZE_ORDERS}")
        silver_cats = spark.sql(f"SELECT DISTINCT Category FROM {SILVER_ORDERS}")
        missing = bronze_cats.exceptAll(silver_cats).count()
        assert missing == 0, (
            f"Category values mismatch: {missing} categories in bronze not found in silver"
        )
        logger.info("Test case: test_distinct_categories_match completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_silver_has_derived_columns(self, spark):
        """Verify silver table has all expected derived columns."""
        logger.info("Test case: test_silver_has_derived_columns started")
        silver_columns = set(spark.table(SILVER_ORDERS).columns)
        for col in SILVER_DERIVED_COLUMNS:
            assert col in silver_columns, (
                f"Derived column '{col}' not found in silver table"
            )
        logger.info("Test case: test_silver_has_derived_columns completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_net_sales_calculation(self, spark):
        """Verify net_sales = Sales - discount_amount in silver."""
        logger.info("Test case: test_net_sales_calculation started")
        mismatch_count = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {SILVER_ORDERS}
            WHERE ABS(net_sales - (Sales - discount_amount)) > 0.01
        """).collect()[0]["cnt"]
        assert mismatch_count == 0, (
            f"net_sales calculation error: {mismatch_count} rows where net_sales != Sales - discount_amount"
        )
        logger.info("Test case: test_net_sales_calculation completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_discount_amount_calculation(self, spark):
        """Verify discount_amount = Sales * Discount in silver."""
        logger.info("Test case: test_discount_amount_calculation started")
        mismatch_count = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {SILVER_ORDERS}
            WHERE ABS(discount_amount - (Sales * Discount)) > 0.01
        """).collect()[0]["cnt"]
        assert mismatch_count == 0, (
            f"discount_amount calculation error: {mismatch_count} rows where discount_amount != Sales * Discount"
        )
        logger.info("Test case: test_discount_amount_calculation completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_profit_margin_pct_calculation(self, spark):
        """Verify profit_margin_pct = (Profit / Sales) * 100 in silver."""
        logger.info("Test case: test_profit_margin_pct_calculation started")
        mismatch_count = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {SILVER_ORDERS}
            WHERE Sales > 0
            AND ABS(profit_margin_pct - ROUND((Profit / Sales) * 100, 2)) > 0.01
        """).collect()[0]["cnt"]
        assert mismatch_count == 0, (
            f"profit_margin_pct calculation error: {mismatch_count} rows with incorrect calculation"
        )
        logger.info("Test case: test_profit_margin_pct_calculation completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_is_profitable_flag(self, spark):
        """Verify is_profitable is True when Profit > 0."""
        logger.info("Test case: test_is_profitable_flag started")
        mismatch_count = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {SILVER_ORDERS}
            WHERE (Profit > 0 AND is_profitable = false)
               OR (Profit <= 0 AND is_profitable = true)
        """).collect()[0]["cnt"]
        assert mismatch_count == 0, (
            f"is_profitable flag error: {mismatch_count} rows with incorrect profitability flag"
        )
        logger.info("Test case: test_is_profitable_flag completed")

    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_order_id_sequence(self, spark):
        """Verify order_id is sequential starting from 0 in silver."""
        logger.info("Test case: test_order_id_sequence started")
        result = spark.sql(f"""
            SELECT MIN(order_id) AS min_id, MAX(order_id) AS max_id, COUNT(DISTINCT order_id) AS distinct_ids
            FROM {SILVER_ORDERS}
        """).collect()[0]
        row_count = spark.table(SILVER_ORDERS).count()
        assert result["min_id"] == 0, (
            f"order_id should start from 0, actual min: {result['min_id']}"
        )
        assert result["distinct_ids"] == row_count, (
            f"order_id has duplicates: {row_count} rows but only {result['distinct_ids']} distinct ids"
        )
        logger.info("Test case: test_order_id_sequence completed")