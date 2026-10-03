# --------------------------------------------------------------------------------------
# Test: Silver vs Gold Table Reconciliation
# --------------------------------------------------------------------------------------
# Verifies that gold aggregation tables match the underlying silver data
# when re-computed independently.
#
# Gold tables:
#   1. gold_segment_summary  — aggregated by Segment + Ship_Mode
#   2. gold_regional_summary — aggregated by Region + State
#
# Pattern: each test re-aggregates silver by the grouping key, joins to gold,
# and counts mismatches. Tolerance of 0.01 handles floating-point differences.
# --------------------------------------------------------------------------------------

import pytest
from test_configuration.config import *
from common_utilities.reconciliation_utils import *


@pytest.mark.usefixtures("spark")
class TestSilverGoldReconciliation:
    """Reconciliation tests between silver and gold tables."""

    # ==================================================================================
    # SECTION 1: Gold Segment Summary (grouped by Segment + Ship_Mode)
    # ==================================================================================

    # Smoke: gold_segment_summary must not be empty
    @pytest.mark.smoke
    @pytest.mark.count_check
    def test_gold_segment_summary_not_empty(self, spark):
        """gold_segment_summary table is not empty."""
        log_start("test_gold_segment_summary_not_empty")
        assert_not_empty(spark, GOLD_SEGMENT_SUMMARY, "gold_segment_summary")
        log_end("test_gold_segment_summary_not_empty")

    # Regression: gold_segment_summary must have all expected columns
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_segment_columns_exist(self, spark):
        """gold_segment_summary has all expected columns."""
        log_start("test_gold_segment_columns_exist")
        assert_columns_exist(spark, GOLD_SEGMENT_SUMMARY, GOLD_SEGMENT_COLUMNS)
        log_end("test_gold_segment_columns_exist")

    # Regression: row count must match distinct (Segment, Ship_Mode) groups in silver
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_segment_group_count_match(self, spark):
        """gold_segment_summary group count matches silver distinct groups."""
        log_start("test_gold_segment_group_count_match")
        silver_groups = get_mismatch_count(spark, f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT DISTINCT Segment, Ship_Mode FROM {SILVER_ORDERS}
            )
        """)
        gold_rows = get_count(spark, GOLD_SEGMENT_SUMMARY)
        assert gold_rows == silver_groups, (
            f"Group count mismatch: silver={silver_groups} groups, gold={gold_rows} rows"
        )
        log_end("test_gold_segment_group_count_match")

    # Regression: order_count must match COUNT(*) from silver per group
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_segment_order_count_match(self, spark):
        """order_count in gold_segment_summary matches silver per group."""
        log_start("test_gold_segment_order_count_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Segment, Ship_Mode, COUNT(*) AS expected_count
                FROM {SILVER_ORDERS} GROUP BY Segment, Ship_Mode
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_SEGMENT_SUMMARY} g
            JOIN silver_agg s ON g.Segment = s.Segment AND g.Ship_Mode = s.Ship_Mode
            WHERE g.order_count != s.expected_count
        """)
        assert mismatch == 0, f"order_count mismatch: {mismatch} groups have incorrect order_count"
        log_end("test_gold_segment_order_count_match")

    # Regression: total_sales must match SUM(Sales) from silver per group
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_segment_total_sales_match(self, spark):
        """total_sales in gold_segment_summary matches silver per group."""
        log_start("test_gold_segment_total_sales_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Segment, Ship_Mode, SUM(Sales) AS expected_sales
                FROM {SILVER_ORDERS} GROUP BY Segment, Ship_Mode
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_SEGMENT_SUMMARY} g
            JOIN silver_agg s ON g.Segment = s.Segment AND g.Ship_Mode = s.Ship_Mode
            WHERE ABS(g.total_sales - s.expected_sales) > 0.01
        """)
        assert mismatch == 0, f"total_sales mismatch: {mismatch} groups incorrect"
        log_end("test_gold_segment_total_sales_match")

    # Regression: total_profit must match SUM(Profit) from silver per group
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_segment_total_profit_match(self, spark):
        """total_profit in gold_segment_summary matches silver per group."""
        log_start("test_gold_segment_total_profit_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Segment, Ship_Mode, SUM(Profit) AS expected_profit
                FROM {SILVER_ORDERS} GROUP BY Segment, Ship_Mode
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_SEGMENT_SUMMARY} g
            JOIN silver_agg s ON g.Segment = s.Segment AND g.Ship_Mode = s.Ship_Mode
            WHERE ABS(g.total_profit - s.expected_profit) > 0.01
        """)
        assert mismatch == 0, f"total_profit mismatch: {mismatch} groups incorrect"
        log_end("test_gold_segment_total_profit_match")

    # Regression: total_quantity must match SUM(Quantity) from silver per group
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_segment_total_quantity_match(self, spark):
        """total_quantity in gold_segment_summary matches silver per group."""
        log_start("test_gold_segment_total_quantity_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Segment, Ship_Mode, SUM(Quantity) AS expected_qty
                FROM {SILVER_ORDERS} GROUP BY Segment, Ship_Mode
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_SEGMENT_SUMMARY} g
            JOIN silver_agg s ON g.Segment = s.Segment AND g.Ship_Mode = s.Ship_Mode
            WHERE g.total_quantity != s.expected_qty
        """)
        assert mismatch == 0, f"total_quantity mismatch: {mismatch} groups incorrect"
        log_end("test_gold_segment_total_quantity_match")

    # Regression: avg_order_value must equal total_sales / order_count
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_segment_avg_order_value_match(self, spark):
        """avg_order_value = total_sales / order_count in gold_segment_summary."""
        log_start("test_gold_segment_avg_order_value_match")
        mismatch = get_mismatch_count(spark, f"""
            SELECT COUNT(*) AS cnt FROM {GOLD_SEGMENT_SUMMARY}
            WHERE order_count > 0
            AND ABS(avg_order_value - (total_sales / order_count)) > 0.01
        """)
        assert mismatch == 0, f"avg_order_value mismatch: {mismatch} groups incorrect"
        log_end("test_gold_segment_avg_order_value_match")

    # Regression: total_discount_given must match SUM(discount_amount) from silver per group
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_segment_total_discount_given_match(self, spark):
        """total_discount_given in gold_segment_summary matches silver per group."""
        log_start("test_gold_segment_total_discount_given_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Segment, Ship_Mode, SUM(discount_amount) AS expected_discount
                FROM {SILVER_ORDERS} GROUP BY Segment, Ship_Mode
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_SEGMENT_SUMMARY} g
            JOIN silver_agg s ON g.Segment = s.Segment AND g.Ship_Mode = s.Ship_Mode
            WHERE ABS(g.total_discount_given - s.expected_discount) > 0.01
        """)
        assert mismatch == 0, f"total_discount_given mismatch: {mismatch} groups incorrect"
        log_end("test_gold_segment_total_discount_given_match")

    # ==================================================================================
    # SECTION 2: Gold Regional Summary (grouped by Region + State)
    # ==================================================================================

    # Smoke: gold_regional_summary must not be empty
    @pytest.mark.smoke
    @pytest.mark.count_check
    def test_gold_regional_not_empty(self, spark):
        """gold_regional_summary table is not empty."""
        log_start("test_gold_regional_not_empty")
        assert_not_empty(spark, GOLD_REGIONAL_SUMMARY, "gold_regional_summary")
        log_end("test_gold_regional_not_empty")

    # Regression: gold_regional_summary must have all expected columns
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_columns_exist(self, spark):
        """gold_regional_summary has all expected columns."""
        log_start("test_gold_regional_columns_exist")
        assert_columns_exist(spark, GOLD_REGIONAL_SUMMARY, GOLD_REGIONAL_COLUMNS)
        log_end("test_gold_regional_columns_exist")

    # Regression: row count must match distinct (Region, State) groups in silver
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_group_count_match(self, spark):
        """gold_regional_summary group count matches silver distinct groups."""
        log_start("test_gold_regional_group_count_match")
        silver_groups = get_mismatch_count(spark, f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT DISTINCT Region, State FROM {SILVER_ORDERS}
            )
        """)
        gold_rows = get_count(spark, GOLD_REGIONAL_SUMMARY)
        assert gold_rows == silver_groups, (
            f"Group count mismatch: silver={silver_groups} groups, gold={gold_rows} rows"
        )
        log_end("test_gold_regional_group_count_match")

    # Regression: order_count must match COUNT(*) from silver per group
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_order_count_match(self, spark):
        """order_count in gold_regional_summary matches silver per group."""
        log_start("test_gold_regional_order_count_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Region, State, COUNT(*) AS expected_count
                FROM {SILVER_ORDERS} GROUP BY Region, State
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_REGIONAL_SUMMARY} g
            JOIN silver_agg s ON g.Region = s.Region AND g.State = s.State
            WHERE g.order_count != s.expected_count
        """)
        assert mismatch == 0, f"order_count mismatch: {mismatch} groups incorrect"
        log_end("test_gold_regional_order_count_match")

    # Regression: total_sales must match SUM(Sales) from silver per group
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_total_sales_match(self, spark):
        """total_sales in gold_regional_summary matches silver per group."""
        log_start("test_gold_regional_total_sales_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Region, State, SUM(Sales) AS expected_sales
                FROM {SILVER_ORDERS} GROUP BY Region, State
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_REGIONAL_SUMMARY} g
            JOIN silver_agg s ON g.Region = s.Region AND g.State = s.State
            WHERE ABS(g.total_sales - s.expected_sales) > 0.01
        """)
        assert mismatch == 0, f"total_sales mismatch: {mismatch} groups incorrect"
        log_end("test_gold_regional_total_sales_match")

    # Regression: total_profit must match SUM(Profit) from silver per group
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_total_profit_match(self, spark):
        """total_profit in gold_regional_summary matches silver per group."""
        log_start("test_gold_regional_total_profit_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Region, State, SUM(Profit) AS expected_profit
                FROM {SILVER_ORDERS} GROUP BY Region, State
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_REGIONAL_SUMMARY} g
            JOIN silver_agg s ON g.Region = s.Region AND g.State = s.State
            WHERE ABS(g.total_profit - s.expected_profit) > 0.01
        """)
        assert mismatch == 0, f"total_profit mismatch: {mismatch} groups incorrect"
        log_end("test_gold_regional_total_profit_match")

    # Regression: total_quantity must match SUM(Quantity) from silver per group
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_total_quantity_match(self, spark):
        """total_quantity in gold_regional_summary matches silver per group."""
        log_start("test_gold_regional_total_quantity_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Region, State, SUM(Quantity) AS expected_qty
                FROM {SILVER_ORDERS} GROUP BY Region, State
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_REGIONAL_SUMMARY} g
            JOIN silver_agg s ON g.Region = s.Region AND g.State = s.State
            WHERE g.total_quantity != s.expected_qty
        """)
        assert mismatch == 0, f"total_quantity mismatch: {mismatch} groups incorrect"
        log_end("test_gold_regional_total_quantity_match")

    # Regression: avg_discount_pct must match AVG(Discount) * 100 from silver per group
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_avg_discount_pct_match(self, spark):
        """avg_discount_pct = AVG(Discount) * 100 in gold_regional_summary."""
        log_start("test_gold_regional_avg_discount_pct_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Region, State, ROUND(AVG(Discount) * 100, 2) AS expected_avg_discount
                FROM {SILVER_ORDERS} GROUP BY Region, State
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_REGIONAL_SUMMARY} g
            JOIN silver_agg s ON g.Region = s.Region AND g.State = s.State
            WHERE ABS(g.avg_discount_pct - s.expected_avg_discount) > 0.01
        """)
        assert mismatch == 0, f"avg_discount_pct mismatch: {mismatch} groups incorrect"
        log_end("test_gold_regional_avg_discount_pct_match")

    # Regression: profit_margin_pct must match (SUM(Profit)/SUM(Sales))*100 from silver
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_profit_margin_pct_match(self, spark):
        """profit_margin_pct = (SUM(Profit)/SUM(Sales))*100 in gold_regional_summary."""
        log_start("test_gold_regional_profit_margin_pct_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Region, State, (SUM(Profit) / SUM(Sales)) * 100 AS expected_margin
                FROM {SILVER_ORDERS} GROUP BY Region, State
                HAVING SUM(Sales) > 0
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_REGIONAL_SUMMARY} g
            JOIN silver_agg s ON g.Region = s.Region AND g.State = s.State
            WHERE ABS(g.profit_margin_pct - s.expected_margin) > 0.01
        """)
        assert mismatch == 0, f"profit_margin_pct mismatch: {mismatch} groups incorrect"
        log_end("test_gold_regional_profit_margin_pct_match")

    # Regression: profitable_orders must match COUNT(is_profitable=true) from silver
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_profitable_orders_match(self, spark):
        """profitable_orders in gold_regional_summary matches silver per group."""
        log_start("test_gold_regional_profitable_orders_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Region, State, COUNT(*) AS expected_profitable
                FROM {SILVER_ORDERS} WHERE is_profitable = true
                GROUP BY Region, State
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_REGIONAL_SUMMARY} g
            JOIN silver_agg s ON g.Region = s.Region AND g.State = s.State
            WHERE g.profitable_orders != s.expected_profitable
        """)
        assert mismatch == 0, f"profitable_orders mismatch: {mismatch} groups incorrect"
        log_end("test_gold_regional_profitable_orders_match")

    # Regression: loss_orders must match COUNT(is_profitable=false) from silver
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_loss_orders_match(self, spark):
        """loss_orders in gold_regional_summary matches silver per group."""
        log_start("test_gold_regional_loss_orders_match")
        mismatch = get_mismatch_count(spark, f"""
            WITH silver_agg AS (
                SELECT Region, State, COUNT(*) AS expected_loss
                FROM {SILVER_ORDERS} WHERE is_profitable = false
                GROUP BY Region, State
            )
            SELECT COUNT(*) AS cnt
            FROM {GOLD_REGIONAL_SUMMARY} g
            JOIN silver_agg s ON g.Region = s.Region AND g.State = s.State
            WHERE g.loss_orders != s.expected_loss
        """)
        assert mismatch == 0, f"loss_orders mismatch: {mismatch} groups incorrect"
        log_end("test_gold_regional_loss_orders_match")

    # Regression: internal consistency — order_count = profitable_orders + loss_orders
    @pytest.mark.regression
    @pytest.mark.reconciliation
    def test_gold_regional_order_count_equals_profitable_plus_loss(self, spark):
        """order_count == profitable_orders + loss_orders for every group."""
        log_start("test_gold_regional_order_count_equals_profitable_plus_loss")
        mismatch = get_mismatch_count(spark, f"""
            SELECT COUNT(*) AS cnt FROM {GOLD_REGIONAL_SUMMARY}
            WHERE order_count != (profitable_orders + loss_orders)
        """)
        assert mismatch == 0, f"order_count inconsistency: {mismatch} groups where order_count != profitable + loss"
        log_end("test_gold_regional_order_count_equals_profitable_plus_loss")