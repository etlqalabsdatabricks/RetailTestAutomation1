# Retail pipeline table configuration

# Bronze table
BRONZE_ORDERS = "retail.bronze.bronze_orders"

# Silver table
SILVER_ORDERS = "retail.silver.silver_orders"

# Columns common to both bronze and silver (used for reconciliation)
# Note: 'Sub-Category' in bronze was renamed to 'Sub_Category' in silver
COMMON_COLUMNS = [
    "Ship_Mode",
    "Segment",
    "Country",
    "City",
    "State",
    "Postal_Code",
    "Region",
    "Category",
    "Sales",
    "Quantity",
    "Discount",
    "Profit",
    "_ingestion_timestamp",
    "_source_file",
]

# Column name mapping: bronze column name -> silver column name
COLUMN_MAPPING = {
    "Sub-Category": "Sub_Category",
}

# Silver-only derived columns (not in bronze)
SILVER_DERIVED_COLUMNS = [
    "order_id",
    "discount_amount",
    "net_sales",
    "profit_margin_pct",
    "is_profitable",
    "silver_processed_timestamp",
]

# Report and log paths
REPORT_DIR = "reports"
LOG_DIR = "logs"
DIFFERENCES_DIR = "differences"