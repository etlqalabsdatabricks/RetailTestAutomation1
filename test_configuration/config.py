# Retail pipeline table configuration

# --------------------------------------------------------------------------------------
# Raw CSV file (source for the bronze layer)
# --------------------------------------------------------------------------------------
# The raw CSV is stored in a Unity Catalog volume and loaded into the bronze table.
# CSV column names use spaces (e.g., "Ship Mode", "Postal Code"), which are renamed
# to underscores in the bronze table (e.g., "Ship_Mode", "Postal_Code").
RAW_CSV_PATH = "/Volumes/retail/bronze/raw/SampleSuperstore.csv"

# Column name mapping: CSV column name -> bronze table column name
# Only columns that differ between CSV and bronze are listed here.
RAW_TO_BRONZE_COLUMN_MAPPING = {
    "Ship Mode": "Ship_Mode",
    "Postal Code": "Postal_Code",
}

# Metadata columns added during bronze ingestion (not present in the raw CSV)
BRONZE_METADATA_COLUMNS = [
    "_ingestion_timestamp",
    "_source_file",
]

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

# --------------------------------------------------------------------------------------
# Gold tables (aggregation layer derived from silver)
# --------------------------------------------------------------------------------------

# Gold segment summary — aggregated by Segment + Ship_Mode
GOLD_SEGMENT_SUMMARY = "retail.gold.gold_segment_summary"

# Gold regional summary — aggregated by Region + State
GOLD_REGIONAL_SUMMARY = "retail.gold.gold_regional_summary"

# Grouping keys for each gold table (used in reconciliation tests to join
# gold data back to silver aggregations for verification)
GOLD_SEGMENT_GROUP_KEYS = ["Segment", "Ship_Mode"]
GOLD_REGIONAL_GROUP_KEYS = ["Region", "State"]

# Columns expected in gold_segment_summary
GOLD_SEGMENT_COLUMNS = [
    "Segment",
    "Ship_Mode",
    "order_count",
    "total_sales",
    "total_profit",
    "avg_order_value",
    "total_quantity",
    "total_discount_given",
]

# Columns expected in gold_regional_summary
GOLD_REGIONAL_COLUMNS = [
    "Region",
    "State",
    "order_count",
    "total_sales",
    "total_profit",
    "total_quantity",
    "avg_discount_pct",
    "profit_margin_pct",
    "profitable_orders",
    "loss_orders",
]

# Report and log paths
REPORT_DIR = "reports"
LOG_DIR = "logs"
DIFFERENCES_DIR = "differences"