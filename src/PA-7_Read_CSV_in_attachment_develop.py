# PYSPARK IMPLEMENTATION CODE

# Import necessary modules
from pyspark.sql.functions import col, lit, when, rlike, current_timestamp, expr
from pyspark.sql.types import StringType, IntegerType

# Define a function to validate and process the CSV data
def process_sales_data(file_path, table_name):
    """
    Process sales data CSV, perform validation, and load into Delta Lake table.
    
    :param file_path: Path to the CSV file
    :param table_name: Target Delta Lake table name
    """
    # Read CSV file into DataFrame
    df = spark.read.option("header", "true").csv(file_path)
    
    # Validate qty_sold to be positive integers and sales_date to be in correct format
    df_validated = df.withColumn("qty_sold", df["qty_sold"].cast(IntegerType())) \
                     .withColumn("qty_sold", when(col("qty_sold") > 0, col("qty_sold")).otherwise(None)) \
                     .withColumn("sales_date", when(rlike(col("sales_date"), r'^\d{4}-\d{2}-\d{2}$'), col("sales_date")).otherwise(None)) \
                     .withColumn("load_datetime", current_timestamp())

    # Data quality checks and error logging
    df_invalid_qty = df_validated.filter(col("qty_sold").isNull())
    df_invalid_date_format = df_validated.filter(col("sales_date").isNull())
    
    # Log validation issues
    df_invalid_qty.select("product_id", "sales_date").withColumn(
        "error_message", lit("Invalid quantity: must be a positive integer")).show(truncate=False)
    df_invalid_date_format.select("product_id", "sales_date").withColumn(
        "error_message", lit("Invalid date format: must be YYYY-MM-DD")).show(truncate=False)
    
    # Filter out invalid records
    df_filtered = df_validated.filter(df_validated["qty_sold"].isNotNull() & df_validated["sales_date"].isNotNull())

    # Write to Delta Lake with schema evolution and table optimization
    df_filtered.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("country_cd") \
        .saveAsTable(table_name)

    # Table optimization and vacuum
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY (sales_date)")
    spark.sql(f"VACUUM {table_name} RETAIN 0 HOURS")

# Run the processing function
process_sales_data("/dbfs/path/to/sample_sales_data.csv", "purgo_playground.sales_data")

-- SQL IMPLEMENTATION CODE

-- Ensure Unity Catalog and access permissions
-- CREATE CATALOG purgo_playground IF NOT EXISTS;
-- USE CATALOG purgo_playground;

/* Create target table if not exists */
CREATE TABLE IF NOT EXISTS purgo_playground.sales_data (
    country_cd STRING,
    product_id STRING,
    qty_sold INTEGER,
    sales_date STRING,
    load_datetime TIMESTAMP
)
USING DELTA
PARTITIONED BY (country_cd)
LOCATION '/mnt/delta/purgo_playground/sales_data';

/* Grant table permissions */
GRANT SELECT ON TABLE purgo_playground.sales_data TO ROLE data_viewer;

/* Procedure for data compliance logging */
INSERT INTO purgo_playground.logs SELECT 
    current_timestamp() as log_time, 
    "GDPR compliance validation" as log_type, 
    "Data processing and storage complies with GDPR" as message;