# PYSPARK DATA PROCESSING CODE

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, to_date, when
from delta.tables import DeltaTable

# Define schema for input data
input_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", StringType(), True)  # Use StringType to handle date validation
])

# Read CSV into DataFrame
df = spark.read.csv("/path/to/sample_sales_data.csv", header=True, schema=input_schema)

# Validate and transform data
df_clean = df.withColumn(
    "sales_date", 
    when(~col("sales_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), None).otherwise(col("sales_date"))
).withColumn(
    "qty_sold", 
    when(col("qty_sold") <= 0, None).otherwise(col("qty_sold"))
)

# Data Quality Checks and Logging
if df_clean.filter((col("sales_date").isNull()) | (col("qty_sold").isNull())).count() > 0:
    df_clean.filter(col("sales_date").isNull()).foreach(lambda row: print(f"Invalid date format for {row.product_id} at {row.sales_date}"))
    df_clean.filter(col("qty_sold").isNull()).foreach(lambda row: print(f"Invalid quantity for {row.product_id} at {row.sales_date}. Quantity must be a positive integer."))

# Perform necessary transformations
df_final = df_clean.dropna()  # Remove rows with NULL values post validation

# Prepare Delta Lake Table
table_name = "purgo_playground.sales_data"

# Optimization Strategies and Table Management
if not DeltaTable.isDeltaTable(spark, f"/delta/{table_name}"):
    df_final.write.format("delta").mode("overwrite").saveAsTable(table_name)
else:
    delta_table = DeltaTable.forPath(spark, f"/delta/{table_name}")
    delta_table.alias("tgt").merge(
        df_final.alias("src"),
        "tgt.product_id = src.product_id AND tgt.sales_date = src.sales_date"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Optimize and Vacuum the Delta Table
spark.sql(f"OPTIMIZE {table_name} ZORDER BY (product_id)")
spark.sql(f"VACUUM {table_name} RETAIN 0 HOURS")

# Note: Ensure the path '/path/to/sample_sales_data.csv' is accessible with the correct path in Databricks
# and that data security measures like access control are set up accordingly.

-- SQL DATA PROCESSING AND INTEGRITY CHECKS

-- Drop and create the table for new data ingestion
DROP TABLE IF EXISTS purgo_playground.sales_data;
CREATE TABLE purgo_playground.sales_data (
    country_cd STRING,
    product_id STRING,
    qty_sold INTEGER,
    sales_date STRING
) USING DELTA;

-- Insert only valid data, ensuring integrity constraints
INSERT INTO purgo_playground.sales_data
SELECT * FROM (
    SELECT DISTINCT country_cd, product_id, qty_sold, sales_date
    FROM delta.`/delta/purgo_playground.sales_data`
    WHERE qty_sold > 0 AND sales_date RLIKE '^\d{4}-\d{2}-\d{2}$'
);

-- Perform data compliance checks
-- Check GDPR compliance logging
INSERT INTO logging_table -- Placeholder for actual logging table
SELECT current_timestamp() AS log_time, "GDPR compliance check: Valid" AS message
WHERE EXISTS (SELECT * FROM purgo_playground.sales_data WHERE sales_date IS NOT NULL);

-- Ensure only authorized access based on roles
GRANT SELECT ON purgo_playground.sales_data TO `data_viewer`;

# BASH SCRIPT FOR LIBRARY INSTALLATION (ONLY IF NECESSARY)

# Libraries installation (if not already available)
# These commands should be checked and executed in Databricks notebook terminal or environment
# Double-check library availability within Databricks Runtime

# Install Delta Lake package
# Assuming Delta Lake libraries are necessary but often come pre-installed in Databricks
# This would typically be handled by cluster configuration or administration settings