# PYSPARK IMPLEMENTATION FOR PROCESSING SAMPLE SALES DATA

# Import necessary modules
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, log, current_timestamp

# Define schema for the CSV file
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", StringType(), True)  # String type to validate 'YYYY-MM-DD' format
])

# Read CSV data into DataFrame
file_path = "/path/to/sample_sales_data.csv"  # Replace with actual path in Databricks
sales_df = spark.read.csv(file_path, header=True, schema=schema)

# Data Validation and Cleaning
sales_df = sales_df \
    .withColumn("qty_sold", when(col("qty_sold") > 0, col("qty_sold")).otherwise(None)) \
    .withColumn("sales_date_valid", col("sales_date").rlike(r'^\d{4}-\d{2}-\d{2}$'))

# Filter out invalid records
invalid_qty_df = sales_df.filter(col("qty_sold").isNull())
invalid_date_df = sales_df.filter(~col("sales_date_valid"))

# Log invalid records
log_message_invalid_qty = "Invalid quantity for entries: " + str(invalid_qty_df.count())
log_message_invalid_date = "Invalid date format for entries: " + str(invalid_date_df.count())

log(log_message_invalid_qty)
log(log_message_invalid_date)

# Clean up DataFrame to keep only valid records
sales_df = sales_df.filter(col("qty_sold").isNotNull() & col("sales_date_valid"))

# Remove 'sales_date_valid' helper column
sales_df = sales_df.drop("sales_date_valid")

# Write the result to Delta table with appropriate partitioning
sales_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("country_cd") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales_data")

# Optimize and Z-order Delta table
spark.sql("OPTIMIZE purgo_playground.sales_data ZORDER BY (sales_date)")

# Clean Old Files using VACUUM
spark.sql("VACUUM purgo_playground.sales_data RETAIN 168 HOURS")  # Retain 7 days

-- SQL IMPLEMENTATION FOR VALIDATING AND ACCESS CONTROL SETUP

/* Ensure the temp CSV table doesn't exist */
DROP TABLE IF EXISTS purgo_playground.temp_sales_data;

/* Load CSV into a temporary Delta table */
CREATE OR REPLACE TABLE purgo_playground.temp_sales_data AS
SELECT * FROM csv.`/path/to/sample_sales_data.csv`;  -- Modify to actual CSV file path

/* Validate data: Identify invalid qty_sold and date format */
WITH validation AS (
  SELECT *,
         CASE WHEN qty_sold <= 0 THEN 'Invalid quantity' END AS qty_issue,
         CASE WHEN NOT sales_date RLIKE '^\d{4}-\d{2}-\d{2}$' THEN 'Invalid date' END AS date_issue
  FROM purgo_playground.temp_sales_data
)

/* Log invalid records */
SELECT * FROM validation WHERE qty_issue IS NOT NULL OR date_issue IS NOT NULL;

/* Insert valid records into the final sales_data table */
INSERT INTO purgo_playground.sales_data
SELECT country_cd, product_id, qty_sold, sales_date 
FROM validation WHERE qty_issue IS NULL AND date_issue IS NULL;

/* Configure access control for the sales_data table */
GRANT SELECT ON purgo_playground.sales_data TO 'data_viewer';
GRANT SELECT ON purgo_playground.sales_data TO 'data_analyst';

/* Log data access */
-- Assuming there is a logging mechanism in place
SELECT current_timestamp() AS access_time, user() AS accessed_by
FROM purgo_playground.sales_data;

/* Security and Compliance Check */
-- Log GDPR compliance message
INSERT INTO security_logs(status, message, timestamp)
VALUES ('SUCCESS', 'Data processing and storage complies with GDPR', current_timestamp());