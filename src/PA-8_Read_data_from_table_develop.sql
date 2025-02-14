-- SQL Code for Data Integration and Validation

/* 
  Section: Data Integration and Validation
  This section handles the integration of data from sample_users_data to sample_sales_data
  and includes validation and error handling.
*/

-- Step 1: Filter valid records from sample_users_data
CREATE OR REPLACE TEMP VIEW valid_users_data AS
SELECT 
  country_cd, 
  product_id, 
  qty_sold, 
  sales_date
FROM purgo_playground.sample_users_data
WHERE valid = 1;

-- Step 2: Merge valid records into sample_sales_data
MERGE INTO purgo_playground.sample_sales_data AS target
USING valid_users_data AS source
ON target.country_cd = source.country_cd AND target.product_id = source.product_id
WHEN MATCHED THEN
  UPDATE SET target.qty_sold = target.qty_sold + source.qty_sold
WHEN NOT MATCHED THEN
  INSERT (country_cd, product_id, qty_sold, sales_date)
  VALUES (source.country_cd, source.product_id, source.qty_sold, source.sales_date);

-- Step 3: Log error for invalid records
SELECT 
  "Record is invalid and cannot be integrated" AS error_message,
  country_cd, 
  product_id, 
  qty_sold, 
  sales_date
FROM purgo_playground.sample_users_data
WHERE valid = 0;

-- Step 4: Log error for missing data
SELECT 
  "Missing data in " || column_name AS error_message
FROM (
  SELECT 
    CASE 
      WHEN country_cd IS NULL THEN 'country_cd'
      WHEN product_id IS NULL THEN 'product_id'
      WHEN qty_sold IS NULL THEN 'qty_sold'
      WHEN sales_date IS NULL THEN 'sales_date'
    END AS column_name
  FROM purgo_playground.sample_users_data
) WHERE column_name IS NOT NULL;

-- Step 5: Optimize the sample_sales_data table
OPTIMIZE purgo_playground.sample_sales_data ZORDER BY (country_cd, product_id);

-- Step 6: Vacuum the sample_sales_data table to remove old files
VACUUM purgo_playground.sample_sales_data RETAIN 168 HOURS;

# PySpark Code for Data Integration and Validation

# Section: Data Integration and Validation
# This section handles the integration of data from sample_users_data to sample_sales_data
# and includes validation and error handling.

from pyspark.sql.functions import col, sum as spark_sum, when, lit

# Load sample_users_data into DataFrame
users_df = spark.read.format("delta").load("/path/to/sample_users_data")

# Step 1: Filter valid records
valid_users_df = users_df.filter(col("valid") == 1)

# Step 2: Aggregate qty_sold for valid records
aggregated_df = valid_users_df.groupBy("country_cd", "product_id").agg(
    spark_sum("qty_sold").alias("total_qty_sold")
)

# Step 3: Merge valid records into sample_sales_data
aggregated_df.createOrReplaceTempView("aggregated_view")
spark.sql("""
    MERGE INTO purgo_playground.sample_sales_data AS target
    USING aggregated_view AS source
    ON target.country_cd = source.country_cd AND target.product_id = source.product_id
    WHEN MATCHED THEN
      UPDATE SET target.qty_sold = target.qty_sold + source.total_qty_sold
    WHEN NOT MATCHED THEN
      INSERT (country_cd, product_id, qty_sold, sales_date)
      VALUES (source.country_cd, source.product_id, source.total_qty_sold, current_date())
""")

# Step 4: Log error for invalid records
invalid_records_df = users_df.filter(col("valid") == 0)
invalid_records_df.withColumn("error_message", lit("Record is invalid and cannot be integrated")).show()

# Step 5: Log error for missing data
missing_data_df = users_df.filter(
    col("country_cd").isNull() | 
    col("product_id").isNull() | 
    col("qty_sold").isNull() | 
    col("sales_date").isNull()
)
missing_data_df.withColumn("error_message", lit("Missing data in one or more columns")).show()

# Step 6: Optimize the sample_sales_data table
spark.sql("OPTIMIZE purgo_playground.sample_sales_data ZORDER BY (country_cd, product_id)")

# Step 7: Vacuum the sample_sales_data table to remove old files
spark.sql("VACUUM purgo_playground.sample_sales_data RETAIN 168 HOURS")