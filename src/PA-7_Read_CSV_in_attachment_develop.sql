-- Setup for Delta Lake and Sales Data Processing

-- Create Unity Catalog schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS purgo_playground;

-- Create Delta table for sales data
CREATE TABLE IF NOT EXISTS purgo_playground.sales_data_delta (
  country_cd STRING,
  product_id STRING,
  qty_sold INT,
  sales_date DATE
) USING DELTA
PARTITIONED BY (country_cd)
TBLPROPERTIES (
  'delta.appendOnly' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Vacuum to remove old data
VACUUM purgo_playground.sales_data_delta RETAIN 0 HOURS;

-- Insert data into Delta table from CSV
COPY INTO purgo_playground.sales_data_delta
FROM (SELECT * FROM '/mnt/data/sample_sales_data.csv')
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

-- Merge operation for schema evolution and versioning
MERGE INTO purgo_playground.sales_data_delta AS target
USING (
  SELECT * FROM purgo_playground.sales_data_delta
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
  UPDATE SET target.qty_sold = source.qty_sold, target.sales_date = source.sales_date
WHEN NOT MATCHED
  THEN INSERT (country_cd, product_id, qty_sold, sales_date)
  VALUES (source.country_cd, source.product_id, source.qty_sold, source.sales_date);

-- Z-order optimization on sales_date
OPTIMIZE purgo_playground.sales_data_delta
ZORDER BY (sales_date);

-- Validation and error handling for incorrect data formats or future sales_date
SELECT * FROM purgo_playground.sales_data_delta
WHERE qty_sold IS NULL AND TRY_CAST(qty_sold AS INTEGER) IS NULL
  OR sales_date > CURRENT_DATE;

-- Data skew handling: Identify skewed distributions
SELECT country_cd, COUNT(*) AS record_count
FROM purgo_playground.sales_data_delta
GROUP BY country_cd
ORDER BY record_count DESC;

-- Validate partition strategy by checking storage distribution
DESCRIBE DETAIL purgo_playground.sales_data_delta;

# PySpark Data Processing for Sales Data

# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import datetime

# Define schema for sales data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Read CSV file into DataFrame
df_sales = spark.read.csv("/mnt/data/sample_sales_data.csv", schema=schema, header=True)

# Data transformation: Convert sales_date to specified format
df_transformed = df_sales.withColumn(
    "sales_date_transformed",
    F.date_format(df_sales.sales_date, "yyyyMMdd")
)

# Handling errors and edge cases
def validate_sales_data(df):
    # Error handling: Check for future dates and invalid quantities
    error_cond = (df.sales_date > F.current_date()) | (F.isnan(df.qty_sold))
    df_errors = df.filter(error_cond).select("*")
    if df_errors.count() > 0:
        df_errors.show()  # Log errors; in production use a logger

    return df.filter(~error_cond)

df_validated = validate_sales_data(df_transformed)

# Persist data to Delta table for further analysis
df_validated.write.format("delta").mode("overwrite").partitionBy("country_cd").saveAsTable("purgo_playground.sales_data_transformed")

# Test caching strategy
df_cached = df_validated.cache()
df_cached.count()  # Trigger cache

# Test data quality: Ensure no future dates exist
assert df_cached.filter(df_cached.sales_date > F.current_date()).count() == 0, "Future sales dates found"

# Cleanup: Unpersist the cached DataFrame
df_cached.unpersist()

# Log completion of data processing
print("Sales data processing completed successfully.")