-- SQL Test Code for Databricks Environment

/* 
  Section: Setup and Configuration
  Ensure necessary libraries are installed and configured
  This section is for SQL-based tests
*/

-- Test for Data Type Conversion and NULL Handling
SELECT 
  CAST(country_cd AS STRING) AS country_cd,
  CAST(product_id AS STRING) AS product_id,
  CAST(qty_sold AS INT) AS qty_sold,
  CAST(sales_date AS DATE) AS sales_date,
  CAST(valid AS INT) AS valid
FROM purgo_playground.sample_users_data
WHERE valid IS NOT NULL;

-- Validate Complex Types and NULL Handling
SELECT 
  ARRAY(country_cd, product_id) AS country_product_array,
  STRUCT(country_cd, product_id, qty_sold) AS sales_struct,
  MAP(country_cd, qty_sold) AS sales_map
FROM purgo_playground.sample_users_data
WHERE country_cd IS NOT NULL AND product_id IS NOT NULL;

-- Test Delta Lake Operations: MERGE, UPDATE, DELETE
MERGE INTO purgo_playground.sample_sales_data AS target
USING purgo_playground.sample_users_data AS source
ON target.country_cd = source.country_cd AND target.product_id = source.product_id
WHEN MATCHED AND source.valid = 1 THEN
  UPDATE SET target.qty_sold = target.qty_sold + source.qty_sold
WHEN NOT MATCHED AND source.valid = 1 THEN
  INSERT (country_cd, product_id, qty_sold, sales_date)
  VALUES (source.country_cd, source.product_id, source.qty_sold, source.sales_date);

-- Validate Window Functions and Analytics Features
SELECT 
  country_cd, 
  product_id, 
  SUM(qty_sold) OVER (PARTITION BY country_cd ORDER BY sales_date) AS cumulative_qty_sold
FROM purgo_playground.sample_sales_data;

-- Cleanup Operations
DELETE FROM purgo_playground.sample_sales_data WHERE qty_sold < 0;

-- Validate Schema
DESCRIBE TABLE purgo_playground.sample_sales_data;

# PySpark Test Code for Databricks Environment

# Section: Setup and Configuration
# Ensure necessary libraries are installed and configured
# This section is for PySpark-based tests

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, sum as spark_sum, when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DatabricksTest") \
    .getOrCreate()

# Define schema for sample_users_data
users_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True),
    StructField("valid", IntegerType(), True)
])

# Load data into DataFrame
users_df = spark.read.format("delta").schema(users_schema).load("/path/to/sample_users_data")

# Unit Test: Validate Data Type Conversion
assert users_df.select("country_cd").dtypes[0][1] == 'string'
assert users_df.select("qty_sold").dtypes[0][1] == 'int'

# Integration Test: Validate Data Integration
integrated_df = users_df.filter(col("valid") == 1)
assert integrated_df.count() > 0

# Performance Test: Validate Data Transformation
transformed_df = integrated_df.groupBy("country_cd", "product_id").agg(spark_sum("qty_sold").alias("total_qty_sold"))
assert transformed_df.count() > 0

# Data Quality Validation Test: NULL Handling
null_handling_df = users_df.filter(col("country_cd").isNull() | col("product_id").isNull())
assert null_handling_df.count() == 0

# Delta Lake Operations: Validate MERGE
users_df.createOrReplaceTempView("users_view")
spark.sql("""
    MERGE INTO purgo_playground.sample_sales_data AS target
    USING users_view AS source
    ON target.country_cd = source.country_cd AND target.product_id = source.product_id
    WHEN MATCHED AND source.valid = 1 THEN
      UPDATE SET target.qty_sold = target.qty_sold + source.qty_sold
    WHEN NOT MATCHED AND source.valid = 1 THEN
      INSERT (country_cd, product_id, qty_sold, sales_date)
      VALUES (source.country_cd, source.product_id, source.qty_sold, source.sales_date)
""")

# Cleanup Operations
users_df.filter(col("qty_sold") < 0).drop().show()