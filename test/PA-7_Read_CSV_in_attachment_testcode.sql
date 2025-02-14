/*
  Set up and configuration information.
  Ensure Unity Catalog schema "purgo_playground" is created and accessible.
*/

-- Create the necessary tables for testing
CREATE TABLE IF NOT EXISTS purgo_playground.sales_data (
    country_cd STRING,
    product_id STRING,
    qty_sold INTEGER,
    sales_date DATE
);

-- Ensure clean slate before tests
DELETE FROM purgo_playground.sales_data;

/*
  Test for Happy Path - Successful Data Ingestion and Storage
*/

-- Validate schema integrity on data ingestion
INSERT INTO purgo_playground.sales_data (country_cd, product_id, qty_sold, sales_date)
VALUES 
    ("US", "P1001", 50, "2024-01-15"),
    ("CA", "P1003", 25, "2024-01-17"),
    ("IN", "P1001", 60, "2024-01-20"),
    ("AU", "P1004", 55, "2024-01-22");

/* 
  Validate correct data storage 
*/
SELECT 
    assert_typeof(country_cd, 'STRING'),
    assert_typeof(product_id, 'STRING'),
    assert_typeof(qty_sold, 'INTEGER'),
    assert_typeof(sales_date, 'DATE')
FROM purgo_playground.sales_data;

/*
  Test for Validation - Incorrect Data Format
*/

-- Attempt to insert incorrect data formats

-- Expected to fail with "Invalid data format for 'qty_sold'. Expected INTEGER."
INSERT INTO purgo_playground.sales_data (country_cd, product_id, qty_sold, sales_date)
VALUES ("US", "P1001", "invalid", "2024-01-15");

-- Validate failure
SELECT *,
       CASE WHEN IS_NUMBER(qty_sold) THEN 1 ELSE error("Invalid data format for 'qty_sold'. Expected INTEGER.") END AS validation
FROM purgo_playground.sales_data;

/*
  Test for Validation - Future Date in Sales Date
*/

-- Attempt to insert a future date
INSERT INTO purgo_playground.sales_data (country_cd, product_id, qty_sold, sales_date)
VALUES ("US", "P1001", 50, "2025-01-15");

-- Validate failure for future dates
SELECT *,
       CASE WHEN sales_date > current_date() THEN error("Sales date cannot be in the future.") ELSE 1 END AS validation
FROM purgo_playground.sales_data;

/*
  Test for Error Handling - Database Connectivity
*/

-- Simulate database connection error by shutting down the database service
-- Validate catching and log of connection error
-- Assert expected error message

/*
  Test SQL functions and Analytic Features
*/

/* 
  Delta Lake Operations Test (if applicable)
*/

-- Test Delta Lake specific operations: MERGE, UPDATE, DELETE
MERGE INTO purgo_playground.sales_data AS target
USING (SELECT 'US' AS country_cd, 'P1001' AS product_id, 60 AS qty_sold, "2024-01-15" AS sales_date) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN 
    UPDATE SET qty_sold = source.qty_sold;

-- Validate the update process
SELECT qty_sold FROM purgo_playground.sales_data WHERE product_id = 'P1001';

/*
  Clean up after tests
*/

-- Drop temporary tables and clean up data if required
DROP TABLE IF EXISTS purgo_playground.sales_data;

# Import necessary libraries for PySpark testing
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# Initialize SparkSession with necessary configurations for tests
spark = SparkSession.builder \
    .appName("Databricks Test Suite") \
    .getOrCreate()

# Define schema according to Databricks native data types
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Sample data matching expected schema formats for valid scenarios
data_valid = [
    ("US", "P1001", 50, "2024-01-15"),
    ("CA", "P1003", 25, "2024-01-17"),
    ("IN", "P1001", 60, "2024-01-20"),
    ("AU", "P1004", 55, "2024-01-22")
]

# Create DataFrame for test data
df_valid = spark.createDataFrame(data_valid, schema)

# Unit test for individual transformation: Verify data type conversion and schema
def test_schema():
    assert df_valid.schema == schema, "Schema validation failed."

test_schema()

# Validate NULL handling within rows
data_with_null = [
    (None, "P1001", 50, "2024-01-15"),
    ("US", None, 50, "2024-01-15"),
    ("US", "P1001", None, "2024-01-15"),
    ("US", "P1001", 50, None)
]

df_null = spark.createDataFrame(data_with_null, schema)

# Test for NULL values handling
def test_null_values_handling():
    assert df_null.filter(col("country_cd").isNull()).count() == 1, "Null country_cd test failed."
    assert df_null.filter(col("product_id").isNull()).count() == 1, "Null product_id test failed."
    assert df_null.filter(col("qty_sold").isNull()).count() == 1, "Null qty_sold test failed."
    assert df_null.filter(col("sales_date").isNull()).count() == 1, "Null sales_date test failed."

test_null_values_handling()

# Integration tests for end-to-end DataFrame operations

# Mock transformation function for example preprocessing step
def preprocessing_func(df):
    return df.withColumn("qty_sold_transformed", col("qty_sold")) \
             .withColumn("sales_date_transformed", col("sales_date").cast("string"))

# Test transformation logic
df_transformed = preprocessing_func(df_valid)

# Validate transformation
def test_transformation():
    assert "qty_sold_transformed" in df_transformed.columns, "Transformation for qty_sold_transformed failed."
    assert "sales_date_transformed" in df_transformed.columns, "Transformation for sales_date_transformed failed."

test_transformation()

# Include cleanup code if necessary to drop test tables or reset states
spark.stop()