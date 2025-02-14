-- SQL Testing for Unity Catalog Integration and Data Ingestion 

-- Ensure Unity Catalog schema is available
CREATE SCHEMA IF NOT EXISTS purgo_playground;

-- Table creation with required data types
CREATE TABLE IF NOT EXISTS purgo_playground.sales_data (
  country_cd STRING,
  product_id STRING,
  qty_sold INT,
  sales_date DATE
);

-- Test Insertion: Successful Data Ingestion Scenario
-- Load sample data into the table for testing ingestion
INSERT INTO purgo_playground.sales_data
VALUES
  ("US", "P1001", 50, DATE("2024-01-15")),
  ("CA", "P1003", 25, DATE("2024-01-17")),
  ("IN", "P1001", 60, DATE("2024-01-20")),
  ("AU", "P1004", 55, DATE("2024-01-22"));

-- Validate successful insertion and correct data types
-- Check row count matches expected number of inserted rows
SELECT 
  COUNT(*) AS inserted_rows 
FROM 
  purgo_playground.sales_data;

-- Schema validation
DESCRIBE TABLE purgo_playground.sales_data;

-- Check for data with future date
-- Expect no rows to satisfy the condition
SELECT 
  * 
FROM 
  purgo_playground.sales_data 
WHERE 
  sales_date > CURRENT_DATE;

-- Cleanup operation
-- Remove test data to maintain a clean state
TRUNCATE TABLE purgo_playground.sales_data;

/* Code for Testing Data Type Conversions and NULL Handling in PySpark */
/* Begin PySpark Code Block */

# Import necessary libraries
import sys

# Install any required libraries if not already available
# %pip install some_required_library

# Import Spark required modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType, StructType, MapType
from pyspark.sql import functions as F
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Databricks PySpark Testing") \
    .getOrCreate()

# Define schema using Databricks compatible types
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Null handling test data
data_null_handling = [
    (None, "P1001", 50, datetime.strptime("2024-01-15", "%Y-%m-%d").date()),  # Null country
    ("US", None, 50, datetime.strptime("2024-01-15", "%Y-%m-%d").date()),     # Null product_id
    ("US", "P1001", None, datetime.strptime("2024-01-15", "%Y-%m-%d").date()),# Null qty_sold
    ("US", "P1001", 50, None)                                                 # Null sales_date
]

# Create DataFrame
df_null_handling = spark.createDataFrame(data_null_handling, schema)

# Test for null values presence
assert df_null_handling.where(F.col("country_cd").isNull()).count() == 1, "Null country_cd test failed"
assert df_null_handling.where(F.col("product_id").isNull()).count() == 1, "Null product_id test failed"
assert df_null_handling.where(F.col("qty_sold").isNull()).count() == 1, "Null qty_sold test failed"

# Example transformation: Convert qty_sold to STRING type
df_type_conversion = df_null_handling.withColumn("qty_sold_str", F.col("qty_sold").cast(StringType()))

# Validate converted column datatype
assert df_type_conversion.schema["qty_sold_str"].dataType == StringType(), "Data type conversion to STRING failed"

# Test complex types: ARRAY, STRUCT, MAP
# Create a DataFrame with complex types
complex_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("features", StructType([
        StructField("qty_sold", IntegerType(), True),
        StructField("sales_date", DateType(), True)
    ]), True),
    StructField("sales_dates", ArrayType(DateType()), True),
    StructField("product_map", MapType(StringType(), IntegerType()), True)
])

complex_data = [
    ("US", "P1001", {"qty_sold": 50, "sales_date": datetime.strptime("2024-01-15", "%Y-%m-%d").date()}, 
     [datetime.strptime("2024-01-15", "%Y-%m-%d").date()], {"product1": 100}),
]

df_complex = spark.createDataFrame(complex_data, complex_schema)

# Test if complex types are handled
assert df_complex.schema["features"].dataType == StructType, "STRUCT type handling failed"
assert df_complex.schema["sales_dates"].dataType == ArrayType(DateType()), "ARRAY type handling failed"
assert df_complex.schema["product_map"].dataType.keyType == StringType(), "MAP type handling failed"

# Finish Spark Session
spark.stop()

# PySpark code ends here