-- SQL Test Code for Databricks Environment

/* 
  Section: Setup and Configuration
  Ensure necessary libraries are installed and configured
  This section is for any setup that might be needed before running tests
*/

-- Test for Successful Data Extraction for Valid Records
SELECT * FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1;

-- Validate that the 'valid' column only contains the value 1
SELECT DISTINCT valid FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1;

-- Test for Error when Accessing Non-Existent Table
-- This should throw an error
-- SELECT * FROM agilisium_playground.purgo_playground.non_existent_table;

-- Data Validation for Extracted Records
SELECT * FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1
AND country_cd NOT REGEXP '^[A-Z]{2}$';

SELECT * FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1
AND product_id NOT REGEXP '^P\d{4}$';

SELECT * FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1
AND qty_sold < 0;

SELECT * FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1
AND sales_date NOT REGEXP '^\d{4}-\d{2}-\d{2}$';

-- Cleanup operations if necessary
-- DROP TABLE IF EXISTS agilisium_playground.purgo_playground.sample_users_data;

# PySpark Test Code for Databricks Environment

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, regexp_extract, lit

# Initialize Spark session
spark = SparkSession.builder.appName("DatabricksTest").getOrCreate()

# Define schema for sample_users_data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True),
    StructField("valid", IntegerType(), True)
])

# Load data from the table
df = spark.read.format("delta").schema(schema).load("/mnt/delta/agilisium_playground/purgo_playground/sample_users_data")

# Unit Test: Validate Data Types
assert df.schema["country_cd"].dataType == StringType()
assert df.schema["product_id"].dataType == StringType()
assert df.schema["qty_sold"].dataType == IntegerType()
assert df.schema["sales_date"].dataType == DateType()
assert df.schema["valid"].dataType == IntegerType()

# Integration Test: Validate Data Extraction
valid_df = df.filter(col("valid") == 1)
assert valid_df.count() > 0

# Data Quality Test: Validate country_cd format
invalid_country_cd_df = valid_df.filter(~col("country_cd").rlike("^[A-Z]{2}$"))
assert invalid_country_cd_df.count() == 0, "Invalid country code format detected"

# Data Quality Test: Validate product_id format
invalid_product_id_df = valid_df.filter(~col("product_id").rlike("^P\d{4}$"))
assert invalid_product_id_df.count() == 0, "Invalid product ID format detected"

# Data Quality Test: Validate qty_sold is positive
negative_qty_sold_df = valid_df.filter(col("qty_sold") < 0)
assert negative_qty_sold_df.count() == 0, "Quantity sold cannot be negative"

# Data Quality Test: Validate sales_date format
invalid_sales_date_df = valid_df.filter(~col("sales_date").cast("string").rlike("^\d{4}-\d{2}-\d{2}$"))
assert invalid_sales_date_df.count() == 0, "Invalid sales date format detected"

# Cleanup operations if necessary
# df.unpersist()