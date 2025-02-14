-- SQL Test Code for Databricks Environment

-- Install necessary SQL libraries if not already installed
-- (Currently, no additional installations needed for SQL.)

-- Test to validate correct data typing
CREATE OR REPLACE TEMPORARY VIEW sales_data_correct_types AS
SELECT CAST(country_cd AS STRING) AS country_cd,
       CAST(product_id AS STRING) AS product_id,
       CAST(qty_sold AS INT) AS qty_sold,
       CAST(sales_date AS DATE) AS sales_date
FROM purgo_playground.sales_data;

-- Unit test for data type validation
SELECT ASSERT(
    (DATA_TYPE = 'STRING' AND FIELD = 'country_cd') OR
    (DATA_TYPE = 'STRING' AND FIELD = 'product_id') OR
    (DATA_TYPE = 'INT' AND FIELD = 'qty_sold') OR
    (DATA_TYPE = 'DATE' AND FIELD = 'sales_date'),
    'Data type does not match', FIELD)
FROM (DESCRIBE FORMATTED sales_data_correct_types) AS temp
WHERE TEMP.COL_NAME NOT IN ('# col_name', 'country_cd', 'product_id', 'qty_sold', 'sales_date', '');

-- Test for invalid data format
-- Assuming execution fails elsewhere, use try-catch or transaction log to assert failure
-- This section is for demonstration purposes
BEGIN
    TRY
        SELECT CAST(qty_sold AS INT) FROM purgo_playground.invalid_sales_data;
    CATCH (err)
        THEN RAISE 'Invalid data format for qty_sold. Expected INTEGER.';
    END TRY;

-- Test future date in sales_date
CREATE OR REPLACE TEMPORARY VIEW future_sales_date AS
SELECT country_cd, product_id, qty_sold, sales_date
FROM purgo_playground.sales_data
WHERE sales_date > CURRENT_DATE();

-- Expected outcome: 0 rows
SELECT COUNT(*) AS future_date_records FROM future_sales_date;

-- Cleanup operations
DROP VIEW IF EXISTS sales_data_correct_types;
DROP VIEW IF EXISTS future_sales_date;

# PySpark Test Code for Databricks Environment

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as F

# Initialize Spark Session if not already initialized
spark = SparkSession.builder \
    .appName("Databricks Testing") \
    .getOrCreate()

# Schema definition for the test data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Load test data
test_data = [("US", "P1001", 50, "2024-01-15"),"]

# Create DataFrame from test data
df_test = spark.createDataFrame(test_data, schema)

# Validate schema
assert df_test.schema == schema, "Schema does not match"

# Add dummy column to test transformations
df_test_transformed = df_test.withColumn("qty_sold_transformed", df_test["qty_sold"] * 1)

# Assert transformation correctness
assert df_test_transformed.select("qty_sold_transformed").collect() == df_test.select("qty_sold").collect(), \
    "Transformation not applied correctly"

# Validate NULL handling
df_null_check = df_test.filter(df_test["country_cd"].isNull())
assert df_null_check.count() == 0, "NULL values detected in 'country_cd'"

# Clean up temporary data
df_test.unpersist()
df_test_transformed.unpersist()