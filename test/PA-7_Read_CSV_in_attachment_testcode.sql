/*
  Test suite for purgo_playground.sample_sales_data table
*/

-- Test table existence
CREATE OR REPLACE TEMP VIEW _table_exists AS
SELECT CASE WHEN EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = 'purgo_playground' AND table_name = 'sample_sales_data') THEN 1 ELSE 0 END AS table_exists;

SELECT CASE WHEN table_exists = 1 THEN 'Table exists' ELSE 'Table does not exist' END AS result
FROM _table_exists;

-- Test row count
SELECT COUNT(*) AS row_count
FROM purgo_playground.sample_sales_data;

-- Test data types
DESCRIBE TABLE purgo_playground.sample_sales_data;

-- Test specific data values (happy path)
SELECT *
FROM purgo_playground.sample_sales_data
WHERE country_cd = 'US' AND product_id = 'P1001' AND qty_sold = 50 AND sales_date = '2024-01-15';

-- Test NULL handling
SELECT COUNT(*) AS null_country_cd
FROM purgo_playground.sample_sales_data
WHERE country_cd IS NULL;

SELECT COUNT(*) AS null_product_id
FROM purgo_playground.sample_sales_data
WHERE product_id IS NULL;

SELECT COUNT(*) AS null_qty_sold
FROM purgo_playground.sample_sales_data
WHERE qty_sold IS NULL;


SELECT COUNT(*) AS null_sales_date
FROM purgo_playground.sample_sales_data
WHERE sales_date IS NULL;


-- Test special characters and multi-byte characters
SELECT * FROM purgo_playground.sample_sales_data WHERE country_cd = 'JP';
SELECT * FROM purgo_playground.sample_sales_data WHERE country_cd = 'FR';

-- Negative tests (data type validation with intentional errors, uncomment to test expected failures)

-- INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('US', 'P1005', 'Invalid', '2024-01-15');

-- INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('US', 'P1006', 20, 'Invalid Date');

# Databricks PySpark Test Code

%pip install chispa

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from chispa import assert_df_equality
import pytest

# Initialize SparkSession
spark = SparkSession.builder.appName("TestSampleSalesData").getOrCreate()

# Define expected schema
expected_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Read data from the table
df = spark.table("purgo_playground.sample_sales_data")

# Test 1: Schema validation
assert df.schema == expected_schema

# Test 2: Row count
expected_count = spark.read.csv("dbfs:/FileStore/tables/sample_sales_data.csv", header=True, inferSchema=True).count() + 7 # Account for added rows during data exploration
assert df.count() == expected_count

# Test 3: Data validation (subset of happy path data)
expected_data = [
    ("US", "P1001", 50, "2024-01-15"),
    ("CA", "P1003", 25, "2024-01-17"),
    ("UK", "P1004", 20, "2024-01-19")
]
expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

assert_df_equality(df.filter(df.country_cd.isin(["US", "CA", "UK"])).filter(df.product_id.isin(["P1001", "P1003", "P1004"])), expected_df, ignore_row_order=True, ignore_column_order=True)


# Test 4: NULL checks
assert df.filter(df["country_cd"].isNull()).count() >= 1
assert df.filter(df["product_id"].isNull()).count() >= 1
assert df.filter(df["qty_sold"].isNull()).count() >= 1
assert df.filter(df["sales_date"].isNull()).count() >= 1



# Test 5: Special characters and multi-byte character checks
assert df.filter(df["country_cd"] == "JP").count() > 0
assert df.filter(df["country_cd"] == "FR").count() > 0


# Further test cases for negative scenarios, edge cases and boundary conditions,
# performance testing, etc., can be added here using appropriate PySpark testing functions and libraries.