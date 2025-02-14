-- SQL Code Block for Databricks Testing

-- Framework and structure imports for Databricks SQL
-- No additional libraries are needed for SQL-only Databricks tests

-- Setup the test table if not already available
CREATE OR REPLACE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
    id BIGINT,
    dnsa_flag STRING,
    financial_qty DOUBLE
);

-- Insert test data
INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES
(1, "Y", 150.0),
(2, "N", 200.0),
(3, "Y", 300.0),
(7, "Y", NULL),
(8, NULL, 120.0);

-- Unit test for individual transformations
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

-- Integration test for end-to-end flows
WITH total_inventory AS (
    SELECT SUM(financial_qty) AS total_financial_qty
    FROM agilisium_playground.purgo_playground.f_inv_movmnt
),
inventory_at_risk AS (
    SELECT SUM(financial_qty) AS risk_qty
    FROM agilisium_playground.purgo_playground.f_inv_movmnt
    WHERE dnsa_flag = "Y"
)
SELECT 
    (risk_qty / total_financial_qty) * 100 AS percentage_of_inventory_at_risk
FROM total_inventory, inventory_at_risk;

-- Databricks-specific operations tests
-- Validate Delta Lake operations (if applicable)
-- Not applicable in the current context due to the absence of Delta operations

-- SQL Cleanup
DROP TABLE IF EXISTS agilisium_playground.purgo_playground.f_inv_movmnt;

# PySpark Code Block for Databricks Testing

# Install necessary libraries if not already installed.
# In Databricks, commonly required libraries for testing may be already available.
# Example: dbutils.library.installPyPI("pytest")

# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col

# Initiate Spark Session
spark = SparkSession.builder.appName("Databricks Test").getOrCreate()

# Test: Schema Validation
schema = "id LONG, dnsa_flag STRING, financial_qty DOUBLE"
assert str(spark.read.table("agilisium_playground.purgo_playground.f_inv_movmnt").schema) == schema

# Unit test for individual transformation, ensure correctness of SQL logic
df_risk = spark.sql(
    """
    SELECT SUM(financial_qty) AS inventory_at_risk
    FROM agilisium_playground.purgo_playground.f_inv_movmnt
    WHERE dnsa_flag = 'Y'
    """
)

assert df_risk.collect()[0]['inventory_at_risk'] == 450.0  # As per example test data

# Integration test for calculation
df_total = spark.sql(
    """
    SELECT SUM(financial_qty) AS total_inventory
    FROM agilisium_playground.purgo_playground.f_inv_movmnt
    """
)

df_percentage = df_risk.crossJoin(df_total).select(
    (col("inventory_at_risk") / col("total_inventory") * 100).alias("percentage_of_inventory_at_risk")
)

assert df_percentage.collect()[0]['percentage_of_inventory_at_risk'] == 75.0  # Validate with mock data

# Additional test for NULL handling
df_null_check = spark.sql(
    """
    SELECT *
    FROM agilisium_playground.purgo_playground.f_inv_movmnt
    WHERE financial_qty IS NULL
    """
)

assert df_null_check.count() == 1  # Based on the test data provided

-- Cleanup Code Block

-- This block should be run after all tests to ensure no residual data
-- DROP TABLE IF EXISTS statement to clean up created test data

-- Cleanup test table after execution
DROP TABLE IF EXISTS agilisium_playground.purgo_playground.f_inv_movmnt;