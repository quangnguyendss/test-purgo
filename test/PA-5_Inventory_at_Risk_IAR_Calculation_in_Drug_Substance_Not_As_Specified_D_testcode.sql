/* Spark SQL Test for Inventory at Risk Calculation */

/* 
-- Setup Section
-- Ensure necessary permissions and environment setup
-- Import any additional required libraries
-- Establish connection with Unity Catalog
*/

-- Unit Test: Calculate inventory at risk where DNSA flag is 'Y'
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y"

-- Validate the results
-- Expected: Should return the sum of all financial_qty where dnsa_flag is 'Y'

-- Integration Test: Calculate percentage of inventory at risk
WITH total_inventory AS (
  -- Simulated Query to get total inventory
  SELECT SUM(financial_qty) AS total_inventory
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
),
inventory_at_risk AS (
  SELECT SUM(financial_qty) AS risk_qty
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
  WHERE dnsa_flag = "Y"
)
SELECT (risk_qty / total_inventory) * 100 AS percentage_of_inventory_at_risk
FROM inventory_at_risk, total_inventory

-- Validate the Calculation Result
-- Expected: Should return the correct percentage of inventory at risk

-- Performance Test: Validate loading times
-- Measure performance implications of the queries especially for large datasets

-- Data Quality Tests
-- Check for NULL values in dnsa_flag or financial_qty
SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag IS NULL OR financial_qty IS NULL

-- Validate if data transformation handles NULL values correctly

-- Edge Case Test: Maximum and Minimum VALUES
SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE financial_qty > 1.79769e+308 OR financial_qty < -1.79769e+308

-- Window Function Test
-- Testing Delta Lake operations (if applicable)
-- Assume delta table, perform MERGE/UPDATE/DELETE test

/* 
Python Test Code for PySpark Unit Testing 
Ensure that PySpark is enabled in Databricks context
*/

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import pytest

# Initialize Spark session for testing
spark = SparkSession.builder.appName("UnitTest").getOrCreate()

# Define schema for test data, aligning with target database schema
schema = StructType([
    StructField("id", LongType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

@pytest.fixture
def sample_data():
    # Setup test data
    data = [
        (1, "Y", 150.0, "2024-03-21 00:00:00"),
        (2, "N", 200.0, "2024-03-21 01:00:00"),
        -- Additional sample data rows...
    ]
    return spark.createDataFrame(data, schema)

def test_inventory_at_risk(sample_data):
    # Filter and calculate inventory at risk
    inventory_at_risk = sample_data.filter(col("dnsa_flag") == "Y") \
                                   .agg(_sum("financial_qty").alias("inventory_risk")) \
                                   .collect()[0]["inventory_risk"]

    # Expected result calculation for assertion
    expected_risk = 150.0  # Set expected result manually based on fixture data

    assert inventory_at_risk == expected_risk

def test_percentage_of_inventory_at_risk(sample_data):
    total_inventory = sample_data.agg(_sum("financial_qty").alias("total")).collect()[0]["total"]
    inventory_at_risk = sample_data.filter(col("dnsa_flag") == "Y") \
                                   .agg(_sum("financial_qty").alias("inventory_risk")) \
                                   .collect()[0]["inventory_risk"]

    percentage_of_risk = (inventory_at_risk / total_inventory) * 100

    # Perform assertions based on expected results
    expected_percentage = (150.0 / total_inventory) * 100  # Correct calculated percentage based on sample_data

    assert percentage_of_risk == expected_percentage

# Additional tests here such as error handling, real-time validation, etc.

# Add cleanup operations to remove test data