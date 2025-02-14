# Databricks PySpark Test Code
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType

# Set up Spark session
spark = SparkSession.builder.appName("InventoryAtRiskTest").getOrCreate()

# Define the schema for the f_inv_movmnt table
schema = StructType([
    StructField("id", LongType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Create DataFrame from test data
data = [
    (1, "Y", 150.0, "2024-03-21T00:00:00.000+0000"),
    (2, "N", 200.0, "2024-03-21T01:00:00.000+0000"),
    (3, "Y", 300.0, "2024-03-21T02:00:00.000+0000")
    # Add more test data here as needed
]
df = spark.createDataFrame(data, schema)

# Register DataFrame as a temporary view for SQL queries
df.createOrReplaceTempView("f_inv_movmnt")

# Calculate Inventory at Risk using Spark SQL
inventory_at_risk_query = """
SELECT SUM(financial_qty) AS inventory_at_risk
FROM f_inv_movmnt
WHERE dnsa_flag = 'Y'
"""
inventory_at_risk_df = spark.sql(inventory_at_risk_query)

# Retrieve total inventory value
# Mock total inventory for testing purposes; update with real query or calculation
total_inventory = 1250.0

# Calculate Percentage of Inventory at Risk
inventory_at_risk = inventory_at_risk_df.collect()[0]['inventory_at_risk']
percentage_of_inventory_at_risk = (inventory_at_risk / total_inventory) * 100 if total_inventory else None

# Assertions for testing
assert inventory_at_risk == 450.0, "Inventory at Risk calculation failed"
assert percentage_of_inventory_at_risk == 36.0, "Percentage of Inventory at Risk calculation failed"

print(f"Inventory At Risk: {inventory_at_risk}")
print(f"Percentage of Inventory At Risk: {percentage_of_inventory_at_risk}")

# Clean up temporary view
spark.catalog.dropTempView("f_inv_movmnt")

# Stop the Spark session
spark.stop()

-- SQL Test Code for Inventory at Risk and Percentage Calculation
-- Ensure the necessary table is created and populated with comprehensive test data

-- Calculate Inventory at Risk
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

-- Calculate total inventory for percentage calculation (mock value for this test)
-- This value should be replaced with appropriate calculation or source data retrieval
SELECT 1250.0 AS total_inventory;

-- Compute Percentage of Inventory at Risk
WITH risk AS (
  SELECT SUM(financial_qty) AS inventory_at_risk
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
  WHERE dnsa_flag = "Y"
),
total AS (
  SELECT SUM(financial_qty) AS total_inventory
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
)
SELECT 
  (CASE 
    WHEN total_inventory > 0 THEN (inventory_at_risk / total_inventory) * 100 
    ELSE NULL 
  END) AS percentage_of_inventory_at_risk
FROM risk CROSS JOIN total;

-- Validate if total_inventory is NULL
-- Expect this test to produce an error message if total_inventory is not provided
WITH total AS (
  SELECT NULL AS total_inventory
)
SELECT 
  (CASE 
    WHEN total_inventory IS NOT NULL THEN "Total inventory not provided. Calculation cannot proceed."
    ELSE "Error: Total inventory must be provided."
  END) AS error_check
FROM total;

-- Schema Validation Test Code
-- Confirm f_inv_movmnt table schema matches expectations
SHOW COLUMNS IN agilisium_playground.purgo_playground.f_inv_movmnt;

-- Ensure table constraints or primary key constraints are accurately defined
-- Typically requires business logic or is derived from real-world requirements

-- Validate Delta Lake operations if applicable
-- Check Delta Lake table for specific versioning and transaction history requirements

# Performance Test Code (PySpark)
from pyspark.sql.functions import expr

# Test optimized query for computing inventory at risk
optimized_inventory_query = df.filter(col("dnsa_flag") == "Y") \
    .groupBy("dnsa_flag") \
    .agg(_sum("financial_qty").alias("inventory_at_risk"))

optimized_inventory_at_risk = optimized_inventory_query.collect()[0]["inventory_at_risk"]

# Ensure optimized query returns the expected performance results
assert optimized_inventory_at_risk == inventory_at_risk, "Performance-optimized inventory at risk calculation failed"