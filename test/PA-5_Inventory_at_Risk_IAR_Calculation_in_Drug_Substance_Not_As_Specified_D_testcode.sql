-- Configure Databricks SQL environment (setup required libraries if necessary)
-- Assuming Databricks has necessary libraries pre-installed for SQL operations

/* SQL Test Code Block for Inventory at Risk Calculation */

-- Create a test table for our SQL operations
DROP TABLE IF EXISTS agilisium_playground.purgo_playground.f_inv_movmnt;
CREATE OR REPLACE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
    id BIGINT,
    dnsa_flag STRING,
    financial_qty DOUBLE,
    timestamp_event TIMESTAMP
);

-- Insert test data into the f_inv_movmnt table
INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES
(1, "Y", 150.0, TIMESTAMP('2024-03-21T00:00:00.000+0000')),
(2, "N", 200.0, TIMESTAMP('2024-03-21T01:00:00.000+0000')),
(3, "Y", 300.0, TIMESTAMP('2024-03-21T02:00:00.000+0000')),
-- Additional test data here...

-- Validate SQL operations for calculating inventory_at_risk
-- Calculate the inventory_at_risk where dnsa_flag is "Y"
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

-- Validate the percentage of inventory at risk
WITH total_inventory AS (
    SELECT SUM(financial_qty) AS total FROM agilisium_playground.purgo_playground.f_inv_movmnt
)
SELECT 
    (SUM(CASE WHEN dnsa_flag = "Y" THEN financial_qty ELSE 0 END) / (SELECT total FROM total_inventory)) * 100 AS percentage_of_inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt;

-- Error handling for undefined total inventory
-- This query assumes that total inventory should not be zero. In a real testing environment, this would be asserted with expected outcomes.

-- Handling potentially NULL financial_qty
SELECT id, COALESCE(financial_qty, 0) AS financial_qty_handling_null
FROM agilisium_playground.purgo_playground.f_inv_movmnt;

-- Cleanup operations for testing environment
DROP TABLE IF EXISTS agilisium_playground.purgo_playground.f_inv_movmnt;

# PySpark Test Code Block

# Import necessary utilities for PySpark testing
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType
from pyspark.sql.functions import sum as _sum, col, when, coalesce

# Create Spark session for testing
spark = SparkSession.builder \
        .appName("Databricks SQL Testing") \
        .getOrCreate()

# Schema for the f_inv_movmnt table
schema = StructType([
    StructField("id", LongType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Test data for PySpark testing
data = [
    (1, "Y", 150.0, "2024-03-21 00:00:00"),
    (2, "N", 200.0, "2024-03-21 01:00:00"),
    (3, "Y", 300.0, "2024-03-21 02:00:00"),
    # Additional test data here...
]

# Create DataFrame for testing
df = spark.createDataFrame(data, schema)

# Perform transformation to calculate inventory_at_risk
inventory_at_risk = df.where(col("dnsa_flag") == "Y") \
                      .agg(_sum("financial_qty").alias("inventory_at_risk")) \
                      .collect()[0]["inventory_at_risk"]

# Schema validation example
assert inventory_at_risk is not None, "Inventory at risk should not be null."

# Calculate total inventory for percentage calculations
total_inventory = df.agg(_sum("financial_qty").alias("total")).collect()[0]["total"]

# Calculate percentage of inventory at risk
if total_inventory and total_inventory != 0:
    percentage_of_inventory_at_risk = (inventory_at_risk / total_inventory) * 100
else:
    raise ValueError("Total inventory not defined or zero. Cannot calculate percentage at risk.")

# Null handling and assertions
df_with_null_handling = df.withColumn("financial_qty_handling_null", coalesce(col("financial_qty"), _sum(0)))
assert df_with_null_handling.filter(col("financial_qty_handling_null").isNull()).count() == 0, "Null values found in processed column."

# Stop Spark session after tests
spark.stop()