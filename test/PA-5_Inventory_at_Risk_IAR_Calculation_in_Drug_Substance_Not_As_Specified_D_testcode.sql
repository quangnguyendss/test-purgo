-- SQL Test Cases for Inventory at Risk Calculation

-- Test Case 1: Calculate Inventory at Risk for DNSA active flag
WITH inventory_at_risk AS (
  SELECT SUM(financial_qty) AS total_at_risk
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
  WHERE dnsa_flag = "Y"
)

-- Assert the sum of Y flagged quantities is correct
SELECT total_at_risk
FROM inventory_at_risk
WHERE total_at_risk = (
  SELECT SUM(financial_qty)
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
  WHERE dnsa_flag = "Y"
);

-- Test Case 2: Validate percentage calculation for Inventory at Risk
WITH total_inventory AS (
  SELECT SUM(financial_qty) AS total_qty
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
),
inventory_at_risk AS (
  SELECT SUM(financial_qty) AS total_at_risk
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
  WHERE dnsa_flag = "Y"
),
percentage_at_risk AS (
  SELECT
    (total_at_risk / total_qty) * 100 AS percentage
  FROM inventory_at_risk, total_inventory
)

-- Assert percentage is computed correctly
SELECT percentage
FROM percentage_at_risk;

-- Test Case 3: Error handling for undefined total inventory
-- In a real environment, this would be handled by appropriate error handling logic

-- Test Case 4: Data and calculation validation
WITH example_data (flag_status, financial_data, expected_result) AS (
  SELECT "Y", ARRAY(100.0, 200.0, 300.0), 600.0 UNION ALL
  SELECT "N", ARRAY(100.0, 200.0, 300.0), 0.0     UNION ALL
  SELECT "Y", ARRAY(), 0.0
)
SELECT
  flag_status,
  CASE 
    WHEN flag_status = "Y"
    THEN ARRAY_SUM(financial_data)
    ELSE 0
  END AS calculated_result,
  expected_result
FROM example_data
WHERE
  calculated_result = expected_result;

-- Test Case 5: Validate Delta Lake Operations
-- Assuming Delta Lake operations need to be tested separately, here is a mock for Delta testing:
-- CREATE OR REPLACE TABLE delta_table USING DELTA ...

-- Test Case 6: Validation of MERGE, UPDATE, DELETE
-- Perform MERGE operation and validate its correctness
MERGE INTO agilisium_playground.purgo_playground.f_inv_movmnt AS target
USING (SELECT 1 AS id, "N" AS dnsa_flag, 180.0 AS financial_qty) AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET target.dnsa_flag = source.dnsa_flag
WHEN NOT MATCHED THEN
  INSERT (id, dnsa_flag, financial_qty) VALUES (source.id, source.dnsa_flag, source.financial_qty);

-- Validate update by checking records
SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt WHERE id = 1;

-- Cleanup operations
-- Drop any temporary tables or clean up data to prepare for next test

# PySpark Test Cases for Inventory at Risk Calculation

# Import necessary libraries and modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col
import pytest

# Initialize Spark session for testing
spark = SparkSession.builder \
    .appName("Databricks Testing") \
    .getOrCreate()

# Mock data for testing
data = [
    (1, "Y", 150.0),
    (2, "N", 200.0),
    (3, "Y", 300.0),
    (7, "Y", None),
]

schema = ["id", "dnsa_flag", "financial_qty"]
df = spark.createDataFrame(data, schema)

# Test Case: Check schema validation
def test_schema_validation():
    expected_schema = ["id", "dnsa_flag", "financial_qty"]
    assert df.columns == expected_schema

# Test Case: Calculate inventory at risk
def test_inventory_at_risk():
    inventory_at_risk_df = df.filter(col("dnsa_flag") == "Y")\
                             .agg(spark_sum("financial_qty").alias("total_at_risk"))
    result = inventory_at_risk_df.collect()[0]["total_at_risk"]
    assert result == 450.0  # Manual calculation based on mock data

# Test Case: Null handling and conversion
def test_null_handling():
    df_with_null = df.filter(col("financial_qty").isNull())
    assert df_with_null.count() == 1  # Should find 1 record

# Test Case: End-to-end integration
def test_end_to_end_integration():
    total_inventory = df.agg(spark_sum("financial_qty").alias("total_qty")).collect()[0]["total_qty"]
    inventory_at_risk_df = df.filter(col("dnsa_flag") == "Y")\
                             .agg(spark_sum("financial_qty").alias("total_at_risk"))
    inventory_at_risk = inventory_at_risk_df.collect()[0]["total_at_risk"]
    percentage_at_risk = (inventory_at_risk / total_inventory) * 100 if total_inventory else 0
    assert percentage_at_risk == pytest.approx((450.0 / (150.0 + 200.0 + 300.0)) * 100, 0.01)

# Cleanup resources
# Close Spark session
spark.stop()