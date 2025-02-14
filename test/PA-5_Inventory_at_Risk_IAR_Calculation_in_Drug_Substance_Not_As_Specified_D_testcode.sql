-- Databricks SQL test code for inventory at risk calculations

/* Set up for Databricks environment */
-- Install any necessary libraries here. Most required libraries are natively present in Databricks.

/* Schema Definitions */
/* Validate the structure of f_inv_movmnt table to ensure it matches expectations */

-- Check if the table and its columns exist and have the correct data types
DESCRIBE TABLE agilisium_playground.purgo_playground.f_inv_movmnt;

-- Validate proper data types for schema fields
-- We expect 'dnsa_flag' as STRING and 'financial_qty' as DOUBLE

-- Test for Schema Validation
SELECT 
  CASE 
    WHEN data_type = "STRING" THEN "Valid"
    ELSE "Invalid"
  END AS dnsa_flag_type, 
  CASE 
    WHEN data_type = "DOUBLE" THEN "Valid"
    ELSE "Invalid"
  END AS financial_qty_type
FROM 
  (DESCRIBE TABLE agilisium_playground.purgo_playground.f_inv_movmnt)
WHERE 
  col_name IN ("dnsa_flag", "financial_qty");

/* Data Type Testing and Type Conversions */
/* Test conversion of financial_qty to STRING and back to DOUBLE for data consistency */
-- First convert DOUBLE to STRING, then back to DOUBLE and validate
SELECT
  financial_qty,
  CAST(CAST(financial_qty AS STRING) AS DOUBLE) AS converted_qty
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt
WHERE
  id = 1;  -- Using a specific ID for a focused test

-- NULL handling test for financial_qty
SELECT
  id,
  financial_qty IS NULL AS is_null_qty,
  dnsa_flag IS NULL AS is_null_flag
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt;

/* Calculate Inventory at Risk and Percentage */
-- Unit Test: Calculate inventory_at_risk for DNSA flag active
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

-- Integration Test: Calculate percentage of inventory at risk
WITH total_inventory AS (
  -- Assume we have a definition or query to find the total inventory
  -- Here we're using an arbitrary placeholder value for the sake of testing
  SELECT SUM(financial_qty) AS total FROM agilisium_playground.purgo_playground.f_inv_movmnt
)
SELECT 
  SR.inventory_at_risk,
  TI.total,
  (SR.inventory_at_risk / TI.total) * 100 AS percentage_of_inventory_at_risk
FROM 
  (SELECT SUM(financial_qty) AS inventory_at_risk FROM agilisium_playground.purgo_playground.f_inv_movmnt WHERE dnsa_flag = "Y") SR,
  total_inventory TI;

/* Error Handling and Assertions */
-- Validate error message for undefined total inventory
-- Simulate a condition where total_inventory is NULL
WITH total_inventory_defined AS (
  SELECT IFNULL(SUM(financial_qty), 0) AS total FROM agilisium_playground.purgo_playground.f_inv_movmnt WHERE dnsa_flag IS NOT NULL
)
SELECT 
  CASE 
    WHEN total_inventory_defined.total = 0 THEN "Total inventory not provided. Calculation cannot proceed."
    ELSE NULL
  END AS error_message
FROM total_inventory_defined;

/* Performance Test: Index or Analyze Table */
-- Ensure table statistics are up-to-date to optimize query performance
ANALYZE TABLE agilisium_playground.purgo_playground.f_inv_movmnt COMPUTE STATISTICS;

/* Cleanup after tests to ensure no test data affects production */
-- No-op for cleanup as we're using live data in a read-only manner here. In actual tests, ensure temp tables or data inserts are cleaned up.

# PySpark test code for Data Processing in Databricks

# Importing necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, expr

# Initialize a SparkSession
spark = SparkSession.builder.appName("InventoryAtRiskTest").getOrCreate()

# Define test data as a DataFrame (Normally, test data would come from your testing data source)
f_inv_movmnt_data = [
    (1, "Y", 150.0, "2024-03-21T00:00:00.000+0000"),
    (2, "N", 200.0, "2024-03-21T01:00:00.000+0000"),
    # Additional test data goes here...
]

# Define the schema for the DataFrame
schema = ["id", "dnsa_flag", "financial_qty", "timestamp_event"]

# Create DataFrame
f_inv_movmnt_df = spark.createDataFrame(data=f_inv_movmnt_data, schema=schema)

# Unit Test: Calculate inventory_at_risk for DNSA flag active
inventory_at_risk_df = (f_inv_movmnt_df.filter(col("dnsa_flag") == "Y")
                        .agg(_sum("financial_qty").alias("inventory_at_risk")))

# Display the result for manual verification (remove in actual unit tests, used here for illustration)
# inventory_at_risk_df.show()

# Assertion for Unit Test (using PySpark DataFrame API)
inventory_at_risk = inventory_at_risk_df.collect()[0]["inventory_at_risk"]
assert inventory_at_risk == 150.0  # Modify expected result as per the actual test data scenario

# Integration Test: Calculate the percentage of inventory at risk
total_inventory_df = f_inv_movmnt_df.agg(_sum("financial_qty").alias("total_inventory"))

result_df = inventory_at_risk_df.crossJoin(total_inventory_df).withColumn(
    "percentage_of_inventory_at_risk",
    expr("inventory_at_risk / total_inventory * 100")
)

# Display the result for manual verification
# result_df.show()

# Cleanup: Remove any temporary data structures used in testing
spark.stop()