/* Databricks SQL Test for Inventory at Risk Calculation */

-- Setup: Verify Unity Catalog configuration
/*
  Check if Unity Catalog schema and table exist
*/
SHOW TABLES IN agilisium_playground.purgo_playground;

-- Test Setup: Verifying Data Ingestion
/*
  Ensure data exists in the `f_inv_movmnt` table
*/
SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt LIMIT 5;

-- Unit Test: Calculation of Inventory at Risk when DNSA flag is active
/*
  Expectation: Calculate sum of `financial_qty` for records with `dnsa_flag` as "Y"
*/
SELECT
  SUM(financial_qty) AS inventory_at_risk
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt
WHERE
  dnsa_flag = "Y";

-- Unit Test: Error Handling for NULL Inputs
/*
  Expectation: Handle potential NULL values in financial_qty
*/
SELECT
  id,
  CASE
    WHEN financial_qty IS NULL THEN "Error: NULL financial_qty"
    ELSE "Valid"
  END AS qty_status
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt;

-- Integration Test: Calculate Percentage of Inventory at Risk
/*
  Expectation: Use the formula (inventory_at_risk / total_inventory) * 100
  Note: Replace `total_inventory_value` with actual total inventory value
*/
WITH CTE AS (
  SELECT
    SUM(financial_qty) AS inventory_at_risk
  FROM
    agilisium_playground.purgo_playground.f_inv_movmnt
  WHERE
    dnsa_flag = "Y"
)
SELECT
  inventory_at_risk,
  (inventory_at_risk / <total_inventory_value>) * 100 AS percentage_of_inventory_at_risk
FROM
  CTE;

-- Function Test: Complex Type Handling
/*
  Expectation: Validate complex types, no complex types involved directly in current schema but ensure proper handling
*/
SELECT
  STRUCT(id, dnsa_flag, financial_qty) AS record_struct
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt;

-- Cleanup: Validate table drops after test concludes
/*
  Ensure clean up of test tables or records if modifications were made
*/
/*
DROP TABLE IF EXISTS agilisium_playground.purgo_playground.test_table;
*/

-- Performance Test: Comparative analysis on large datasets
/*
  Expectation: Compare execution plans on full dataset vs filtered
*/
EXPLAIN EXTENDED
SELECT
  SUM(financial_qty)
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt
WHERE
  dnsa_flag = "Y";

-- Security Test: Access Validation for Unity Catalog
/*
  Ensure that access to data in Unity Catalog is compliant with set security protocols
*/
SELECT
  CURRENT_USER(),
  CURRENT_ROLE()
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt
LIMIT 1;

-- Test for Window Functions: Verify Over Time Analysis
/*
  Expectation: Use window functions to analyze trends over time
*/
SELECT
  id,
  financial_qty,
  SUM(financial_qty) OVER (PARTITION BY dnsa_flag ORDER BY timestamp_event) AS cumsum_qty
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt;

# PySpark Test for Inventory at Risk Calculation

# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Databricks Test") \
    .getOrCreate()

# Define a schema for testing purposes
schema = StructType([
    StructField("id", StringType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Load table into DataFrame
df = spark.read.table("agilisium_playground.purgo_playground.f_inv_movmnt")

# Unit Test: Calculation of Inventory at Risk
# Expectation: Calculate the sum of financial_qty where the DNSA flag is "Y"
inventory_at_risk_df = df.filter(col("dnsa_flag") == "Y").agg(_sum("financial_qty").alias("inventory_at_risk"))

# Display result
inventory_at_risk_df.show()

# Handling NULL inputs
# Expectation: Test to check how NULL financial_qty is handled
df_null_test = df.withColumn("qty_status", when(col("financial_qty").isNull(), "Error: NULL financial_qty").otherwise("Valid"))

# Display NULL handling results
df_null_test.select("id", "qty_status").show()

# Integration Test: Percentage of Inventory at Risk
# Placeholder for total inventory value, should be replaced with actual retrieval logic
total_inventory_value = 1000000.0

def calculate_percentage_of_at_risk(inventory_risk, total_inventory):
    if total_inventory == 0:
        return "Error: Total inventory not provided. Calculation cannot proceed."
    return (inventory_risk / total_inventory) * 100

# Assuming inventory_at_risk_df has been calculated
inventory_at_risk = inventory_at_risk_df.first()["inventory_at_risk"]
percentage_at_risk = calculate_percentage_of_at_risk(inventory_at_risk, total_inventory_value)
print(f"Percentage of Inventory at Risk: {percentage_at_risk}")

# Security compliance: Ensure current user roles can access catalog
# Display current user and role
security_test_df = spark.sql("SELECT CURRENT_USER(), CURRENT_ROLE() FROM agilisium_playground.purgo_playground.f_inv_movmnt LIMIT 1")
security_test_df.show()

# Performance Test: Explain plan
# Performance analysis on a large dataset
df_filtered = df.filter(col("dnsa_flag") == "Y")
df_filtered.explain(extended=True)

# Window Function Test: Verify cumulative calculations over timestamps
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as _sum

window_spec = Window.partitionBy("dnsa_flag").orderBy("timestamp_event")
df_with_window = df.withColumn("cumsum_qty", _sum("financial_qty").over(window_spec))

# Show results of window function
df_with_window.select("id", "cumsum_qty").show()

# Clean Up: Drop temporary table/view if created
# spark.sql("DROP TABLE IF EXISTS some_temp_table")