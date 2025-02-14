/* SQL Test Code: Inventory at Risk Calculation */

-- Ensure Databricks SQL syntax is used
-- Define the calculation for inventory_at_risk and percentage_of_inventory_at_risk

-- NOTE: Define total inventory (total_inventory) based on your specific use case or available data

-- Calculate the inventory at risk based on DNSA flag being 'Y'
CREATE OR REPLACE VIEW v_inventory_at_risk AS
SELECT
  SUM(CASE WHEN dnsa_flag = "Y" THEN financial_qty ELSE 0 END) AS inventory_at_risk
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt;

-- Validate the result for inventory_at_risk calculation
SELECT * FROM v_inventory_at_risk;

-- Calculate the percentage of inventory at risk
CREATE OR REPLACE VIEW v_percentage_of_inventory_at_risk AS
SELECT
  inventory_at_risk,
  (inventory_at_risk / total_inventory) * 100 AS percentage_of_inventory_at_risk
FROM
  v_inventory_at_risk, -- Self-join to access inventory_at_risk
  (SELECT CAST(10000 AS DOUBLE) AS total_inventory) AS tmp_total_inventory -- Placeholder for total_inventory

-- Validate the output of percentage calculation
SELECT * FROM v_percentage_of_inventory_at_risk;

-- Assert that total_inventory is defined and not null
SELECT CASE WHEN total_inventory IS NULL THEN RAISE_ERROR("Total inventory not provided. Calculation cannot proceed.") END
FROM v_percentage_of_inventory_at_risk;

/* Test Cases for SQL-based Calculation */

-- Test the view creation and data aggregation on the table
SELECT
  id,
  dnsa_flag,
  financial_qty,
  timestamp_event,
  CASE WHEN dnsa_flag = "Y" THEN financial_qty ELSE 0 END AS calc_financial_qty,
  (SUM(CASE WHEN dnsa_flag = "Y" THEN financial_qty ELSE 0 END) OVER ()) AS total_inventory
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt;

/** PySpark Test Code **/

# Import necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder.appName("Databricks InventoryTesting").getOrCreate()

# Schema definition for DataFrame; ensure it matches the table structure
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Load the table into a DataFrame
df = spark.table("agilisium_playground.purgo_playground.f_inv_movmnt")

# Unit test for calculating inventory at risk
inventory_at_risk_df = df.groupBy().agg(_sum(when(col("dnsa_flag") == "Y", col("financial_qty")).otherwise(0)).alias("inventory_at_risk"))

# Schema validation assertion
assert inventory_at_risk_df.schema.names == ["inventory_at_risk"], "Schema mismatch for inventory_at_risk calculation."

# Unit test for calculating percentage of inventory at risk
total_inventory = 10000.0  # Placeholder for total inventory
percentage_inventory_df = inventory_at_risk_df.withColumn("percentage_of_inventory_at_risk", col("inventory_at_risk") / total_inventory * 100)

# Output for manual verification, replace asserts/logic with unittest or pytest for structured testing
percentage_inventory_df.show()

# Check for defined total inventory
if total_inventory is None:
    raise ValueError("Total inventory not provided. Calculation cannot proceed.")

# Clean up the created resources
spark.sql("DROP VIEW IF EXISTS v_inventory_at_risk")
spark.sql("DROP VIEW IF EXISTS v_percentage_of_inventory_at_risk")

# Stop the Spark session
spark.stop()