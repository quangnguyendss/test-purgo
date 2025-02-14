/* This SQL test code block sets up and performs testing on the Databricks environment
   for calculating inventory at risk and the percentage of inventory at risk based on a DNSA flag
   from a test table in Databricks SQL using Unity Catalog and Delta Lake operations. */

/* Setup the necessary environment and ensure connection
   Install any missing packages necessary for the test environment
   and access Unity Catalog verified permissions */

/* Create the test table within Databricks SQL using specifications */

/* Setup the environment and test data as per Unity Catalog specifications */
CREATE OR REPLACE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
    id BIGINT,
    dnsa_flag STRING,
    financial_qty DOUBLE,
    timestamp_event TIMESTAMP
);

/* Insert test data into the table */
INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES
(1, "Y", 150.0, TIMESTAMP('2024-03-21T00:00:00.000+0000')),
(2, "N", 200.0, TIMESTAMP('2024-03-21T01:00:00.000+0000')),
(3, "Y", 300.0, TIMESTAMP('2024-03-21T02:00:00.000+0000'));

/* Test SQL queries to calculate inventory at risk and percentage of inventory at risk */

/* Step 1: Calculate inventory at risk */
SELECT
  SUM(financial_qty) AS inventory_at_risk
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt
WHERE
  dnsa_flag = "Y";

/* Step 2: Calculate total inventory for percentage calculation */
-- Assume total_inventory value is retrieved from another data source or computed 
WITH total_inventory_data AS (
  SELECT SUM(financial_qty) AS total_inventory 
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
)
SELECT total_inventory FROM total_inventory_data;

/* Step 3: Calculate percentage of inventory at risk */
SELECT
  (inventory_at_risk / total_inventory) * 100 AS percentage_of_inventory_at_risk
FROM
  (SELECT SUM(financial_qty) AS inventory_at_risk FROM agilisium_playground.purgo_playground.f_inv_movmnt WHERE dnsa_flag = "Y"),
  total_inventory_data;

/* Test and Assertions
   Validate that the inventory_at_risk calculation is correct */
SELECT
  CASE 
    WHEN inventory_at_risk = expected_value THEN "PASSED"
    ELSE "FAILED"
  END AS test_status
FROM
  (SELECT SUM(financial_qty) AS inventory_at_risk FROM agilisium_playground.purgo_playground.f_inv_movmnt WHERE dnsa_flag = "Y"),
  (SELECT 450.0 AS expected_value);  -- Expectation based on sample data inserted

/* Testing SQL Error handling for undefined total inventory */
SELECT 
  CASE 
    WHEN total_inventory IS NOT NULL THEN "Total inventory available"
    ELSE "Total inventory not provided. Calculation cannot proceed."
  END AS error_message
FROM total_inventory_data;

/* Testing for proper SQL data validation and character set compliance */
SELECT
  id,
  dnsa_flag,
  ISNULL(dnsa_flag) AS is_flag_null,
  ISNULL(financial_qty) AS is_fin_qty_null,
  CASE 
    WHEN LENGTH(dnsa_flag) > 5 THEN "Flag has special characters or multi-byte"
  ELSE "Flag is valid"
  END AS flag_character_status
FROM
  agilisium_playground.purgo_playground.f_inv_movmnt;

# PySpark Test Code block for comprehensive testing of transformations and functional operations
# Ensure the necessary modules and packages are imported

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col, when

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DatabricksSQLTest") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Load test dataset
input_df = spark.table("agilisium_playground.purgo_playground.f_inv_movmnt")

# Calculate inventory at risk using PySpark transformation
inventory_at_risk_df = input_df.filter(col("dnsa_flag") == "Y") \
    .agg(spark_sum(col("financial_qty")).alias("inventory_at_risk"))

# Display results
inventory_at_risk_df.show()

# Integrate with test cases for validation
def test_inventory_at_risk():
    result = inventory_at_risk_df.first()["inventory_at_risk"]
    expected = 450.0  # Based on SQL insertion content
    assert result == expected, f"Inventory at Risk calculation failed: Expected {expected}, but got {result}"

test_inventory_at_risk()

# Calculate total inventory
total_inventory_df = input_df.agg(spark_sum(col("financial_qty")).alias("total_inventory"))

# Calculate percentage of inventory at risk
def calculate_percentage_of_inventory_at_risk(inventory_at_risk, total_inventory):
    if total_inventory != 0:
        return (inventory_at_risk / total_inventory) * 100
    else:
        raise ValueError("Total inventory not provided. Calculation cannot proceed.")

percentage_df = inventory_at_risk_df.crossJoin(total_inventory_df) \
    .withColumn("percentage_of_inventory_at_risk",
                calculate_percentage_of_inventory_at_risk(col("inventory_at_risk"), col("total_inventory")))

# Show percentage results
percentage_df.show()

# Testing integration
def test_percentage_of_inventory_at_risk():
    percentage_result = percentage_df.first()["percentage_of_inventory_at_risk"]
    expected_percentage = (450.0 / (150.0 + 200.0 + 300.0)) * 100  # Based on SQL insertion content
    assert percentage_result == expected_percentage, f"Percentage of Inventory at Risk calculation failed: Expected {expected_percentage}, but got {percentage_result}"

test_percentage_of_inventory_at_risk()

# Cleanup any created test data for proper environment maintenance
spark.sql("DROP TABLE IF EXISTS agilisium_playground.purgo_playground.f_inv_movmnt")