-- Databricks SQL Test Cases for Inventory at Risk Calculation

-- Section 1: Setting up test cases for the Delta Table
-- This section creates a table and inserts test data to analyze Inventory at Risk calculations.
CREATE OR REPLACE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
    id BIGINT,
    dnsa_flag STRING,
    financial_qty DOUBLE,
    timestamp_event TIMESTAMP
);

-- Insert test data into the table for various scenarios
INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES
(1, "Y", 150.0, TIMESTAMP('2024-03-21T00:00:00.000+0000')),
(2, "N", 200.0, TIMESTAMP('2024-03-21T01:00:00.000+0000')),
(3, "Y", 300.0, TIMESTAMP('2024-03-21T02:00:00.000+0000')),
(4, "Y", NULL, TIMESTAMP('2024-03-21T06:00:00.000+0000')),
(5, NULL, 120.0, TIMESTAMP('2024-03-21T07:00:00.000+0000')),
(6, "Y", 9876543210.0, TIMESTAMP('2024-03-21T20:00:00.000+0000')),
(7, "Y", -500.0, TIMESTAMP('2024-03-21T21:00:00.000+0000')),
(8, "你", 720.0, TIMESTAMP('2024-03-21T22:00:00.000+0000')),
(9, "Y", 815.0, TIMESTAMP('2024-03-21T23:00:00.000+0000'));

-- Define the total inventory. This is assumed from an external information source.
CREATE OR REPLACE TEMP VIEW total_inventory AS
SELECT 1000000 AS total_qty;

-- Section 2: Test Inventory at Risk Calculation
/* Test: Calculate the inventory_at_risk when dnsa_flag is 'Y' */
CREATE OR REPLACE TEMP VIEW inventory_at_risk AS
SELECT SUM(COALESCE(financial_qty, 0)) AS total_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = 'Y';

-- Validate that the total inventory at risk is calculated as expected
SELECT * FROM inventory_at_risk;

-- Section 3: Calculate and assert the percentage of Inventory at Risk
/* Test: Calculate the percentage of inventory at risk */
SELECT 
  r.total_at_risk / t.total_qty * 100 AS percentage_at_risk
FROM inventory_at_risk r
JOIN total_inventory t;

/* Expected Assertion here, to compare calculated value with expected value
-- Assuming the expected percentage is known */

-- Section 4: Null and data type handling
/* Test: Verify handling of NULL in financial_qty; expected result should exclude NULL values */
SELECT SUM(CASE WHEN dnsa_flag = 'Y' THEN COALESCE(financial_qty, 0) ELSE 0 END) AS expected_total_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt;

/* Test: Handling complex data types and character edge cases */
-- This tests if special characters can be handled correctly without errors
SELECT COUNT(*)
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag LIKE '%❤️%' OR dnsa_flag LIKE '%你%';

/* Cleanup operations after tests */
-- Dropping the views used for testing to clean up the environment
DROP VIEW IF EXISTS inventory_at_risk;
DROP VIEW IF EXISTS total_inventory;

# PySpark Test Code for Inventory at Risk Calculations

# Import necessary PySpark libraries
# Install missing libraries if needed
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import sum as _sum, col, lit, when
    from pyspark.sql.types import DoubleType, StructType, StructField, StringType, TimestampType
except ImportError:
    raise ImportError("Required PySpark libraries are missing")

spark = SparkSession.builder \
    .appName("InventoryAtRiskTests") \
    .getOrCreate()

# Section 1: Setup test data
# Define schema for test data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Create a DataFrame for test data
test_data = [
    (1, "Y", 150.0, "2024-03-21 00:00:00"),
    (2, "N", 200.0, "2024-03-21 01:00:00"),
    (3, "Y", 300.0, "2024-03-21 02:00:00"),
    (4, "Y", None, "2024-03-21 06:00:00"),
    (5, None, 120.0, "2024-03-21 07:00:00"),
    (6, "Y", 9876543210.0, "2024-03-21 20:00:00"),
    (7, "Y", -500.0, "2024-03-21 21:00:00"),
    (8, "你", 720.0, "2024-03-21 22:00:00"),
    (9, "Y", 815.0, "2024-03-21 23:00:00")
]

df = spark.createDataFrame(test_data, schema=schema)

# Section 2: Calculate Inventory at Risk
# Calculate total inventory at risk based on dnsa_flag
inventory_at_risk = df.filter(df.dnsa_flag == "Y") \
                      .agg(_sum(when(col("financial_qty").isNotNull(), col("financial_qty")).otherwise(lit(0))).alias("total_at_risk"))

inventory_at_risk.show()

# Section 3: Calculate percentage of Inventory at Risk
# Define a total inventory for calculation
total_inventory = 1000000.0

# Calculate the percentage of inventory at risk
result_df = inventory_at_risk.withColumn("percentage_at_risk", (col("total_at_risk") / lit(total_inventory)) * 100)
result_df.show()

# Section 4: Cleanup
# Unpersist any cached dataframes and stop the SparkSession
spark.stop()