-- Databricks SQL Test Script for Real-Time Calculation of Inventory at Risk

-- Creating a table for testing purposes
CREATE OR REPLACE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
    id BIGINT,
    dnsa_flag STRING,
    financial_qty DOUBLE,
    timestamp_event TIMESTAMP
);

-- Inserting test data into the table
INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES
-- Example Test Data
(1, "Y", 150.0, TIMESTAMP('2024-03-21T00:00:00.000+0000')),
(2, "N", 200.0, TIMESTAMP('2024-03-21T01:00:00.000+0000')),
-- More test data can be inserted as needed

/*------------------------------------------------------------------
   Test Case: Calculate Inventory at Risk for DNSA flag 'Y'
-------------------------------------------------------------------*/
-- Calculate the sum of financial_qty where dnsa_flag is "Y"
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

/*------------------------------------------------------------------
   Test Case: Calculate Percentage of Inventory at Risk
-------------------------------------------------------------------*/
-- Define total_inventory for testing the percentage calculation
-- Note: Replace with actual total inventory value as needed
DECLARE total_inventory DOUBLE;
SET total_inventory = 1000.0;

-- Calculate percentage of inventory at risk
SELECT 
    (SUM(financial_qty) / total_inventory) * 100 AS percentage_of_inventory_at_risk
FROM
    agilisium_playground.purgo_playground.f_inv_movmnt
WHERE 
    dnsa_flag = "Y";

/*------------------------------------------------------------------
   Test Case: Handle undefined total_inventory gracefully
-------------------------------------------------------------------*/
-- Test the error handling when total_inventory is not set
DECLARE total_inventory DOUBLE DEFAULT NULL;

SELECT 
    CASE 
        WHEN total_inventory IS NULL THEN "Error: Total inventory not provided. Calculation cannot proceed."
        ELSE CAST((SUM(financial_qty) / total_inventory) * 100 AS STRING)
    END AS percentage_of_inventory_at_risk
FROM
    agilisium_playground.purgo_playground.f_inv_movmnt
WHERE 
    dnsa_flag = "Y";

/*------------------------------------------------------------------
   Clean-up operations after tests
-------------------------------------------------------------------*/
-- Drop the test table after validation
DROP TABLE IF EXISTS agilisium_playground.purgo_playground.f_inv_movmnt;

# PySpark Test Script for Real-Time Calculation of Inventory at Risk

# Import necessary libraries
# Assuming the necessary libraries are already installed in the Databricks environment

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when, lit
from pyspark.sql.utils import AnalysisException

# Initialize Spark session
spark = SparkSession.builder.appName("InventoryAtRiskTest").getOrCreate()

# Load the test data into a DataFrame
# Assuming that the table is already created and populated with test data
df = spark.sql("SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt")

# Test Case: Calculate Inventory at Risk for DNSA flag 'Y'
inventory_at_risk_df = df.filter(col("dnsa_flag") == "Y").agg(_sum("financial_qty").alias("inventory_at_risk"))
inventory_at_risk_df.show()

# Test Case: Calculate Percentage of Inventory at Risk
# Define a mock total inventory for testing
total_inventory = 1000.0

percentage_df = inventory_at_risk_df.withColumn(
    "percentage_of_inventory_at_risk", 
    (col("inventory_at_risk") / lit(total_inventory)) * 100
)
percentage_df.show()

# Error handling when total inventory is undefined
try:
    # Assuming total_inventory is unset or set to None
    total_inventory = None
    if total_inventory is None:
        raise ValueError("Total inventory not provided. Calculation cannot proceed.")
    percentage_df = inventory_at_risk_df.withColumn(
        "percentage_of_inventory_at_risk", 
        (col("inventory_at_risk") / lit(total_inventory)) * 100
    )
    percentage_df.show()
except ValueError as e:
    print(e)

# Clean-up operations
# If any temporary tables or resources were created, they should be cleaned up here
# In this demo, there are no resources to clean up in the PySpark script

# Perform cleanup of Spark session
spark.stop()