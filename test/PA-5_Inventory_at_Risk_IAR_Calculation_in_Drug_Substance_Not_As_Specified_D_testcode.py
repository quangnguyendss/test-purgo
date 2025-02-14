/* 
 * Setup Section
 * Please ensure the Unity Catalog and necessary privileges are properly configured
 * Ensure the Databricks SQL warehouse is available for real-time calculations
 * Install necessary libraries via %pip if additional packages are needed
 */

# Run this in a Databricks notebook cell to install any required libraries
# %pip install any-required-library

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, when
from pyspark.sql.types import DoubleType, StringType, StructType, StructField, TimestampType

# Start Spark session
spark = SparkSession.builder.appName("InventoryAtRiskTest").getOrCreate()

# Set up the schema for the test data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Load test data into DataFrame
data_path = "/path/to/test_data.csv"  # Path to test data
df = spark.read.csv(data_path, schema=schema, header=True)

# SQL Test: Calculate inventory at risk
df.createOrReplaceTempView("f_inv_movmnt")

inventory_at_risk_sql = """
SELECT SUM(financial_qty) AS inventory_at_risk
FROM f_inv_movmnt
WHERE dnsa_flag = "Y"
"""

inventory_at_risk = spark.sql(inventory_at_risk_sql).collect()[0][0]

if inventory_at_risk is None:
    inventory_at_risk = 0.0

print(f"Inventory at Risk: {inventory_at_risk}")

# SQL Test: Calculate percentage of inventory at risk
total_inventory_sql = """
SELECT SUM(financial_qty) AS total_inventory
FROM f_inv_movmnt
WHERE dnsa_flag IN ("Y", "N")
"""
total_inventory = spark.sql(total_inventory_sql).collect()[0][0]

percentage_of_inventory_at_risk = (inventory_at_risk / total_inventory) * 100 if total_inventory else None

if percentage_of_inventory_at_risk is None:
    print("Error: Total inventory not provided. Calculation cannot proceed.")
else:
    print(f"Percentage of Inventory at Risk: {percentage_of_inventory_at_risk:.2f}%")

# PySpark Data Validation for Unit Test
risk_df = df.filter(df.dnsa_flag == "Y").groupBy().sum("financial_qty").withColumnRenamed("sum(financial_qty)", "inventory_at_risk")

assert risk_df.collect()[0]["inventory_at_risk"] == inventory_at_risk, "Inventory at Risk calculation mismatch."

# PySpark Data Validation for Null Handling
null_handling_df = df.filter(df.financial_qty.isNull())
assert null_handling_df.count() > 0, "Null handling test failed, please check the dataset."

# Integration Test for end-to-end flow with necessary assumptions
final_df = df.withColumn("risk_status", when(col("dnsa_flag") == "Y", "At Risk").otherwise("Not At Risk"))
final_df.show()

# Delta Lake Operations
# Ensure Delta Lake operations if applicable, handling MERGE, UPDATE, DELETE

try:
    spark.sql("""
    MERGE INTO agilisium_playground.purgo_playground.f_inv_movmnt AS target
    USING (SELECT * FROM f_inv_movmnt WHERE dnsa_flag = "Y") AS source
    ON target.id = source.id
    WHEN MATCHED THEN
    UPDATE SET target.dnsa_flag = "Y"
    WHEN NOT MATCHED
    THEN INSERT (id, dnsa_flag, financial_qty, timestamp_event)
    VALUES (source.id, source.dnsa_flag, source.financial_qty, source.timestamp_event)
    """)
    
    print("Delta Lake Merge Operation Successful")
except Exception as e:
    print(f"Error in Delta Lake Operations: {e}")

# Cleanup Procedure
# Drop temporary tables and views
spark.catalog.dropTempView("f_inv_movmnt")

/* 
 * Setup SQL Table for Testing 
 * Ensure you are running this in Databricks SQL environment
 */

DROP TABLE IF EXISTS agilisium_playground.purgo_playground.f_inv_movmnt;

CREATE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
    id BIGINT,
    dnsa_flag STRING,
    financial_qty DOUBLE,
    timestamp_event TIMESTAMP
);

-- Insert Diverse Test Records
INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES
(1, "Y", 150.0, TIMESTAMP('2024-03-21T00:00:00.000+0000')),
(2, "N", 200.0, TIMESTAMP('2024-03-21T01:00:00.000+0000')),
-- more test data as per the requirements above...

/* 
 * SQL Test Queries
 * Run these to validate calculations and data accuracy
 */

/* Calculate Inventory At Risk */
SELECT 
    SUM(financial_qty) AS inventory_at_risk 
FROM 
    agilisium_playground.purgo_playground.f_inv_movmnt 
WHERE 
    dnsa_flag = "Y";

/* Calculate Total Inventory */
SELECT 
    SUM(financial_qty) AS total_inventory 
FROM 
    agilisium_playground.purgo_playground.f_inv_movmnt 
WHERE 
    dnsa_flag IN ("Y", "N");

/* Calculate Percentage of Inventory At Risk */
WITH inventory_calc AS (
    SELECT 
        SUM(financial_qty) AS inventory_at_risk 
    FROM 
        agilisium_playground.purgo_playground.f_inv_movmnt 
    WHERE 
        dnsa_flag = "Y"
),
total_calc AS (
    SELECT 
        SUM(financial_qty) AS total_inventory 
    FROM 
        agilisium_playground.purgo_playground.f_inv_movmnt
)
SELECT 
    (inventory_calc.inventory_at_risk / total_calc.total_inventory) * 100 AS percentage_of_inventory_at_risk 
FROM 
    inventory_calc, total_calc
WHERE total_calc.total_inventory IS NOT NULL;