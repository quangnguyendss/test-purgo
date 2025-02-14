# Databricks SQL and PySpark Test Code for Inventory at Risk Calculation

# ---------------------------------------
# Setting up Databricks Notebook for SQL-based Testing
# ---------------------------------------
-- Install any necessary libraries (e.g., delta-core) if not pre-installed in Databricks cluster
-- %pip install [required-library] 

-- SQL Code: Setting context and initial test setup for Inventory at Risk Calculation
-- Creating test scenario setup
CREATE OR REPLACE TEMP VIEW test_f_inv_movmnt AS
SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt;

-- Calculating Inventory at Risk when DNSA flag is active
CREATE OR REPLACE TEMP VIEW inventory_at_risk AS
SELECT SUM(financial_qty) AS inv_at_risk
FROM test_f_inv_movmnt
WHERE dnsa_flag = "Y";

-- Calculating Total Inventory (define and replace with correct source or computation)
CREATE OR REPLACE TEMP VIEW total_inventory AS
SELECT SUM(financial_qty) AS total_inv
FROM test_f_inv_movmnt
WHERE dnsa_flag IS NOT NULL; -- Adjust this logic based on actual definition of total inventory

-- Calculating Percentage of Inventory at Risk
CREATE OR REPLACE TEMP VIEW percentage_inventory_at_risk AS
SELECT
    (CASE WHEN total_inv > 0 THEN (inv_at_risk / total_inv) * 100 ELSE NULL END) AS pct_inv_at_risk
FROM inventory_at_risk join total_inventory;

-- Assertion: Validate if inventory_at_risk calculation is correct
SELECT assert((SELECT inv_at_risk FROM inventory_at_risk) = (150 + 300 + 1.79769e+308 + 100 + 350 + 0 + 500 + 9876543210 + -500 + 815), "Inventory at risk calculation failed");

-- Assertion: Validate if percentage_of_inventory_at_risk calculation is correct
SELECT assert((SELECT pct_inv_at_risk FROM percentage_inventory_at_risk) IS NOT NULL, "Percentage inventory at risk calculation failed due to no valid total inventory");

-- Clean up temporary views
DROP VIEW IF EXISTS inventory_at_risk;
DROP VIEW IF EXISTS total_inventory;
DROP VIEW IF EXISTS percentage_inventory_at_risk;
DROP VIEW IF EXISTS test_f_inv_movmnt;

# ---------------------------------------
# PySpark unit tests for Data Type Conversion and Schema Validation
# ---------------------------------------
# Import necessary libraries for PySpark Testing
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql import Row

# Setup the Spark Session for Testing
spark = SparkSession.builder \
    .appName("InventoryAtRiskTest") \
    .getOrCreate()

# Validate complex data type schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True),
])

# Create a sample DataFrame for testing
rdd = spark.sparkContext.parallelize([
    Row(id="1", dnsa_flag="Y", financial_qty=150.0, timestamp_event="2024-03-21T00:00:00.000+0000"),
    Row(id="2", dnsa_flag="N", financial_qty=200.0, timestamp_event="2024-03-21T01:00:00.000+0000"),
    # More test rows...
])

# Creating a DataFrame with the predefined schema
inventory_df = spark.createDataFrame(rdd, schema=schema)
inventory_df.createOrReplaceTempView("f_inv_movmnt")

# Test case for schema validation
expected_schema = StructType([
    StructField("id", StringType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

assert inventory_df.schema == expected_schema, "Schema validation failed!"

# Test case for datatype conversions
converted_df = inventory_df.withColumn("financial_qty_str", col("financial_qty").cast(StringType()))
assert converted_df.schema["financial_qty_str"].dataType == StringType(), "Data type conversion to STRING failed!"

# Null handling test case
null_handling_df = inventory_df.withColumn("financial_qty",
                                           when(col("financial_qty").isNull(), 0).otherwise(col("financial_qty")))

assert null_handling_df.filter(col("financial_qty").isNull()).count() == 0, "NULL handling test failed!"

# Stream processing unit tests
input_stream_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
transformed_stream_df = input_stream_df.select(col("value").alias("inventory_value"))

# Test function for ensuring stream processing
def process_stream(df, epoch_id):
    processed_count = df.count()
    assert processed_count > 0, f"Stream processing failed at micro-batch {epoch_id}!"

stream_query = transformed_stream_df.writeStream.foreachBatch(process_stream).start()
stream_query.awaitTermination(10)  # Run stream for a few seconds for testing

# Cleanup operations
spark.catalog.dropTempView("f_inv_movmnt")
stream_query.stop()

# ---------------------------------------
# Testing Delta Lake Features
# ---------------------------------------
-- Delta Lake MERGE, UPDATE, DELETE scenarios

-- Sample Delta table creation
CREATE OR REPLACE TABLE delta_inventory
USING DELTA AS SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE FALSE; -- Create an empty delta table for testing

-- Testing Delta MERGE operation
MERGE INTO delta_inventory AS target
USING (SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt) AS source
ON target.id = source.id
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED
THEN INSERT *;

-- Validate MERGE results
SELECT assert(sum(financial_qty) IS NOT NULL, "Delta MERGE test failed!");

-- Cleanup
DROP TABLE IF EXISTS delta_inventory;

# -----------------------------------------