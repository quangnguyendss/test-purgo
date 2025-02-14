-- Unity Catalog Test Data Setup
-- Create or replace table for testing
CREATE OR REPLACE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
    id BIGINT,
    dnsa_flag STRING,
    financial_qty DOUBLE,
    timestamp_event TIMESTAMP
);

-- Insert diverse test records
-- Happy Path Test Data
INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES
(1, "Y", 150.0, TIMESTAMP('2024-03-21T00:00:00')),
(2, "N", 200.0, TIMESTAMP('2024-03-21T01:00:00')),
(3, "Y", 300.0, TIMESTAMP('2024-03-21T02:00:00'));

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
from pyspark.sql.functions import col, sum as _sum
from delta.tables import DeltaTable

# Define schema for validation
schema = StructType([
    StructField("id", LongType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Read the test data into a DataFrame
df = spark.read.table("agilisium_playground.purgo_playground.f_inv_movmnt")

# Schema validation test
assert df.schema == schema, "Schema does not match expected schema"

# Calculate inventory at risk when DNSA flag is active
inventory_at_risk_df = df.filter(df.dnsa_flag == "Y").agg(_sum("financial_qty").alias("inventory_at_risk"))
inventory_at_risk = inventory_at_risk_df.collect()[0]["inventory_at_risk"]

# Total Inventory Calculation
total_inventory = df.agg(_sum("financial_qty").alias("total_inventory")).collect()[0]["total_inventory"]

# Percentage of Inventory at Risk Calculation
try:
    percentage_of_inventory_at_risk = (inventory_at_risk / total_inventory) * 100
except ZeroDivisionError:
    percentage_of_inventory_at_risk = None
    print("Error: Cannot calculate percentage_of_inventory_at_risk with undefined total inventory")

assert percentage_of_inventory_at_risk is not None, "Error: Cannot calculate percentage_of_inventory_at_risk."

# Print results
print(f"Inventory at Risk: {inventory_at_risk}")
print(f"Percentage of Inventory at Risk: {percentage_of_inventory_at_risk}")

# Delta Lake operations
# Define Delta Lake write path
delta_path = "/delta/path/to/f_inv_movmnt"
df.write.format("delta").mode("overwrite").save(delta_path)

# Read back from Delta Lake to validate
delta_df = spark.read.format("delta").load(delta_path)
assert delta_df.count() == df.count(), "Delta Lake write/read operation failed"

# Perform Merge Operation
delta_table = DeltaTable.forPath(spark, delta_path)
delta_table.alias("tgt").merge(
    df.alias("src"),
    "tgt.id = src.id"
).whenMatchedUpdate(set={"financial_qty": "src.financial_qty"}).execute()

assert delta_table.toDF().filter(col("id") == 1).select("financial_qty").collect()[0][0] == 150.0, "Delta Merge failed"

# Cleanup test resources
spark.sql("DROP TABLE IF EXISTS agilisium_playground.purgo_playground.f_inv_movmnt")

-- SQL Queries to validate the results
-- Calculate Inventory at Risk
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

-- Calculate Total Inventory
SELECT SUM(financial_qty) AS total_inventory
FROM agilisium_playground.purgo_playground.f_inv_movmnt;