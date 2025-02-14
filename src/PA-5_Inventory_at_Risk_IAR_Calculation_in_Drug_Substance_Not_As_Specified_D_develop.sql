-- Use proper SQL syntax block for calculation in Databricks

-- Calculate the inventory_at_risk, defined as the sum of financial_qty where dnsa_flag is 'Y'
CREATE OR REPLACE TEMP VIEW inventory_at_risk_view AS
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = 'Y';

-- Assume total_inventory is retrieved from a reliable source
-- Here, we'll set it as a constant for demonstration purposes
SET total_inventory = 10000.0;

-- Validate if total_inventory is properly set
IF (${total_inventory} IS NULL OR ${total_inventory} <= 0) THEN
    THROW "Error: Total inventory value should be greater than zero for percentage calculation.";
END IF;

-- Calculate the percentage of inventory at risk
CREATE OR REPLACE TEMP VIEW percentage_inventory_at_risk_view AS
SELECT 
    inventory_at_risk,
    (inventory_at_risk / ${total_inventory}) * 100 AS percentage_of_inventory_at_risk
FROM inventory_at_risk_view;

-- Display results
SELECT * FROM percentage_inventory_at_risk_view;

-- Implement Delta Lake features for data integrity and performance

-- Optimizing the table using Z-Ordering on the column expected to maximize performance
OPTIMIZE agilisium_playground.purgo_playground.f_inv_movmnt ZORDER BY (dnsa_flag);

-- Example of handling data versioning and life cycle management using VACUUM
VACUUM agilisium_playground.purgo_playground.f_inv_movmnt RETAIN 168 HOURS; -- Retain data for 7 days

from pyspark.sql import functions as F

# Load the data from the Unity Catalog using PySpark
df = spark.table("agilisium_playground.purgo_playground.f_inv_movmnt")

# Calculate the inventory_at_risk using PySpark DataFrame transformations
inventory_at_risk_df = (
    df.filter(df.dnsa_flag == 'Y')
    .agg(F.sum("financial_qty").alias("inventory_at_risk"))
)

# Collect the result to calculate percentages
inventory_at_risk = inventory_at_risk_df.collect()[0]["inventory_at_risk"]

# Assuming total_inventory is fetched from a reliable source
total_inventory = 10000.0  # Placeholder value for demonstration

# Validate total inventory value
if total_inventory <= 0:
    raise ValueError("Total inventory value should be greater than zero for percentage calculation.")

# Calculate percentage of inventory at risk
percentage_of_inventory_at_risk = (inventory_at_risk / total_inventory) * 100

# Display calculated results
print(f"Inventory at Risk: {inventory_at_risk}")
print(f"Percentage of Inventory at Risk: {percentage_of_inventory_at_risk}%")

# Example of performing a Delta Lake operation - MERGE
spark.sql("""
    MERGE INTO agilisium_playground.purgo_playground.f_inv_movmnt AS target
    USING (
      SELECT * FROM VALUES
      (28, 'Y', -1e308, TIMESTAMP('2024-03-22T01:00:00.000+0000'))
    ) AS source (id, dnsa_flag, financial_qty, timestamp_event)
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")