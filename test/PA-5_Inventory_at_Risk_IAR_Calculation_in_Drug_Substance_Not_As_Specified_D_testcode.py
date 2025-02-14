# Import necessary libraries for PySpark and other utility functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Databricks Testing for Inventory at Risk") \
    .getOrCreate()

# Define the schema for the f_inv_movmnt table
schema = StructType([
    StructField("id", StringType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Create DataFrame with test data directly from the SQL table using the defined schema
df = spark.sql("SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt").toDF("id", "dnsa_flag", "financial_qty", "timestamp_event")

# Calculate inventory_at_risk based on dnsa_flag being 'Y'
inventory_at_risk_df = df.filter(col("dnsa_flag") == "Y").agg(_sum("financial_qty").alias("inventory_at_risk"))

# Show the calculated inventory_at_risk
inventory_at_risk_df.show()

# Assuming total_inventory is provided somehow
# For illustration, set a static value for total_inventory
total_inventory = 10000  # This needs to be dynamically calculated/defined in the actual implementation

# Calculate the percentage of Inventory at Risk
if total_inventory:
    inventory_at_risk_value = inventory_at_risk_df.first()["inventory_at_risk"] or 0
    percentage_of_inventory_at_risk = (inventory_at_risk_value / total_inventory) * 100
    print(f"Percentage of Inventory at Risk: {percentage_of_inventory_at_risk}%")
else:
    raise ValueError("Total inventory not provided. Calculation cannot proceed.")

# Validate proper schema
assert df.schema == schema, "Schema mismatch detected!"

# Perform cleanup operations if necessary
spark.catalog.dropTempView("f_inv_movmnt")

# Stop the Spark session at the end
spark.stop()

/* SQL for testing the Databricks SQL syntax and performing schema validation and aggregation operations */

/* Setup for test environment and table creation */
/* Ensure that the environment has the necessary setup and the test data is prepared */

/* Calculate the inventory at risk using a SQL query */
WITH InventoryRiskCalculation AS (
    SELECT
        SUM(financial_qty) AS inventory_at_risk
    FROM
        agilisium_playground.purgo_playground.f_inv_movmnt
    WHERE
        dnsa_flag = "Y"
)
-- Display the result to confirm correctness
SELECT * FROM InventoryRiskCalculation;

/* Assuming total_inventory is defined elsewhere */
-- Setting a static number for illustration purposes
-- In reality, this should be calculated or retrieved
DECLARE @total_inventory DOUBLE;
SET @total_inventory = 10000; -- Example value

/* Calculate and display the percentage of Inventory at Risk */
SELECT
    (ir.inventory_at_risk / @total_inventory) * 100 AS percentage_of_inventory_at_risk
FROM
    InventoryRiskCalculation ir;

/* Perform checks and assertions on schema structure */
-- Use DESCRIBE command to validate the schema structure
DESCRIBE agilisium_playground.purgo_playground.f_inv_movmnt;

/* Cleanup operations if necessary */
-- Ensure that temporary objects or changes are reverted if needed