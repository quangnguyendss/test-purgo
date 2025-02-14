# Databricks PySpark test setup and execution

# Install necessary libraries
# %pip install <specific-libraries-if-any>

# Import required modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType
import unittest

# Spark session setup
spark = SparkSession.builder \
    .appName("Test Inventory at Risk Calculations") \
    .getOrCreate()

# Define schema for test validation
schema = StructType([
    StructField("id", LongType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Test Data Creation
test_data = [
    (1, "Y", 150.0, "2024-03-21T00:00:00.000+0000"),
    (2, "N", 200.0, "2024-03-21T01:00:00.000+0000"),
    (3, "Y", 300.0, "2024-03-21T02:00:00.000+0000"),
    # ... (continue from test data SQL)
]

# Create DataFrame from test data
df = spark.createDataFrame(test_data, schema=schema)

# Define unit test class
class TestInventoryAtRiskCalculations(unittest.TestCase):

    # Test inventory at risk calculations
    def test_inventory_at_risk_calculation(self):
        # Filter and calculate inventory at risk
        inventory_at_risk = df.filter(col("dnsa_flag") == "Y") \
            .agg(spark_sum("financial_qty").alias("inventory_at_risk")) \
            .first()["inventory_at_risk"]
        
        # Assert the sum of Y financial_qty is correct
        expected_sum = 450.0  # 150 + 300
        self.assertEqual(inventory_at_risk, expected_sum, "Inventory at risk calculation is incorrect.")

    # Test percentage of inventory at risk calculation
    def test_percentage_of_inventory_at_risk(self):
        # Assume total_inventory is provided
        total_inventory = 1000.0

        # Calculate inventory at risk
        inventory_at_risk = df.filter(col("dnsa_flag") == "Y") \
            .agg(spark_sum("financial_qty").alias("inventory_at_risk")) \
            .first()["inventory_at_risk"]

        # Calculate percentage of inventory at risk
        percentage_of_inventory_at_risk = (inventory_at_risk / total_inventory) * 100
        
        # Assert the calculated percentage is correct
        expected_percentage = 45.0  # (450 / 1000) * 100
        self.assertAlmostEqual(percentage_of_inventory_at_risk, expected_percentage, "Percentage of inventory at risk calculation is incorrect.")

    # Schema validation test
    def test_schema_validation(self):
        expected_schema = schema
        self.assertTrue(df.schema == expected_schema, "Schema validation failed.")

# Run unit tests
if __name__ == '__main__':
    unittest.main(argv=['ignored', '-v'], exit=False, verbosity=2)

-- Databricks SQL testing and validation

-- Calculate Inventory at Risk
WITH RiskyInventory AS (
    SELECT financial_qty
    FROM agilisium_playground.purgo_playground.f_inv_movmnt
    WHERE dnsa_flag = "Y"
)

-- Summarize inventory at risk
SELECT SUM(financial_qty) AS inventory_at_risk
FROM RiskyInventory;

-- Calculate Total Inventory
-- Assuming there's a total inventory value available for calculations
WITH TotalInventory AS (
    SELECT SUM(financial_qty) AS total_inventory
    FROM agilisium_playground.purgo_playground.f_inv_movmnt
)

-- Calculate Percentage of Inventory at Risk
SELECT 
    (SUM(RiskyInventory.financial_qty) / TotalInventory.total_inventory) * 100 AS percentage_of_inventory_at_risk
FROM RiskyInventory, TotalInventory;

-- Validate Delta Lake operations
-- Example: INSERT INTO delta lake
MERGE INTO agilisium_playground.purgo_playground.f_inv_movmnt AS target
USING (
    SELECT 29 AS id, "N" AS dnsa_flag, 123.0 AS financial_qty, CURRENT_TIMESTAMP AS timestamp_event
) AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET target.financial_qty = source.financial_qty
WHEN NOT MATCHED
    THEN INSERT (id, dnsa_flag, financial_qty, timestamp_event) VALUES (source.id, source.dnsa_flag, source.financial_qty, source.timestamp_event);

-- Cleanup operations
-- Example: Delete test data if needed
DELETE FROM agilisium_playground.purgo_playground.f_inv_movmnt WHERE id >= 29;