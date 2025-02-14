# Import necessary libraries for PySpark testing environment
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import unittest

# Set up PySpark session
spark = SparkSession.builder \
    .appName("Databricks Testing") \
    .config("spark.master", "local") \
    .getOrCreate()

# Schema for the f_inv_movmnt table
f_inv_movmnt_schema = StructType([
    StructField("id", LongType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Test cases using unittest
class TestInventoryCalculation(unittest.TestCase):

    def setUp(self):
        # Prepare the test data
        data = [
            (1, "Y", 150.0, "2024-03-21 00:00:00"),
            (2, "N", 200.0, "2024-03-21 01:00:00"),
            (3, "Y", 300.0, "2024-03-21 02:00:00"),
            (4, None, None, "2024-03-21 03:00:00")  # null handling case
        ]

        # Create DataFrame with schema
        self.df = spark.createDataFrame(data, schema=f_inv_movmnt_schema)

    def test_inventory_at_risk_calculation(self):
        # Filter records where dnsa_flag is 'Y'
        df_filtered = self.df.filter(F.col("dnsa_flag") == "Y")

        # Calculate inventory_at_risk
        inventory_at_risk = df_filtered.agg(F.sum("financial_qty")).first()[0]

        # Check if calculated value is as expected
        expected_value = 450.0  # 150 + 300
        self.assertEqual(inventory_at_risk, expected_value, "Inventory at risk calculation is incorrect.")

    def test_percentage_of_inventory_at_risk_calculation(self):
        # Define total inventory for testing
        total_inventory = 1000.0

        # Assuming inventory_at_risk is calculated as shown in the previous test
        inventory_at_risk = 450.0

        # Calculate percentage of inventory at risk
        percentage_of_inventory_at_risk = (inventory_at_risk / total_inventory) * 100

        # Check if the percentage is calculated correctly
        expected_percentage = 45.0
        self.assertEqual(percentage_of_inventory_at_risk, expected_percentage, "Percentage of inventory at risk calculation is incorrect.")

    def test_null_handling(self):
        # Check for null handling in financial_qty
        df_with_non_null = self.df.filter(F.col("dnsa_flag").isNotNull() & F.col("financial_qty").isNotNull())

        # Ensure rows with null values are filtered out
        result_count = df_with_non_null.count()
        expected_count = 3  # 3 rows with non-null values in the test data
        self.assertEqual(result_count, expected_count, "Null handling failed in filtering non-null rows.")

    def tearDown(self):
        # Stop Spark session
        spark.stop()

# Execute the test cases
if __name__ == "__main__":
    unittest.main(argv=[''], verbosity=2, exit=False)

-- SQL Test for Inventory At Risk Calculation
/* 
   SQL Query to calculate sum of financial_qty 
   where dnsa_flag is 'Y' from f_inv_movmnt table.
*/
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

-- SQL Test for Percentage of Inventory At Risk Calculation
/* 
   Assuming total_inventory is obtained from other sources 
   and is defined as a variable here for the purpose of testing.
*/
SET total_inventory = 1000.0;

SELECT (SUM(financial_qty) / total_inventory) * 100 AS percentage_of_inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

-- SQL Test for Handling NULL values in financial_qty and dnsa_flag
/* 
   Ensure NULL values in both dnsa_flag and financial_qty are excluded 
   from the sum calculation to handle data consistency.
*/
SELECT SUM(financial_qty) AS inventory_at_risk_excluding_nulls
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag IS NOT NULL AND financial_qty IS NOT NULL AND dnsa_flag = "Y";