# PySpark Test Code using unittest framework

# Required Libraries for PySpark Testing
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import unittest

# Initiate Spark Session
spark = SparkSession.builder \
    .appName("Inventory at Risk Test Suite") \
    .getOrCreate()

# Test Suite for Databricks Environment
class InventoryAtRiskTest(unittest.TestCase):
  
    @classmethod
    def setUpClass(cls):
        """
        Set up method to create initial configurations, common initialization functions, 
        and instantiate any needed resources before all tests.
        """
        # Define schema for test data
        cls.schema = StructType([
            StructField("id", StringType(), True),
            StructField("dnsa_flag", StringType(), True),
            StructField("financial_qty", DoubleType(), True),
            StructField("timestamp_event", TimestampType(), True)
        ])
    
    def setUp(self):
        """
        Set up any state specific to the test method. This method is called before the invocation of each test method.
        """
        # Sample data for testing
        self.data = [
            ('1', 'Y', 100.0, '2024-03-21T01:00:00.000'),
            ('2', 'N', 200.0, '2024-03-21T02:00:00.000'),
            ('3', 'Y', 300.0, '2024-03-21T03:00:00.000'),
            ('4', 'N', None, '2024-03-21T04:00:00.000'),
            ('5', None, 400.0, '2024-03-21T05:00:00.000'),
        ]

        # Initialize DataFrame
        self.df = spark.createDataFrame(self.data, schema=self.schema)
      
    def test_sum_inventory_at_risk(self):
        """
        Calculate the sum of the financial_qty where the dnsa_flag is 'Y' to represent inventory at risk.
        """
        # Perform sum operation
        inventory_at_risk = self.df \
            .filter(self.df.dnsa_flag == "Y") \
            .agg(F.sum("financial_qty").alias("inventory_at_risk")) \
            .collect()[0]["inventory_at_risk"]
        
        # Assertion
        self.assertEqual(inventory_at_risk, 400.0)

    def test_percentage_inventory_at_risk(self):
        """
        Test the calculation of percentage of inventory at risk.
        """
        # Define total inventory for testing purpose
        total_inventory = 1000

        # Calculate inventory at risk
        inventory_at_risk = self.df \
            .filter(self.df.dnsa_flag == "Y") \
            .agg(F.sum("financial_qty").alias("inventory_at_risk")) \
            .collect()[0]["inventory_at_risk"]

        # Calculate percentage
        percentage_at_risk = (inventory_at_risk / total_inventory) * 100

        # Assertion
        self.assertAlmostEqual(percentage_at_risk, 40.0, delta=0.01)

    def test_null_financial_qty_handling(self):
        """
        Ensure the NULL values in financial_qty do not affect calculations.
        """
        # Calculate with and without filtering NULLs
        risky_qty_with_nulls = self.df \
            .filter(self.df.dnsa_flag == "Y") \
            .agg(F.sum("financial_qty").alias("inventory_at_risk")) \
            .collect()[0]["inventory_at_risk"]
        
        risky_qty_without_nulls = self.df \
            .filter((self.df.dnsa_flag == "Y") & (~self.df.financial_qty.isNull())) \
            .agg(F.sum("financial_qty").alias("inventory_at_risk")) \
            .collect()[0]["inventory_at_risk"]

        # Assertions
        self.assertIsNone(risky_qty_with_nulls)
        self.assertEqual(risky_qty_without_nulls, 400.0)

    def tearDown(self):
        """
        Clean up any state or resources initialized in the setUp. This is executed after each test method.
        """
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

-- SQL Test Queries for Databricks SQL Environment

/* Test Case: Calculate Inventory at Risk when DNSA flag is active */
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

/* Test Case: Calculate Percentage of Inventory at Risk */
WITH inventory_data AS (
  SELECT 
    SUM(financial_qty) AS inventory_at_risk
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
  WHERE dnsa_flag = "Y"
), total_inventory_data AS (
  SELECT 
    SUM(financial_qty) AS total_inventory
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
)
SELECT 
  (inventory_at_risk / total_inventory) * 100 AS percentage_of_inventory_at_risk
FROM 
  inventory_data, total_inventory_data;

/* Error handling: Check for undefined total inventory scenario */
SELECT COUNT(*) AS inventory_defined 
FROM agilisium_playground.purgo_playground.f_inv_movmnt;

/* Validate proper handling of NULL values */
SELECT 
  SUM(CASE WHEN dnsa_flag = "Y" THEN financial_qty ELSE 0 END) AS inventory_with_nulls,
  SUM(CASE WHEN dnsa_flag = "Y" AND financial_qty IS NOT NULL THEN financial_qty ELSE 0 END) AS inventory_without_nulls
FROM agilisium_playground.purgo_playground.f_inv_movmnt;