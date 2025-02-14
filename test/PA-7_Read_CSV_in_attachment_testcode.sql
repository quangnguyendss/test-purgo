/* SQL Test Code Section */

/* Test Delta Lake Table Schema Validation */
SELECT *
FROM purgo_playground.sales_data
LIMIT 1;

/* Validate Country Code Format */
SELECT country_cd
FROM purgo_playground.sales_data
WHERE LENGTH(country_cd) != 2 OR NOT (country_cd GLOB '[A-Z][A-Z]')
LIMIT 1;

/* Validate Non-Negative Quantity Sold */
SELECT *
FROM purgo_playground.sales_data
WHERE qty_sold < 0
LIMIT 1;

/* Validate Product ID Format */
SELECT *
FROM purgo_playground.sales_data
WHERE product_id NOT LIKE 'P____'
LIMIT 1;

/* Validate Sales Date Format */
SELECT sales_date
FROM purgo_playground.sales_data
WHERE sales_date NOT LIKE '____-__-__'
LIMIT 1;

/* Test MERGE Operation */
MERGE INTO purgo_playground.sales_data AS target
USING purgo_playground.new_sales_data AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
  UPDATE SET qty_sold = source.qty_sold
WHEN NOT MATCHED
  THEN INSERT (country_cd, product_id, qty_sold, sales_date)
  VALUES (source.country_cd, source.product_id, source.qty_sold, source.sales_date);

/* Test DELETE Operation */
DELETE FROM purgo_playground.sales_data
WHERE sales_date < '2024-01-01';

/* Test UPDATE Operation */
UPDATE purgo_playground.sales_data
SET qty_sold = qty_sold + 10
WHERE sales_date = '2024-01-15';

/* Cleanup Operation */
DELETE FROM purgo_playground.sales_data
WHERE product_id LIKE '%TEST%';

# PySpark Test Code Section

# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, when, expr
import unittest

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Databricks Test Suite") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Define a schema for input data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Sample data for tests
data = [
    ("US", "P1001", 50, "2024-01-15T00:00:00.000Z"),
    ("IN", "P1003", 45, "2024-01-21T00:00:00.000Z"),
    ("ZZ", "P0000", -10, "not-a-date")  # Test invalid data
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

class DataQualityTests(unittest.TestCase):

    def test_country_code_format(self):
        """Test that all country codes are valid ISO 3166-1 alpha-2 codes."""
        invalid_country_df = df.filter(~(col("country_cd").rlike("^[A-Z]{2}$")))
        self.assertEqual(invalid_country_df.count(), 0)

    def test_qty_sold_non_negative(self):
        """Test that quantity sold is non-negative."""
        negative_qty_df = df.filter(col("qty_sold") < 0)
        self.assertEqual(negative_qty_df.count(), 0)

    def test_sales_date_format(self):
        """Test that all sales dates are in the expected format."""
        invalid_date_df = df.filter(~(df.sales_date.cast("date").isNotNull()))
        self.assertEqual(invalid_date_df.count(), 1)  # Known invalid case

    def test_null_handling(self):
        """Test Null handling mechanism in DataFrame."""
        null_filtered_df = df.filter(
            col("country_cd").isNull() |
            col("product_id").isNull() |
            col("qty_sold").isNull() |
            col("sales_date").isNull()
        )
        self.assertEqual(null_filtered_df.count(), 0)

    def test_product_id_format(self):
        """Test the product id format adheres to 'P1000' pattern."""
        invalid_product_id_df = df.filter(~col("product_id").rlike("^P\\d{4}$"))
        self.assertEqual(invalid_product_id_df.count(), 0)

if __name__ == "__main__":
    # Run tests
    unittest.main(argv=[''], exit=False)

# Stop the SparkSession
spark.stop()