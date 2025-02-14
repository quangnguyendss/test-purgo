# PYTHON TEST CODE

# Import necessary modules
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, to_date

# Initialize a Spark session for testing
spark = SparkSession.builder \
    .appName("Databricks Test Cases for sales_data") \
    .getOrCreate()

# Define schema for input data
input_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Define schema for the desired table
expected_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", StringType(), True)  # Use StringType to support "YYYY-MM-DD" format
])

# Define sample data for testing
test_data = [
    ("US", "P1001", 50, "2024-01-15"),
    ("CA", "P1003", 25, "2024-01-17"),
    ("UK", "INVALID", -10, "2023-30-02"),  # Invalid record for tests
    (None, "P1004", 55, "2024-01-23"),
    ("US", None, None, None)
]

# Create dataframe for testing
input_df = spark.createDataFrame(data=test_data, schema=input_schema)

# Convert 'sales_date' to String type for validation
input_df = input_df.withColumn("sales_date", col("sales_date").cast("string"))

class TestSalesDataProcessing(unittest.TestCase):
    def setUp(self):
        # Setup any preliminary configurations before each test run
        self.spark = spark
        self.df = input_df
        self.expected_schema = expected_schema

    def test_schema_validation(self):
        """
        Verify if the schema of the processed DataFrame matches the expected schema.
        """
        self.assertEqual(self.df.schema, self.expected_schema, "Schema does not match the expected schema")

    def test_positive_qty_sold(self):
        """
        Check if 'qty_sold' only contains positive integers.
        """
        invalid_records = self.df.filter(col("qty_sold") <= 0).count()
        self.assertEqual(invalid_records, 1, "Found records with non-positive qty_sold")

    def test_valid_date_format(self):
        """
        Validate if 'sales_date' follows 'YYYY-MM-DD' format.
        Each date is tested with a conversion and invalid conversions are verified.
        """
        self.df.filter(
            ~col("sales_date").rlike(r'^\d{4}-\d{2}-\d{2}$')
        ).show()  # This can be commented out in production
        
        invalid_dates = self.df.filter(
            ~col("sales_date").rlike(r'^\d{4}-\d{2}-\d{2}$')
        ).count()
        self.assertEqual(invalid_dates, 1, "Found records with invalid sales_date format")

    def test_null_handling(self):
        """
        Ensure NULLs are managed properly in necessary fields.
        """
        count_nulls = self.df.filter(
            col("country_cd").isNull() |
            col("product_id").isNull() |
            col("qty_sold").isNull()
        ).count()
        self.assertEqual(count_nulls, 3, "Unexpected null value handling in important fields")

    def tearDown(self):
        # Clean resources or teardown configurations after each test
        pass

# Run the tests
if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)

-- SQL TEST QUERIES AND ASSERTIONS

/* Prepare environment for SQL unit tests */

/* Clear any pre-existing sales_data table */
DROP TABLE IF EXISTS purgo_playground.sales_data;

/* Create the sales_data table */
CREATE TABLE purgo_playground.sales_data (
    country_cd STRING,
    product_id STRING,
    qty_sold INTEGER,
    sales_date STRING  -- Ensure this is in 'YYYY-MM-DD' format
);

/* Insert sample data into the sales_data table */
INSERT INTO purgo_playground.sales_data VALUES
("US", "P1001", 50, "2024-01-15"),
("CA", "P1003", 25, "2024-01-17"),
("UK", "INVALID", -10, "2023-30-02"),  -- Invalid record for tests
(NULL, "P1004", 55, "2024-01-23"),
("US", NULL, NULL, NULL);

/* SQL test query: Validate schema structure */
-- Check if the table schema is as expected
DESCRIBE TABLE purgo_playground.sales_data;

/* SQL test query: Validate data integrity */
-- Validate that qty_sold is always positive
SELECT * FROM purgo_playground.sales_data WHERE qty_sold <= 0;

/* SQL test query: Validate data formats */
-- Validate the format of sales_date
SELECT * FROM purgo_playground.sales_data WHERE NOT sales_date RLIKE '^\d{4}-\d{2}-\d{2}$';

/* SQL test query: Validate NULL handling */
-- Validate that essential fields are not NULL
SELECT * FROM purgo_playground.sales_data WHERE country_cd IS NULL;
SELECT * FROM purgo_playground.sales_data WHERE product_id IS NULL;

/* SQL test assertion: Report any issues found in assertions */
-- The following queries should return zero results if data is valid
-- Adjust expectation as per testing results:
-- assert(result_positives == 0, "Detected non-positive qty_sold values")
-- assert(result_date_format == 0, "Detected incorrectly formatted sales_date values")
-- assert(result_nulls_country_cd == 0, "Detected NULL values in key fields: country_cd")
-- assert(result_nulls_product_id == 0, "Detected NULL values in key fields: product_id")

/* Clean up actions */
-- Drop the test data table after tests
DROP TABLE IF EXISTS purgo_playground.sales_data;