# Import required PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, lit

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Databricks Test Data Generation") \
    .getOrCreate()

# Define schema for the sample sales data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Sample test data containing happy path, edge cases, error cases, nulls, and special characters
test_data = [
    # Happy Path records
    ("US", "P1001", 50, "2024-01-15T00:00:00.000+0000"),
    ("CA", "P1003", 25, "2024-01-17T00:00:00.000+0000"),
    
    # Edge case records
    ("UK", None, 0, "2024-01-18T00:00:00.000+0000"),  # qty_sold = 0 (edge case)
    ("IN", "P1003", 2147483647, "2024-01-21T00:00:00.000+0000"),  # max int value for qty_sold
    
    # Error cases
    ("AU", "PXXXX", -1, "2024-01-22T00:00:00.000+0000"),  # invalid product_id and negative qty_sold
    ("ZZ", "P1002", 35, "2022-13-40T00:00:00.000+0000"),  # invalid date format
    
    # NULL handling
    (None, "P1004", 55, "2024-01-23T00:00:00.000+0000"),  # NULL country_cd
    ("US", None, None, None),  # All NULL values
    
    # Special characters and multi-byte characters
    ("JP", "P1001", 60, "2024-02-15T00:00:00.000+0000"),  # Valid Japanese country
    ("CN", "P1005☃️", 45, "2024-02-21T00:00:00.000+0000")  # Special snowman character in product_id
]

# Create DataFrame using the above schema and data
test_df = spark.createDataFrame(test_data, schema)

# Print schema and data to verify
test_df.printSchema()
test_df.show(20, False)