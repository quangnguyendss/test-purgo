from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import lit

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Databricks Test Data Generation") \
    .getOrCreate()

# Define the schema for the dataset
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Generate test data for different scenarios

# Happy path test data
happy_path_data = [
    ("US", "P1001", 50, "2024-03-21T00:00:00.000+0000"),
    ("CA", "P1003", 25, "2024-03-22T00:00:00.000+0000")
]

# Edge cases
edge_cases_data = [
    ("", "P1001", 0, "2024-03-23T00:00:00.000+0000"),   # Empty country code
    ("XX", "P1234", 100000, "2024-03-24T00:00:00.000+0000"),  # Large quantity
]

# Error cases
error_cases_data = [
    ("ZZ", "P0000", -10, "not-a-date"),  # Invalid country, negative qty, invalid date
]

# NULL handling scenarios
null_handling_data = [
    (None, "P0001", 30, "2024-03-25T00:00:00.000+0000"),  # NULL country code
    ("US", None, None, "2024-03-26T00:00:00.000+0000")    # NULL product_id and qty_sold
]

# Special characters and multi-byte characters
special_characters_data = [
    ("JP", "P1002", 15, "2024-03-27T00:00:00.000+0000"),
    ("CN", "产品1004", 20, "2024-03-28T00:00:00.000+0000")  # Multibyte product_id
]

# Aggregate all test data
all_test_data = happy_path_data + edge_cases_data + error_cases_data + null_handling_data + special_characters_data

# Create DataFrame with all test data
test_df = spark.createDataFrame(all_test_data, schema)

# Show the DataFrame
test_df.show(truncate=False)

# Stop the SparkSession
spark.stop()