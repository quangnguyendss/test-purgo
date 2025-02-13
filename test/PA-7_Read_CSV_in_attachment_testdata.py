from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, lit
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("TestDataGeneration").getOrCreate()

# Define schema
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Happy path test data
happy_path_data = [
    ("US", "P1001", 50, datetime.strptime("2024-03-21T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    ("CA", "P1003", 30, datetime.strptime("2024-03-22T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    ("UK", "P1004", 40, datetime.strptime("2024-03-23T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z"))
]

# Edge cases (boundary conditions)
edge_case_data = [
    ("IN", "P1000", 1, datetime.strptime("2024-01-01T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")), # Minimum qty_sold
    ("AU", "P9999", 1000, datetime.strptime("2024-12-31T23:59:59.999+0000", "%Y-%m-%dT%H:%M:%S.%f%z")) # Maximum qty_sold
]

# Error cases (invalid inputs)
error_case_data = [
    ("", "P1001", 20, datetime.strptime("2024-03-21T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")), # Missing country_cd
    ("US", "1001", 50, datetime.strptime("2024-03-21T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")), # Invalid product_id
    ("CA", "P1003", -1, datetime.strptime("2024-03-21T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")), # Negative quantity
    ("UK", "P1004", 20, datetime.strptime("21st March 2024", "%dth %B %Y")) # Invalid date format
]

# NULL handling scenarios
null_scenario_data = [
    (None, "P1005", 15, datetime.strptime("2024-04-01T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    ("EU", None, 70, datetime.strptime("2024-04-02T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    ("JP", "P1006", None, datetime.strptime("2024-04-03T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    ("KR", "P1007", 5, None)
]

# Special characters and multi-byte characters
special_char_data = [
    ("CN", "P1008", 100, datetime.strptime("2024-04-04T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    ("DE", "PX@#$", 75, datetime.strptime("2024-04-05T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    ("FR", "P1009", 60, datetime.strptime("2024-04-06T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    ("JP", "P1010", 85, datetime.strptime("2024-04-07T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z"))
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + null_scenario_data + special_char_data

# Create DataFrame
df = spark.createDataFrame(data=all_test_data, schema=schema)

# Show DataFrame
df.show(truncate=False)
