from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder.appName("TestDataGeneration").getOrCreate()

# Define schema for sales data based on Unity Catalog requirements
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Happy path test data (valid scenarios)
happy_path_data = [
    ("US", "P1001", 50, "2024-01-15T00:00:00.000+0000"),
    ("US", "P1002", 30, "2024-01-16T00:00:00.000+0000"),
    ("CA", "P1001", 40, "2024-01-15T00:00:00.000+0000"),
    ("CA", "P1003", 25, "2024-01-17T00:00:00.000+0000")
]

# Edge cases (boundary conditions)
edge_cases_data = [
    ("XX", "P1005", 0, "2024-12-31T23:59:59.999+0000"),  # Invalid country code, zero quantity, max timestamp
    ("ZZ", "P9999", 9999, "2024-01-01T00:00:00.000+0000")  # Another invalid country, high quantity, min timestamp
]

# Error cases (invalid inputs)
error_cases_data = [
    ("US", "P1001", -5, "2024-01-15T00:00:00.000+0000"),  # Negative quantity
    ("UK", "INVALID", 10, "NOT_A_DATE")  # Invalid date
]

# NULL handling scenarios
null_handling_data = [
    (None, "P1002", 20, "2024-01-17T00:00:00.000+0000"),  # NULL country code
    ("IN", "P1003", None, "2024-01-21T00:00:00.000+0000")  # NULL quantity
]

# Special characters and multi-byte characters
special_characters_data = [
    ("CN", "P2002", 45, "2024-02-29T00:00:00.000+0000"),  # Leap year date
    ("JP", "プロダクトID", 33, "2024-04-01T00:00:00.000+0000"),  # Product ID with multibyte character
]

# Combine all test data
combined_data = happy_path_data + edge_cases_data + error_cases_data + null_handling_data + special_characters_data

# Create DataFrame
df = spark.createDataFrame(combined_data, schema)

# Show DataFrame
df.show(truncate=False)

# Save to Databricks table (assuming Unity Catalog setup)
df.write.mode("overwrite").format("delta").saveAsTable("purgo_playground.sales_data_test")

# Stop Spark session
spark.stop()

