from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql import functions as F
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Databricks Test Data Generation") \
    .getOrCreate()

# Define schema according to Databricks data types
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Happy path test data (valid scenarios)
data_happy_path = [
    ("US", "P1001", 50, datetime.strptime("2024-01-15", "%Y-%m-%d").date()),
    ("CA", "P1003", 25, datetime.strptime("2024-01-17", "%Y-%m-%d").date()),
    ("IN", "P1001", 60, datetime.strptime("2024-01-20", "%Y-%m-%d").date()),
    ("AU", "P1004", 55, datetime.strptime("2024-01-22", "%Y-%m-%d").date())
]

# Edge cases (boundary conditions)
data_edge_cases = [
    ("UK", "P1004", 999999999, datetime.strptime("2099-12-31", "%Y-%m-%d").date()),  # Max qty sold, future date
    ("UK", "P1004", 0, datetime.strptime("2023-01-01", "%Y-%m-%d").date()),         # Min qty sold, past date
]

# Error cases (invalid input scenarios)
data_error_cases = [
    ("US", "P1001", -10, datetime.strptime("2024-01-15", "%Y-%m-%d").date()),            # Negative qty sold
    ("US", "P1001", 50, datetime.strptime("2025-01-15", "%Y-%m-%d").date()),             # Future sales_date
]

# NULL handling scenarios
data_null_handling = [
    (None, "P1001", 50, datetime.strptime("2024-01-15", "%Y-%m-%d").date()),  # Null country
    ("US", None, 50, datetime.strptime("2024-01-15", "%Y-%m-%d").date()),     # Null product_id
    ("US", "P1001", None, datetime.strptime("2024-01-15", "%Y-%m-%d").date()),# Null qty_sold
    ("US", "P1001", 50, None)                                                 # Null sales_date
]

# Special characters and multi-byte characters
data_special_characters = [
    ("JP", "Pあい", 50, datetime.strptime("2024-01-15", "%Y-%m-%d").date()),  # Multi-byte character in product_id
    ("$#", "P1001*", 50, datetime.strptime("2024-01-15", "%Y-%m-%d").date())  # Special characters in country and product_id
]

# Combine all test data
test_data = data_happy_path + data_edge_cases + data_error_cases + data_null_handling + data_special_characters

# Create DataFrame
df_test_data = spark.createDataFrame(test_data, schema)

# Add a timestamp column to match Databricks timestamp format
df_test_data = df_test_data.withColumn("timestamp_col", F.current_timestamp())

# Display the DataFrame
df_test_data.show(truncate=False)

# Save test data to a table or location for future use if necessary
# df_test_data.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.test_data")