from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import lit, current_timestamp

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Databricks Test Data Generation") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Define test data
data = [
    # Happy Path Test Data
    ("US", "P1001", 50, "2024-01-15T00:00:00.000+0000"),
    ("US", "P1002", 30, "2024-03-21T00:00:00.000+0000"),
    ("CA", "P1001", 40, "2024-03-21T00:00:00.000+0000"),

    # Edge Cases
    ("FR", "P9999", 1, "2024-03-21T00:00:00.000+0000"),  # Maximum product_id
    ("DE", "P1000", 1000, "2024-03-21T00:00:00.000+0000"),  # Maximum qty_sold

    # Error Cases
    (None, "P1003", 20, "2024-03-21T00:00:00.000+0000"),  # NULL country_cd
    ("US", "1234", 50, "2024-01-15T00:00:00.000+0000"),  # Invalid product_id format
    ("CA", "P1004", -10, "2024-01-15T00:00:00.000+0000"),  # Negative qty_sold

    # Special Characters
    ("JP", "P100@\u2603", 75, "2024-03-21T00:00:00.000+0000"),  # Special character in product_id

    # Multi-byte Characters
    ("CN", "P1005", 65, "2024-03-21T00:00:00.000+0000"),  # Typical multi-byte usage scenario

    # NULL Handling Scenarios
    ("US", None, 48, "2024-03-21T00:00:00.000+0000"),  # NULL product_id
    ("US", "P1006", None, "2024-03-21T00:00:00.000+0000"),  # NULL qty_sold
    ("US", "P1007", 52, None),  # NULL sales_date

    # Future Date
    ("US", "P1008", 48, "2025-01-15T00:00:00.000+0000"),  # Future sales_date
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show(truncate=False)

# Write DataFrame to Unity Catalog in Databricks
df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.sample_sales_data")

